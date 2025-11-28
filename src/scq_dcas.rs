/// ----------------------------------------------------------------------------
///
/// Dual 2-BSD/MIT license. Either or both licenses can be used.
///
/// ----------------------------------------------------------------------------
///
/// Copyright (c) 2019 Ruslan Nikolaev.  All Rights Reserved.
///
/// Redistribution and use in source and binary forms, with or without
/// modification, are permitted provided that the following conditions
/// are met:
/// 1. Redistributions of source code must retain the above copyright
///    notice, this list of conditions and the following disclaimer.
/// 2. Redistributions in binary form must reproduce the above copyright
///    notice, this list of conditions and the following disclaimer in the
///    documentation and/or other materials provided with the distribution.
///
/// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS
/// OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
/// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
/// DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
/// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
/// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
/// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
/// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
/// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
/// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
/// SUCH DAMAGE.
///
/// ----------------------------------------------------------------------------
///
/// Copyright (c) 2019 Ruslan Nikolaev
///
/// Permission is hereby granted, free of charge, to any person obtaining a
/// copy of this software and associated documentation files (the "Software"),
/// to deal in the Software without restriction, including without limitation
/// the rights to use, copy, modify, merge, publish, distribute, sublicense,
/// and/or sell copies of the Software, and to permit persons to whom the
/// Software is furnished to do so, subject to the following conditions:
///
/// The above copyright notice and this permission notice shall be included in
/// all copies or substantial portions of the Software.
///
/// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
/// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
/// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
/// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
/// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
/// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
/// IN THE SOFTWARE.
///
/// ----------------------------------------------------------------------------
use std::{
    cell::UnsafeCell,
    cmp,
    marker::PhantomData,
    ptr,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
    usize,
};

use crossbeam_utils::CachePadded;

use crate::{
    atomic_types::{AtomicDUsize, DUsize},
    lcrq::LCRQ,
    queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull},
    ring_buffer::RingBuffer,
};

#[derive(Debug)]
pub struct SCQDCasHandle {
    lhead: usize,
}

#[derive(Debug)]
struct SCQDCasRing<T, const CLOSABLE: bool> {
    head: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    tail: CachePadded<AtomicUsize>,
    array: Box<[CachePadded<AtomicDUsize>]>,
    _phantom: PhantomData<T>,
}

unsafe impl<T, const CLOSABLE: bool> Send for SCQDCasRing<T, CLOSABLE> {}
unsafe impl<T, const CLOSABLE: bool> Sync for SCQDCasRing<T, CLOSABLE> {}

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

impl<T, const CLOSABLE: bool> SCQDCasRing<T, CLOSABLE> {
    const CLOSED_SHIFT: usize = usize::BITS as usize - 1;
    const CLOSED_MASK: usize = 1 << Self::CLOSED_SHIFT;

    const fn threshold4(n: usize) -> isize {
        (2 * n - 1) as isize
    }

    pub fn new_empty(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[CachePadded<AtomicDUsize>]> = (0..n)
            .map(|_| CachePadded::new(AtomicDUsize::new(0)))
            .collect();
        Self {
            head: CachePadded::new(AtomicUsize::new(n)),
            threshold: CachePadded::new(AtomicIsize::new(-1)),
            tail: CachePadded::new(AtomicUsize::new(n)),
            array: data,
            _phantom: PhantomData,
        }
    }

    fn get_array_entry(array_pair: &AtomicDUsize) -> &AtomicUsize {
        if cfg!(target_endian = "little") {
            unsafe { AtomicUsize::from_ptr((array_pair.as_ptr() as *mut usize).add(1)) }
        } else {
            unsafe { AtomicUsize::from_ptr(array_pair.as_ptr() as *mut usize) }
        }
    }

    fn get_array_pointer(array_pair: &AtomicDUsize) -> &AtomicUsize {
        if cfg!(target_endian = "little") {
            unsafe { AtomicUsize::from_ptr(array_pair.as_ptr() as *mut usize) }
        } else {
            unsafe { AtomicUsize::from_ptr((array_pair.as_ptr() as *mut usize).add(1)) }
        }
    }

    fn get_entry(pair: DUsize) -> usize {
        ((pair >> usize::BITS) & usize::MAX as DUsize) as usize
    }

    fn get_pointer(pair: DUsize) -> usize {
        (pair & usize::MAX as DUsize) as usize
    }

    fn create_pair(entry: usize, ptr: *mut T) -> DUsize {
        ((entry as DUsize) << usize::BITS) | (ptr as DUsize)
    }

    pub fn enqueue(&self, item: T, lhead: &mut usize) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        let n = self.array.len();

        let tail = self.tail.load(Ordering::SeqCst);
        if tail >= *lhead + n {
            *lhead = self.head.load(Ordering::SeqCst);
            if tail >= *lhead + n {
                let item = unsafe { *Box::from_raw(item) };
                return Err(QueueFull(item));
            }
        }
        loop {
            let tail = self.tail.fetch_add(1, Ordering::AcqRel);
            if CLOSABLE && tail & Self::CLOSED_MASK != 0 {
                let item = unsafe { *Box::from_raw(item) };
                return Err(QueueFull(item));
            }
            let tail_cycle = tail & !(n - 1);
            let tail_index = tail % n;
            let mut pair = self.array[tail_index].load(Ordering::Acquire);

            // retry:
            'retry: loop {
                let entry = Self::get_entry(pair);
                let entry_cycle = entry & !(n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less)
                    && (entry == entry_cycle
                        || ((entry == (entry_cycle | 0x2))
                            && self.head.load(Ordering::Acquire) <= tail))
                {
                    match self.array[tail_index].compare_exchange_weak(
                        pair,
                        Self::create_pair(tail_cycle | 1, item),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            if self.threshold.load(Ordering::SeqCst) != Self::threshold4(n) {
                                self.threshold.store(Self::threshold4(n), Ordering::SeqCst);
                            }
                            return Ok(());
                        }
                        Err(val) => {
                            pair = val;
                            // goto retry
                            continue 'retry;
                        }
                    }
                } else {
                    // return to main loop
                    break 'retry;
                }
            }
            if tail + 1 >= *lhead + n {
                *lhead = self.head.load(Ordering::SeqCst);
                if tail + 1 >= *lhead + n {
                    let item = unsafe { *Box::from_raw(item) };
                    return Err(QueueFull(item));
                }
            }
        }
    }

    fn catchup(&self, mut tail: usize, mut head: usize) {
        loop {
            match self
                .tail
                .compare_exchange_weak(tail, head, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(_) => {
                    head = self.head.load(Ordering::Acquire);
                    tail = self.tail.load(Ordering::Acquire);
                    if !compare_signed(tail, head, cmp::Ordering::Less) {
                        break;
                    }
                }
            }
        }
    }

    pub fn dequeue(&self) -> Option<T> {
        let n = self.array.len();
        if self.threshold.load(Ordering::SeqCst) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::AcqRel);
            let head_cycle = head & !(n - 1);
            let head_index = head % n;
            let mut entry = Self::get_array_entry(&self.array[head_index]).load(Ordering::Acquire);
            let mut entry_new;
            'inner: loop {
                let entry_cycle = entry & !(n - 1);
                if entry_cycle == head_cycle {
                    let pair = self.array[head_index]
                        .fetch_and(Self::create_pair(!0x1, ptr::null_mut()), Ordering::AcqRel);
                    let ptr = Self::get_pointer(pair);
                    let item = unsafe { *Box::from_raw(ptr as *mut T) };
                    return Some(item);
                }
                if (entry & (!0x2)) != entry_cycle {
                    entry_new = entry | 0x2;
                    if entry == entry_new {
                        break 'inner;
                    }
                } else {
                    entry_new = head_cycle | (entry & 0x2);
                }
                // while condition
                if !compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                    break 'inner;
                }
                match Self::get_array_entry(&self.array[head_index])
                    .compare_exchange_weak(entry, entry_new, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => break 'inner,
                    Err(val) => {
                        entry = val;
                    }
                }
            }
            let tail = self.tail.load(Ordering::Acquire);
            let tail = if CLOSABLE {
                tail & !Self::CLOSED_MASK
            } else {
                tail
            };
            if !compare_signed(tail, head + 1, cmp::Ordering::Greater) {
                self.catchup(tail, head + 1);
                self.threshold.fetch_sub(1, Ordering::AcqRel);
                return None;
            }
            if self.threshold.fetch_sub(1, Ordering::AcqRel) <= 0 {
                return None;
            }
        }
    }
}

impl<T> SCQDCasRing<T, true> {
    pub fn close(&self) {
        _ = self.tail.fetch_or(Self::CLOSED_MASK, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct SCQDCas<T, const CLOSABLE: bool> {
    ring: SCQDCasRing<T, CLOSABLE>,
    handles: Box<[CachePadded<UnsafeCell<SCQDCasHandle>>]>,
    current_thread: AtomicUsize,
    num_threads: usize,
}

impl<T, const CLOSABLE: bool> SCQDCas<T, CLOSABLE> {
    fn create_handle(len: usize) -> SCQDCasHandle {
        SCQDCasHandle { lhead: len * 2 }
    }
}

impl<T> RingBuffer<T> for SCQDCas<T, true> {
    fn new(len: usize, num_threads: usize) -> Self {
        let len = len.next_power_of_two();
        let handles: Box<[CachePadded<UnsafeCell<SCQDCasHandle>>]> = (0..num_threads)
            .map(|_| CachePadded::new(UnsafeCell::new(Self::create_handle(len))))
            .collect();
        Self {
            ring: SCQDCasRing::new_empty(len),
            handles,
            current_thread: AtomicUsize::new(0),
            num_threads,
        }
    }
}

impl<T> RingBuffer<T> for SCQDCas<T, false> {
    fn new(len: usize, num_threads: usize) -> Self {
        let len = len.next_power_of_two();
        let handles: Box<[CachePadded<UnsafeCell<SCQDCasHandle>>]> = (0..num_threads)
            .map(|_| CachePadded::new(UnsafeCell::new(Self::create_handle(len))))
            .collect();
        Self {
            ring: SCQDCasRing::new_empty(len),
            handles,
            current_thread: AtomicUsize::new(0),
            num_threads,
        }
    }
}

impl<T> Queue<T> for SCQDCas<T, true> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let handle = unsafe { &mut *self.handles[handle].get() };
        match self.ring.enqueue(item, &mut handle.lhead) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.ring.close();
                Err(err)
            }
        }
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        self.ring.dequeue()
    }

    fn register(&self) -> HandleResult {
        let thread_id = self.current_thread.fetch_add(1, Ordering::AcqRel);
        if thread_id < self.num_threads {
            Ok(thread_id)
        } else {
            Err(HandleError)
        }
    }
}

impl<T> Queue<T> for SCQDCas<T, false> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let handle = unsafe { &mut *self.handles[handle].get() };
        self.ring.enqueue(item, &mut handle.lhead)
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        self.ring.dequeue()
    }

    fn register(&self) -> HandleResult {
        let thread_id = self.current_thread.fetch_add(1, Ordering::AcqRel);
        if thread_id < self.num_threads {
            Ok(thread_id)
        } else {
            Err(HandleError)
        }
    }
}

impl<T, const CLOSABLE: bool> Drop for SCQDCas<T, CLOSABLE> {
    fn drop(&mut self) {
        while let Some(_) = self.ring.dequeue() {}
    }
}

unsafe impl<T, const CLOSABLE: bool> Send for SCQDCas<T, CLOSABLE> {}
unsafe impl<T, const CLOSABLE: bool> Sync for SCQDCas<T, CLOSABLE> {}

pub type LSCQDCas<T> = LCRQ<T, SCQDCas<T, true>>;
