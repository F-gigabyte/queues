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

use std::{cell::UnsafeCell, cmp, mem::MaybeUninit, sync::atomic::{AtomicIsize, AtomicUsize, Ordering}, usize};

use crossbeam_utils::CachePadded;

use crate::{lcrq::LCRQ, queue::{EnqueueResult, HandleResult, Queue, QueueFull}, ring_buffer::RingBuffer};

#[derive(Debug)]
struct SCQRing<const CLOSABLE: bool> {
    head: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    tail: CachePadded<AtomicUsize>,
    array: Box<[CachePadded<AtomicUsize>]>,
}

unsafe impl<const CLOSABLE: bool> Send for SCQRing<CLOSABLE> {}
unsafe impl<const CLOSABLE: bool> Sync for SCQRing<CLOSABLE> {}

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

#[derive(Debug)]
struct Entry {
    value: usize
}

impl<const CLOSABLE: bool> SCQRing<CLOSABLE> {
    const CLOSED_SHIFT: usize = usize::BITS as usize - 1;
    const CLOSED_MASK: usize = 1 << Self::CLOSED_SHIFT;

    const fn threshold3(n: usize) -> isize {
        (n / 2 + n - 1) as isize
    }

    pub fn new_empty(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[CachePadded<AtomicUsize>]> = (0..n).map(|_| CachePadded::new(AtomicUsize::new(usize::MAX))).collect();
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            threshold: CachePadded::new(AtomicIsize::new(-1)), 
            tail: CachePadded::new(AtomicUsize::new(0)), 
            array: data,
        }
    }

    pub fn new_full(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[CachePadded<AtomicUsize>]> = (0..n).map(|_| CachePadded::new(AtomicUsize::new(usize::MAX))).collect();
        for i in 0..len {
            data[i].store(n + i % len, Ordering::Release);
        }
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            threshold: CachePadded::new(AtomicIsize::new(Self::threshold3(n))), 
            tail: CachePadded::new(AtomicUsize::new(len)), 
            array: data, 
        }
    }

    pub fn new_fill(len: usize, start: usize, end: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        // start and end are less than n
        assert!(start < n);
        assert!(end < n);
        let array: Box<[CachePadded<AtomicUsize>]> = (0..n).map(|_| CachePadded::new(AtomicUsize::new(usize::MAX))).collect();
        for i in 0..start {
            array[i % n].store(2 * n - 1, Ordering::Release);
        }
        for i in start..end {
            array[i % n].store(n + i, Ordering::Release);
        }
        Self { 
            head: CachePadded::new(AtomicUsize::new(start)), 
            threshold: CachePadded::new(AtomicIsize::new(Self::threshold3(n))), 
            tail: CachePadded::new(AtomicUsize::new(end)), 
            array, 
        }
    }

    pub fn enqueue(&self, mut elem: usize) -> Result<(), QueueFull<()>>{
        let n = self.array.len();
        elem ^= n - 1;
        loop {
            let tail = self.tail.fetch_add(1, Ordering::Acquire);
            if CLOSABLE {
                if tail & Self::CLOSED_MASK != 0 {
                    return Err(QueueFull(()));
                }
            }
            let tail_cycle = (tail << 1) | (2 * n - 1);
            let tail_index = tail % n;
            let entry = self.array[tail_index].load(Ordering::Acquire);

            // retry:
            'retry: loop {
                let entry_cycle = entry | (2 * n - 1);
                // entry == entry_cycle -> is entry empty?
                // entry == entry_cycle ^ n -> is entry safe?
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && 
                    (entry == entry_cycle || 
                     ((entry == (entry_cycle ^ n)) && !compare_signed(self.head.load(Ordering::Acquire), tail, cmp::Ordering::Greater))) {
                        match self.array[tail_index].compare_exchange_weak(entry, tail_cycle ^ elem, Ordering::Acquire, Ordering::Relaxed) {
                            Ok(_) => {
                                if self.threshold.load(Ordering::Acquire) != Self::threshold3(n) {
                                    self.threshold.store(Self::threshold3(n), Ordering::Release);
                                }
                                return Ok(());
                            },
                            Err(_) => {
                                // goto retry
                                continue 'retry;
                            }
                        }
                } else {
                    // return to main loop
                    break 'retry;
                }
            }
        }
    }

    fn catchup(&self, mut tail: usize, mut head: usize) {
        loop {
            match self.tail.compare_exchange_weak(tail, head, Ordering::Acquire, Ordering::Relaxed) {
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

    pub fn dequeue(&self) -> Option<usize> {
        let n = self.array.len();
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::Acquire);
            let head_cycle = (head << 1) | (2 * n - 1);
            let head_index = head % n;
            let mut attempt = 0;
            'again: loop {
                let entry = self.array[head_index].load(Ordering::Acquire);
                let mut entry_new;
                'inner: loop {
                    let entry_cycle = entry | (2 * n - 1);
                    if entry_cycle == head_cycle {
                        self.array[head_index].fetch_or(n - 1, Ordering::Release);
                        return Some(entry & (n - 1));
                    }

                    if (entry | n) != entry_cycle {
                        entry_new = entry & !n;
                        if entry == entry_new {
                            break 'inner;
                        }
                    } else {
                        attempt += 1;
                        if attempt <= 10000 {
                            continue 'again;
                        }
                        entry_new = head_cycle ^ ((!entry) & n);
                    }
                    // do while loop
                    if !compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                        break 'inner;
                    }
                    if self.array[head_index].compare_exchange_weak(entry, entry_new, Ordering::Release, Ordering::Relaxed).is_ok() { break 'inner }
                }
                let tail = self.tail.load(Ordering::Acquire);
                let tail = if CLOSABLE {
                    tail & !Self::CLOSED_MASK
                } else {
                    tail
                };
                if !compare_signed(tail, head + 1, cmp::Ordering::Greater) {
                    self.catchup(tail, head + 1);
                    self.threshold.fetch_sub(1, Ordering::Release);
                    return None;
                }

                if self.threshold.fetch_sub(1, Ordering::Acquire) <= 0 {
                    return None;
                }
                break 'again;
            }
        }
    }
}

impl SCQRing<true> {
    pub fn close(&self) {
        _ = self.tail.fetch_or(Self::CLOSED_MASK, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct SCQCas<T, const CLOSABLE: bool> {
    aq: SCQRing<CLOSABLE>,
    fq: SCQRing<false>,
    data: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]>,
}

impl<T> RingBuffer<T> for SCQCas<T, true> {
    fn new(len: usize, _: usize) -> Self {
        let len = len.next_power_of_two();
        let data: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]> = (0..len).map(|_| CachePadded::new(UnsafeCell::new(MaybeUninit::uninit()))).collect();
        Self { 
            aq: SCQRing::new_empty(len), 
            fq: SCQRing::new_full(len), 
            data,
        }
    }
}

impl<T> RingBuffer<T> for SCQCas<T, false> {
    fn new(len: usize, _: usize) -> Self {
        let len = len.next_power_of_two();
        let data: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]> = (0..len).map(|_| CachePadded::new(UnsafeCell::new(MaybeUninit::uninit()))).collect();
        Self { 
            aq: SCQRing::new_empty(len), 
            fq: SCQRing::new_full(len), 
            data,
        }
    }
}

impl<T> Queue<T> for SCQCas<T, true> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        if let Some(index) = self.fq.dequeue() {
            match self.aq.enqueue(index) {
                Ok(_) => {
                    unsafe {
                        self.data[index].get().write(MaybeUninit::new(item));
                    }
                    Ok(())
                },
                Err(_) => {
                    _ = self.fq.enqueue(index);
                    self.aq.close();
                    Err(QueueFull(item))
                },
            }
        } else {
            Err(QueueFull(item))
        }
    }
    
    fn dequeue(&self, _: usize) -> Option<T> {
        if let Some(index) = self.aq.dequeue() {
            let val = unsafe {
                self.data[index].get().read().assume_init()
            };
            _ = self.fq.enqueue(index);
            Some(val)
        } else {
            None
        }
    }
    
    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

impl<T> Queue<T> for SCQCas<T, false> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        if let Some(index) = self.fq.dequeue() {
            unsafe {
                self.data[index].get().write(MaybeUninit::new(item));
            }
            _ = self.aq.enqueue(index);
            Ok(())
        } else {
            Err(QueueFull(item))
        }
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        if let Some(index) = self.aq.dequeue() {
            let val = unsafe {
                self.data[index].get().read().assume_init()
            };
            _ = self.fq.enqueue(index);
            Some(val)
        } else {
            None
        }
    }

    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

impl<T, const CLOSABLE: bool> Drop for SCQCas<T, CLOSABLE> {
    fn drop(&mut self) {
        while let Some(index) = self.aq.dequeue() {
            unsafe {
                self.data[index].get().read().assume_init_drop();
            }
        }
    }
}

unsafe impl<T, const CLOSABLE: bool> Send for SCQCas<T, CLOSABLE> {}
unsafe impl<T, const CLOSABLE: bool> Sync for SCQCas<T, CLOSABLE> {}

pub type LSCQ<T> = LCRQ<T, SCQCas<T, true>>;
