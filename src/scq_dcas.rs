/* ----------------------------------------------------------------------------
 *
 * Dual 2-BSD/MIT license. Either or both licenses can be used.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2019 Ruslan Nikolaev.  All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2019 Ruslan Nikolaev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 * ----------------------------------------------------------------------------
 */
use std::{cmp, marker::PhantomData, ptr, sync::atomic::{AtomicIsize, AtomicUsize, Ordering}, usize};

use crossbeam_utils::CachePadded;

use crate::{atomic_types::{AtomicDUsize, DUsize}, queue::{EnqueueResult, HandleResult, Queue, QueueFull}};

pub struct SCQ2Handle {
    lhead: usize,
}

unsafe impl Send for SCQ2Handle {}
unsafe impl Sync for SCQ2Handle {}

struct SCQ2Ring<T> {
    head: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    tail: CachePadded<AtomicUsize>,
    array: Box<[CachePadded<AtomicDUsize>]>,
    _phantom: PhantomData<T>,
}

unsafe impl<T> Send for SCQ2Ring<T> {}
unsafe impl<T> Sync for SCQ2Ring<T> {}

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

impl<T> SCQ2Ring<T> {
    const fn threshold4(n: usize) -> isize {
        (2 * n - 1) as isize
    }

    pub fn new_empty(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[CachePadded<AtomicDUsize>]> = (0..n).map(|_| CachePadded::new(AtomicDUsize::new(0))).collect();
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
            unsafe {
                AtomicUsize::from_ptr((array_pair.as_ptr() as *mut usize).add(1))
            }
        } else {
            unsafe {
                AtomicUsize::from_ptr(array_pair.as_ptr() as *mut usize)
            }
        }
    }
    
    fn get_array_pointer(array_pair: &AtomicDUsize) -> &AtomicUsize {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicUsize::from_ptr(array_pair.as_ptr() as *mut usize)
            }
        } else {
            unsafe {
                AtomicUsize::from_ptr((array_pair.as_ptr() as *mut usize).add(1))
            }
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

        let tail = self.tail.load(Ordering::Acquire);
        if tail >= *lhead + n {
            *lhead = self.head.load(Ordering::Acquire);
            if tail >= *lhead + n {
                let item = unsafe {
                    *Box::from_raw(item)
                };
                return Err(QueueFull(item));
            }
        }
        loop {
            let tail = self.tail.fetch_add(1, Ordering::Acquire);
            let tail_cycle = tail & !(n - 1);
            let tail_index = tail % n;
            let pair = self.array[tail_index].load(Ordering::Acquire);

            // retry:
            'retry: loop {
                let entry = Self::get_entry(pair) as usize;
                let entry_cycle = entry & !(n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && 
                    (entry == entry_cycle || 
                     ((entry == (entry_cycle | 0x2)) && self.head.load(Ordering::Acquire) <= tail)) {
                        match self.array[tail_index].compare_exchange_weak(pair, Self::create_pair(tail_cycle | 1, item), Ordering::Acquire, Ordering::Relaxed) {
                            Ok(_) => {
                                if self.threshold.load(Ordering::Acquire) != Self::threshold4(n) {
                                    self.threshold.store(Self::threshold4(n), Ordering::Release);
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
            if tail + 1 >= *lhead + n {
                *lhead = self.head.load(Ordering::Acquire);
                if tail + 1 >= *lhead + n {
                    let item = unsafe {
                        *Box::from_raw(item)
                    };
                    return Err(QueueFull(item));
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

    pub fn dequeue(&self) -> Option<T> {
        let n = self.array.len();
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::Acquire);
            let head_cycle = head & !(n - 1);
            let head_index = head % n;
            let entry = Self::get_array_entry(&self.array[head_index]).load(Ordering::Acquire) as usize;
            let mut entry_new;
            'inner: loop {
                let entry_cycle = entry & !(n - 1);
                if entry_cycle == head_cycle {
                    let pair = self.array[head_index].fetch_and(Self::create_pair(!0x1, ptr::null_mut()), Ordering::Release);
                    let ptr = Self::get_pointer(pair);
                    let item = unsafe {
                        *Box::from_raw(ptr as *mut T)
                    };
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
                if Self::get_array_entry(&self.array[head_index]).compare_exchange_weak(entry, entry_new, Ordering::Release, Ordering::Relaxed).is_ok() { 
                    break 'inner; 
                }
            }
            let tail = self.tail.load(Ordering::Acquire);
            if !compare_signed(tail, head + 1, cmp::Ordering::Greater) {
                self.catchup(tail, head + 1);
                self.threshold.fetch_sub(1, Ordering::Release);
                return None;
            }
            if self.threshold.fetch_sub(1, Ordering::Acquire) <= 0 {
                return None;
            }
        }
    }
}

pub struct SCQ2Cas<T> {
    ring: SCQ2Ring<T>,
}

impl<T> SCQ2Cas<T> {
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        Self { 
            ring: SCQ2Ring::new_empty(len),
        }
    }
}

impl<T> Queue<T> for SCQ2Cas<T> {
    type Handle = SCQ2Handle;
    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T> {
        self.ring.enqueue(item, &mut handle.lhead)
    }

    fn dequeue(&self, _: &mut Self::Handle) -> Option<T> {
        self.ring.dequeue()
    }

    fn register(&self) -> HandleResult<Self::Handle> {
        Ok(SCQ2Handle {
            lhead: self.ring.array.len(),
        })
    }
}

impl<T> Drop for SCQ2Cas<T> {
    fn drop(&mut self) {
        while let Some(_) = self.ring.dequeue() {}
    }
}

unsafe impl<T> Send for SCQ2Cas<T> {}
unsafe impl<T> Sync for SCQ2Cas<T> {}
