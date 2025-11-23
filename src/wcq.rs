/// ----------------------------------------------------------------------------
///
/// Dual 2-BSD/MIT license. Either or both licenses can be used.
///
/// ----------------------------------------------------------------------------
///
/// Copyright (c) 2021 Ruslan Nikolaev.  All Rights Reserved.
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
/// Copyright (c) 2021 Ruslan Nikolaev
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

use std::{cell::UnsafeCell, cmp, mem::MaybeUninit, ptr, sync::atomic::{AtomicIsize, AtomicUsize, Ordering}};

use crossbeam_utils::CachePadded;
use portable_atomic::AtomicPtr;

use crate::{atomic_types::{AtomicDUsize, DUsize}, queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull}};

const WCQ_PATIENCE_ENQUEUE: usize = 16;
const WCQ_PATIENCE_DEQUEUE: usize = 64;
const WCQ_DELAY: usize = 16;

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

struct WCQRing {
    head: CachePadded<AtomicDUsize>,
    tail: CachePadded<AtomicDUsize>,
    threshold: CachePadded<AtomicIsize>,
    array: Box<[CachePadded<AtomicDUsize>]>,
    states: Box<[CachePadded<WCQState>]>,
    handle_tail: AtomicPtr<WCQState>,
    current_thread: AtomicUsize,
    num_threads: usize,
}

struct WCQPhase2 {
    seq1: AtomicUsize,
    local: UnsafeCell<*mut AtomicUsize>,
    count: UnsafeCell<usize>,
    seq2: AtomicUsize
}

impl WCQPhase2 {
    pub fn new() -> Self {
        Self {
            seq1: AtomicUsize::new(1),
            local: UnsafeCell::new(ptr::null_mut()),
            count: UnsafeCell::new(0),
            seq2: AtomicUsize::new(0),
        }
    }
}

struct WCQState {
    next: AtomicPtr<WCQState>,

    phase2: WCQPhase2, 
    seq1: AtomicUsize,
    tail: AtomicUsize,
    init_tail: AtomicUsize,
    head: AtomicUsize,
    init_head: AtomicUsize,
    index: AtomicUsize,
    seq2: AtomicUsize
}

const WCQ_FIN: usize = 1;
const WCQ_INC: usize = 2;

impl WCQState {
    pub const INDEX_DEQUEUE: usize = 1;
    pub const INDEX_TERMINATE: usize = 0;

    pub fn new() -> Self {
        Self {
            next: AtomicPtr::new(ptr::null_mut()),
            phase2: WCQPhase2::new(),
            seq1: AtomicUsize::new(1),
            tail: AtomicUsize::new(0),
            init_tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            init_head: AtomicUsize::new(0),
            index: AtomicUsize::new(Self::INDEX_TERMINATE),
            seq2: AtomicUsize::new(0)
        }
    }
}

pub struct WCQRingHandle {
    next_check: usize,
    state: *const WCQState,
    current_thread: *const WCQState
}

impl WCQRing {
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

    fn get_entry(pair: DUsize) -> usize {
        ((pair >> usize::BITS) & usize::MAX as DUsize) as usize
    }

    fn get_addon(pair: DUsize) -> usize {
        (pair & usize::MAX as DUsize) as usize
    }

    fn create_pair(entry: usize, addon: usize) -> DUsize {
        ((entry as DUsize) << usize::BITS) | (addon as DUsize)
    }
   
    fn new_empty(len: usize, num_threads: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..n).map(|_| {
            CachePadded::new(AtomicDUsize::new(Self::create_pair(usize::MAX, (-(n as isize) - 1) as usize)))
        }).collect();
        let states: Box<[CachePadded<WCQState>]> = (0..num_threads).map(|_| CachePadded::new(WCQState::new())).collect();
        Self {
            head: CachePadded::new(AtomicDUsize::new(0)),
            tail: CachePadded::new(AtomicDUsize::new(0)),
            threshold: CachePadded::new(AtomicIsize::new(-1)),
            array,
            states,
            handle_tail: AtomicPtr::new(ptr::null_mut()),
            current_thread: AtomicUsize::new(0),
            num_threads
        }
    }

    fn new_full(len: usize, num_threads: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..n).map(|index| {
            CachePadded::new(AtomicDUsize::new(
                    if index < len {
                        Self::create_pair((3 * n + index % len) as usize, usize::MAX)
                    } else {
                        Self::create_pair(usize::MAX, (-(n as isize) - 1) as usize)
                    }
        ))}).collect();
        let states: Box<[CachePadded<WCQState>]> = (0..num_threads).map(|_| CachePadded::new(WCQState::new())).collect();
        Self {
            head: CachePadded::new(AtomicDUsize::new(0)),
            tail: CachePadded::new(AtomicDUsize::new(Self::create_pair(len as usize * 4, 0))),
            array,
            threshold: CachePadded::new(AtomicIsize::new(len as isize + n as isize - 1)),
            states,
            handle_tail: AtomicPtr::new(ptr::null_mut()),
            current_thread: AtomicUsize::new(0),
            num_threads
        }
    }

    fn new_fill(len: usize, num_threads: usize, start: usize, end: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..n).map(|index| {
            CachePadded::new(AtomicDUsize::new(
                    if index < start {
                        Self::create_pair(4 * n - 1, usize::MAX)
                    } else if index < end {
                        Self::create_pair(3 * n + index, usize::MAX)
                    } else {
                        Self::create_pair(usize::MAX, (-(n as isize) - 1) as usize)
                    }
            ))}).collect();
        let states: Box<[CachePadded<WCQState>]> = (0..num_threads).map(|_| CachePadded::new(WCQState::new())).collect();
        Self {
            head: CachePadded::new(AtomicDUsize::new(Self::create_pair(start * 4, 0))),
            tail: CachePadded::new(AtomicDUsize::new(Self::create_pair(end * 4, 0))),
            threshold: CachePadded::new(AtomicIsize::new(len as isize + n as isize - 1)),
            array,
            states,
            handle_tail: AtomicPtr::new(ptr::null_mut()),
            current_thread: AtomicUsize::new(0),
            num_threads
        }
    }

    fn load_global_help_phase2(global: &AtomicDUsize, global_val: DUsize) -> usize {
        loop {
            let phase2 = Self::get_addon(global_val) as *const WCQPhase2;
            if phase2.is_null() {
                break;
            }
            let phase2 = unsafe {
                &*phase2
            };
            let seq = phase2.seq2.load(Ordering::Acquire);
            let local = unsafe {
                &**phase2.local.get()
            };
            let count = unsafe {
                *phase2.count.get()
            };
            if phase2.seq1.load(Ordering::Acquire) == seq {
                let count_inc = count + WCQ_INC;
                _ = local.compare_exchange(count_inc, count, Ordering::Release, Ordering::Relaxed);
            }
            match global.compare_exchange(global_val, Self::create_pair(Self::get_entry(global_val), 0), Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => {},
            }
        }
        Self::get_entry(global_val)
    }

    fn slow_inc(global: &AtomicDUsize, local: &AtomicUsize, prev: &mut usize, threshold: Option<&AtomicIsize>, phase2: &WCQPhase2) -> bool {
        let mut global_val = global.load(Ordering::Acquire);
        let mut count;
        loop {
            if local.load(Ordering::Acquire) & WCQ_FIN != 0 {
                return false;
            }
            count = Self::load_global_help_phase2(global, global_val);
            match local.compare_exchange(*prev, count + WCQ_INC, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    *prev = count + WCQ_INC;
                },
                Err(_) => {
                    if *prev & WCQ_FIN != 0{
                        return false;
                    }
                    if *prev & WCQ_INC == 0 {
                        return true;
                    }
                    count = *prev - WCQ_INC;
                }
            }
            let seq = phase2.seq1.load(Ordering::Acquire) + 1;
            phase2.seq1.store(seq, Ordering::Release);
            unsafe {
                *phase2.local.get() = local as *const AtomicUsize as *mut AtomicUsize;
                *phase2.count.get() = count;
            }
            phase2.seq2.store(seq, Ordering::Release);
            global_val = Self::create_pair(count, 0);
            match global.compare_exchange(global_val, Self::create_pair(count + 1, phase2 as *const WCQPhase2 as usize), Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {},
                Err(_) => break,
            }
        }
        if let Some(threshold) = threshold {
            threshold.fetch_sub(1, Ordering::Release);
        }
        let count_inc = count + WCQ_INC;
        _ = local.compare_exchange(count_inc, count, Ordering::Release, Ordering::Relaxed);
        global_val = Self::create_pair(count + 1, phase2 as *const WCQPhase2 as usize);
        let _ = global.compare_exchange(global_val, Self::create_pair(count + 1, 0), Ordering::Release, Ordering::Relaxed);
        *prev = count;
        return true;
    }

    fn do_enqueue_slow(&self, index: usize, seq: usize, mut tail: usize, state: &WCQState) {
        let n = self.array.len();
        let half = n / 2;
        while Self::slow_inc(&self.tail, &state.tail, &mut tail, None, &state.phase2) {
            if state.seq1.load(Ordering::Acquire) != seq {
                break;
            }
            let tail_cycle = tail | (4 * n - 1);
            let tail_index = (tail / 4) % n;
            let pair = self.array[tail_index].load(Ordering::Acquire);
            'retry: loop {
                let mut entry = Self::get_entry(pair);
                let note = Self::get_addon(pair);
                let entry_cycle = entry | (4 * n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && compare_signed(note, tail_cycle, cmp::Ordering::Less) {
                    if entry | 1 == entry_cycle || 
                        (entry | 1 == entry_cycle ^ n && 
                         !compare_signed(Self::get_array_entry(&self.head).load(Ordering::Acquire), tail, cmp::Ordering::Greater)) {
                            match self.array[tail_index].compare_exchange_weak(pair, Self::create_pair(tail_cycle ^ index ^ n, note), Ordering::Release, Ordering::Relaxed) {
                                Ok(_) => {},
                                Err(_) => continue 'retry,
                            }

                            entry = tail_cycle ^ index;
                            match state.tail.compare_exchange(tail, tail + 1, Ordering::Release, Ordering::Relaxed) {
                                Ok(_) => {
                                    _ = Self::get_array_entry(&self.array[tail_index]).compare_exchange(entry, entry ^ n, Ordering::Release, Ordering::Relaxed);
                                },
                                Err(_) => {},
                            }

                            if self.threshold.load(Ordering::Acquire) != (n + half - 1) as isize {
                                self.threshold.store((n + half - 1) as isize, Ordering::Release);
                            }
                            return;
                    }
                } else if entry | (3 * n) == tail_cycle {
                    return;
                } else {
                    match self.array[tail_index].compare_exchange_weak(pair, Self::create_pair(entry, tail_cycle), Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {},
                        Err(_) => continue 'retry,
                    }
                }
                break;
            }
        }
    }

    fn enqueue_slow(&self, index: usize, tail: usize, state: &WCQState) {
        let seq = state.seq1.load(Ordering::Acquire);
        state.tail.store(tail, Ordering::Release);
        state.init_tail.store(tail, Ordering::Release);
        state.index.store(index, Ordering::Release);
        state.seq2.store(seq, Ordering::Release);

        self.do_enqueue_slow(index, seq, tail, state);

        state.seq1.store(seq + 1, Ordering::Release);
        state.index.store(WCQState::INDEX_TERMINATE, Ordering::Release);
    }

    fn help_enqueue(&self, state: &WCQState) {
        let seq = state.seq2.load(Ordering::Acquire);
        let index = state.index.load(Ordering::Acquire);
        let tail = state.init_tail.load(Ordering::Acquire);
        if index <= WCQState::INDEX_DEQUEUE || state.seq1.load(Ordering::Acquire) != seq {
            return;
        }
        _ = self.do_enqueue_slow(index, seq, tail, state);
    }

    fn help(&self, handle: &mut WCQRingHandle) {
        let mut current_ptr = handle.current_thread;
        if current_ptr != handle.state {
            let current = unsafe {
                &*current_ptr
            };
            let index = current.index.load(Ordering::Acquire);
            if index != WCQState::INDEX_TERMINATE {
                if index != WCQState::INDEX_DEQUEUE {
                    self.help_enqueue(current);
                } else {
                    // help dequeue
                    self.help_dequeue(current);
                }
            }
            current_ptr = current.next.load(Ordering::Acquire);
        }
        handle.current_thread = unsafe {
            (*current_ptr).next.load(Ordering::Acquire)
        };
        handle.next_check = WCQ_DELAY;
    }

    fn lookup(state: &WCQState, tail: usize) {
        let state_ptr = state as *const WCQState;
        let mut  current_ptr = state.next.load(Ordering::Acquire) as *const WCQState;
        while current_ptr != state_ptr {
            let current = unsafe {
                &*current_ptr
            };
            if current.tail.load(Ordering::Acquire) & !0x3 == tail {
                _ = current.tail.compare_exchange(tail, tail ^ 1, Ordering::Release, Ordering::Relaxed);
                return;
            }
            current_ptr = current.next.load(Ordering::Acquire) as *const WCQState;

        }
    }

    fn catchup(&self, mut tail: usize, mut head: usize) {
        loop {
            match Self::get_array_entry(&self.tail).compare_exchange_weak(tail, head, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => {},
            }
            head = Self::get_array_entry(&self.head).load(Ordering::Acquire);
            tail = Self::get_array_entry(&self.tail).load(Ordering::Acquire);
            if !compare_signed(tail, head, cmp::Ordering::Less) {
                break;
            }
        }
    }

    fn do_dequeue_slow(&self, mut head: usize, state: &WCQState) {
        let n = self.array.len();
        let mut entry_new;
        while Self::slow_inc(&self.head, &state.head, &mut head, Some(&self.threshold), &state.phase2) {
            let head_cycle = head | (4 * n - 1);
            let head_index = (head >> 2) % n;
            let pair = self.array[head_index].load(Ordering::Acquire);
            'retry: loop {
                let entry = Self::get_entry(pair);
                let note = Self::get_addon(pair);
                let entry_cycle = entry | (4 * n - 1);
                if entry_cycle == head_cycle && (entry & (n - 1)) != (n - 2) {
                    _ = state.head.compare_exchange(head, head ^ 1, Ordering::Release, Ordering::Relaxed);
                    return;
                }
                if entry | (2 * n) | 1 != entry_cycle {
                    if compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) &&
                        compare_signed(note, head_cycle, cmp::Ordering::Less) {
                            match self.array[head_index].compare_exchange_weak(pair, Self::create_pair(entry, head_cycle), Ordering::Release, Ordering::Relaxed) {
                                Ok(_) => {},
                                Err(_) => continue 'retry,
                            }
                    }
                    entry_new = entry & !(2 * n);
                    if entry == entry_new {
                        break;
                    }
                } else {
                    entry_new = head_cycle ^ ((!entry) & (2 * n)) ^ 1;
                }
                if !compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                    break;
                }
                match self.array[head_index].compare_exchange_weak(pair, Self::create_pair(entry_new, note), Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
            }
            let tail = Self::get_array_entry(&self.tail).load(Ordering::Acquire);
            if !compare_signed(tail, head + 4, cmp::Ordering::Greater) {
                self.catchup(tail, head + 4);
            }
            if self.threshold.load(Ordering::Acquire) < 0 {
                _ = state.head.compare_exchange(head, head + WCQ_FIN, Ordering::Release, Ordering::Relaxed);
            }
        }
    }

    fn dequeue_slow(&self, head: usize, state: &WCQState) -> Option<usize> {
        let n = self.array.len();
        let seq = state.seq1.load(Ordering::Acquire);
        state.head.store(head, Ordering::Release);
        state.init_head.store(head, Ordering::Release);
        state.index.store(WCQState::INDEX_DEQUEUE, Ordering::Release);
        state.seq2.store(seq, Ordering::Release);

        self.do_dequeue_slow(head, state);

        state.seq1.store(seq + 1, Ordering::Release);
        state.index.store(WCQState::INDEX_TERMINATE, Ordering::Release);

        let head = state.head.load(Ordering::Acquire);
        let head_cycle = head | (4 * n - 1);
        let head_index = (head >> 2) % n;
        let entry = Self::get_array_entry(&self.array[head_index]).load(Ordering::Acquire);
        if entry | (3 * n) == head_cycle {
            if entry & n == 0 {
                Self::lookup(state, head);
            }
            Self::get_array_entry(&self.array[head_index]).fetch_or(2 * n - 1, Ordering::Release);
            return Some(entry % n);
        }
        None
    }

    fn help_dequeue(&self, state: &WCQState) {
        let seq = state.seq2.load(Ordering::Acquire);
        let index = state.index.load(Ordering::Acquire);
        let head = state.init_head.load(Ordering::Acquire);
        if index != WCQState::INDEX_DEQUEUE || state.seq1.load(Ordering::Acquire) != seq {
            return;
        }
        self.do_dequeue_slow(head, state);
    }

    pub fn enqueue(&self, elem: usize, handle: &mut WCQRingHandle) {
        let n = self.array.len();
        let half = n / 2;
        let mut patience = WCQ_PATIENCE_ENQUEUE;
        let item_index = elem ^ (n - 1);
        handle.next_check -= 1;
        if handle.next_check == 0 {
            self.help(handle);
        }
        let mut tail;
        loop {
            tail = Self::get_array_entry(&self.tail).fetch_add(4, Ordering::Release);
            let tail_cycle = tail | (4 * n - 1);
            let tail_index = (tail / 4) % n;
            let entry = Self::get_array_entry(&self.array[tail_index]).load(Ordering::Acquire);
            'retry: loop {
                let entry_cycle = entry | (4 * n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && ((entry | 1 == entry_cycle) ||
                    ((entry | 1 == entry_cycle ^ (2 * n)) &&
                     !compare_signed(Self::get_array_entry(&self.head).load(Ordering::Acquire), tail, cmp::Ordering::Greater))) {
                    match Self::get_array_entry(&self.array[tail_index]).compare_exchange_weak(entry, tail_cycle ^ item_index, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {},
                        Err(_) => continue 'retry,
                    }

                    if self.threshold.load(Ordering::Acquire) != (n + half - 1) as isize {
                        self.threshold.store((n + half - 1) as isize, Ordering::Release);
                    }
                    return;
                }
                break 'retry;
            }
            patience -= 1;
            if patience == 0 {
                break;
            }
        }
        self.enqueue_slow(item_index, tail, unsafe {&*handle.state});
    }


    pub fn dequeue(&self, handle: &mut WCQRingHandle) -> Option<usize> {
        let n = self.array.len();
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }
        let mut patience = WCQ_PATIENCE_DEQUEUE;
        handle.next_check -= 1;
        if handle.next_check == 0 {
            self.help(handle);
        }
        let mut entry_new;
        let mut head;
        loop {
            head = Self::get_array_entry(&self.head).fetch_add(4, Ordering::Acquire);
            let head_cycle = head | (4 * n - 1);
            let head_index = (head >> 2) % n;
            let entry = Self::get_array_entry(&self.array[head_index]).load(Ordering::Acquire);
            loop {
                let entry_cycle = entry | (4 * n - 1);
                if entry_cycle == head_cycle {
                    if entry & n == 0 {
                        Self::lookup(unsafe {&*handle.state}, head);
                    }
                    Self::get_array_entry(&self.array[head_index]).fetch_or(2 * n - 1, Ordering::Release);
                    let index = entry % n;
                    return Some(index);
                }

                if entry | (2 * n) | 1 != entry_cycle {
                    entry_new = entry &!(2 * n);
                    if entry == entry_new {
                        break;
                    }
                } else {
                    entry_new = head_cycle ^ ((!entry) & (2 * n)) ^ 1;
                }
                if compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                    break;
                }
                match Self::get_array_entry(&self.array[head_index]).compare_exchange_weak(entry, entry_new, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
            }
            let tail = Self::get_array_entry(&self.tail).load(Ordering::Acquire);
            if !compare_signed(tail, head + 4, cmp::Ordering::Greater) {
                self.catchup(tail, head + 4);
                return None;
            }
            if self.threshold.fetch_sub(1, Ordering::Acquire) <= 0 {
                return None;
            }
            patience -= 1;
            if patience == 0 {
                break;
            }
        }
        self.dequeue_slow(head, unsafe {&*handle.state})
    }

    pub fn register(&self) -> HandleResult<WCQRingHandle> {
        let thread_id = self.current_thread.fetch_add(1, Ordering::Acquire);
        if thread_id < self.num_threads {
            let state = &self.states[thread_id];
            let state_ptr = &**state as *const WCQState as *mut WCQState;
            let tail_ptr = self.handle_tail.load(Ordering::Acquire);
            if tail_ptr.is_null() {
                state.next.store(state_ptr, Ordering::Release);
                match self.handle_tail.compare_exchange(tail_ptr, state_ptr, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => return Ok(WCQRingHandle {
                        next_check: WCQ_DELAY,
                        state: state_ptr as *const WCQState,
                        current_thread: state_ptr as *const WCQState,
                    }),
                    Err(_) => {},
                }
            }
            let tail = unsafe {
                &*tail_ptr
            };
            let next = tail.next.load(Ordering::Acquire);
            loop {
                state.next.store(next, Ordering::Release);
                match tail.next.compare_exchange_weak(next, state_ptr, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
            }
            Ok(WCQRingHandle {
                next_check: WCQ_DELAY,
                state: state_ptr as *const WCQState,
                current_thread: state_ptr as *const WCQState,
            })
        } else {
            Err(HandleError)
        }
    }
}

unsafe impl Send for WCQRing {}
unsafe impl Sync for WCQRing {}

pub struct WCQ<T> {
    aq: WCQRing,
    fq: WCQRing,
    data: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]>,
}

impl<T> WCQ<T> {
    pub fn new(len: usize, num_threads: usize) -> Self {
        let len = len.next_power_of_two();
        let data: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]> = (0..len).map(|_| CachePadded::new(UnsafeCell::new(MaybeUninit::uninit()))).collect();
        Self {
            aq: WCQRing::new_empty(len, num_threads),
            fq: WCQRing::new_full(len, num_threads),
            data,
        }
    }
}

pub struct WCQHandle {
    aq_handle: WCQRingHandle,
    fq_handle: WCQRingHandle,
}

impl<T> Queue<T> for WCQ<T> {
    type Handle = WCQHandle;

    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T> {
        if let Some(index) = self.fq.dequeue(&mut handle.fq_handle) {
            unsafe {
                self.data[index].get().write(MaybeUninit::new(item));
            }
            self.aq.enqueue(index, &mut handle.aq_handle);
            Ok(())
        } else {
            Err(QueueFull(item))
        }
    }

    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        if let Some(index) = self.aq.dequeue(&mut handle.aq_handle) {
            let val = unsafe {
                self.data[index].get().read().assume_init()
            };
            self.fq.enqueue(index, &mut handle.fq_handle);
            Some(val)
        } else {
            None
        }
    }

    fn register(&self) -> HandleResult<Self::Handle> {
        if let Ok(aq_handle) = self.aq.register() {
            let fq_handle = self.fq.register().unwrap();
            Ok(WCQHandle {
                aq_handle,
                fq_handle
            })
        } else {
            Err(HandleError)
        }
    }
}

impl<T> Drop for WCQ<T> {
    fn drop(&mut self) {
        let mut head = unsafe {
            *WCQRing::get_array_entry(&self.aq.head).as_ptr()
        } >> 4;
        let tail = unsafe {
            *WCQRing::get_array_entry(&self.aq.tail).as_ptr()
        } >> 4;
        while head < tail {
            let head_index = head % self.aq.array.len();
            let index = unsafe {
                *WCQRing::get_array_entry(&self.aq.array[head_index]).as_ptr()
            };
            unsafe {
                self.data[index].get().read().assume_init_drop();
            }
            head += 1;
        }
    }
}

unsafe impl<T> Send for WCQ<T> {}
unsafe impl<T> Sync for WCQ<T> {}
