/// *****************************************************************************
/// Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
/// All rights reserved.
///
/// Redistribution and use in source and binary forms, with or without
/// modification, are permitted provided that the following conditions are met:
///     * Redistributions of source code must retain the above copyright
///       notice, this list of conditions and the following disclaimer.
///     * Redistributions in binary form must reproduce the above copyright
///       notice, this list of conditions and the following disclaimer in the
///       documentation and/or other materials provided with the distribution.
///     * Neither the name of Concurrency Freaks nor the
///       names of its contributors may be used to endorse or promote products
///       derived from this software without specific prior written permission.
///
/// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
/// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
/// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
/// DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
/// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
/// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
/// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
/// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
/// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
/// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
/// *****************************************************************************

use std::{cell::UnsafeCell, marker::PhantomData, mem::{self, MaybeUninit}, ptr, sync::atomic::Ordering};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};
use portable_atomic::{AtomicBool, AtomicPtr, AtomicUsize};

use crate::{atomic_types::{AtomicDUsize, DUsize}, queue::{EnqueueResult, HandleError, HandleResult, Queue}};

#[derive(Debug)]
struct Node<T> {
    item: MaybeUninit<T>,
    enq_thread_id: usize,
    deq_thread_id: CachePadded<AtomicUsize>,
    next: CachePadded<AtomicPtr<Node<T>>>,
    can_delete: CachePadded<AtomicBool>,
    retired: CachePadded<AtomicBool>,
}

#[derive(Debug)]
pub struct FpSpHandle {
    thread_id: usize,
    help_record: HelpRecord,
}

type Handle = FpSpHandle;

impl<T> Node<T> {
    const INDEX_NONE: usize = usize::MAX;
    pub fn new(enq_thread_id: usize) -> Self {
        Self {
            item: MaybeUninit::uninit(),
            enq_thread_id,
            deq_thread_id: CachePadded::new(AtomicUsize::new(Self::INDEX_NONE)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            can_delete: CachePadded::new(AtomicBool::new(true)),
            retired: CachePadded::new(AtomicBool::new(false)),
        }
    }

    pub fn with_item(enq_thread_id: usize, item: T) -> Self {
        Self {
            item: MaybeUninit::new(item),
            enq_thread_id,
            deq_thread_id: CachePadded::new(AtomicUsize::new(Self::INDEX_NONE)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            can_delete: CachePadded::new(AtomicBool::new(false)),
            retired: CachePadded::new(AtomicBool::new(false))
        }
    }
}

#[derive(Debug)]
struct OpDesc<T> {
    phase: usize,
    pending: bool,
    enqueue: bool,
    node: *const Node<T>
}

#[derive(Debug)]
struct HelpRecord {
    current_thread_id: usize,
    last_phase: usize,
    next_check: usize,
}

impl HelpRecord {
    const HELPING_DELAY: usize = 10;
    fn reset<T>(&mut self, num_threads: usize, desc: &OpDesc<T>) {
        self.current_thread_id = (self.current_thread_id + 1) % num_threads;
        self.last_phase = desc.phase;
        self.next_check = Self::HELPING_DELAY;
    }
}

#[derive(Debug)]
struct StampedPtr<T> {
    ptr: *mut T,
    stamp: usize,
}

impl<T> StampedPtr<T> {
    const STAMP_MASK: DUsize = usize::MAX as DUsize;
    const STAMP_SHIFT: u32 = 0;
    const PTR_MASK: DUsize = (usize::MAX as DUsize) << Self::PTR_SHIFT;
    const PTR_SHIFT: u32 = usize::BITS;

    pub fn new(ptr: *mut T, stamp: usize) -> Self {
        Self {
            ptr,
            stamp
        }
    }
}

impl<T> From<DUsize> for StampedPtr<T> {
    fn from(value: DUsize) -> Self {
        let ptr = ((value & Self::PTR_MASK) >> Self::PTR_SHIFT) as *mut T;
        let stamp = (value & Self::STAMP_MASK) as usize;
        Self {
            ptr,
            stamp
        }
    }
}

impl<T> From<StampedPtr<T>> for DUsize {
    fn from(value: StampedPtr<T>) -> Self {
        (((value.ptr as DUsize) << StampedPtr::<T>::PTR_SHIFT) & StampedPtr::<T>::PTR_MASK) | value.stamp as DUsize
    }
}

impl<T> Clone for StampedPtr<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            stamp: self.stamp
        }
    }
}

impl<T> Copy for StampedPtr<T> {}

#[derive(Debug)]
struct StampedAtomicPtr<T> {
    atomic: AtomicDUsize,
    _phantom: PhantomData<*mut T>,
}

impl<T> StampedAtomicPtr<T> {

    pub fn new(stamped: StampedPtr<T>) -> Self {
        Self { 
            atomic: AtomicDUsize::new(stamped.into()), 
            _phantom: PhantomData 
        }
    }

    pub fn load(&self, order: Ordering) -> StampedPtr<T> {
        let val = self.atomic.load(order);
        val.into()
    }

    pub fn store(&self, stamped: StampedPtr<T>, order: Ordering) {
        self.atomic.store(stamped.into(), order);
    }

    pub fn compare_exchange(&self, expected: StampedPtr<T>, new: StampedPtr<T>, success: Ordering, failure: Ordering) -> Result<StampedPtr<T>, StampedPtr<T>> {
        self.atomic.compare_exchange(expected.into(), new.into(), success, failure).map(|v| v.into()).map_err(|v| v.into())
    }
    
    pub fn compare_exchange_weak(&self, expected: StampedPtr<T>, new: StampedPtr<T>, success: Ordering, failure: Ordering) -> Result<StampedPtr<T>, StampedPtr<T>> {
        self.atomic.compare_exchange_weak(expected.into(), new.into(), success, failure).map(|v| v.into()).map_err(|v| v.into())
    }
    
    pub fn load_stamp(&self, order: Ordering) -> usize {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicUsize::from_ptr(self.atomic.as_ptr() as *mut usize).load(order)
            }
        } else {
            unsafe {
                AtomicUsize::from_ptr((self.atomic.as_ptr() as *mut usize).add(1)).load(order)
            }
        }
    }
    
    pub fn load_ptr(&self, order: Ordering) -> *mut T {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicPtr::from_ptr((self.atomic.as_ptr() as *mut *mut T).add(1)).load(order)
            }
        } else {
            unsafe {
                AtomicPtr::from_ptr(self.atomic.as_ptr() as *mut *mut T).load(order)
            }
        }
    }
}

#[derive(Debug)]
pub struct FpSp<T> {
    head: CachePadded<StampedAtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
    states: Box<[CachePadded<AtomicPtr<OpDesc<T>>>]>,
    hazard_ops: Pointers<OpDesc<T>, BoxMemory>,
    hazard_nodes: Pointers<Node<T>, BoxMemory>,
    opdesc_end: *const OpDesc<T>,
    handles: Box<[CachePadded<UnsafeCell<FpSpHandle>>]>,
    current_thread: AtomicUsize,
    num_threads: usize
}

impl<T> FpSp<T> {
    const HAZARD_PTR_CURRENT: usize = 0;
    const HAZARD_PTR_NEXT: usize = 1;
    const HAZARD_PTR_PREV: usize = 2;
    const MAX_FAILURES: usize = 20;
    pub fn new(num_threads: usize) -> Self {
        let sentinal = Box::into_raw(Box::new(Node::new(Node::<T>::INDEX_NONE)));
        let opdesc_end = Box::into_raw(Box::new(OpDesc {
            phase: Node::<T>::INDEX_NONE,
            pending: false,
            enqueue: true,
            node: ptr::null()
        }));
        let states: Box<[CachePadded<AtomicPtr<OpDesc<T>>>]> = (0..num_threads).map(|_| CachePadded::new(AtomicPtr::new(opdesc_end))).collect();
        let handles: Box<[CachePadded<UnsafeCell<FpSpHandle>>]> = (0..num_threads).map(|i| {
            let last_phase = unsafe {
                (*states[i].load(Ordering::Acquire)).phase
            };
            CachePadded::new(UnsafeCell::new(Self::create_handle(i, last_phase)))
        }).collect();
        Self { 
            head: CachePadded::new(StampedAtomicPtr::new(StampedPtr::new(sentinal, usize::MAX))), 
            tail: CachePadded::new(AtomicPtr::new(sentinal)), 
            states: states, 
            hazard_ops: Pointers::new(BoxMemory, num_threads, 2, num_threads * 2), 
            hazard_nodes: Pointers::new(BoxMemory, num_threads, 3, num_threads * 2),
            opdesc_end,
            current_thread: AtomicUsize::new(0),
            num_threads
        }
    }

    fn help(&self, phase: usize, thread_id: usize) {
        for i in 0..self.num_threads {
            let mut desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[i].load(Ordering::Acquire));
            let mut it = 0;
            while it < self.num_threads + 1 {
                if desc == self.states[i].load(Ordering::Acquire) {
                    break;
                }
                desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[i].load(Ordering::Acquire));
                it += 1;
            }
            if it == self.num_threads + 1 && desc != self.states[i].load(Ordering::Acquire) {
                continue;
            }
            let desc = unsafe {
                &*desc
            };
            if desc.pending && desc.phase <= phase {
                if desc.enqueue {
                    self.help_enqueue(i, phase, thread_id);
                } else {
                    self.help_dequeue(i, phase, thread_id);
                }
            }
        }
    }

    fn get_max_phase(&self, thread_id: usize) -> usize {
        let mut max_phase = None;
        for i in 0..self.num_threads {
            let mut desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[i].load(Ordering::Acquire));
            let mut it = 0;
            while it < self.num_threads + 1 {
                if desc == self.states[i].load(Ordering::Acquire) {
                    break;
                }
                desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[i].load(Ordering::Acquire));
                it += 1;
            }
            if it == self.num_threads + 1 && desc != self.states[i].load(Ordering::Acquire) {
                continue;
            }
            let desc = unsafe {
                &*desc
            };
            let phase = desc.phase;
            max_phase = Some(if let Some(max_phase) = max_phase {
                if phase > max_phase { 
                    phase 
                } else { 
                    max_phase 
                }
            } else {
                phase
            })
        }
        max_phase.unwrap_or(usize::MAX)
    }

    fn is_pending(&self, query_id: usize, phase: usize, thread_id: usize) -> bool {
        let mut desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_NEXT, self.states[query_id].load(Ordering::Acquire));
        let mut it = 0;
        while it < self.num_threads + 1 {
            if desc == self.states[query_id].load(Ordering::Acquire) {
                break;
            }
            desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_NEXT, self.states[query_id].load(Ordering::Acquire));
            it += 1;
        }
        if it == self.num_threads + 1 && desc != self.states[query_id].load(Ordering::Acquire) {
            return false;
        }
        let desc = unsafe {
            &*desc
        };
        desc.pending && desc.phase <= phase
    }

    fn help_enqueue(&self, query_id: usize, phase: usize, thread_id: usize) {
        while self.is_pending(query_id, phase, thread_id) {
            let last = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.tail.load(Ordering::Acquire));
            if last != self.tail.load(Ordering::Acquire) {
                continue;
            }
            let next = unsafe {
                (*last).next.load(Ordering::Acquire)
            };
            if last == self.tail.load(Ordering::Acquire) {
                let last = unsafe {
                    &*last
                };
                if next.is_null() {
                    if self.is_pending(query_id, phase, thread_id) {
                        let current_desc = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[query_id].load(Ordering::Acquire));
                        if current_desc != self.states[query_id].load(Ordering::Acquire) {
                            continue;
                        }
                        let current_desc = unsafe {
                            &*current_desc
                        };
                        match last.next.compare_exchange(next, current_desc.node as *mut Node<T>, Ordering::Release, Ordering::Relaxed) {
                            Ok(_) => {
                                self.help_finish_enq(thread_id);
                                return;
                            },
                            Err(_) => {},
                        }
                    }
                }
            } else {
                self.help_finish_enq(thread_id);
            }
        }
    }

    fn help_finish_enq(&self, thread_id: usize) {
        let last = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.tail.load(Ordering::Acquire));
        if last != self.tail.load(Ordering::Acquire) {
            return;
        }
        let next = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_NEXT, unsafe {(*last).next.load(Ordering::Acquire)});
        if last == self.tail.load(Ordering::Acquire) && !next.is_null() {

            let query_id = unsafe {
                (*next).enq_thread_id
            };
            if query_id != Node::<T>::INDEX_NONE {
                let current_desc_ptr = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[query_id].load(Ordering::Acquire));
                if current_desc_ptr != self.states[query_id].load(Ordering::Acquire) {
                    return;
                }
                let current_desc = unsafe {
                    &*current_desc_ptr
                };
                if last == self.tail.load(Ordering::Acquire) && current_desc.node == next {
                    let new_desc = Box::into_raw(Box::new(OpDesc {
                        phase: current_desc.phase,
                        pending: false,
                        enqueue: true,
                        node: next
                    }));
                    match self.states[query_id].compare_exchange(current_desc_ptr, new_desc, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {
                            self.hazard_ops.retire(thread_id, current_desc_ptr);
                        },
                        Err(_) => {
                            unsafe {
                                _ = Box::from_raw(new_desc);
                            };
                        },
                    }
                    _ = self.tail.compare_exchange(last, next, Ordering::Release, Ordering::Relaxed);
                }
            }
        }
    }

    fn help_dequeue(&self, query_id: usize, phase: usize, thread_id: usize) {
        while self.is_pending(query_id, phase, thread_id) {
            let first = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_PREV, self.head.load_ptr(Ordering::Acquire));
            let last = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.tail.load(Ordering::Acquire));
            if first != self.head.load_ptr(Ordering::Acquire) || last != self.tail.load(Ordering::Acquire) {
                continue;
            }
            let next = unsafe { (*first).next.load(Ordering::Acquire) };
            if first == self.head.load_ptr(Ordering::Acquire) {
                if first == last {
                    if next.is_null() {
                        let current_desc_ptr = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[query_id].load(Ordering::Acquire));
                        if current_desc_ptr != self.states[query_id].load(Ordering::Acquire) {
                            continue;
                        }
                        if last == self.tail.load(Ordering::Acquire) && self.is_pending(query_id, phase, thread_id) {
                            let current_desc = unsafe {
                                &*current_desc_ptr
                            };
                            let new_desc = Box::into_raw(Box::new(OpDesc {
                                phase: current_desc.phase,
                                pending: false,
                                enqueue: false,
                                node: ptr::null()
                            }));
                            match self.states[query_id].compare_exchange(current_desc_ptr, new_desc, Ordering::Release, Ordering::Relaxed) {
                                Ok(_) => {
                                    self.hazard_ops.retire(thread_id, current_desc_ptr);
                                },
                                Err(_) => {
                                    unsafe {
                                        _ = Box::from_raw(new_desc)
                                    }
                                },
                            }
                        }
                    } else {
                        self.help_finish_enq(thread_id);
                    }
                } else {
                    let current_desc_ptr = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[query_id].load(Ordering::Acquire));
                    if current_desc_ptr != self.states[query_id].load(Ordering::Acquire) {
                        continue;
                    }
                    let current_desc = unsafe {
                        &*current_desc_ptr
                    };
                    let node = current_desc.node;
                    if !self.is_pending(query_id, phase, thread_id) {
                        break;
                    }
                    if first == self.head.load_ptr(Ordering::Acquire) && node != first {
                        let new_desc = Box::into_raw(Box::new(OpDesc {
                            phase: current_desc.phase,
                            pending: true,
                            enqueue: false,
                            node: first
                        }));
                        match self.states[query_id].compare_exchange(current_desc_ptr, new_desc, Ordering::Release, Ordering::Relaxed) {
                            Ok(_) => {
                                self.hazard_ops.retire(thread_id, current_desc_ptr);
                            },
                            Err(_) => {
                                unsafe {
                                    _ = Box::from_raw(new_desc);
                                }
                                continue;
                            },
                        }
                    }
                    _ = self.head.compare_exchange(StampedPtr::new(first, Node::<T>::INDEX_NONE), StampedPtr::new(first, query_id), Ordering::Release, Ordering::Relaxed);
                    self.help_finish_deq(thread_id);
                }
            }

        }
    }

    fn help_finish_deq(&self, thread_id: usize) {
        let first_ptr = self.hazard_nodes.mark_ptr(thread_id, Self::HAZARD_PTR_PREV, self.head.load_ptr(Ordering::Acquire));
        if first_ptr != self.head.load_ptr(Ordering::Acquire) {
            return;
        }
        let first = unsafe {
            &*first_ptr
        };
        let next = first.next.load(Ordering::Acquire);
        let query_id = self.head.load_stamp(Ordering::Acquire);
        if query_id != Node::<T>::INDEX_NONE {
            let mut current_desc_ptr = ptr::null_mut();
            for i in 0..self.num_threads {
                current_desc_ptr = self.hazard_ops.mark_ptr(thread_id, Self::HAZARD_PTR_CURRENT, self.states[query_id].load(Ordering::Acquire));
                if current_desc_ptr == self.states[query_id].load(Ordering::Acquire) {
                    break;
                }
                if i == self.num_threads - 1 {
                    return;
                }
            }
            if first_ptr == self.head.load_ptr(Ordering::Acquire) && !next.is_null() {
                let current_desc = unsafe {
                    &*current_desc_ptr
                };
                let new_desc = Box::into_raw(Box::new(OpDesc {
                    phase: current_desc.phase,
                    pending: false,
                    enqueue: false,
                    node: current_desc.node
                }));
                match self.states[query_id].compare_exchange(current_desc_ptr, new_desc, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {
                        self.hazard_ops.retire(thread_id, current_desc_ptr);
                    },
                    Err(_) => {
                        unsafe {
                            _ = Box::from_raw(new_desc)
                        }
                    },
                }
                _ = self.head.compare_exchange(StampedPtr::new(first_ptr, query_id), StampedPtr::new(next, Node::<T>::INDEX_NONE), Ordering::Release, Ordering::Relaxed);
            }
        }
    }

    fn help_if_needed(&self, handle: &mut FpSpHandle) {
        if handle.help_record.next_check == 0 {
            let current_desc_ptr = self.hazard_ops.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.states[handle.help_record.current_thread_id].load(Ordering::Acquire));
            let desc = unsafe {
                &*current_desc_ptr
            };
            if desc.pending && desc.phase == handle.help_record.last_phase {
                if desc.enqueue {
                    self.help_enqueue(handle.help_record.current_thread_id, handle.help_record.last_phase, handle.thread_id);
                } else {
                    self.help_dequeue(handle.help_record.current_thread_id, handle.help_record.last_phase, handle.thread_id);
                }
            }
            handle.help_record.reset(self.num_threads, desc);
        } else {
            handle.help_record.next_check -= 1;
        }
    }

    fn slow_enqueue(&self, node_ptr: *mut Node<T>, handle: &mut FpSpHandle) -> EnqueueResult<T> {
        let current_desc_ptr = self.hazard_ops.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.states[handle.thread_id].load(Ordering::Acquire));
        let current_desc = unsafe {
            &*current_desc_ptr
        };
        let node = unsafe {
            &mut *node_ptr
        };
        let phase = current_desc.phase;
        node.enq_thread_id = handle.thread_id;
        let op_desc = Box::into_raw(Box::new(OpDesc {
            phase,
            pending: true,
            enqueue: true,
            node: node_ptr
        }));
        self.states[handle.thread_id].store(op_desc, Ordering::Release);
        self.help(phase, handle.thread_id);
        self.help_finish_enq(handle.thread_id);
        self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
        self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
        self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
        self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
        self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_PREV);
        let mut desc = self.states[handle.thread_id].load(Ordering::Acquire);
        for _ in 0..self.num_threads * 2 {
            if desc == self.opdesc_end as *mut OpDesc<T> {
                break;
            }
            match self.states[handle.thread_id].compare_exchange(desc, self.opdesc_end as *mut OpDesc<T>, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => {},
            }
            desc = self.states[handle.thread_id].load(Ordering::Acquire);
        }
        self.hazard_ops.retire(handle.thread_id, desc);
        Ok(())
    }

    fn slow_dequeue(&self, handle: &mut FpSpHandle) -> Option<T> {
        let current_desc_ptr = self.hazard_ops.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.states[handle.thread_id].load(Ordering::Acquire));
        let current_desc = unsafe {
            &*current_desc_ptr
        };
        let phase = current_desc.phase.wrapping_add(1);
        let op_desc = Box::into_raw(Box::new(OpDesc {
            phase,
            pending: true,
            enqueue: false,
            node: ptr::null()
        }));
        self.states[handle.thread_id].store(op_desc, Ordering::Release);
        self.help(phase, handle.thread_id);
        self.help_finish_deq(handle.thread_id);
        let current_desc = self.hazard_ops.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.states[handle.thread_id].load(Ordering::Acquire));
        let node = unsafe {
            (*current_desc).node
        };
        if node.is_null() {
            self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
            self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_PREV);
            let mut desc = self.states[handle.thread_id].load(Ordering::Acquire);
            for _ in 0..self.num_threads {
                match self.states[handle.thread_id].compare_exchange(desc, self.opdesc_end as *mut OpDesc<T>, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
                desc = self.states[handle.thread_id].load(Ordering::Acquire);
                if desc == self.opdesc_end as *mut OpDesc<T> {
                    break;
                }
            }
            self.hazard_ops.retire(handle.thread_id, desc);
            None
        } else {
            let next_ptr = unsafe {
                (*node).next.load(Ordering::Acquire)
            };
            let next = unsafe {
                &mut *next_ptr
            };
            let val = unsafe {
                mem::replace(&mut next.item, MaybeUninit::uninit()).assume_init()
            };
            next.can_delete.store(true, Ordering::Release);
            if next.retired.load(Ordering::Acquire) {
                self.hazard_nodes.retire(handle.thread_id, next_ptr);
            }

            self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
            self.hazard_ops.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_PREV);
            let can_delete = unsafe {
                (*node).retired.store(true, Ordering::Release);
                (*node).can_delete.load(Ordering::Acquire)
            };
            if can_delete {
                self.hazard_nodes.retire(handle.thread_id, node as *mut Node<T>);
            }
            let mut desc = self.states[handle.thread_id].load(Ordering::Acquire);
            for _ in 0..self.num_threads * 2 {
                if desc == self.opdesc_end as *mut OpDesc<T> {
                    break;
                }
                match self.states[handle.thread_id].compare_exchange(desc, self.opdesc_end as *mut OpDesc<T>, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
                desc = self.states[handle.thread_id].load(Ordering::Acquire);
            }
            self.hazard_ops.retire(handle.thread_id, desc);
            Some(val)
        }
    }

    fn fix_tail(&self, last: *mut Node<T>, next_ptr: *mut Node<T>, thread_id: usize) {
        let next = unsafe {
            &*next_ptr
        };
        if next.enq_thread_id == Node::<T>::INDEX_NONE {
            _ = self.tail.compare_exchange(last, next_ptr, Ordering::Release, Ordering::Relaxed);
        } else {
            self.help_finish_enq(thread_id);
        }
    }

    fn create_handle(thread_id: usize, last_phase: usize) -> FpSpHandle {
        Handle {
            thread_id,
            help_record: HelpRecord { 
                current_thread_id: 0, 
                last_phase, 
                next_check: HelpRecord::HELPING_DELAY 
            }
        }
    }

}

impl<T> Queue<T, FpSpHandle> for FpSp<T> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let handle = unsafe {
            &mut *self.handles[handle].get()
        };
        self.help_if_needed(handle);
        let node = Box::into_raw(Box::new(Node::with_item(Node::<T>::INDEX_NONE, item)));
        let mut tries = 0;
        while tries < Self::MAX_FAILURES {
            let last_ptr = self.hazard_nodes.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.tail.load(Ordering::Acquire));
            let last = unsafe {
                &*last_ptr
            };
            let next = self.hazard_nodes.mark_ptr(handle.thread_id, Self::HAZARD_PTR_NEXT, last.next.load(Ordering::Acquire));
            if last_ptr == self.tail.load(Ordering::Acquire) {
                if next.is_null() {
                    match last.next.compare_exchange(next, node, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {
                            _ = self.tail.compare_exchange(last_ptr, node, Ordering::Release, Ordering::Relaxed);
                            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
                            return Ok(());
                        },
                        Err(_) => {},
                    }
                } else {
                    self.fix_tail(last_ptr, next, handle.thread_id);
                }
            }
            tries += 1;
        }
        self.slow_enqueue(node, handle)
    }

    fn dequeue(&self, handle: usize) -> Option<T> {
        let handle = unsafe {
            &mut *self.handles[handle].get()
        };
        self.help_if_needed(handle);
        let mut tries = 0;
        while tries < Self::MAX_FAILURES {
            let first_ptr = self.hazard_nodes.mark_ptr(handle.thread_id, Self::HAZARD_PTR_PREV, self.head.load_ptr(Ordering::Acquire));
            let first = unsafe {
                &*first_ptr
            };
            let last = self.hazard_nodes.mark_ptr(handle.thread_id, Self::HAZARD_PTR_CURRENT, self.tail.load(Ordering::Acquire));
            let next_ptr = first.next.load(Ordering::Acquire);
            if first_ptr == self.head.load_ptr(Ordering::Acquire) {
                if first_ptr == last {
                    if next_ptr.is_null() {
                        self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_PREV);
                        self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
                        return None;
                    }
                    self.fix_tail(last, next_ptr, handle.thread_id);
                } else if self.head.load_stamp(Ordering::Acquire) == Node::<T>::INDEX_NONE {
                    let next = unsafe {
                        &mut *next_ptr
                    };
                    match self.head.compare_exchange(StampedPtr::new(first_ptr, Node::<T>::INDEX_NONE), StampedPtr::new(next_ptr, Node::<T>::INDEX_NONE), Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {
                            let val = unsafe {
                                mem::replace(&mut next.item, MaybeUninit::uninit()).assume_init()
                            };
                            next.can_delete.store(true, Ordering::Release);
                            if next.retired.load(Ordering::Acquire) {
                                self.hazard_nodes.retire(handle.thread_id, next_ptr);
                            }
                            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_PREV);
                            self.hazard_nodes.clear(handle.thread_id, Self::HAZARD_PTR_CURRENT);
                            first.retired.store(true, Ordering::Release);
                            if first.can_delete.load(Ordering::Acquire) {
                                self.hazard_nodes.retire(handle.thread_id, first_ptr);
                            }
                            return Some(val);
                        },
                        Err(_) => {},
                    }
                } else {
                    self.help_finish_deq(handle.thread_id);
                }
            }
            tries += 1;
        }
        self.slow_dequeue(handle)
    }

    
    fn register(&self) -> HandleResult {
        let thread_id = self.current_thread.fetch_add(1, Ordering::Acquire);
        if thread_id < self.num_threads {
            Ok(thread_id)
        } else {
            Err(HandleError)
        }
    }
}

impl<T> Drop for FpSp<T> {
    fn drop(&mut self) {
        while let Some(_) = self.dequeue(0) {}
        unsafe {
            _ = Box::from_raw(self.head.load_ptr(Ordering::Acquire));
            _ = Box::from_raw(self.opdesc_end as *mut OpDesc<T>);
        }
    }
}

unsafe impl<T> Send for FpSp<T> {}
unsafe impl<T> Sync for FpSp<T> {}
