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
use std::{
    mem::{self, MaybeUninit},
    ptr,
    sync::atomic::Ordering,
};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};
use portable_atomic::{AtomicBool, AtomicPtr, AtomicUsize};

use crate::queue::{EnqueueResult, HandleError, HandleResult, Queue};

#[derive(Debug)]
struct Node<T> {
    item: MaybeUninit<T>,
    enq_thread_id: usize,
    deq_thread_id: CachePadded<AtomicUsize>,
    next: CachePadded<AtomicPtr<Node<T>>>,
    can_delete: CachePadded<AtomicBool>,
    retired: CachePadded<AtomicBool>,
}

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
            retired: CachePadded::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug)]
struct OpDesc<T> {
    phase: usize,
    pending: bool,
    enqueue: bool,
    node: *const Node<T>,
}

#[derive(Debug)]
pub struct MSWaitFree<T> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
    states: Box<[CachePadded<AtomicPtr<OpDesc<T>>>]>,
    hazard_ops: Pointers<OpDesc<T>, BoxMemory>,
    hazard_nodes: Pointers<Node<T>, BoxMemory>,
    opdesc_end: *const OpDesc<T>,
    current_thread: AtomicUsize,
    num_threads: usize,
}

impl<T> MSWaitFree<T> {
    const HAZARD_PTR_CURRENT: usize = 0;
    const HAZARD_PTR_NEXT: usize = 1;
    const HAZARD_PTR_PREV: usize = 2;
    pub fn new(num_threads: usize) -> Self {
        let sentinal = Box::into_raw(Box::new(Node::new(Node::<T>::INDEX_NONE)));
        let opdesc_end = Box::into_raw(Box::new(OpDesc {
            phase: Node::<T>::INDEX_NONE,
            pending: false,
            enqueue: true,
            node: ptr::null(),
        }));
        let states: Box<[CachePadded<AtomicPtr<OpDesc<T>>>]> = (0..num_threads)
            .map(|_| CachePadded::new(AtomicPtr::new(opdesc_end)))
            .collect();
        Self {
            head: CachePadded::new(AtomicPtr::new(sentinal)),
            tail: CachePadded::new(AtomicPtr::new(sentinal)),
            states,
            hazard_ops: Pointers::new(BoxMemory, num_threads, 2, num_threads * 2),
            hazard_nodes: Pointers::new(BoxMemory, num_threads, 3, num_threads * 2),
            opdesc_end,
            current_thread: AtomicUsize::new(0),
            num_threads,
        }
    }

    fn help(&self, phase: usize, thread_id: usize) {
        for i in 0..self.num_threads {
            let mut desc = self.hazard_ops.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_CURRENT,
                self.states[i].load(Ordering::SeqCst),
            );
            let mut it = 0;
            while it < self.num_threads + 1 {
                if desc == self.states[i].load(Ordering::SeqCst) {
                    break;
                }
                desc = self.hazard_ops.mark_ptr(
                    thread_id,
                    Self::HAZARD_PTR_CURRENT,
                    self.states[i].load(Ordering::SeqCst),
                );
                it += 1;
            }
            if it == self.num_threads + 1 && desc != self.states[i].load(Ordering::SeqCst) {
                continue;
            }
            let desc = unsafe { &*desc };
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
            let mut desc = self.hazard_ops.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_CURRENT,
                self.states[i].load(Ordering::SeqCst),
            );
            let mut it = 0;
            while it < self.num_threads + 1 {
                if desc == self.states[i].load(Ordering::SeqCst) {
                    break;
                }
                desc = self.hazard_ops.mark_ptr(
                    thread_id,
                    Self::HAZARD_PTR_CURRENT,
                    self.states[i].load(Ordering::SeqCst),
                );
                it += 1;
            }
            if it == self.num_threads + 1 && desc != self.states[i].load(Ordering::SeqCst) {
                continue;
            }
            let desc = unsafe { &*desc };
            let phase = desc.phase;
            max_phase = Some(if let Some(max_phase) = max_phase {
                if phase > max_phase { phase } else { max_phase }
            } else {
                phase
            })
        }
        max_phase.unwrap_or(usize::MAX)
    }

    fn is_pending(&self, query_id: usize, phase: usize, thread_id: usize) -> bool {
        let mut desc = self.hazard_ops.mark_ptr(
            thread_id,
            Self::HAZARD_PTR_NEXT,
            self.states[query_id].load(Ordering::SeqCst),
        );
        let mut it = 0;
        while it < self.num_threads + 1 {
            if desc == self.states[query_id].load(Ordering::SeqCst) {
                break;
            }
            desc = self.hazard_ops.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_NEXT,
                self.states[query_id].load(Ordering::SeqCst),
            );
            it += 1;
        }
        if it == self.num_threads + 1 && desc != self.states[query_id].load(Ordering::SeqCst) {
            return false;
        }
        let desc = unsafe { &*desc };
        desc.pending && desc.phase <= phase
    }

    fn help_enqueue(&self, query_id: usize, phase: usize, thread_id: usize) {
        while self.is_pending(query_id, phase, thread_id) {
            let last = self.hazard_nodes.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_CURRENT,
                self.tail.load(Ordering::SeqCst),
            );
            if last != self.tail.load(Ordering::SeqCst) {
                continue;
            }
            let next = unsafe { (*last).next.load(Ordering::SeqCst) };
            if last == self.tail.load(Ordering::SeqCst) {
                let last = unsafe { &*last };
                if next.is_null() && self.is_pending(query_id, phase, thread_id) {
                    let current_desc = self.hazard_ops.mark_ptr(
                        thread_id,
                        Self::HAZARD_PTR_CURRENT,
                        self.states[query_id].load(Ordering::SeqCst),
                    );
                    if current_desc != self.states[query_id].load(Ordering::SeqCst) {
                        continue;
                    }
                    let current_desc = unsafe { &*current_desc };
                    if last
                        .next
                        .compare_exchange(
                            next,
                            current_desc.node as *mut Node<T>,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        self.help_finish_enq(thread_id);
                        return;
                    }
                }
            } else {
                self.help_finish_enq(thread_id);
            }
        }
    }

    fn help_finish_enq(&self, thread_id: usize) {
        let last = self.hazard_nodes.mark_ptr(
            thread_id,
            Self::HAZARD_PTR_CURRENT,
            self.tail.load(Ordering::SeqCst),
        );
        if last != self.tail.load(Ordering::SeqCst) {
            return;
        }
        let next = self
            .hazard_nodes
            .mark_ptr(thread_id, Self::HAZARD_PTR_NEXT, unsafe {
                (*last).next.load(Ordering::SeqCst)
            });
        if last == self.tail.load(Ordering::SeqCst) && !next.is_null() {
            let query_id = unsafe { (*next).enq_thread_id };
            let current_desc_ptr = self.hazard_ops.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_CURRENT,
                self.states[query_id].load(Ordering::SeqCst),
            );
            if current_desc_ptr != self.states[query_id].load(Ordering::SeqCst) {
                return;
            }
            let current_desc = unsafe { &*current_desc_ptr };
            if last == self.tail.load(Ordering::SeqCst) && current_desc.node == next {
                let new_desc = Box::into_raw(Box::new(OpDesc {
                    phase: current_desc.phase,
                    pending: false,
                    enqueue: true,
                    node: next,
                }));
                match self.states[query_id].compare_exchange(
                    current_desc_ptr,
                    new_desc,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        self.hazard_ops.retire(thread_id, current_desc_ptr);
                    }
                    Err(_) => {
                        unsafe {
                            _ = Box::from_raw(new_desc);
                        };
                    }
                }
                _ = self
                    .tail
                    .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst);
            }
        }
    }

    fn help_dequeue(&self, query_id: usize, phase: usize, thread_id: usize) {
        while self.is_pending(query_id, phase, thread_id) {
            let first = self.hazard_nodes.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_PREV,
                self.head.load(Ordering::SeqCst),
            );
            let last = self.hazard_nodes.mark_ptr(
                thread_id,
                Self::HAZARD_PTR_CURRENT,
                self.tail.load(Ordering::SeqCst),
            );
            if first != self.head.load(Ordering::SeqCst) || last != self.tail.load(Ordering::SeqCst)
            {
                continue;
            }
            let next = unsafe { (*first).next.load(Ordering::SeqCst) };
            if first == self.head.load(Ordering::SeqCst) {
                if first == last {
                    if next.is_null() {
                        let current_desc_ptr = self.hazard_ops.mark_ptr(
                            thread_id,
                            Self::HAZARD_PTR_CURRENT,
                            self.states[query_id].load(Ordering::SeqCst),
                        );
                        if current_desc_ptr != self.states[query_id].load(Ordering::SeqCst) {
                            continue;
                        }
                        if last == self.tail.load(Ordering::SeqCst)
                            && self.is_pending(query_id, phase, thread_id)
                        {
                            let current_desc = unsafe { &*current_desc_ptr };
                            let new_desc = Box::into_raw(Box::new(OpDesc {
                                phase: current_desc.phase,
                                pending: false,
                                enqueue: false,
                                node: ptr::null(),
                            }));
                            match self.states[query_id].compare_exchange(
                                current_desc_ptr,
                                new_desc,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    self.hazard_ops.retire(thread_id, current_desc_ptr);
                                }
                                Err(_) => unsafe { _ = Box::from_raw(new_desc) },
                            }
                        }
                    } else {
                        self.help_finish_enq(thread_id);
                    }
                } else {
                    let current_desc_ptr = self.hazard_ops.mark_ptr(
                        thread_id,
                        Self::HAZARD_PTR_CURRENT,
                        self.states[query_id].load(Ordering::SeqCst),
                    );
                    if current_desc_ptr != self.states[query_id].load(Ordering::SeqCst) {
                        continue;
                    }
                    let current_desc = unsafe { &*current_desc_ptr };
                    let node = current_desc.node;
                    if !self.is_pending(query_id, phase, thread_id) {
                        break;
                    }
                    if first == self.head.load(Ordering::SeqCst) && node != first {
                        let new_desc = Box::into_raw(Box::new(OpDesc {
                            phase: current_desc.phase,
                            pending: true,
                            enqueue: false,
                            node: first,
                        }));
                        match self.states[query_id].compare_exchange(
                            current_desc_ptr,
                            new_desc,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                self.hazard_ops.retire(thread_id, current_desc_ptr);
                            }
                            Err(_) => {
                                unsafe {
                                    _ = Box::from_raw(new_desc);
                                }
                                continue;
                            }
                        }
                    }
                    let temp = usize::MAX;
                    unsafe {
                        _ = (*first).deq_thread_id.compare_exchange(
                            temp,
                            query_id,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
                        self.help_finish_deq(thread_id);
                    }
                }
            }
        }
    }

    fn help_finish_deq(&self, thread_id: usize) {
        let first_ptr = self.hazard_nodes.mark_ptr(
            thread_id,
            Self::HAZARD_PTR_PREV,
            self.head.load(Ordering::SeqCst),
        );
        if first_ptr != self.head.load(Ordering::SeqCst) {
            return;
        }
        let first = unsafe { &*first_ptr };
        let next = first.next.load(Ordering::SeqCst);
        let query_id = first.deq_thread_id.load(Ordering::SeqCst);
        if query_id != usize::MAX {
            let mut current_desc_ptr = ptr::null_mut();
            for i in 0..self.num_threads {
                current_desc_ptr = self.hazard_ops.mark_ptr(
                    thread_id,
                    Self::HAZARD_PTR_CURRENT,
                    self.states[query_id].load(Ordering::SeqCst),
                );
                if current_desc_ptr == self.states[query_id].load(Ordering::SeqCst) {
                    break;
                }
                if i == self.num_threads - 1 {
                    return;
                }
            }
            if first_ptr == self.head.load(Ordering::SeqCst) && !next.is_null() {
                let current_desc = unsafe { &*current_desc_ptr };
                let new_desc = Box::into_raw(Box::new(OpDesc {
                    phase: current_desc.phase,
                    pending: false,
                    enqueue: false,
                    node: current_desc.node,
                }));
                match self.states[query_id].compare_exchange(
                    current_desc_ptr,
                    new_desc,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        self.hazard_ops.retire(thread_id, current_desc_ptr);
                    }
                    Err(_) => unsafe { _ = Box::from_raw(new_desc) },
                }
                _ = self.head.compare_exchange(
                    first_ptr,
                    next,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
    }
}

impl<T> Queue<T> for MSWaitFree<T> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let phase = self.get_max_phase(handle).wrapping_add(1);
        let node = Box::into_raw(Box::new(Node::with_item(handle, item)));
        let op_desc = Box::into_raw(Box::new(OpDesc {
            phase,
            pending: true,
            enqueue: true,
            node,
        }));
        self.states[handle].store(op_desc, Ordering::SeqCst);
        self.help(phase, handle);
        self.help_finish_enq(handle);
        self.hazard_ops.clear(handle, Self::HAZARD_PTR_CURRENT);
        self.hazard_ops.clear(handle, Self::HAZARD_PTR_NEXT);
        self.hazard_nodes.clear(handle, Self::HAZARD_PTR_CURRENT);
        self.hazard_nodes.clear(handle, Self::HAZARD_PTR_NEXT);
        self.hazard_nodes.clear(handle, Self::HAZARD_PTR_PREV);
        let mut desc = self.states[handle].load(Ordering::SeqCst);
        for _ in 0..self.num_threads * 2 {
            if std::ptr::eq(desc, self.opdesc_end) {
                break;
            }
            if self.states[handle]
                .compare_exchange(
                    desc,
                    self.opdesc_end as *mut OpDesc<T>,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
            desc = self.states[handle].load(Ordering::SeqCst);
        }
        self.hazard_ops.retire(handle, desc);
        Ok(())
    }

    fn dequeue(&self, handle: usize) -> Option<T> {
        let phase = self.get_max_phase(handle).wrapping_add(1);
        let op_desc = Box::into_raw(Box::new(OpDesc {
            phase,
            pending: true,
            enqueue: false,
            node: ptr::null(),
        }));
        self.states[handle].store(op_desc, Ordering::SeqCst);
        self.help(phase, handle);
        self.help_finish_deq(handle);
        let current_desc = self.hazard_ops.mark_ptr(
            handle,
            Self::HAZARD_PTR_CURRENT,
            self.states[handle].load(Ordering::SeqCst),
        );
        let node = unsafe { (*current_desc).node };
        if node.is_null() {
            self.hazard_ops.clear(handle, Self::HAZARD_PTR_CURRENT);
            self.hazard_ops.clear(handle, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_CURRENT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_PREV);
            let mut desc = self.states[handle].load(Ordering::SeqCst);
            for _ in 0..self.num_threads {
                if self.states[handle]
                    .compare_exchange(
                        desc,
                        self.opdesc_end as *mut OpDesc<T>,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }
                desc = self.states[handle].load(Ordering::SeqCst);
                if std::ptr::eq(desc, self.opdesc_end) {
                    break;
                }
            }
            self.hazard_ops.retire(handle, desc);
            None
        } else {
            let next_ptr = unsafe { (*node).next.load(Ordering::SeqCst) };
            let next = unsafe { &mut *next_ptr };
            let val = unsafe { mem::replace(&mut next.item, MaybeUninit::uninit()).assume_init() };
            next.can_delete.store(true, Ordering::SeqCst);
            if next.retired.load(Ordering::SeqCst) {
                self.hazard_nodes.retire(handle, next_ptr);
            }

            self.hazard_ops.clear(handle, Self::HAZARD_PTR_CURRENT);
            self.hazard_ops.clear(handle, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_CURRENT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_NEXT);
            self.hazard_nodes.clear(handle, Self::HAZARD_PTR_PREV);
            let can_delete = unsafe {
                (*node).retired.store(true, Ordering::SeqCst);
                (*node).can_delete.load(Ordering::SeqCst)
            };
            if can_delete {
                self.hazard_nodes.retire(handle, node as *mut Node<T>);
            }
            let mut desc = self.states[handle].load(Ordering::SeqCst);
            for _ in 0..self.num_threads * 2 {
                if std::ptr::eq(desc, self.opdesc_end) {
                    break;
                }
                if self.states[handle]
                    .compare_exchange(
                        desc,
                        self.opdesc_end as *mut OpDesc<T>,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }
                desc = self.states[handle].load(Ordering::SeqCst);
            }
            self.hazard_ops.retire(handle, desc);
            Some(val)
        }
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

impl<T> Drop for MSWaitFree<T> {
    fn drop(&mut self) {
        while let Some(_) = self.dequeue(0) {}
        unsafe {
            _ = Box::from_raw(self.head.load(Ordering::SeqCst));
            _ = Box::from_raw(self.opdesc_end as *mut OpDesc<T>);
        }
    }
}

unsafe impl<T> Send for MSWaitFree<T> {}
unsafe impl<T> Sync for MSWaitFree<T> {}
