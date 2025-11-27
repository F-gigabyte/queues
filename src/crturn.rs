/// ******************************************************************************
/// Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
/// All rights reserved.
///
/// Redistribution and use in source and binary forms, with or without
/// modification, are permitted provided that the following conditions are met:
///     - Redistributions of source code must retain the above copyright
///       notice, this list of conditions and the following disclaimer.
///     - Redistributions in binary form must reproduce the above copyright
///       notice, this list of conditions and the following disclaimer in the
///       documentation and/or other materials provided with the distribution.
///     - Neither the name of Concurrency Freaks nor the
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

use portable_atomic::{AtomicPtr, AtomicUsize};
use std::{mem::{self, MaybeUninit}, ptr, sync::atomic::Ordering};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};

use crate::queue::{EnqueueResult, HandleError, HandleResult, Queue};

#[derive(Debug)]
struct Node<T> {
    item: MaybeUninit<T>,
    enq_thread_id: usize,
    deq_thread_id: AtomicUsize,
    next: AtomicPtr<Node<T>>
}

impl<T> Node<T> {
    const INDEX_NONE: usize = usize::MAX;
    pub fn new(thread_id: usize) -> Self {
        Self {
            item: MaybeUninit::uninit(),
            enq_thread_id: thread_id,
            deq_thread_id: AtomicUsize::new(Self::INDEX_NONE),
            next: AtomicPtr::new(ptr::null_mut())
        }
    }
    pub fn new_with(item: T, thread_id: usize) -> Self {
        Self {
            item: MaybeUninit::new(item),
            enq_thread_id: thread_id,
            deq_thread_id: AtomicUsize::new(Self::INDEX_NONE),
            next: AtomicPtr::new(ptr::null_mut())
        }
    }
}

#[derive(Debug)]
pub struct CRTurn<T> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
    enqueuers: Box<[CachePadded<AtomicPtr<Node<T>>>]>,
    deq_self: Box<[CachePadded<AtomicPtr<Node<T>>>]>,
    deq_help: Box<[CachePadded<AtomicPtr<Node<T>>>]>,
    hazard: Pointers<Node<T>, BoxMemory>,
    sentinal: *mut Node<T>,
    current_thread: AtomicUsize,
    num_threads: usize
}

impl<T> CRTurn<T> {
    const HAZARD_TAIL: usize = 0;
    const HAZARD_HEAD: usize = 0;
    const HAZARD_NEXT: usize = 1;
    const HAZARD_DEQUEUE: usize = 2;
    const INDEX_NONE: usize = usize::MAX;
    pub fn new(num_threads: usize) -> Self {
        let sentinal = Box::into_raw(Box::new(Node::new(0)));
        let enqueuers: Box<[CachePadded<AtomicPtr<Node<T>>>]> = (0..num_threads).map(|_| {
            CachePadded::new(AtomicPtr::new(ptr::null_mut()))
        }).collect();
        let deq_self: Box<[CachePadded<AtomicPtr<Node<T>>>]> = (0..num_threads).map(|_| {
            let node = Box::into_raw(Box::new(Node::new(0)));
            CachePadded::new(AtomicPtr::new(node))
        }).collect();
        let deq_help: Box<[CachePadded<AtomicPtr<Node<T>>>]> = (0..num_threads).map(|_| {
            let node = Box::into_raw(Box::new(Node::new(0)));
            CachePadded::new(AtomicPtr::new(node))
        }).collect();
        Self {
            sentinal,
            head: CachePadded::new(AtomicPtr::new(sentinal)),
            tail: CachePadded::new(AtomicPtr::new(sentinal)),
            enqueuers,
            deq_self,
            deq_help,
            hazard: Pointers::new(BoxMemory, num_threads, 3, num_threads * 2),
            current_thread: AtomicUsize::new(0),
            num_threads,
        }
    }

    fn search_next(&self, head: &Node<T>, next: &Node<T>) -> usize {
        let turn = head.deq_thread_id.load(Ordering::Acquire) % self.num_threads;
        for i in turn + 1..turn + self.num_threads + 1 {
            let id_deq = i % self.num_threads;
            if self.deq_self[id_deq].load(Ordering::Acquire) != self.deq_help[id_deq].load(Ordering::Acquire) {
                continue;
            }
            if next.deq_thread_id.load(Ordering::Acquire) == Self::INDEX_NONE {
                _ = next.deq_thread_id.compare_exchange(Self::INDEX_NONE, id_deq, Ordering::Release, Ordering::Relaxed);
            }
            break;
        }
        return next.deq_thread_id.load(Ordering::Acquire);
    }

    fn cas_dequeue_and_head(&self, head_ptr: *const Node<T>, next_ptr: *const Node<T>, thread_id: usize) {
        let next = unsafe {
            &*next_ptr
        };
        let deq_thread_id = next.deq_thread_id.load(Ordering::Acquire);
        if deq_thread_id == thread_id {
            self.deq_help[deq_thread_id].store(next_ptr as *mut Node<T>, Ordering::Release);
        } else {
            let deq_help = self.hazard.mark_ptr(thread_id, Self::HAZARD_DEQUEUE, self.deq_help[deq_thread_id].load(Ordering::Acquire));
            if deq_help as *const Node<T> != next_ptr && head_ptr != self.head.load(Ordering::Acquire) {
                _ = self.deq_help[deq_thread_id].compare_exchange(deq_help, next_ptr as *mut Node<T>, Ordering::Release, Ordering::Relaxed);
            }
        }
        _ = self.head.compare_exchange(head_ptr as *mut Node<T>, next_ptr as *mut Node<T>, Ordering::Release, Ordering::Relaxed);
    }

    fn give_up(&self, my_request: *const Node<T>, thread_id: usize) {
        let head = self.head.load(Ordering::Acquire);
        if self.deq_help[thread_id].load(Ordering::Acquire) as *const Node<T> != my_request || head == self.tail.load(Ordering::Acquire) {
            return;
        }
        let next = self.hazard.mark_ptr(thread_id, Self::HAZARD_NEXT, unsafe {(*head).next.load(Ordering::Acquire)});
        if head != self.head.load(Ordering::Acquire) {
            return;
        }
        if self.search_next(unsafe {&*head}, unsafe {&*next}) == Self::INDEX_NONE {
            unsafe {
                _ = (*next).deq_thread_id.compare_exchange(Self::INDEX_NONE, thread_id, Ordering::Release, Ordering::Relaxed);
            }
        }
        self.cas_dequeue_and_head(head, next, thread_id);
    }
}

#[derive(Debug)]
pub struct CRTurnHandle {
    thread_id: usize
}

type Handle = CRTurnHandle;

impl<T> Queue<T, CRTurnHandle> for CRTurn<T> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let thread_id = handle;
        let my_node = Box::into_raw(Box::new(Node::new_with(item, thread_id)));
        self.enqueuers[thread_id].store(my_node, Ordering::Release);
        for _ in 0..self.num_threads {
            if self.enqueuers[thread_id].load(Ordering::Acquire) == ptr::null_mut() {
                self.hazard.clear(thread_id, Self::HAZARD_TAIL);
                self.hazard.clear(thread_id, Self::HAZARD_NEXT);
                self.hazard.clear(thread_id, Self::HAZARD_DEQUEUE);
                return Ok(());
            }
            let tail_ptr = self.hazard.mark_ptr(thread_id, Self::HAZARD_TAIL, self.tail.load(Ordering::Acquire));
            if tail_ptr != self.tail.load(Ordering::Acquire) {
                continue;
            }
            let tail = unsafe {
                &*tail_ptr
            };
            if self.enqueuers[tail.enq_thread_id].load(Ordering::Acquire) == tail_ptr {
                _ = self.enqueuers[tail.enq_thread_id].compare_exchange(tail_ptr, ptr::null_mut(), Ordering::Release, Ordering::Relaxed);
            }
            for j in 1..self.num_threads + 1 {
                let node_help = self.enqueuers[(j + tail.enq_thread_id) % self.num_threads].load(Ordering::Acquire);
                if node_help == ptr::null_mut() {
                    continue;
                }
                _ = tail.next.compare_exchange(ptr::null_mut(), node_help, Ordering::Release, Ordering::Relaxed);
                break;
            }
            let next = tail.next.load(Ordering::Acquire);
            if !next.is_null() {
                _ = self.tail.compare_exchange(tail_ptr, next, Ordering::Release, Ordering::Relaxed);
            }
        }
        self.enqueuers[thread_id].store(ptr::null_mut(), Ordering::Release);
        self.hazard.clear(thread_id, Self::HAZARD_TAIL);
        self.hazard.clear(thread_id, Self::HAZARD_NEXT);
        self.hazard.clear(thread_id, Self::HAZARD_DEQUEUE);
        Ok(())
    }
    
    fn dequeue(&self, handle: usize) -> Option<T> {
        let thread_id = handle;
        let prev_request = self.deq_self[thread_id].load(Ordering::Acquire);
        let my_request = self.deq_help[thread_id].load(Ordering::Acquire);
        self.deq_self[thread_id].store(my_request, Ordering::Release);
        for _ in 0..self.num_threads {
            if self.deq_help[thread_id].load(Ordering::Acquire) != my_request {
                break;
            }
            let head_ptr = self.hazard.mark_ptr(thread_id, Self::HAZARD_HEAD, self.head.load(Ordering::Acquire));
            if head_ptr != self.head.load(Ordering::Acquire) {
                continue;
            }
            if head_ptr == self.tail.load(Ordering::Acquire) {
                self.deq_self[thread_id].store(prev_request, Ordering::Release);
                self.give_up(my_request, thread_id);
                if self.deq_help[thread_id].load(Ordering::Acquire) != my_request {
                    self.deq_self[thread_id].store(my_request, Ordering::Relaxed);
                    break;
                }
                self.hazard.clear(thread_id, Self::HAZARD_HEAD);
                self.hazard.clear(thread_id, Self::HAZARD_NEXT);
                self.hazard.clear(thread_id, Self::HAZARD_DEQUEUE);
                return None;
            }

            let head = unsafe {
                &*head_ptr
            };

            let next = self.hazard.mark_ptr(thread_id, Self::HAZARD_NEXT, head.next.load(Ordering::Acquire));
            if head_ptr != self.head.load(Ordering::Acquire) {
                continue;
            }
            if self.search_next(head, unsafe { &*next }) != Self::INDEX_NONE {
                self.cas_dequeue_and_head(head_ptr, next, thread_id);
            }
        }
        let my_node = self.deq_help[thread_id].load(Ordering::Acquire);
        let head = self.hazard.mark_ptr(thread_id, Self::HAZARD_HEAD, self.head.load(Ordering::Acquire));
        if head == self.head.load(Ordering::Acquire) && my_node == unsafe {(*head).next.load(Ordering::Acquire)} {
            _ = self.head.compare_exchange(head, my_node, Ordering::Release, Ordering::Relaxed);
        }
        self.hazard.clear(thread_id, Self::HAZARD_HEAD);
        self.hazard.clear(thread_id, Self::HAZARD_NEXT);
        self.hazard.clear(thread_id, Self::HAZARD_DEQUEUE);
        self.hazard.retire(thread_id, prev_request);
        let item = unsafe {
            mem::replace(&mut (*my_node).item, MaybeUninit::uninit()).assume_init()
        };
        Some(item)
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

impl<T> Drop for CRTurn<T> {
    fn drop(&mut self) {
        unsafe {
            _ = *Box::from_raw(self.sentinal);
        }
        while let Some(_) = self.dequeue(0) {}
        for val in &self.deq_self {
            unsafe {
                _ = *Box::from_raw(val.load(Ordering::Acquire));
            }
        }
        for val in &self.deq_help {
            unsafe {
                _ = *Box::from_raw(val.load(Ordering::Acquire));
            }
        }
    }
}

unsafe impl<T> Send for CRTurn<T> {}
unsafe impl<T> Sync for CRTurn<T> {}
