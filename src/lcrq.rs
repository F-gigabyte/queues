use std::{
    marker::PhantomData,
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};

use crate::{
    atomic_types::{AtomicDUsize, DUsize},
    queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull},
    ring_buffer::RingBuffer,
};

#[derive(Debug)]
struct Node<T> {
    index: usize,
    ptr: *mut T,
    _phantom: PhantomData<T>,
}

impl<T> Node<T> {
    const SAFE_SHIFT: u32 = usize::BITS - 1;
    const SAFE_MASK: usize = 1 << Self::SAFE_SHIFT;

    pub fn from_index(index: usize) -> Self {
        Self {
            index: index | Self::SAFE_MASK,
            ptr: ptr::null_mut(),
            _phantom: PhantomData,
        }
    }

    pub fn new(index: usize, safe: bool, ptr: *mut T) -> Self {
        assert!(index < Self::SAFE_MASK);
        Self {
            index: index | if safe { Self::SAFE_MASK } else { 0 },
            ptr,
            _phantom: PhantomData,
        }
    }

    pub fn is_safe(&self) -> bool {
        self.index & Self::SAFE_MASK != 0
    }

    pub fn set_safe(&mut self, safe: bool) {
        if safe {
            self.index |= Self::SAFE_MASK;
        } else {
            self.index &= !Self::SAFE_MASK;
        }
    }

    pub fn get_index(&self) -> usize {
        self.index & !Self::SAFE_MASK
    }
}

impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

impl<T> Copy for Node<T> {}

impl<T> From<DUsize> for Node<T> {
    fn from(value: DUsize) -> Self {
        Node {
            index: ((value >> usize::BITS) & usize::MAX as DUsize) as usize,
            ptr: (value & usize::MAX as DUsize) as *mut T,
            _phantom: PhantomData,
        }
    }
}

impl<T> From<Node<T>> for DUsize {
    fn from(value: Node<T>) -> Self {
        ((value.index as DUsize) << usize::BITS) | value.ptr as DUsize
    }
}

#[derive(Debug)]
pub struct CRQ<T, const CLOSABLE: bool> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    array: Box<[CachePadded<AtomicDUsize>]>,
    _phantom: PhantomData<T>,
}

impl<T, const CLOSABLE: bool> RingBuffer<T> for CRQ<T, CLOSABLE> {
    fn new(len: usize, _: usize) -> Self {
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..len)
            .map(|v| CachePadded::new(AtomicDUsize::new(DUsize::from(Node::<T>::from_index(v)))))
            .collect();
        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            array,
            _phantom: PhantomData,
        }
    }

    fn is_closed(&self) -> bool {
        if CLOSABLE {
            self.tail.load(Ordering::Acquire) & Self::CLOSED_MASK != 0
        } else {
            false
        }
   }
}

impl<T, const CLOSABLE: bool> CRQ<T, CLOSABLE> {
    const CLOSED_SHIFT: usize = usize::BITS as usize - 1;
    const CLOSED_MASK: usize = 1 << Self::CLOSED_SHIFT;

    pub fn with_elem(len: usize, elem: T) -> Self {
        let elem = Box::into_raw(Box::new(elem));
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..len)
            .map(|v| {
                CachePadded::new(AtomicDUsize::new(DUsize::from(if v == 0 {
                    Node::new(v, true, elem)
                } else {
                    Node::<T>::from_index(v)
                })))
            })
            .collect();
        Self {
            array,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(1)),
            _phantom: PhantomData,
        }
    }

    fn fix_state(&self) {
        loop {
            let h = self.head.load(Ordering::Acquire);
            let t = self.tail.load(Ordering::Acquire);

            if self.tail.load(Ordering::Acquire) != t {
                continue;
            }

            if h <= t {
                return;
            }

            if self
                .tail
                .compare_exchange(t, h, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn close(&self, t: usize, tries: usize) -> bool {
        if CLOSABLE {
            let tt = t + 1;
            if tries < 10 {
                self.tail
                    .compare_exchange(
                        tt,
                        tt | Self::CLOSED_MASK,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            } else {
                _ = self.tail.fetch_or(Self::CLOSED_MASK, Ordering::AcqRel);
                true
            }
        } else {
            true
        }
    }
}

impl<T, const CLOSABLE: bool> Queue<T> for CRQ<T, CLOSABLE> {
    fn enqueue(&self, mut val: T, _: usize) -> EnqueueResult<T> {
        let mut h;
        loop {
            let t = self.tail.load(Ordering::Acquire);
            let (t, closed) = if CLOSABLE {
                (t & !Self::CLOSED_MASK, t & Self::CLOSED_MASK != 0)
            } else {
                (t, false)
            };
            if closed {
                return Err(QueueFull(val));
            }
            let slot = &self.array[t % self.array.len()];
            let node = Node::<T>::from(slot.load(Ordering::Acquire));
            if node.ptr.is_null() {
                if node.get_index() <= t && (node.is_safe() || self.head.load(Ordering::Acquire) <= t) {
                    let ptr = Box::into_raw(Box::new(val));
                    let new_node = Node::new(t, true, ptr);
                    match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            return Ok(());
                        },
                        Err(_) => {
                        }
                    }
                    val = unsafe {
                        *Box::from_raw(ptr)
                    };
                }
            }
            h = self.head.load(Ordering::Acquire);
            if t as i64 - h as i64 >= self.array.len() as i64 {
                if CLOSABLE {
                    _ = self.tail.fetch_or(Self::CLOSED_MASK, Ordering::AcqRel);
                }
                return Err(QueueFull(val));
            }

        }
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        loop {
            let h = self.head.fetch_add(1, Ordering::AcqRel);
            let slot = &self.array[h % self.array.len()];
            let mut node = Node::<T>::from(slot.load(Ordering::Acquire));
            loop {
                if node.get_index() > h {
                    break;
                }
                if !node.ptr.is_null() {
                    if node.get_index() == h {
                        let new_node = Node::<T>::new(h + self.array.len(), node.is_safe(), ptr::null_mut());
                        match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::AcqRel, Ordering::Acquire) {
                            Ok(_) => {
                                let val = unsafe {
                                    *Box::from_raw(node.ptr)
                                };
                                return Some(val);
                            },
                            Err(val) => {
                                node = Node::<T>::from(val);
                            }
                        }
                    } else {
                        let mut new_node = node;
                        new_node.set_safe(false);
                        match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::AcqRel, Ordering::Acquire) {
                            Ok(_) => break,
                            Err(val) => {
                                node = Node::<T>::from(val);
                            }
                        }
                    }
                } else {
                    let new_node = Node::<T>::new(h + self.array.len(), node.is_safe(), ptr::null_mut());
                    match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => break,
                        Err(val) => {
                            node = Node::<T>::from(val);
                        }
                    }
                }
            }
            let t = self.tail.load(Ordering::Acquire);
            let (t, closed) = if CLOSABLE {
                (t & !Self::CLOSED_MASK, t & Self::CLOSED_MASK != 0)
            } else {
                (t, false)
            };
            if t <= h + 1 {
                self.fix_state();
                return None;
            }
        }
    }

    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

impl<T, const CLOSABLE: bool> Drop for CRQ<T, CLOSABLE> {
    fn drop(&mut self) {
        while let Some(_) = self.dequeue(0) {}
    }
}

#[derive(Debug)]
struct LCRQNode<QUEUE> {
    node: QUEUE,
    next: CachePadded<AtomicPtr<LCRQNode<QUEUE>>>,
}

impl<QUEUE> LCRQNode<QUEUE> {
    pub fn new(queue: QUEUE) -> Self {
        Self {
            node: queue,
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
        }
    }
}

#[derive(Debug)]
pub struct LCRQ<T, QUEUE = CRQ<T, true>>
where
    QUEUE: RingBuffer<T>,
{
    head: CachePadded<AtomicPtr<LCRQNode<QUEUE>>>,
    tail: CachePadded<AtomicPtr<LCRQNode<QUEUE>>>,
    current_thread: AtomicUsize,
    num_threads: usize,
    ring_size: usize,
    hazard: Pointers<LCRQNode<QUEUE>, BoxMemory>,
    _phantom: PhantomData<T>,
}

impl<T, QUEUE> LCRQ<T, QUEUE>
where
    QUEUE: RingBuffer<T>,
{
    pub fn new(ring_size: usize, num_threads: usize) -> Self {
        let crq = Box::into_raw(Box::new(LCRQNode::new(QUEUE::new(ring_size, num_threads))));
        Self {
            head: CachePadded::new(AtomicPtr::new(crq)),
            tail: CachePadded::new(AtomicPtr::new(crq)),
            hazard: Pointers::new(BoxMemory, num_threads, 1, num_threads * 2),
            num_threads,
            ring_size,
            current_thread: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<T, QUEUE> Queue<T> for LCRQ<T, QUEUE>
where
    QUEUE: RingBuffer<T>,
{
    fn enqueue(&self, mut item: T, handle: usize) -> EnqueueResult<T> {
        loop {
            let crq_ptr = self.hazard.mark_ptr(handle, 0, self.tail.load(Ordering::Acquire));
            let crq = unsafe {
                &*crq_ptr
            };
            let next = crq.next.load(Ordering::Acquire);
            if !next.is_null() {
                _ = self.tail.compare_exchange(crq_ptr, next, Ordering::AcqRel, Ordering::Acquire);
                continue;
            }
            match crq.node.enqueue(item, handle) {
                Ok(_) => {
                    self.hazard.clear(handle, 0);
                    return Ok(());
                },
                Err(QueueFull(item2)) => {
                    let new_crq = QUEUE::new(self.ring_size, self.num_threads);
                    new_crq.enqueue(item2, handle).unwrap_or_else(|_| {
                        panic!("Queue full but empty");
                    });
                    let new_crq = Box::into_raw(Box::new(LCRQNode {
                        node: new_crq,
                        next: CachePadded::new(AtomicPtr::new(ptr::null_mut()))
                    }));
                    match crq.next.compare_exchange(ptr::null_mut(), new_crq, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            _ = self.tail.compare_exchange(crq_ptr, new_crq, Ordering::AcqRel, Ordering::Acquire);
                            self.hazard.clear(handle, 0);
                            return Ok(());
                        },
                        Err(_) => {
                            let new_crq = unsafe {
                                *Box::from_raw(new_crq)
                            };
                            item = new_crq.node.dequeue(handle).unwrap();
                        }
                    }
                }
            }
        }
    }

    fn dequeue(&self, handle: usize) -> Option<T> {
        loop {
            let crq_ptr = self.hazard.mark(handle, 0, &*self.head);
            let crq = unsafe { &*crq_ptr };
            if let Some(v) = crq.node.dequeue(handle) {
                self.hazard.clear(handle, 0);
                return Some(v);
            }
            let next = crq.next.load(Ordering::Acquire);
            if next.is_null() {
                self.hazard.clear(handle, 0);
                return None;
            }
            self.hazard.clear(handle, 0);
            match self.head.compare_exchange_weak(
                crq_ptr,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.hazard.retire(handle, crq_ptr);
                },
                Err(_) => {},
            }
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

impl<T, QUEUE> Drop for LCRQ<T, QUEUE>
where
    QUEUE: RingBuffer<T>,
{
    fn drop(&mut self) {
        while let Some(_) = self.dequeue(0) {}
    }
}

unsafe impl<T, QUEUE> Send for LCRQ<T, QUEUE> where QUEUE: RingBuffer<T> + Send {}
unsafe impl<T, QUEUE> Sync for LCRQ<T, QUEUE> where QUEUE: RingBuffer<T> + Send {}
