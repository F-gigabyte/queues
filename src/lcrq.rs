use std::{marker::PhantomData, ptr::{self, null_mut}, sync::atomic::{AtomicPtr, AtomicUsize, Ordering}};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};

use crate::{atomic_types::{AtomicDUsize, DUsize}, queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull}, ring_buffer::RingBuffer};

#[derive(Debug)]
struct Node<T> {
    index: usize,
    ptr: *mut T,
    _phantom: PhantomData<T>
}

impl<T> Node<T> {
    const SAFE_SHIFT: u32 = usize::BITS - 1;
    const SAFE_MASK: usize = 1 << Self::SAFE_SHIFT;

    pub fn from_index(index: usize) -> Self {
        Self { 
            index: index | Self::SAFE_MASK, 
            ptr: ptr::null_mut(), 
            _phantom: PhantomData
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

    pub fn get_index(&self) -> usize {
        self.index & !Self::SAFE_MASK
    }
}

impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            ptr: self.ptr,
            _phantom: PhantomData
        }
    }
}

impl<T> Copy for Node<T> {}

impl<T> From<DUsize> for Node<T> {
    fn from(value: DUsize) -> Self {
        Node { 
            index: ((value >> usize::BITS) & usize::MAX as DUsize) as usize, 
            ptr: (value & usize::MAX as DUsize) as *mut T, 
            _phantom: PhantomData
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
    fn new(len: usize) -> Self {
        let array: Box<[CachePadded<AtomicDUsize>]> = (0..len).map(|v| CachePadded::new(AtomicDUsize::new(DUsize::from(Node::<T>::from_index(v))))).collect();
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            tail: CachePadded::new(AtomicUsize::new(0)), 
            array, 
            _phantom: PhantomData 
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
                CachePadded::new(AtomicDUsize::new(DUsize::from(
                if v == 0 {
                    Node::new(v as usize, true, elem)
                } else {
                    Node::<T>::from_index(v)
                })))
            }).collect();
        Self {
            array,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(1)),
            _phantom: PhantomData
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

            match self.tail.compare_exchange(t, h, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return,
                Err(_) => {}
            }
        }
    }

    fn close(&self, t: usize, tries: usize) -> bool {
        if CLOSABLE {
            let tt = t + 1;
            if tries < 10 {
                match self.tail.compare_exchange(tt, tt | Self::CLOSED_MASK, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => true,
                    Err(_) => false,
                }
            } else {
                self.tail.fetch_or(Self::CLOSED_MASK, Ordering::Release);
                true
            }
        } else {
            true
        }
    }
}

impl<T, const CLOSABLE: bool> Queue<T> for CRQ<T, CLOSABLE> {
    fn enqueue(&self, val: T, _: &mut ()) -> EnqueueResult<T> {
        let mut tries = 0;
        let val = Box::into_raw(Box::new(val));
        loop {
            let t = self.tail.fetch_add(1, Ordering::Acquire);
            let t = if CLOSABLE {
                let closed = t & Self::CLOSED_MASK != 0;
                if closed {
                    let val = unsafe {
                        *Box::from_raw(val)
                    };
                    return Err(QueueFull(val));
                }
                t & !Self::CLOSED_MASK
            } else {
                t
            };
            let slot = &self.array[t % self.array.len()];
            let node = Node::<T>::from(slot.load(Ordering::Acquire));
            if node.ptr.is_null() && 
                node.get_index() as usize <= t && 
                    (node.is_safe() || self.head.load(Ordering::Acquire) <= t) {
                        let new_node = Node::new(t, true, val);
                        match slot.compare_exchange(DUsize::from(node), DUsize::from(new_node), Ordering::Release, Ordering::Relaxed) {
                            Ok(_) => {
                                return Ok(())
                            },
                            Err(_) => {},
                        }
            }
            let h = self.head.load(Ordering::Acquire);
            if t - h >= self.array.len() {
                if self.close(t, tries) {
                    if CLOSABLE {
                        self.tail.fetch_or(Self::CLOSED_MASK, Ordering::Release);
                    }
                    let val = unsafe {
                        *Box::from_raw(val)
                    };
                    return Err(QueueFull(val));
                } else {
                    tries += 1;
                }
            }
        }
    }
    
    fn dequeue(&self, _: &mut ()) -> Option<T> {
        loop {
            let h = self.head.fetch_add(1, Ordering::Acquire);
            let slot = &self.array[h % self.array.len()];
            let node = Node::<T>::from(slot.load(Ordering::Acquire));
            loop {
                if node.get_index() > h {
                    break;
                }
                if !node.ptr.is_null() {
                    if node.get_index() == h {
                        let new_node = Node::<T>::new(h + self.array.len(), node.is_safe(), ptr::null_mut());
                        match slot.compare_exchange(DUsize::from(node), DUsize::from(new_node), Ordering::Release, Ordering::Relaxed) {
                            Ok(_) => {
                                let val = unsafe {
                                    *Box::from_raw(node.ptr)
                                };
                                return Some(val)
                            },
                            Err(_) => {},
                        }
                    }
                } else {
                    let new_node = Node::<T>::new(h + self.array.len(), node.is_safe(), ptr::null_mut());
                    match slot.compare_exchange(DUsize::from(node), DUsize::from(new_node), Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(_) => {},
                    }
                }
            }
            let t = self.tail.load(Ordering::Acquire);
            if CLOSABLE {
                let t = t & !Self::CLOSED_MASK;
            }
            if t <= h + 1 {
                self.fix_state();
                return None;
            }
        }
    }


    fn register(&self) -> HandleResult<()> {
        Ok(())
    }
}

impl<T, const CLOSABLE: bool> Drop for CRQ<T, CLOSABLE> {
    fn drop(&mut self) {
        let mut h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire) & if CLOSABLE { !Self::CLOSED_MASK } else { usize::MAX };
        while h < t {
            let node = Node::<T>::from(self.array[h % self.array.len()].load(Ordering::Acquire));
            unsafe {
                _ = Box::from_raw(node.ptr);
            }
            h += 1;
        }
    }
}

#[derive(Debug)]
pub struct LCRQHandle {
    thread_id: usize,
}

type Handle = LCRQHandle;

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
pub struct LCRQ<T, QUEUE=CRQ<T, true>> 
where 
    QUEUE: RingBuffer<T>
{
    head: CachePadded<AtomicPtr<LCRQNode<QUEUE>>>,
    tail: CachePadded<AtomicPtr<LCRQNode<QUEUE>>>,
    current_thread: AtomicUsize,
    num_threads: usize,
    ring_size: usize,
    hazard: Pointers<LCRQNode<QUEUE>, BoxMemory>,
    _phantom: PhantomData<T>
}

impl<T, QUEUE> LCRQ<T, QUEUE>
where 
    QUEUE: RingBuffer<T>
{
    pub fn new(ring_size: usize, num_threads: usize) -> Self {
        let crq = Box::into_raw(Box::new(LCRQNode::new(QUEUE::new(ring_size))));
        Self { 
            head: CachePadded::new(AtomicPtr::new(crq)), 
            tail: CachePadded::new(AtomicPtr::new(crq)), 
            hazard: Pointers::new(BoxMemory, num_threads, 1, num_threads * 2),
            num_threads,
            ring_size,
            current_thread: AtomicUsize::new(0),
            _phantom: PhantomData
        }
    }
}

impl<T, QUEUE> Queue<T, LCRQHandle> for LCRQ<T, QUEUE>
where 
    QUEUE: RingBuffer<T>
{
    fn enqueue(&self, mut item: T, handle: &mut Handle) -> EnqueueResult<T> {
        loop {
            let crq_ptr = self.hazard.mark(handle.thread_id, 0, &*self.tail);
            let crq = unsafe {
                &*crq_ptr
            };
            let next = crq.next.load(Ordering::Acquire);
            if !next.is_null() {
                let _ = self.tail.compare_exchange(crq_ptr, next, Ordering::Release, Ordering::Relaxed);
                continue;
            }
            match crq.node.enqueue(item, &mut ()) {
                Ok(_) => {
                    self.hazard.clear(handle.thread_id, 0);
                    return Ok(())
                },
                Err(QueueFull(item2)) => {
                    item = item2;
                    let queue = QUEUE::new(self.ring_size);
                    queue.enqueue(item, &mut ()).unwrap_or_else(|_| {
                        panic!("Have full queue with an empty queue");
                    });
                    let new_crq = Box::into_raw(Box::new(LCRQNode::new(queue)));
                    match crq.next.compare_exchange(ptr::null_mut(), new_crq, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => {
                            _ = self.tail.compare_exchange(crq_ptr, new_crq, Ordering::Release, Ordering::Relaxed);
                            self.hazard.clear(handle.thread_id, 0);
                            return Ok(());
                        },
                        Err(_) => {
                            let new_crq = unsafe {
                                *Box::from_raw(new_crq)
                            };
                            item = new_crq.node.dequeue(&mut ()).unwrap_or_else(|| {
                                panic!("Unable to retrieve enqueued item");
                            });
                        },
                    }
                }
            }
            self.hazard.clear(handle.thread_id, 0);
        }
    }

    fn dequeue(&self, handle: &mut Handle) -> Option<T> {
        loop {
            let crq_ptr = self.hazard.mark(handle.thread_id, 0, &*self.head);
            let crq = unsafe {
                &*crq_ptr
            };
            if let Some(v) = crq.node.dequeue(&mut ()) {
                self.hazard.clear(handle.thread_id, 0);
                return Some(v);
            }
            let next = crq.next.load(Ordering::Acquire);
            if next.is_null() {
                self.hazard.clear(handle.thread_id, 0);
                return None;
            }
            self.hazard.clear(handle.thread_id, 0);
            self.hazard.retire(handle.thread_id, crq_ptr);
            _ = self.head.compare_exchange_weak(crq_ptr, next, Ordering::Release, Ordering::Relaxed);
        }
    }

    fn register(&self) -> HandleResult<Handle> {
        let thread_id = self.current_thread.fetch_add(1, Ordering::Acquire);
        if thread_id < self.num_threads {
            Ok(Handle {
                thread_id
            })
        } else {
            Err(HandleError)
        }
    }
}

unsafe impl<T, QUEUE> Send for LCRQ<T, QUEUE>
where 
    QUEUE: RingBuffer<T> + Send
{}
unsafe impl<T, QUEUE> Sync for LCRQ<T, QUEUE> 
where 
    QUEUE: RingBuffer<T> + Send
{}
