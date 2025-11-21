use std::{marker::PhantomData, mem, ptr::{self, null_mut}, sync::atomic::{AtomicPtr, AtomicUsize, Ordering}};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};
use portable_atomic::AtomicU128;

use crate::queue::{EnqueueResult, Queue, QueueFull};

pub const RING_SIZE: usize = 16;

struct Node<T> {
    index: u64,
    ptr: *mut T,
    _phantom: PhantomData<T>
}

impl<T> Node<T> {
    const SAFE_SHIFT: u64 = 63;
    const SAFE_MASK: u64 = 1 << Self::SAFE_SHIFT;

    pub fn from_index(index: usize) -> Self {
        Self { 
            index: index as u64 | Self::SAFE_MASK, 
            ptr: ptr::null_mut(), 
            _phantom: PhantomData
        }
    }

    pub fn new(index: u64, safe: bool, ptr: *mut T) -> Self {
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

    pub fn get_index(&self) -> u64 {
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

impl<T> From<u128> for Node<T> {
    fn from(value: u128) -> Self {
        Node { 
            index: ((value >> 64) & u64::MAX as u128) as u64, 
            ptr: (value & u64::MAX as u128) as *mut T, 
            _phantom: PhantomData
        }
    }
}

impl<T> From<Node<T>> for u128 {
    fn from(value: Node<T>) -> Self {
        ((value.index as u128) << 64) | value.ptr as u128
    }
}

struct CRQ<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    next: CachePadded<AtomicPtr<CRQ<T>>>,
    array: Box<[CachePadded<AtomicU128>]>,
    _phantom: PhantomData<T>,
}

impl<T> CRQ<T> {
    const CLOSED_SHIFT: usize = usize::BITS as usize - 1;
    const CLOSED_MASK: usize = 1 << Self::CLOSED_SHIFT;
    pub fn new(len: usize) -> Self {
        let array: Box<[CachePadded<AtomicU128>]> = (0..len).map(|v| CachePadded::new(AtomicU128::new(u128::from(Node::<T>::from_index(v))))).collect();
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            tail: CachePadded::new(AtomicUsize::new(0)), 
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            array, 
            _phantom: PhantomData 
        }
    }

    pub fn with_elem(len: usize, elem: T) -> Self {
        let elem = Box::into_raw(Box::new(elem));
        let array: Box<[CachePadded<AtomicU128>]> = (0..len)
            .map(|v| {
                CachePadded::new(AtomicU128::new(u128::from(
                if v == 0 {
                    Node::new(v as u64, true, elem)
                } else {
                    Node::<T>::from_index(v)
                })))
            }).collect();
        Self {
            array,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(1)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
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
    }

    pub fn enqueue(&self, val: T) -> EnqueueResult<T> {
        let mut tries = 0;
        let val = Box::into_raw(Box::new(val));
        loop {
            let t = self.tail.fetch_add(1, Ordering::Acquire);
            let closed = t & Self::CLOSED_MASK != 0;
            let t = t & !Self::CLOSED_MASK;
            if closed {
                let val = unsafe {
                    *Box::from_raw(val)
                };
                return Err(QueueFull(val));
            }
            let slot = &self.array[t % self.array.len()];
            let node = Node::<T>::from(slot.load(Ordering::Acquire));
            if node.ptr.is_null() && 
                node.get_index() as usize <= t && 
                    (node.is_safe() || self.head.load(Ordering::Acquire) <= t) {
                        let new_node = Node::new(t as u64, true, val);
                        match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::Release, Ordering::Relaxed) {
                            Ok(_) => {
                                return Ok(())
                            },
                            Err(_) => {},
                        }
            }
            let h = self.head.load(Ordering::Acquire);
            if t - h >= self.array.len() {
                if self.close(t, tries) {
                    self.tail.fetch_or(Self::CLOSED_MASK, Ordering::Release);
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

    pub fn dequeue(&self) -> Option<T> {
        loop {
            let h = self.head.fetch_add(1, Ordering::Acquire);
            let slot = &self.array[h % self.array.len()];
            let node = Node::<T>::from(slot.load(Ordering::Acquire));
            loop {
                if node.get_index() > h as u64 {
                    break;
                }
                if !node.ptr.is_null() {
                    if node.get_index() == h as u64 {
                        let new_node = Node::<T>::new((h + self.array.len()) as u64, node.is_safe(), ptr::null_mut());
                        match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::Release, Ordering::Relaxed) {
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
                    let new_node = Node::<T>::new((h + self.array.len()) as u64, node.is_safe(), ptr::null_mut());
                    match slot.compare_exchange(u128::from(node), u128::from(new_node), Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(_) => {},
                    }
                }
            }
            let t = self.tail.load(Ordering::Acquire);
            let t = t & !Self::CLOSED_MASK;
            if t <= h + 1 {
                self.fix_state();
                return None;
            }
        }
    }
}

impl<T> Drop for CRQ<T> {
    fn drop(&mut self) {
        let mut h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire) & !Self::CLOSED_MASK;
        while h < t {
            let node = Node::<T>::from(self.array[h % self.array.len()].load(Ordering::Acquire));
            unsafe {
                _ = Box::from_raw(node.ptr);
            }
            h += 1;
        }
    }
}

pub struct LCRQHandle {
    thread_id: usize,
}

pub struct LCRQ<T> {
    head: CachePadded<AtomicPtr<CRQ<T>>>,
    tail: CachePadded<AtomicPtr<CRQ<T>>>,
    hazard: Pointers<CRQ<T>, BoxMemory>
}

impl<T> LCRQ<T> {
    pub fn new(num_threads: usize) -> Self {
        let crq = Box::into_raw(Box::new(CRQ::new(RING_SIZE)));
        Self { 
            head: CachePadded::new(AtomicPtr::new(crq)), 
            tail: CachePadded::new(AtomicPtr::new(crq)), 
            hazard: Pointers::new(BoxMemory, num_threads, 1, num_threads * 2),
        }
    }
}

impl<T> Queue<T> for LCRQ<T> {
    type Handle = LCRQHandle;
    fn enqueue(&self, mut item: T, handle: &mut Self::Handle) -> EnqueueResult<T> {
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
            match crq.enqueue(item) {
                Ok(_) => {
                    self.hazard.clear(handle.thread_id, 0);
                    return Ok(())
                },
                Err(QueueFull(item2)) => {
                    let new_crq = Box::into_raw(Box::new(CRQ::with_elem(RING_SIZE, item2)));
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
                            let mut node = unsafe {
                                Node::<T>::from(*new_crq.array[0].as_ptr())
                            };
                            let item_ptr = node.ptr;
                            node.ptr = null_mut();
                            unsafe {
                                *new_crq.array[0].as_ptr() = u128::from(node);
                                *new_crq.head.as_ptr() = 0;
                                *new_crq.tail.as_ptr() = 0;
                            }
                            item = unsafe {
                                *Box::from_raw(item_ptr)
                            };
                        },
                    }
                }
            }
            self.hazard.clear(handle.thread_id, 0);
        }
    }

    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        loop {
            let crq_ptr = self.hazard.mark(handle.thread_id, 0, &*self.head);
            let crq = unsafe {
                &*crq_ptr
            };
            if let Some(v) = crq.dequeue() {
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

    fn register(&self, thread_id: usize) -> Self::Handle {
        Self::Handle {
            thread_id
        }
    }
}

unsafe impl<T> Send for LCRQ<T> {}
unsafe impl<T> Sync for LCRQ<T> {}
