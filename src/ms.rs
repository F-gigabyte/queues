use std::{mem::{self, MaybeUninit}, ptr::{self, NonNull}, sync::{Mutex, atomic::{AtomicPtr, AtomicUsize, Ordering}}};

use crossbeam_utils::CachePadded;
use hazard::{BoxMemory, Pointers};

use crate::queue::{EnqueueResult, HandleError, HandleResult, Queue};

#[derive(Debug)]
struct LockFreeNode<T> {
    value: CachePadded<MaybeUninit<T>>,
    next: CachePadded<AtomicPtr<LockFreeNode<T>>>,
}

#[derive(Debug)]
pub struct MSLockFree<T> {
    head: CachePadded<AtomicPtr<LockFreeNode<T>>>,
    tail: CachePadded<AtomicPtr<LockFreeNode<T>>>,
    current_thread: AtomicUsize,
    num_threads: usize,
    hazard: Pointers<LockFreeNode<T>, BoxMemory>
}

impl<T> MSLockFree<T> {
    pub fn new(num_threads: usize) -> Self {
        let node = Box::into_raw(Box::new(LockFreeNode {
            value: CachePadded::new(MaybeUninit::uninit()),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())) 
        }));
        Self { 
            head: CachePadded::new(AtomicPtr::new(node)),
            tail: CachePadded::new(AtomicPtr::new(node)),
            current_thread: AtomicUsize::new(0),
            num_threads,
            hazard: Pointers::new(BoxMemory, num_threads, 2, 2 * num_threads)
        }
    }
}

impl<T> Queue<T> for MSLockFree<T> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let node = Box::into_raw(Box::new(LockFreeNode {
            value: CachePadded::new(MaybeUninit::new(item)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
        }));
        loop {
            let tail = self.hazard.mark_ptr(handle, 0, self.tail.load(Ordering::Acquire));
            let next = unsafe {
                (*tail).next.load(Ordering::Acquire)
            };
            if tail != self.tail.load(Ordering::Acquire) {
                continue;
            }

            if !next.is_null() {
                _ = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);
                continue;
            }
            unsafe {
                if (*tail).next.compare_exchange(next, node, Ordering::Acquire, Ordering::Relaxed).is_ok() { break }
            }
        }
        Ok(())
    }

    fn dequeue(&self, handle: usize) -> Option<T> {
        let mut data;
        let mut head;
        let mut next;
        loop {
           head = self.hazard.mark_ptr(handle, 0, self.head.load(Ordering::Acquire));
           let tail = self.tail.load(Ordering::Acquire);
           next = self.hazard.mark_ptr(handle, 1, unsafe{ (*head).next.load(Ordering::Acquire)});
           if head != self.head.load(Ordering::Acquire) {
               continue;
           }
           if next.is_null() {
               self.hazard.clear(handle, 0);
               self.hazard.clear(handle, 1);
               return None
           }
           if head == tail { self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed).is_ok(); }
           data = unsafe {
               mem::replace(&mut (*(*next).value), MaybeUninit::uninit()).assume_init()
           };
           if self.head.compare_exchange(head, next, Ordering::Release, Ordering::Relaxed).is_ok() { break }
        }
        self.hazard.clear(handle, 0);
        self.hazard.clear(handle, 1);
        self.hazard.retire(handle, head);
        Some(data)
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

impl<T> Drop for MSLockFree<T> {
    fn drop(&mut self) {
        let mut head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        while head != tail {
            let next = unsafe {
                (*head).next.load(Ordering::Acquire)
            };
            unsafe {
                _ = Box::from_raw(head);
            }
            head = next;
        }
        unsafe {
            _ = Box::from_raw(tail);
        }
    }
}

unsafe impl<T> Send for MSLockFree<T> {}
unsafe impl<T> Sync for MSLockFree<T> {}

#[derive(Debug)]
pub struct Node<T> {
    value: MaybeUninit<T>,
    next: Option<NonNull<Node<T>>>,
}

#[derive(Debug)]
pub struct MSLocking<T> {
    head: CachePadded<Mutex<NonNull<Node<T>>>>,
    tail: CachePadded<Mutex<NonNull<Node<T>>>>,
}

impl<T> Default for MSLocking<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MSLocking<T> {
    pub fn new() -> Self {
        let node = NonNull::new(Box::into_raw(Box::new(Node {
            value: MaybeUninit::uninit(),
            next: None,
        }))).unwrap();
        Self { 
            head: CachePadded::new(Mutex::new(node)), 
            tail: CachePadded::new(Mutex::new(node)), 
        }
    }
}

impl<T> Queue<T> for MSLocking<T> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        let node = NonNull::new(Box::into_raw(Box::new(Node {
            value: MaybeUninit::new(item),
            next: None,
        })));
        {
            let mut tail = self.tail.lock().unwrap();
            unsafe {
                (*tail.as_ptr()).next = node;
            }
            *tail = node.unwrap();
        }
        Ok(())
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        let node;
        let data;
        {
            let mut head = self.head.lock().unwrap();
            node = head.as_ptr();
            let new_head = unsafe {
                (*node).next
            };
            if let Some(new_head) = new_head {
                data = unsafe {
                    mem::replace(&mut (*new_head.as_ptr()).value, MaybeUninit::uninit()).assume_init()
                };
                *head = new_head;
            } else {
                return None;
            }
        }
        unsafe {
            let _ = Box::from_raw(node);
        }
        Some(data)
    }

    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

impl<T> Drop for MSLocking<T> {
    fn drop(&mut self) {
        let mut head = self.head.lock().unwrap().as_ptr();
        let tail = self.tail.lock().unwrap().as_ptr();
        while head != tail {
            let next = unsafe {
                (*head).next
            };
            unsafe {
                _ = Box::from_raw(head);
            }
            head = next.map(|v| v.as_ptr()).unwrap_or(ptr::null_mut());
        }
        unsafe {
            _ = Box::from_raw(tail);
        }
    }
}

unsafe impl<T> Send for MSLocking<T> {}
unsafe impl<T> Sync for MSLocking<T> {}
