use std::{mem::{self, MaybeUninit}, ptr::{self, NonNull}, sync::{Mutex, atomic::{AtomicPtr, Ordering}}};

use hazard::{BoxMemory, Pointers};

use crate::queue::Queue;

struct LockFreeNode<T> {
    value: MaybeUninit<T>,
    next: AtomicPtr<LockFreeNode<T>>,
}

pub struct MSLockFree<T> {
    head: AtomicPtr<LockFreeNode<T>>,
    tail: AtomicPtr<LockFreeNode<T>>,
    num_threads: usize,
}

pub struct MSLockFreeHandle<T> {
    thread_id: usize,
    hazard: Pointers<LockFreeNode<T>, BoxMemory>
}

impl<T> MSLockFree<T> {
    pub fn new(num_threads: usize) -> Self {
        let node = Box::into_raw(Box::new(LockFreeNode {
            value: MaybeUninit::uninit(),
            next: AtomicPtr::new(ptr::null_mut()) 
        }));
        Self { 
            head: AtomicPtr::new(node),
            tail: AtomicPtr::new(node),
            num_threads
        }
    }
}

impl<T> Queue<T> for MSLockFree<T> {
    type Handle = MSLockFreeHandle<T>;

    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> Result<(), crate::queue::QueueFull> {
        let node = Box::into_raw(Box::new(LockFreeNode {
            value: MaybeUninit::new(item),
            next: AtomicPtr::new(ptr::null_mut()),
        }));
        loop {
            let tail = handle.hazard.mark(handle.thread_id, 0, &self.tail);
            let next = unsafe {
                (*tail).next.load(Ordering::Acquire)
            };
            if tail != self.tail.load(Ordering::Acquire) {
                continue;
            }

            if !next.is_null() {
                if let Ok(_) = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed) {}
                continue;
            }
            unsafe {
                if let Ok(_) = (*tail).next.compare_exchange(next, node, Ordering::Acquire, Ordering::Relaxed) { break }
            }
        }
        Ok(())
    }

    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        let mut data;
        let mut head;
        loop {
           head = handle.hazard.mark(handle.thread_id, 0, &self.head);
           let tail = self.tail.load(Ordering::Acquire);
           let next = handle.hazard.mark(handle.thread_id, 1, unsafe{ &(*head).next});
           if head != self.head.load(Ordering::Acquire) {
               continue;
           }
           if next.is_null() {
               return None
           }
           if head == tail {
               if let Ok(_) = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed) {}
           }
           data = unsafe {
               mem::replace(&mut (*next).value, MaybeUninit::uninit()).assume_init()
           };
           if let Ok(_) = self.head.compare_exchange(head, next, Ordering::Release, Ordering::Relaxed) { break }
        }
        handle.hazard.retire(handle.thread_id, head);
        Some(data)
    }

    fn register(&self, thread_id: usize) -> Self::Handle {
        MSLockFreeHandle {
            thread_id,
            hazard: Pointers::new(BoxMemory{}, self.num_threads, 2, 2 * self.num_threads)
        }
    }
}

pub struct Node<T> {
    value: MaybeUninit<T>,
    next: Option<NonNull<Node<T>>>,
}

pub struct MSLocking<T> {
    head: Mutex<NonNull<Node<T>>>,
    tail: Mutex<NonNull<Node<T>>>,
}

impl<T> MSLocking<T> {
    pub fn new() -> Self {
        let node = NonNull::new(Box::into_raw(Box::new(Node {
            value: MaybeUninit::uninit(),
            next: None,
        }))).unwrap();
        Self { 
            head: Mutex::new(node), 
            tail: Mutex::new(node), 
        }
    }
}

impl<T> Queue<T> for MSLocking<T> {
    type Handle = ();
    
    fn enqueue(&self, item: T, _: &mut Self::Handle) -> Result<(), crate::queue::QueueFull> {
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

    fn dequeue(&self, _: &mut Self::Handle) -> Option<T> {
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

    fn register(&self, _: usize) -> Self::Handle {
        ()
    }
}

unsafe impl<T> Send for MSLocking<T> {}
unsafe impl<T> Sync for MSLocking<T> {}
