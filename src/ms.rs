use std::{mem::{self, MaybeUninit}, ptr, sync::atomic::{AtomicPtr, Ordering}};

use hazard::{BoxMemory, Pointers};

use crate::queue::Queue;

struct Node<T> {
    value: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

pub struct MSLockFree<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    num_threads: usize,
}

pub struct MSLockFreeHandle<T> {
    thread_id: usize,
    hazard: Pointers<Node<T>, BoxMemory>
}

impl<T> MSLockFree<T> {
    pub fn new(num_threads: usize) -> Self {
        let node = Box::into_raw(Box::new(Node {
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
        let node = Box::into_raw(Box::new(Node {
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
                match self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {},
                    Err(_) => {},
                }
                continue;
            }
            unsafe {
                match (*tail).next.compare_exchange(next, node, Ordering::Acquire, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {},
                }
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
               match self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed) {
                   Ok(_) => {},
                   Err(_) => {},
               }
           }
           data = unsafe {
               mem::replace(&mut (*next).value, MaybeUninit::uninit()).assume_init()
           };
           match self.head.compare_exchange(head, next, Ordering::Release, Ordering::Relaxed) {
               Ok(_) => break,
               Err(_) => {},
           }
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
