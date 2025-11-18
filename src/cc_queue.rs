use std::{cell::UnsafeCell, mem::{self, MaybeUninit}, ptr::NonNull};

use crossbeam_utils::CachePadded;
use portable_atomic::AtomicPtr;

use crate::{csynch::{CSynch, CSynchHandle}, queue::{EnqueueResult, Queue, QueueFull}};

struct Node<T> {
    data: MaybeUninit<T>,
    next: Option<NonNull<Node<T>>>,
}

type EnqueueFunc<T> = fn(&CCQueue<T>, T) -> EnqueueResult;
type DequeueFunc<T> = fn(&CCQueue<T>, ()) -> Option<T>;

pub struct CCQueueHandle<T> {
    enq: CSynchHandle<T, T, EnqueueResult, EnqueueFunc<T>>,
    deq: CSynchHandle<T, (), Option<T>, DequeueFunc<T>>,
}

pub struct CCQueue<T> {
    enq: CachePadded<CSynch<T, T, Result<(), QueueFull>, EnqueueFunc<T>>>,
    deq: CachePadded<CSynch<T, (), Option<T>, DequeueFunc<T>>>,
    head: UnsafeCell<NonNull<Node<T>>>,
    tail: UnsafeCell<NonNull<Node<T>>>,
}

impl<T> CCQueue<T> {
    pub fn new() -> Self {
        let dummy = NonNull::new(Box::into_raw(Box::new(Node {
            data: MaybeUninit::uninit(),
            next: None,
        }))).unwrap();
        Self {
            enq: CachePadded::new(CSynch::new()),
            deq: CachePadded::new(CSynch::new()),
            head: UnsafeCell::new(dummy),
            tail: UnsafeCell::new(dummy),
        }
    }

    fn serial_enqueue(self: &Self, item: T) -> EnqueueResult {
        let node = NonNull::new(Box::into_raw(Box::new(Node {
            data: MaybeUninit::new(item),
            next: None,
        }))).unwrap();
        unsafe {
            (*(*self.tail.get()).as_ptr()).next = Some(node);
            (*self.tail.get()) = node;
        }
        Ok(())
    }

    fn serial_dequeue(self: &Self, _: ()) -> Option<T> {
        let next = unsafe {
            (*(*self.head.get()).as_ptr()).next
        };
        if let Some(next) = next {
            unsafe {
                let data = mem::replace(&mut (*next.as_ptr()).data, MaybeUninit::uninit()).assume_init();
                let prev_head = (*self.head.get()).as_ptr();
                (*self.head.get()) = next;
                let _ = Box::from_raw(prev_head);
                Some(data)
            }
        } else {
            None
        }
    }
}

impl<T> Queue<T> for CCQueue<T> {
    type Handle = CCQueueHandle<T>;

    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult {
        self.enq.apply(&mut handle.enq, self, item, Self::serial_enqueue)
    }

    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        self.deq.apply(&mut handle.deq, self, (), Self::serial_dequeue)
    }

    fn register(&self, _: usize) -> Self::Handle {
        Self::Handle {
            enq: CSynchHandle::new(),
            deq: CSynchHandle::new(),
        }
    }
}

unsafe impl<T> Send for CCQueue<T> {}
unsafe impl<T> Sync for CCQueue<T> {}
