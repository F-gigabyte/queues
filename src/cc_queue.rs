use std::{cell::UnsafeCell, mem::{self, MaybeUninit}, ptr::NonNull, sync::atomic::Ordering};

use crossbeam_utils::CachePadded;
use portable_atomic::AtomicUsize;

use crate::{csynch::{CSynch, CSynchHandle}, queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull}};

#[derive(Debug)]
struct Node<T> {
    data: MaybeUninit<T>,
    next: Option<NonNull<Node<T>>>,
}

type EnqueueFunc<T> = fn(&CCQueue<T>, T) -> EnqueueResult<T>;
type DequeueFunc<T> = fn(&CCQueue<T>, ()) -> Option<T>;

#[derive(Debug)]
pub struct CCQueueHandle<T> {
    enq: CSynchHandle<T, T, EnqueueResult<T>, EnqueueFunc<T>>,
    deq: CSynchHandle<T, (), Option<T>, DequeueFunc<T>>,
}

type Handle<T>=CCQueueHandle<T>;

#[derive(Debug)]
pub struct CCQueue<T> {
    enq: CachePadded<CSynch<T, T, Result<(), QueueFull<T>>, EnqueueFunc<T>>>,
    deq: CachePadded<CSynch<T, (), Option<T>, DequeueFunc<T>>>,
    head: UnsafeCell<NonNull<Node<T>>>,
    tail: UnsafeCell<NonNull<Node<T>>>,
    current_thread: AtomicUsize,
    num_threads: usize,
    handles: Box<[CachePadded<UnsafeCell<CCQueueHandle<T>>>]>
}

impl<T> CCQueue<T> {
    pub fn new(num_threads: usize) -> Self {
        let dummy = NonNull::new(Box::into_raw(Box::new(Node {
            data: MaybeUninit::uninit(),
            next: None,
        }))).unwrap();
        let handles: Box<[CachePadded<UnsafeCell<CCQueueHandle<T>>>]> = (0..num_threads).map(|_| CachePadded::new(UnsafeCell::new(Self::create_handle()))).collect();
        Self {
            enq: CachePadded::new(CSynch::new()),
            deq: CachePadded::new(CSynch::new()),
            head: UnsafeCell::new(dummy),
            tail: UnsafeCell::new(dummy),
            handles,
            current_thread: AtomicUsize::new(0),
            num_threads,
        }
    }

    fn serial_enqueue(self: &Self, item: T) -> EnqueueResult<T> {
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

    fn create_handle() -> CCQueueHandle<T> {
        CCQueueHandle {
            enq: CSynchHandle::new(),
            deq: CSynchHandle::new(),
        }
    }
}

impl<T> Queue<T, CCQueueHandle<T>> for CCQueue<T> {
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T> {
        let handle = unsafe {
            &mut *self.handles[handle].get()
        };
        self.enq.apply(&mut handle.enq, self, item, Self::serial_enqueue)
    }

    fn dequeue(&self, handle: usize) -> Option<T> {
        let handle = unsafe {
            &mut *self.handles[handle].get()
        };
        self.deq.apply(&mut handle.deq, self, (), Self::serial_dequeue)
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

unsafe impl<T> Send for CCQueue<T> {}
unsafe impl<T> Sync for CCQueue<T> {}
