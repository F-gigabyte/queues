use std::{cell::UnsafeCell, mem::MaybeUninit, sync::Mutex};

use crossbeam_utils::CachePadded;

use crate::queue::{Queue, QueueFull};

pub struct LockQueueInner<T> {
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: CachePadded<usize>,
    tail: CachePadded<usize>,
    len: CachePadded<usize>,
}

impl<T> LockQueueInner<T> {
    pub fn new(len: usize) -> Self {
        let data: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..len).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self { 
            data,
            head: CachePadded::new(0), 
            tail: CachePadded::new(0), 
            len: CachePadded::new(0), 
        }
    }

    pub fn enqueue(&mut self, item: T) -> Result<(), QueueFull> {
        if *self.len < self.data.len() {
            unsafe {
                self.data[*self.tail].get().write(MaybeUninit::new(item));
            }
            *self.tail = (*self.tail + 1) % self.data.len();
            *self.len += 1;
            Ok(())
        } else {
            Err(QueueFull {})
        }
    }

    pub fn dequeue(&mut self) -> Option<T> {
        if *self.len > 0 {
            let item = unsafe {
                self.data[*self.head].get().read().assume_init()
            };
            *self.head = (*self.head + 1) % self.data.len();
            *self.len -= 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<T> Drop for LockQueueInner<T> {
    fn drop(&mut self) {
        while *self.len != 0 {
            unsafe {
                (*self.data[*self.head].get()).assume_init_drop();
            }
            *self.head = (*self.head + 1) % self.data.len();
            *self.len -= 1;
        }
    }
}

pub struct LockQueue<T> {
    inner: Mutex<LockQueueInner<T>>,
}

impl<T> LockQueue<T> {
    pub fn new(len: usize) -> Self {
        Self { inner: Mutex::new(LockQueueInner::new(len)) }
    }
}

impl<T> Queue<T> for LockQueue<T> {
    type Handle = ();
    fn enqueue(&self, item: T, _: &mut Self::Handle) -> Result<(), crate::queue::QueueFull> {
        let mut inner = self.inner.lock().unwrap();
        inner.enqueue(item)
    }

    fn dequeue(&self, _: &mut Self::Handle) -> Option<T> {
        let mut inner = self.inner.lock().unwrap();
        inner.dequeue()
    }

    fn register(&self, _: usize) -> Self::Handle {
        
    }
}

unsafe impl<T> Send for LockQueue<T> {}
unsafe impl<T> Sync for LockQueue<T> {}
