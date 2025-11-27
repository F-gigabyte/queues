use std::{cell::UnsafeCell, mem::MaybeUninit, sync::Mutex};

use crate::{queue::{EnqueueResult, HandleResult, Queue, QueueFull}, ring_buffer::RingBuffer};

#[derive(Debug)]
pub struct LockQueueInner<T> {
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: usize,
    tail: usize,
    len: usize,
}

impl<T> LockQueueInner<T> {
    pub fn new(len: usize) -> Self {
        let data: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..len).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self { 
            data,
            head: 0, 
            tail: 0, 
            len: 0, 
        }
    }

    pub fn enqueue(&mut self, item: T) -> EnqueueResult<T> {
        if self.len < self.data.len() {
            unsafe {
                self.data[self.tail].get().write(MaybeUninit::new(item));
            }
            self.tail = (self.tail + 1) % self.data.len();
            self.len += 1;
            Ok(())
        } else {
            Err(QueueFull(item))
        }
    }

    pub fn dequeue(&mut self) -> Option<T> {
        if self.len > 0 {
            let item = unsafe {
                self.data[self.head].get().read().assume_init()
            };
            self.head = (self.head + 1) % self.data.len();
            self.len -= 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<T> Drop for LockQueueInner<T> {
    fn drop(&mut self) {
        while self.len != 0 {
            unsafe {
                (*self.data[self.head].get()).assume_init_drop();
            }
            self.head = (self.head + 1) % self.data.len();
            self.len -= 1;
        }
    }
}

#[derive(Debug)]
pub struct LockQueue<T> {
    inner: Mutex<LockQueueInner<T>>
}

impl<T> RingBuffer<T> for LockQueue<T> {
    fn new(len: usize, _: usize) -> Self {
        Self { inner: Mutex::new(LockQueueInner::new(len)) }
    }
}

impl<T> Queue<T> for LockQueue<T> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        let mut inner = self.inner.lock().unwrap();
        inner.enqueue(item)
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        let mut inner = self.inner.lock().unwrap();
        inner.dequeue()
    }

    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

unsafe impl<T> Send for LockQueue<T> {}
unsafe impl<T> Sync for LockQueue<T> {}
