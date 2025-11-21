use std::{marker::PhantomData, ptr, sync::atomic::{AtomicU64, Ordering}};

use crossbeam_utils::CachePadded;
use portable_atomic::AtomicU128;

use crate::{queue::{EnqueueResult, Queue, QueueFull}, tagged_ptr::TaggedPtr};

pub struct NBLFQTagged<T> {
    array: Box<[CachePadded<AtomicU64>]>,
    _phantom: PhantomData<T>,
}

pub struct NBLFQHandle {
    head: usize,
    tail: usize,
}

impl<T> NBLFQTagged<T> {
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let array: Box<[CachePadded<AtomicU64>]> = (0..len).map(|_| CachePadded::new(AtomicU64::new(0))).collect();
        Self { 
            array,
            _phantom: PhantomData
        }
    }

    fn prev(&self, i: usize) -> usize {
        (i + self.array.len() - 1) % self.array.len()
    }

    fn compare(i: usize, u: TaggedPtr<T>, j: usize, v: TaggedPtr<T>) -> bool {
        if u.tag == v.tag {
            i < j
        } else {
            v.tag.wrapping_sub(u.tag) < (u16::MAX / 2 + 1)
        }
    }
}

impl<T> Queue<T> for NBLFQTagged<T> {
    type Handle = NBLFQHandle;

    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        loop {
            let mut h = handle.head;
            let mut u: TaggedPtr<T>;
            let mut p: TaggedPtr<T>;
            loop {
                u = TaggedPtr::from(self.array[h].load(Ordering::Acquire));
                let prev = self.prev(h);
                p = TaggedPtr::from(self.array[prev].load(Ordering::Acquire));
                if p.ptr.is_some() && u.ptr.is_none() {
                    break;
                }
                if !Self::compare(prev, p, h, u) {
                    if p.ptr.is_none() && u.ptr.is_none() {
                        break;
                    }
                    else if p.ptr.is_some() && u.ptr.is_some() {
                        handle.head = h;
                        let item = unsafe {
                            *Box::from_raw(item)
                        };
                        return Err(QueueFull(item));
                    }
                }
                h = (h + 1) % self.array.len();
            }
            let mut c = p.tag;
            if p.ptr.is_none() {
                c = p.tag.wrapping_sub(1);
            }
            if h == 0 {
                c = c.wrapping_add(1);
            }
            let item = u64::from(TaggedPtr::from_raw(item, c));
            let expected = u64::from(TaggedPtr::<T>::new(None, c));
            match self.array[h].compare_exchange(expected, item, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    handle.head = (h + 1) % self.array.len();
                    return Ok(());
                },
                Err(_) => {},
            }
        }
    }
    
    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        loop {
            let mut t = handle.tail;
            let mut prev = self.prev(t);
            let mut p: TaggedPtr<T> = TaggedPtr::from(self.array[prev].load(Ordering::Acquire));
            let mut u: TaggedPtr<T> = TaggedPtr::from(self.array[t].load(Ordering::Acquire));
            while Self::compare(prev, p, t, u) {
                t = (t + 1) % self.array.len();
                prev = (prev + 1) % self.array.len();
                p = u;
                u = TaggedPtr::from(self.array[t].load(Ordering::Acquire));
            }
            if p.ptr.is_none() && u.ptr.is_none() {
                return None;
            }
            let c = u.tag.wrapping_add(1);
            let empty = u64::from(TaggedPtr::<T>::new(None, c));
            match self.array[t].compare_exchange(u64::from(u), empty, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    handle.tail = (t + 1) % self.array.len();
                    let data = unsafe {
                        Box::from_raw(u.ptr.unwrap().as_ptr())
                    };
                    return Some(*data);
                },
                Err(_) => {},
            }
        }
    }

    fn register(&self, _: usize) -> Self::Handle {
        NBLFQHandle {
            head: 0,
            tail: 0,
        }
    }
}

pub struct NBLFQDCas<T> {
    array: Box<[CachePadded<AtomicU128>]>,
    _phantom: PhantomData<T>,
}

struct QueueIndex<T> {
    counter: u64,
    ptr: *mut T,
    _phantom: PhantomData<T>,
}

impl<T> From<u128> for QueueIndex<T> {
    fn from(value: u128) -> Self {
        QueueIndex { 
            counter: ((value >> 64) & u64::MAX as u128) as u64, 
            ptr: ((value) & u64::MAX as u128) as *mut T, 
            _phantom: PhantomData,
        }
    }
}

impl<T> From<QueueIndex<T>> for u128 {
    fn from(value: QueueIndex<T>) -> Self {
        ((value.counter as u128) << 64) | value.ptr as u128
    }
}

impl<T> Clone for QueueIndex<T> {
    fn clone(&self) -> Self {
        Self { 
            counter: self.counter, 
            ptr: self.ptr, 
            _phantom: PhantomData 
        }
    }
}

impl<T> Copy for QueueIndex<T> {}

impl<T> NBLFQDCas<T> {
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let array: Box<[CachePadded<AtomicU128>]> = (0..len).map(|_| CachePadded::new(AtomicU128::new(0))).collect();
        Self { 
            array,
            _phantom: PhantomData
        }
    }

    fn prev(&self, i: usize) -> usize {
        (i + self.array.len() - 1) % self.array.len()
    }

    fn compare(i: usize, u: QueueIndex<T>, j: usize, v: QueueIndex<T>) -> bool {
        if u.counter == v.counter {
            i < j
        } else {
            v.counter.wrapping_sub(u.counter) < (u64::MAX / 2 + 1)
        }
    }
}

impl<T> Queue<T> for NBLFQDCas<T> {
    type Handle = NBLFQHandle;

    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        loop {
            let mut h = handle.head;
            let mut u: QueueIndex<T>;
            let mut p: QueueIndex<T>;
            loop {
                u = QueueIndex::from(self.array[h].load(Ordering::Acquire));
                let prev = self.prev(h);
                p = QueueIndex::from(self.array[prev].load(Ordering::Acquire));
                if !p.ptr.is_null() && u.ptr.is_null() {
                    break;
                }
                if !Self::compare(prev, p, h, u) {
                    if p.ptr.is_null() && u.ptr.is_null() {
                        break;
                    }
                    else if !p.ptr.is_null() && !u.ptr.is_null() {
                        handle.head = h;
                        let item = unsafe {
                            *Box::from_raw(item)
                        };
                        return Err(QueueFull(item));
                    }
                }
                h = (h + 1) % self.array.len();
            }
            let mut c = p.counter;
            if p.ptr.is_null() {
                c = p.counter.wrapping_sub(1);
            }
            if h == 0 {
                c = c.wrapping_add(1);
            }
            let item = u128::from(QueueIndex {
                ptr: item, 
                counter: c,
                _phantom: PhantomData,
            });
            let expected = u128::from(QueueIndex {
                ptr: ptr::null_mut::<T>(), 
                counter: c, 
                _phantom: PhantomData,
            });
            match self.array[h].compare_exchange(expected, item, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    handle.head = (h + 1) % self.array.len();
                    return Ok(());
                },
                Err(_) => {},
            }
        }
    }

    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T> {
        loop {
            let mut t = handle.tail;
            let mut prev = self.prev(t);
            let mut p: QueueIndex<T> = QueueIndex::from(self.array[prev].load(Ordering::Acquire));
            let mut u: QueueIndex<T> = QueueIndex::from(self.array[t].load(Ordering::Acquire));
            while Self::compare(prev, p, t, u) {
                t = (t + 1) % self.array.len();
                prev = (prev + 1) % self.array.len();
                p = u;
                u = QueueIndex::from(self.array[t].load(Ordering::Acquire));
            }
            if p.ptr.is_null() && u.ptr.is_null() {
                return None;
            }
            let c = u.counter.wrapping_add(1);
            let empty = u128::from(QueueIndex::<T> {
                ptr: ptr::null_mut(), 
                counter: c,
                _phantom: PhantomData
            });
            match self.array[t].compare_exchange(u128::from(u), empty, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    handle.tail = (t + 1) % self.array.len();
                    let data = unsafe {
                        Box::from_raw(u.ptr)
                    };
                    return Some(*data);
                },
                Err(_) => {},
            }
        }
    }

    fn register(&self, _: usize) -> Self::Handle {
        NBLFQHandle {
            head: 0,
            tail: 0,
        }
    }

}
