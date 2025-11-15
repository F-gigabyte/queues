use std::{cmp, marker::PhantomData, ptr, sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering}, usize};

use crossbeam_utils::CachePadded;
use portable_atomic::AtomicU128;

use crate::queue::{Queue, QueueFull};

pub struct SCQ2Handle {
    lhead: usize,
}

unsafe impl Send for SCQ2Handle {}
unsafe impl Sync for SCQ2Handle {}

struct SCQ2Ring<T> {
    head: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    tail: CachePadded<AtomicUsize>,
    array: CachePadded<Box<[AtomicU128]>>,
    _phantom: PhantomData<T>,
}

unsafe impl<T> Send for SCQ2Ring<T> {}
unsafe impl<T> Sync for SCQ2Ring<T> {}

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

impl<T> SCQ2Ring<T> {
    pub fn new_empty(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[AtomicU128]> = (0..n).map(|_| AtomicU128::new(0)).collect();
        Self { 
            head: CachePadded::new(AtomicUsize::new(n)), 
            threshold: CachePadded::new(AtomicIsize::new(-1)), 
            tail: CachePadded::new(AtomicUsize::new(n)), 
            array: CachePadded::new(data),
            _phantom: PhantomData,
        }
    }

    fn get_array_entry(array_pair: &AtomicU128) -> &AtomicU64 {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicU64::from_ptr((array_pair.as_ptr() as *mut u64).add(1))
            }
        } else {
            unsafe {
                AtomicU64::from_ptr(array_pair.as_ptr() as *mut u64)
            }
        }
    }
    
    fn get_array_pointer(array_pair: &AtomicU128) -> &AtomicU64 {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicU64::from_ptr(array_pair.as_ptr() as *mut u64)
            }
        } else {
            unsafe {
                AtomicU64::from_ptr((array_pair.as_ptr() as *mut u64).add(1))
            }
        }
    }

    fn get_entry(pair: u128) -> u64 {
        ((pair >> 64) & u64::MAX as u128) as u64
    }

    fn get_pointer(pair: u128) -> u64 {
        (pair & u64::MAX as u128) as u64
    }

    fn create_pair(entry: u64, ptr: *mut T) -> u128 {
        ((entry as u128) << 64) | (ptr as u128)
    }

    pub fn enqueue(&self, ptr: *mut T, lhead: &mut usize) -> Result<(), QueueFull> {
        let n = self.array.len();

        let tail = self.tail.load(Ordering::Acquire);
        if tail >= *lhead + n {
            *lhead = self.head.load(Ordering::Acquire);
            if tail >= *lhead + n {
                return Err(QueueFull);
            }
        }
        loop {
            let tail = self.tail.fetch_add(1, Ordering::Acquire);
            let tail_cycle = tail & !(n - 1);
            let tail_index = tail % n;
            let pair = self.array[tail_index].load(Ordering::Acquire);

            // retry:
            'retry: loop {
                let entry = Self::get_entry(pair) as usize;
                let entry_cycle = entry & !(n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && 
                    (entry == entry_cycle || 
                     ((entry == (entry_cycle | 0x2)) && self.head.load(Ordering::Acquire) <= tail)) {
                        match self.array[tail_index].compare_exchange_weak(pair, Self::create_pair((tail_cycle | 1) as u64, ptr), Ordering::Acquire, Ordering::Relaxed) {
                            Ok(_) => {
                                if self.threshold.load(Ordering::Acquire) != (2 * n as isize - 1) {
                                    self.threshold.store(2 * n as isize - 1, Ordering::Release);
                                }
                                return Ok(());
                            },
                            Err(_) => {
                                // goto retry
                                continue 'retry;
                            }
                        }
                } else {
                    // return to main loop
                    break 'retry;
                }
            }
            if tail + 1 >= *lhead + n {
                *lhead = self.head.load(Ordering::Acquire);
                if tail + 1 >= *lhead + n {
                    return Err(QueueFull{});
                }
            }
        }
    }

    fn catchup(&self, mut tail: usize, mut head: usize) {
        loop {
            match self.tail.compare_exchange_weak(tail, head, Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => {
                    head = self.head.load(Ordering::Acquire);
                    tail = self.tail.load(Ordering::Acquire);
                    if !compare_signed(tail, head, cmp::Ordering::Less) {
                        break;
                    }
                }
            }
        }
    }

    pub fn dequeue(&self) -> Option<*mut T> {
        let n = self.array.len();
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::Acquire);
            let head_cycle = head & !(n - 1);
            let head_index = head % n;
            let entry = Self::get_array_entry(&self.array[head_index]).load(Ordering::Acquire) as usize;
            let mut entry_new;
            'inner: loop {
                let entry_cycle = entry & !(n - 1);
                if entry_cycle == head_cycle {
                    let pair = self.array[head_index].fetch_and(Self::create_pair(!0x1, ptr::null_mut()), Ordering::Release);
                    let ptr = Self::get_pointer(pair);
                    return Some(ptr as *mut T);
                }
                if (entry & (!0x2)) != entry_cycle {
                    entry_new = entry | 0x2;
                    if entry == entry_new {
                        break 'inner;
                    }
                } else {
                    entry_new = head_cycle | (entry & 0x2);
                }
                // while condition
                if !compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                    break 'inner;
                }
                if Self::get_array_entry(&self.array[head_index]).compare_exchange_weak(entry as u64, entry_new as u64, Ordering::Release, Ordering::Relaxed).is_ok() { 
                    break 'inner; 
                }
            }
            let tail = self.tail.load(Ordering::Acquire);
            if !compare_signed(tail, head + 1, cmp::Ordering::Greater) {
                self.catchup(tail, head + 1);
                self.threshold.fetch_sub(1, Ordering::Release);
                return None;
            }
            if self.threshold.fetch_sub(1, Ordering::Acquire) <= 0 {
                return None;
            }
        }
    }
}

pub struct SCQ2Cas<T> {
    ring: SCQ2Ring<T>,
}

impl<T> SCQ2Cas<T> {
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        Self { 
            ring: SCQ2Ring::new_empty(len),
        }
    }
}

impl<T> Queue<T> for SCQ2Cas<T> {
    type Handle = SCQ2Handle;
    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> Result<(), QueueFull> {
        self.ring.enqueue(Box::into_raw(Box::new(item)), &mut handle.lhead)
    }

    fn dequeue(&self, _: &mut Self::Handle) -> Option<T> {
        self.ring.dequeue().map(|item| unsafe {
                *Box::from_raw(item)
            })
    }

    fn register(&self, _: usize) -> Self::Handle {
        SCQ2Handle {
            lhead: self.ring.array.len(),
        }
    }
}

impl<T> Drop for SCQ2Cas<T> {
    fn drop(&mut self) {
        while let Some(item) = self.ring.dequeue() {
            unsafe {
                let _ = *Box::from_raw(item);
            }
        }
    }
}

unsafe impl<T> Send for SCQ2Cas<T> {}
unsafe impl<T> Sync for SCQ2Cas<T> {}
