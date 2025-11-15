use std::{cell::UnsafeCell, cmp, mem::MaybeUninit, sync::atomic::{AtomicIsize, AtomicUsize, Ordering}, usize};

use crossbeam_utils::CachePadded;

use crate::queue::{Queue, QueueFull};

struct SCQRing {
    head: CachePadded<AtomicUsize>,
    threshold: CachePadded<AtomicIsize>,
    tail: CachePadded<AtomicUsize>,
    array: CachePadded<Box<[AtomicUsize]>>,
}

unsafe impl Send for SCQRing {}
unsafe impl Sync for SCQRing {}

fn compare_signed(a: usize, b: usize, oper: cmp::Ordering) -> bool {
    let c = a as isize - b as isize;
    c.cmp(&0) == oper
}

impl SCQRing {
    pub fn new_empty(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[AtomicUsize]> = (0..n).map(|_| AtomicUsize::new(usize::MAX)).collect();
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            threshold: CachePadded::new(AtomicIsize::new(-1)), 
            tail: CachePadded::new(AtomicUsize::new(0)), 
            array: CachePadded::new(data),
        }
    }

    pub fn new_full(len: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        let data: Box<[AtomicUsize]> = (0..n).map(|_| AtomicUsize::new(usize::MAX)).collect();
        for i in 0..len {
            data[i].store(n + i % len, Ordering::Release);
        }
        Self { 
            head: CachePadded::new(AtomicUsize::new(0)), 
            threshold: CachePadded::new(AtomicIsize::new(len as isize + n as isize - 1)), 
            tail: CachePadded::new(AtomicUsize::new(len)), 
            array: CachePadded::new(data), 
        }
    }

    pub fn new_fill(len: usize, start: usize, end: usize) -> Self {
        // LEN must be a power of 2
        assert!(len & (len - 1) == 0);
        // LEN greater than 0
        assert!(len > 0);
        let n = len * 2;
        // start and end are less than n
        assert!(start < n);
        assert!(end < n);
        let array: Box<[AtomicUsize]> = (0..n).map(|_| AtomicUsize::new(usize::MAX)).collect();
        for i in 0..start {
            array[i % n].store(2 * n - 1, Ordering::Release);
        }
        for i in start..end {
            array[i % n].store(n + i, Ordering::Release);
        }
        Self { 
            head: CachePadded::new(AtomicUsize::new(start)), 
            threshold: CachePadded::new(AtomicIsize::new(len as isize + n as isize - 1)), 
            tail: CachePadded::new(AtomicUsize::new(end)), 
            array: CachePadded::new(array), 
        }
    }

    pub fn enqueue(&self, mut elem: usize) {
        let n = self.array.len();
        elem ^= n - 1;
        let half = n / 2;
        loop {
            let tail = self.tail.fetch_add(1, Ordering::Acquire);
            let tail_cycle = (tail << 1) | (2 * n - 1);
            let tail_index = tail % n;
            let entry = self.array[tail_index].load(Ordering::Acquire);

            // retry:
            'retry: loop {
                let entry_cycle = entry | (2 * n - 1);
                if compare_signed(entry_cycle, tail_cycle, cmp::Ordering::Less) && 
                    (entry == entry_cycle || 
                     ((entry == (entry_cycle ^ n)) && !compare_signed(self.head.load(Ordering::Acquire), tail, cmp::Ordering::Greater))) {
                        match self.array[tail_index].compare_exchange_weak(entry, tail_cycle ^ elem, Ordering::Acquire, Ordering::Relaxed) {
                            Ok(_) => {
                                if self.threshold.load(Ordering::Acquire) != (half as isize + n as isize - 1) {
                                    self.threshold.store(half as isize + n as isize - 1, Ordering::Release);
                                }
                                return;
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

    pub fn dequeue(&self) -> Option<usize> {
        let n = self.array.len();
        if self.threshold.load(Ordering::Acquire) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::Acquire);
            let head_cycle = (head << 1) | (2 * n - 1);
            let head_index = head % n;
            let mut attempt = 0;
            'again: loop {
                let entry = self.array[head_index].load(Ordering::Acquire);
                let mut entry_new;
                'inner: loop {
                    let entry_cycle = entry | (2 * n - 1);
                    if entry_cycle == head_cycle {
                        self.array[head_index].fetch_or(n - 1, Ordering::Release);
                        return Some(entry & (n - 1));
                    }

                    if (entry | n) != entry_cycle {
                        entry_new = entry & !n;
                        if entry == entry_new {
                            break 'inner;
                        }
                    } else {
                        attempt += 1;
                        if attempt <= 10000 {
                            continue 'again;
                        }
                        entry_new = head_cycle ^ ((!entry) & n);
                    }
                    // do while loop
                    if !compare_signed(entry_cycle, head_cycle, cmp::Ordering::Less) {
                        break 'inner;
                    }
                    if self.array[head_index].compare_exchange_weak(entry, entry_new, Ordering::Release, Ordering::Relaxed).is_ok() { break 'inner }
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
                break 'again;
            }
        }
    }
}

pub struct SCQCas<T> {
    aq: SCQRing,
    fq: SCQRing,
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> SCQCas<T> {
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let data: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..len).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        Self { 
            aq: SCQRing::new_empty(len), 
            fq: SCQRing::new_full(len), 
            data,
        }
    }
}

impl<T> Queue<T> for SCQCas<T> {
    type Handle = ();
    fn enqueue(&self, item: T, _: &mut Self::Handle) -> Result<(), QueueFull> {
        if let Some(index) = self.fq.dequeue() {
            unsafe {
                self.data[index].get().write(MaybeUninit::new(item));
            }
            self.aq.enqueue(index);
            Ok(())
        } else {
            Err(QueueFull {})
        }
    }

    fn dequeue(&self, _: &mut Self::Handle) -> Option<T> {
        if let Some(index) = self.aq.dequeue() {
            let val = unsafe {
                self.data[index].get().read().assume_init()
            };
            self.fq.enqueue(index);
            Some(val)
        } else {
            None
        }
    }

    fn register(&self, _: usize) -> Self::Handle {
        
    }
}

impl<T> Drop for SCQCas<T> {
    fn drop(&mut self) {
        while let Some(index) = self.aq.dequeue() {
            unsafe {
                self.data[index].get().read().assume_init_drop();
            }
        }
    }
}

unsafe impl<T> Send for SCQCas<T> {}
unsafe impl<T> Sync for SCQCas<T> {}
