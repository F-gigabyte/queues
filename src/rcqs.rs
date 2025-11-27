use std::{marker::PhantomData, sync::atomic::{AtomicU32, Ordering}};

use crossbeam_utils::CachePadded;
use linux_futex::{AsFutex, Futex, Private};
use portable_atomic::{AtomicU16, AtomicUsize};

use crate::queue::{EnqueueResult, HandleResult, Queue};

#[derive(Debug)]
struct Slot<T> {
    slot: AtomicUsize,
    waiters: AtomicU16,
    _phantom: PhantomData<T>
}

#[derive(Debug)]
pub struct RCQS<T> {
    slots: Box<[CachePadded<Slot<T>>]>,
    head: AtomicU16,
    tail: AtomicU16
}

impl<T> RCQS<T> {
    const STATE_FREE: u32 = 0;
    const STATE_OCCUPIED: u32 = 1;
    const STATE_MASK: usize = 1 << Self::STATE_SHIFT;
    const STATE_SHIFT: u32 = usize::BITS - 1;
    const MAX_SPINS: usize = 1000;
    const STATE_WORD_MASK: u32 = 1 << Self::STATE_WORD_SHIFT;
    const STATE_WORD_SHIFT: u32 = u32::BITS - 1;
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let slots = (0..len).map(|_| {
            CachePadded::new(Slot {
                slot: AtomicUsize::new((Self::STATE_FREE as usize) << Self::STATE_SHIFT),
                waiters: AtomicU16::new(0),
                _phantom: PhantomData
            })
        }).collect();
        Self {
            slots,
            head: AtomicU16::new(0),
            tail: AtomicU16::new(0)
        }
    }

    fn get_state_word(slot: &AtomicUsize) -> &AtomicU32 {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicU32::from_ptr((slot.as_ptr() as *mut u32).add(usize::BITS as usize / u32::BITS as usize))
            }
        } else {
            unsafe {
                AtomicU32::from_ptr(slot.as_ptr() as *mut u32)
            }
        }

    }

    fn wake_dequeue(s: &Slot<T>) {
        let waiters = s.waiters.load(Ordering::Acquire);
        if waiters > 0 {
            let state = Self::get_state_word(&s.slot);
            let futex: &Futex<Private> = state.as_futex();
            futex.wake(waiters as i32);
        }
    }

    fn wait_enqueue(s: &Slot<T>, state: u32) {
        for _ in 0..Self::MAX_SPINS {
            let slot_state = ((s.slot.load(Ordering::Acquire) & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            if slot_state == state {
                std::hint::spin_loop();
            } else {
                return;
            }
        }
        _ = s.waiters.fetch_add(1, Ordering::Release);
        loop {
            let slot_state = s.slot.load(Ordering::Acquire);
            let slot_state = ((slot_state & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            if slot_state != state {
                break;
            }
            let futex: &Futex<Private> = Self::get_state_word(&s.slot).as_futex();
            _ = futex.wait_bitset(state, Self::STATE_WORD_MASK);
        }
        _ = s.waiters.fetch_sub(1, Ordering::Release);
    }

}

impl<T> Queue<T> for RCQS<T> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        let item = item as usize | ((Self::STATE_OCCUPIED as usize) << Self::STATE_SHIFT);
        let loc_tail = self.tail.fetch_add(1, Ordering::Acquire) as usize % self.slots.len();
        loop {
            let slot = self.slots[loc_tail].slot.load(Ordering::Acquire);
            let loc_state = ((slot & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            if loc_state == Self::STATE_FREE {
                match self.slots[loc_tail].slot.compare_exchange(slot, item, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {
                        Self::wake_dequeue(&self.slots[loc_tail]);
                        return Ok(());
                    },
                    Err(_) => {},
                }
            }
            std::hint::spin_loop();
        }
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        let loc_head = self.head.fetch_add(1, Ordering::Acquire) as usize % self.slots.len();
        loop {
            let slot = self.slots[loc_head].slot.load(Ordering::Acquire);
            let loc_state = ((slot & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            let data = slot & !Self::STATE_MASK;
            if loc_state == Self::STATE_OCCUPIED {
                match self.slots[loc_head].slot.compare_exchange(slot, (Self::STATE_FREE as usize) << Self::STATE_SHIFT, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {
                        let data = unsafe {
                            *Box::from_raw(data as *mut T)
                        };
                        return Some(data);
                    },
                    Err(_) => {},
                }
            } else if loc_state == Self::STATE_FREE {
                Self::wait_enqueue(&self.slots[loc_head], Self::STATE_FREE);
                continue;
            }
        }
    }

    fn register(&self) -> HandleResult {
        Ok(0)
    }
}

unsafe impl<T> Send for RCQS<T> {}
unsafe impl<T> Sync for RCQS<T> {}

impl<T> Drop for RCQS<T> {
    fn drop(&mut self) {
        for slot in &self.slots {
            let slot = slot.slot.load(Ordering::Acquire);
            let state = ((slot & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            let data = slot & !Self::STATE_MASK;
            if state == Self::STATE_OCCUPIED {
                unsafe {
                    _ = Box::from_raw(data as *mut T);
                }
            }
        }
    }
}
