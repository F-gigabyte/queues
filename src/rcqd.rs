use std::{marker::PhantomData, sync::atomic::{AtomicU32, Ordering}};

use crossbeam_utils::CachePadded;
use linux_futex::{AsFutex, Futex, Private};
use portable_atomic::AtomicU16;

use crate::{atomic_types::{AtomicDUsize, DUsize}, queue::{EnqueueResult, HandleResult, Queue}};

#[derive(Debug)]
struct Slot<T> {
    slot: AtomicDUsize,
    waiters: AtomicU16,
    _phantom: PhantomData<T>
}

#[derive(Debug)]
pub struct RCQD<T> {
    slots: Box<[CachePadded<Slot<T>>]>,
    head: AtomicU16,
    tail: AtomicU16
}

impl<T> RCQD<T> {
    const STATE_FREE: u32 = 0;
    const STATE_OCCUPIED: u32 = 1;
    const STATE_MASK: DUsize = (usize::MAX as DUsize) << Self::STATE_SHIFT;
    const STATE_SHIFT: u32 = usize::BITS;
    const MAX_SPINS: usize = 1000;
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let slots = (0..len).map(|_| {
            CachePadded::new(Slot {
                slot: AtomicDUsize::new((Self::STATE_FREE as DUsize) << Self::STATE_SHIFT),
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

    fn get_state_word(slot: &AtomicDUsize) -> &AtomicU32 {
        if cfg!(target_endian = "little") {
            unsafe {
                AtomicU32::from_ptr((slot.as_ptr() as *mut u32).add(usize::BITS as usize / u32::BITS as usize))
            }
        } else {
            unsafe {
                AtomicU32::from_ptr((slot.as_ptr() as *mut u32).add(usize::BITS as usize / u32::BITS as usize - 1))
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
            _ = futex.wait(state);
        }
        _ = s.waiters.fetch_sub(1, Ordering::Release);
    }

}

impl<T> Queue<T> for RCQD<T> {
    fn enqueue(&self, item: T, _: &mut ()) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        let item = item as DUsize | ((Self::STATE_OCCUPIED as DUsize) << Self::STATE_SHIFT);
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

    fn dequeue(&self, _: &mut ()) -> Option<T> {
        let loc_head = self.head.fetch_add(1, Ordering::Acquire) as usize % self.slots.len();
        loop {
            let slot = self.slots[loc_head].slot.load(Ordering::Acquire);
            let loc_state = ((slot & Self::STATE_MASK) >> Self::STATE_SHIFT) as u32;
            let data = slot & !Self::STATE_MASK;
            if loc_state == Self::STATE_OCCUPIED {
                match self.slots[loc_head].slot.compare_exchange(slot, (Self::STATE_FREE as DUsize) << Self::STATE_SHIFT, Ordering::Release, Ordering::Relaxed) {
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

    fn register(&self) -> HandleResult<()> {
        Ok(())
        
    }
}

unsafe impl<T> Send for RCQD<T> {}
unsafe impl<T> Sync for RCQD<T> {}

impl<T> Drop for RCQD<T> {
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
