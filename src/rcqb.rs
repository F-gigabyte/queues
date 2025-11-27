use std::{cell::UnsafeCell, mem::MaybeUninit, sync::atomic::Ordering};

use crossbeam_utils::CachePadded;
use linux_futex::{Futex, Private};
use portable_atomic::{AtomicU16, AtomicU32};

use crate::queue::{EnqueueResult, HandleResult, Queue};

#[derive(Debug)]
struct Slot<T> {
    state: Futex<Private>,
    waiters: AtomicU32,
    data: UnsafeCell<MaybeUninit<T>>
}

#[derive(Debug)]
pub struct RCQB<T> {
    slots: Box<[CachePadded<Slot<T>>]>,
    head: AtomicU16,
    tail: AtomicU16
}

impl<T> RCQB<T> {
    const STATE_FREE: u32 = 0;
    const STATE_OCCUPIED: u32 = 1;
    const STATE_ENQ_PENDING: u32 = 2;
    const STATE_DEQ_PENDING: u32 = 3;
    const MAX_SPINS: usize = 1000;
    pub fn new(len: usize) -> Self {
        let len = len.next_power_of_two();
        let slots = (0..len).map(|_| {
            CachePadded::new(Slot {
                state: Futex::new(Self::STATE_FREE),
                waiters: AtomicU32::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit())
            })
        }).collect();
        Self {
            slots,
            head: AtomicU16::new(0),
            tail: AtomicU16::new(0)
        }
    }

    fn wake_dequeue(s: &Slot<T>) {
        let waiters = s.waiters.load(Ordering::Acquire);
        if waiters > 0 {
            s.state.wake(waiters as i32);
        }
    }

    fn wait_enqueue(s: &Slot<T>, state: u32) {
        for _ in 0..Self::MAX_SPINS {
            if s.state.value.load(Ordering::Acquire) == state {
                std::hint::spin_loop();
            } else {
                return;
            }
        }
        _ = s.waiters.fetch_add(1, Ordering::Release);
        while s.state.value.load(Ordering::Acquire) == state {
            _ = s.state.wait(state);
        }
        _ = s.waiters.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Queue<T> for RCQB<T> {
    fn enqueue(&self, item: T, _: usize) -> EnqueueResult<T> {
        let loc_tail = self.tail.fetch_add(1, Ordering::Acquire) as usize % self.slots.len();
        loop {
            let loc_state = self.slots[loc_tail].state.value.load(Ordering::Acquire);
            if loc_state == Self::STATE_FREE
                && self.slots[loc_tail].state.value.compare_exchange(loc_state, Self::STATE_ENQ_PENDING, Ordering::Release, Ordering::Relaxed).is_ok() {
                    unsafe {
                        self.slots[loc_tail].data.get().write(MaybeUninit::new(item));
                    }
                    self.slots[loc_tail].state.value.store(Self::STATE_OCCUPIED, Ordering::Release);
                    Self::wake_dequeue(&self.slots[loc_tail]);
                    return Ok(())
                }
            std::hint::spin_loop();
        }
    }

    fn dequeue(&self, _: usize) -> Option<T> {
        let loc_head = self.head.fetch_add(1, Ordering::Acquire) as usize % self.slots.len();
        loop {
            let loc_state = self.slots[loc_head].state.value.load(Ordering::Acquire);
            if loc_state == Self::STATE_OCCUPIED {
                if self.slots[loc_head].state.value.compare_exchange(Self::STATE_OCCUPIED, Self::STATE_DEQ_PENDING, Ordering::Release, Ordering::Relaxed).is_ok() {
                    let data = unsafe {
                        std::mem::replace(&mut *self.slots[loc_head].data.get(), MaybeUninit::uninit()).assume_init()
                    };
                    self.slots[loc_head].state.value.store(Self::STATE_FREE, Ordering::Release);
                    return Some(data);
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

unsafe impl<T> Send for RCQB<T> {}
unsafe impl<T> Sync for RCQB<T> {}

impl<T> Drop for RCQB<T> {
    fn drop(&mut self) {
        for slot in &self.slots {
            if slot.state.value.load(Ordering::Acquire) == Self::STATE_OCCUPIED {
                unsafe {
                    (*slot.data.get()).assume_init_drop();
                }
            }
        }
    }
}
