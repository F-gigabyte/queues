use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, thread, time::Instant};


use crate::{cc_queue::CCQueue, crturn::CRTurn, fp_sp::FpSp, lcrq::LCRQ, ms::{MSLockFree, MSLocking}, nblfq::{NBLFQDCas, NBLFQTagged}, queue::Queue, rcqb::RCQB, rcqd::RCQD, rcqs::RCQS, scq_cas::{LSCQ, SCQCas}, wcq::WCQ, wfq::WFQ, wfq_ms::MSWaitFree};

pub mod queue;
pub mod lock_queue;
pub mod scq_cas;
pub mod scq_dcas;
pub mod ms;
pub mod nblfq;
pub mod tagged_ptr;
pub mod csynch;
pub mod cc_queue;
pub mod lcrq;
pub mod wcq;
pub mod atomic_types;
pub mod crturn;
pub mod wfq_ms;
pub mod fp_sp;
pub mod wfq;
pub mod rcqb;
pub mod rcqs;
pub mod rcqd;
pub mod ring_buffer;

fn main() {
    let num_threads = 64;
    let items = Arc::new([10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]);
    //let queue = LockQueue::new(16);
    let queue2 = Arc::new(LSCQ::new(4096, num_threads + 1));

    let mut threads = Vec::new();
    let begin_tasks = Arc::new(AtomicBool::new(false));

    for i in 0..num_threads {
        let queue2 = Arc::clone(&queue2);
        let items = Arc::clone(&items);
        let begin_tasks = Arc::clone(&begin_tasks);
        threads.push(thread::spawn(move || {
            let mut handle = queue2.register().unwrap();
            while !begin_tasks.load(Ordering::Acquire) {}
            for item in *items {
                queue2.enqueue((i, item), &mut handle).unwrap();
            }
        }));
    }
    let start = Instant::now();
    begin_tasks.store(true, Ordering::Release);
    for thread in threads {
        thread.join().unwrap();
    }
    let mut handle = queue2.register().unwrap();
    while let Some(item) = queue2.dequeue(&mut handle) {
        println!("Have item {item:?}");
    }
    let duration = start.elapsed();
    println!("Time {duration:?}");
}
