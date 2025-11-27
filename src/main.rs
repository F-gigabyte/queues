use std::{env, sync::{atomic::{AtomicBool, Ordering}, Arc}, thread, time::Instant};


use crate::{cc_queue::CCQueue, crturn::CRTurn, fp_sp::FpSp, lcrq::LCRQ, lock_queue::LockQueue, ms::{MSLockFree, MSLocking}, nblfq::{NBLFQDCas, NBLFQTagged}, queue::Queue, rcqb::RCQB, rcqd::RCQD, rcqs::RCQS, ring_buffer::RingBuffer, scq_cas::{SCQCas, LSCQ}, wcq::WCQ, wfq::WFQ, wfq_ms::MSWaitFree};

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

#[derive(Debug)]
struct Message {
    from: usize,
    to: usize,
    data: [usize; 500]
}

fn run_queue_producer_round<QUEUE, HANDLER>(queue: &Arc<QUEUE>, num_threads: usize) -> u128
where 
    QUEUE: Queue<Message, HANDLER> + Send + Sync + 'static
{
    let mut threads = Vec::new();
    let begin_tasks = Arc::new(AtomicBool::new(false));
    for _ in 0..num_threads {
        let queue = Arc::clone(&queue);
        let begin_tasks = Arc::clone(&begin_tasks);
        threads.push(thread::spawn(move || {
            let mut handle = queue.register().unwrap();
            while !begin_tasks.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            for _ in 0..1000 {
                loop {
                    if let Some(_) = queue.dequeue(&mut handle) {
                        break;
                    }
                }
            }
        }));
    }
    let mut handle = queue.register().unwrap();
    let start = Instant::now();
    begin_tasks.store(true, Ordering::Release);
    for i in 0..num_threads * 1000 {
        queue.enqueue(Message { from: num_threads + 1, to: i, data: [i; 500] }, &mut handle).unwrap();
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let duration = start.elapsed();
    duration.as_nanos()
}

fn run_queue_producer<QUEUE, HANDLER>(queue: &Arc<QUEUE>, rounds: usize) -> Vec<(usize, Vec<u128>)> 
where
    QUEUE: Queue<Message, HANDLER> + Send + Sync + 'static
{
    let mut results = Vec::new();
    for i in 0..rounds {
        let mut round_res = Vec::new();
        let num_threads = (i + 1) * 10;
        for _ in 0..5 {
            _ = run_queue_producer_round(queue, num_threads)
        }
        println!("Round {i}");
        for _ in 0..10 {
            round_res.push(run_queue_producer_round(queue, num_threads));
        }
        results.push((i, round_res))
    }
    results
}

fn main() {
    let num_threads = 64;
    let queue = Arc::new(LockQueue::new(num_threads * 1000));
    println!("{:?}", run_queue_producer(&queue, num_threads));
}
