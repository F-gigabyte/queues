use std::{collections::HashMap, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread, time::Instant};


use crate::{cc_queue::CCQueue, crturn::CRTurn, fp_sp::FpSp, lcrq::{CRQ, LCRQ}, lock_queue::LockQueue, ms::{MSLockFree, MSLocking}, nblfq::{NBLFQDCas, NBLFQTagged}, queue::Queue, rcqb::RCQB, rcqd::RCQD, rcqs::RCQS, ring_buffer::RingBuffer, scq_cas::{LSCQ, SCQCas}, scq_dcas::{LSCQDCas, SCQDCas}, wfq::WFQ, wfq_ms::MSWaitFree};

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
    from: u64,
    to: u64,
    data: [u64; 500]
}

type GenericQueue = Box<dyn Queue<Message> + Send + Sync + 'static>;
type GenericQueueRef<'a> = &'a(dyn Queue<Message> + Send + Sync + 'static);
fn queue_from_name(name: &str, num_items: usize, ring_size: usize, num_threads: usize) -> Option<GenericQueue> {
    match name {
        "lock" => Some(Box::new(LockQueue::new(num_items, num_threads)) as GenericQueue),
        "scq_cas" => Some(Box::new(SCQCas::<Message, false>::new(num_items, num_threads)) as GenericQueue),
        "lscq_cas" => Some(Box::new(LSCQ::new(num_items, num_threads)) as GenericQueue),
        "scq_dcas" => Some(Box::new(SCQDCas::<Message, false>::new(num_items, num_threads)) as GenericQueue),
        "lscq_dcas" => Some(Box::new(LSCQDCas::new(num_items, num_threads)) as GenericQueue),
        "ms" => Some(Box::new(MSLockFree::new(num_threads)) as GenericQueue),
        "ms_lock" => Some(Box::new(MSLocking::new()) as GenericQueue),
        "nblfq_tagged" => Some(Box::new(NBLFQTagged::new(num_items, num_threads)) as GenericQueue),
        "nblfq_dcas" => Some(Box::new(NBLFQDCas::new(num_items, num_threads)) as GenericQueue),
        "cc_queue" => Some(Box::new(CCQueue::new(num_threads)) as GenericQueue),
        "lcrq" => Some(Box::new(LCRQ::<Message, CRQ<Message, true>>::new(ring_size, num_threads)) as GenericQueue),
        "wcq" => Some(Box::new(LockQueue::new(num_items, num_threads)) as GenericQueue),
        "crturn" => Some(Box::new(CRTurn::new(num_threads)) as GenericQueue),
        "wfq_ms" => Some(Box::new(MSWaitFree::new(num_threads)) as GenericQueue),
        "fp_sp" => Some(Box::new(FpSp::new(num_threads)) as GenericQueue),
        "wfq" => Some(Box::new(WFQ::new(ring_size, num_threads)) as GenericQueue),
        "rcqs" => Some(Box::new(RCQS::new(num_items)) as GenericQueue),
        "rcqb" => Some(Box::new(RCQB::new(num_items)) as GenericQueue),
        "rcqd" => Some(Box::new(RCQD::new(num_items)) as GenericQueue),
        _ => None,
    }
}

fn queue_single_producer_round(queue: &str, num_threads: usize, thread_items: usize) -> u128 {
    let num_items = num_threads * thread_items;
    let queue = Arc::new(queue_from_name(queue, num_items, num_items / 4, num_threads + 1).unwrap());
    let handle = queue.register().unwrap();
    let start_tasks = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let handle = queue.register().unwrap();
        let queue = Arc::clone(&queue);
        let start_tasks = Arc::clone(&start_tasks);
        threads.push(thread::spawn(move || {
            while !start_tasks.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            let mut items: Box<[Option<Message>]> = (0..thread_items).map(|_| None).collect();
            for i in 0..thread_items {
                loop {
                    if let Some(val) = queue.dequeue(handle) {
                        items[i as usize] = Some(val);
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }));
    }
    let start = Instant::now();
    start_tasks.store(true, Ordering::Release);
    for i in 0..num_items as u64 {
        queue.enqueue(Message { from: i, to: i, data: [i; 500] }, handle).unwrap();
    }
    for t in threads {
        t.join().unwrap();
    }
    start.elapsed().as_nanos()
}

fn run_queue_single_producer(queue: &str, start_threads: usize, thread_inc: usize, rounds: usize, thread_items: usize) -> Vec<Vec<u128>>{
    let mut res = Vec::new();
    for i in 0..rounds {
        println!("Single Producer round {i}");
        let mut round_values = Vec::new();
        let num_threads = i * thread_inc + start_threads;
        round_values.push(num_threads as u128);
        for _ in 0..5 {
            _ = queue_single_producer_round(queue, num_threads, thread_items);
        }
        for _ in 0..10 {
            round_values.push(queue_single_producer_round(queue, num_threads, thread_items));
        }
        res.push(round_values);
    }
    res
}

fn queue_single_consumer_round(queue: &str, num_threads: usize, thread_items: usize) -> u128 {
    let num_items = num_threads * thread_items;
    let queue = Arc::new(queue_from_name(queue, num_items, num_items / 4, num_threads + 1).unwrap());
    let handle = queue.register().unwrap();
    let start_tasks = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::with_capacity(num_threads);
    for t in 0..num_threads {
        let handle = queue.register().unwrap();
        let queue = Arc::clone(&queue);
        let start_tasks = Arc::clone(&start_tasks);
        threads.push(thread::spawn(move || {
            let mut items: Box<[Option<Message>]> = (0..thread_items).map(|_| None).collect();
            while !start_tasks.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            for i in 0..thread_items as u64 {
                items[i as usize] = Some(Message {from: t as u64, to: i, data: [i; 500]});
                queue.enqueue(Message { from: t as u64, to: i, data: [i; 500] }, handle).unwrap();
            }
        }));
    }
    let start = Instant::now();
    start_tasks.store(true, Ordering::Release);
    for i in 0..num_items {
        loop {
            if let Some(_) = queue.dequeue(handle) {
                break;
            }
            std::hint::spin_loop();
        }
    }
    for t in threads {
        t.join().unwrap();
    }
    start.elapsed().as_nanos()
}

fn run_queue_single_consumer(queue: &str, start_threads: usize, thread_inc: usize, rounds: usize, thread_items: usize) -> Vec<Vec<u128>>{
    let mut res = Vec::new();
    for i in 0..rounds {
        println!("Single Consumer round {i}");
        let mut round_values = Vec::new();
        let num_threads = i * thread_inc + start_threads;
        round_values.push(num_threads as u128);
        for _ in 0..5 {
            _ = queue_single_consumer_round(queue, num_threads, thread_items);
        }
        for _ in 0..10 {
            round_values.push(queue_single_consumer_round(queue, num_threads, thread_items));
        }
        res.push(round_values);
    }
    res
}

fn queue_mpmc_round(queue: &str, num_threads: usize, thread_items: usize) -> u128 {
    let num_items = num_threads * thread_items;
    let queue = Arc::new(queue_from_name(queue, num_items, num_items / 4, num_threads * 2).unwrap());
    let start_tasks = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::with_capacity(num_threads);
    for t in 0..num_threads {
        {
            let handle = queue.register().unwrap();
            let queue = Arc::clone(&queue);
            let start_tasks = Arc::clone(&start_tasks);
            threads.push(thread::spawn(move || {
                while !start_tasks.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                for i in 0..thread_items as u64 {
                    queue.enqueue(Message { from: t as u64, to: i, data: [i; 500] }, handle).unwrap();
                }
            }));
        }
        {
            let handle = queue.register().unwrap();
            let queue = Arc::clone(&queue);
            let start_tasks = Arc::clone(&start_tasks);
            threads.push(thread::spawn(move || {
                while !start_tasks.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                for _ in 0..thread_items {
                    loop {
                        if let Some(_) = queue.dequeue(handle) {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }));
        }
    }
    let start = Instant::now();
    start_tasks.store(true, Ordering::Release);
    for t in threads {
        t.join().unwrap();
    }
    start.elapsed().as_nanos()
}

fn run_queue_mpmc(queue: &str, start_threads: usize, thread_inc: usize, rounds: usize, thread_items: usize) -> Vec<Vec<u128>>{
    let mut res = Vec::new();
    for i in 0..rounds {
        println!("MPMC round {i}");
        let mut round_values = Vec::new();
        let num_threads = i * thread_inc + start_threads;
        round_values.push(num_threads as u128);
        for _ in 0..5 {
            _ = queue_mpmc_round(queue, num_threads, thread_items);
        }
        for _ in 0..10 {
            round_values.push(queue_mpmc_round(queue, num_threads, thread_items));
        }
        res.push(round_values);
    }
    res
}



fn main() {
    let queues = [
        "nblfq_tagged",
        "nblfq_dcas",
        "cc_queue",
        "scq_cas",
        "scq_dcas",
        "ms_lock",
        "ms",
        "lock",
        "lcrq",
        "wcq",
        "rcqs",
        "rcqb",
        "rcqd",
        "lscq_cas",
        "lscq_dcas",
    ];
    let mut single_producer = vec![vec!["".to_string()], vec!["".to_string()]];
    let mut single_consumer = vec![vec!["".to_string()], vec!["".to_string()]];
    let mut mpmc = vec![vec!["".to_string()], vec!["".to_string()]];
    let start_threads = 1;
    let thread_inc = 2;
    let thread_items = 1000;
    let rounds = 10;
    for queue in queues {
        println!("Running {queue}");
        let round = run_queue_single_producer(queue, start_threads, thread_inc, rounds, thread_items);
        single_producer[0].push(queue.to_string());
        for _ in 0..9 {
            single_producer[0].push("".to_string());
        }
        for i in 1..=10 {
            single_producer[1].push(i.to_string());
        }
        for i in 0..round.len() {
            if i + 2 >= single_producer.len() {
                single_producer.push(Vec::new());
                single_producer[i + 2].push(round[i][0].to_string());
            }
            single_producer[i + 2].extend(round[i][1..].iter().map(|v| v.to_string()));
        }
        let round = run_queue_single_consumer(queue, start_threads, thread_inc, rounds, thread_items);
        single_consumer[0].push(queue.to_string());
        for _ in 0..9 {
            single_consumer[0].push("".to_string());
        }
        for i in 1..=10 {
            single_consumer[1].push(i.to_string());
        }
        for i in 0..round.len() {
            if i + 2 >= single_consumer.len() {
                single_consumer.push(Vec::new());
                single_consumer[i + 2].push(round[i][0].to_string());
            }
            single_consumer[i + 2].extend(round[i][1..].iter().map(|v| v.to_string()));
        }
        let round = run_queue_mpmc(queue, start_threads, thread_inc, rounds, thread_items);
        mpmc[0].push(queue.to_string());
        for _ in 0..9 {
            mpmc[0].push("".to_string());
        }
        for i in 1..=10 {
            mpmc[1].push(i.to_string());
        }
        for i in 0..round.len() {
            if i + 2 >= mpmc.len() {
                mpmc.push(Vec::new());
                mpmc[i + 2].push(round[i][0].to_string());
            }
            mpmc[i + 2].extend(round[i][1..].iter().map(|v| v.to_string()));
        }
    }

    let mut writer = csv::Writer::from_path("single_consumer.csv").unwrap();
    for v in single_consumer {
        writer.write_record(v).unwrap();
    }
    let mut writer = csv::Writer::from_path("single_producer.csv").unwrap();
    for v in single_producer {
        writer.write_record(v).unwrap();
    }
    let mut writer = csv::Writer::from_path("mpmc.csv").unwrap();
    for v in mpmc {
        writer.write_record(v).unwrap();
    }
}
