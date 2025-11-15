use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, thread, time::Instant};


use crate::{ms::MSLockFree, queue::Queue};

pub mod queue;
pub mod lock_queue;
pub mod scq_cas;
pub mod scq_dcas;
pub mod ms;

fn main() {
    let num_threads = 32;
    let items = Arc::new([10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]);
    //let queue = LockQueue::new(16);
    let queue2 = Arc::new(MSLockFree::new(items.len() * num_threads));

    let mut threads = Vec::new();
    let begin_tasks = Arc::new(AtomicBool::new(false));

    for i in 0..num_threads {
        let queue2 = Arc::clone(&queue2);
        let items = Arc::clone(&items);
        let begin_tasks = Arc::clone(&begin_tasks);
        threads.push(thread::spawn(move || {
            let mut handle = queue2.register(i);
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
    let mut handle = queue2.register(0);
    while let Some(item) = queue2.dequeue(&mut handle) {
        println!("Have item {item:?}");
    }
    let duration = start.elapsed();
    println!("Time {duration:?}");
}
