use std::{sync::Arc, thread, time::{Duration, Instant}};

use crate::{lock_queue::LockQueue, queue::Queue};

pub mod queue;
pub mod lock_queue;
pub mod scq_cas;
pub mod scq_dcas;

fn main() {
    let items = Arc::new([10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]);
    //let queue = LockQueue::new(16);
    let queue2 = Arc::new(LockQueue::new(16 * 32));

    let mut threads = Vec::new();
    let start = Instant::now();

    for i in 0..32 {
        let queue2 = Arc::clone(&queue2);
        let items = Arc::clone(&items);
        threads.push(thread::spawn(move || {
            for item in *items {
                queue2.enqueue(item, &mut ()).unwrap();
                thread::sleep(Duration::new(0, i * 1000000));
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    while let Some(item) = queue2.dequeue(&mut ()) {
        println!("Have item {item}");
    }
    let duration = start.elapsed();
    println!("Time {duration:?}")
}
