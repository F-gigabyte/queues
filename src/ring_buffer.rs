use crate::queue::Queue;

pub trait RingBuffer<T>: Queue<T> {
    fn new(len: usize, num_threads: usize) -> Self;
    fn is_closed(&self) -> bool;
}
