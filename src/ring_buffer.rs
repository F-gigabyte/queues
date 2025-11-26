use crate::queue::Queue;

pub trait RingBuffer<T>: Queue<T> {
    fn new(len: usize) -> Self;
}
