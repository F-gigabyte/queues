use std::cell::UnsafeCell;

use crossbeam_utils::CachePadded;

#[derive(Debug)]
pub struct QueueFull<T>(pub T);

pub type EnqueueResult<T> = Result<(), QueueFull<T>>;

pub trait Queue<T>
{
    type Handle;
    fn register(&self, thread_id: usize) -> Self::Handle; 
    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T>;
    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T>;
}

pub trait ThreadHandles<HANDLE> {
    fn allocate_handles(threads: usize) -> Vec<CachePadded<UnsafeCell<HANDLE>>>;
}
