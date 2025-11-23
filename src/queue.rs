use std::cell::UnsafeCell;

use crossbeam_utils::CachePadded;

#[derive(Debug)]
pub struct QueueFull<T>(pub T);
#[derive(Debug, Clone, Copy)]
pub struct HandleError;

pub type EnqueueResult<T> = Result<(), QueueFull<T>>;
pub type HandleResult<HANDLE> = Result<HANDLE, HandleError>;

pub trait Queue<T>
{
    type Handle;
    fn register(&self) -> HandleResult<Self::Handle>; 
    fn enqueue(&self, item: T, handle: &mut Self::Handle) -> EnqueueResult<T>;
    fn dequeue(&self, handle: &mut Self::Handle) -> Option<T>;
}
