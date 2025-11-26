#[derive(Debug)]
pub struct QueueFull<T>(pub T);
#[derive(Debug, Clone, Copy)]
pub struct HandleError;

pub type EnqueueResult<T> = Result<(), QueueFull<T>>;
pub type HandleResult<HANDLE> = Result<HANDLE, HandleError>;

pub trait Queue<T, HANDLE=()>
{
    fn register(&self) -> HandleResult<HANDLE>; 
    fn enqueue(&self, item: T, handle: &mut HANDLE) -> EnqueueResult<T>;
    fn dequeue(&self, handle: &mut HANDLE) -> Option<T>;
}
