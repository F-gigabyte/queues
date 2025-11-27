#[derive(Debug)]
pub struct QueueFull<T>(pub T);
#[derive(Debug, Clone, Copy)]
pub struct HandleError;

pub type EnqueueResult<T> = Result<(), QueueFull<T>>;
pub type HandleResult = Result<usize, HandleError>;

pub trait Queue<T, HANDLE=()>
{
    fn register(&self) -> HandleResult; 
    fn enqueue(&self, item: T, handle: usize) -> EnqueueResult<T>;
    fn dequeue(&self, handle: usize) -> Option<T>;
}
