use std::{mem::{self, MaybeUninit}, ptr::{self, NonNull}, sync::atomic::{AtomicBool, AtomicPtr, Ordering}};

use crossbeam_utils::CachePadded;

use crate::cc_queue::CCQueue;

enum Request<T, F, P2> {
    Uncomplete(F, P2),
    Complete(T),
}

#[derive(Debug)]
pub struct CSynchNode<P1, P2, R, F> 
    where 
        F: FnOnce(&CCQueue<P1>, P2) -> R
{
    next: CachePadded<AtomicPtr<CSynchNode<P1, P2, R, F>>>,
    request: MaybeUninit<Request<R, F, P2>>,
    wait: AtomicBool,
}

#[derive(Debug)]
pub struct CSynchHandle<P1, P2, R, F> 
        where 
            F: FnOnce(&CCQueue<P1>, P2) -> R
{
    pub next: NonNull<CSynchNode<P1, P2, R, F>>,
}

impl<P1, P2, R, F> CSynchHandle<P1, P2, R, F> 
where 
    F: FnOnce(&CCQueue<P1>, P2) -> R
{
    pub fn new() -> Self {
        let node = Box::into_raw(Box::new(CSynchNode {
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            request: MaybeUninit::uninit(),
            wait: AtomicBool::new(true),
        }));
        Self { 
            next: NonNull::new(node).unwrap()
        }
    }
}

#[derive(Debug)]
pub struct CSynch<P1, P2, R, F>
where 
    F: FnOnce(&CCQueue<P1>, P2) -> R
{
    tail: CachePadded<AtomicPtr<CSynchNode<P1, P2, R, F>>>
}

impl<P1, P2, R, F> CSynch<P1, P2, R, F>
    where 
    F: FnOnce(&CCQueue<P1>, P2) -> R
{
    pub fn new() -> Self {
        let node = Box::into_raw(Box::new(CSynchNode {
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            request: MaybeUninit::uninit(),
            wait: AtomicBool::new(false),
        }));
        Self { 
            tail: CachePadded::new(AtomicPtr::new(node)) 
        }
    }

    pub fn apply(&self, handle: &mut CSynchHandle<P1, P2, R, F>, queue: &CCQueue<P1>, param: P2, func: F) -> R {
        let next = unsafe {
            &mut *handle.next.as_ptr()
        };
        next.next.store(ptr::null_mut(), Ordering::Release);
        next.wait.store(true, Ordering::Release);
        let current = self.tail.swap(next, Ordering::Acquire);
        let current = unsafe {
            &mut *current
        };
        current.request = MaybeUninit::new(Request::Uncomplete(func, param));
        current.next.store(next, Ordering::Release);
        handle.next = NonNull::new(current).unwrap();
        let mut wait = current.wait.load(Ordering::Acquire);
        while wait {
            std::hint::spin_loop();
            wait = current.wait.load(Ordering::Acquire);
        }
        let request = unsafe {
            mem::replace(&mut current.request, MaybeUninit::uninit()).assume_init()
        };
        match request {
            Request::Complete(val) => {
                return val;
            },
            Request::Uncomplete(func, param) => {
                let _ = mem::replace(&mut current.request, MaybeUninit::new(Request::Uncomplete(func, param)));
            }
        }
        let mut temp_node = current;
        let mut temp_node_next = temp_node.next.load(Ordering::Acquire);
        let mut counter = 0;
        while !temp_node_next.is_null() && counter < 10 {
            counter += 1;
            let request = unsafe {
                mem::replace(&mut temp_node.request, MaybeUninit::uninit()).assume_init()
            };
            match request {
                Request::Uncomplete(func, param) => {
                    temp_node.request = MaybeUninit::new(Request::Complete(func(queue, param)));
                },
                Request::Complete(val) => {
                    temp_node.request = MaybeUninit::new(Request::Complete(val));
                },
            }
            temp_node.wait.store(false, Ordering::Release);
            temp_node = unsafe {
                &mut *temp_node_next
            };
            temp_node_next = temp_node.next.load(Ordering::Acquire);
        }
        temp_node.wait.store(false, Ordering::Release);
        unsafe {
            match mem::replace(&mut (*handle.next.as_ptr()).request, MaybeUninit::uninit()).assume_init() {
                Request::Complete(val) => {
                    val
                },
                Request::Uncomplete(_, _) => {
                    unreachable!();
                }
            }
        }
    }
}
