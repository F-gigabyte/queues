use std::{
    cell::UnsafeCell, ptr, sync::atomic::{Ordering, compiler_fence}
};

use crossbeam_utils::CachePadded;
use portable_atomic::{AtomicIsize, AtomicPtr, AtomicUsize};
use stackalloc::stackalloc;

use crate::queue::{EnqueueResult, HandleError, HandleResult, Queue, QueueFull};

#[derive(Debug)]
struct EnqRequest<T> {
    val: AtomicPtr<T>,
    state: AtomicIsize,
}

#[derive(Debug)]
struct DeqRequest {
    id: AtomicIsize,
    state: AtomicIsize,
}

#[derive(Debug)]
struct Cell<T> {
    val: AtomicPtr<T>,
    enq: AtomicPtr<EnqRequest<T>>,
    deq: AtomicPtr<DeqRequest>,
}

#[derive(Debug)]
struct Segment<T> {
    id: isize,
    next: CachePadded<AtomicPtr<Segment<T>>>,
    cells: Box<[CachePadded<Cell<T>>]>,
}

impl<T> Segment<T> {
    pub fn new(id: isize, len: usize) -> Self {
        let cells: Box<[CachePadded<Cell<T>>]> = (0..len)
            .map(|_| {
                CachePadded::new(Cell {
                    val: AtomicPtr::new(ptr::null_mut()),
                    enq: AtomicPtr::new(ptr::null_mut()),
                    deq: AtomicPtr::new(ptr::null_mut()),
                })
            })
            .collect();
        Self {
            id,
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            cells,
        }
    }

    pub fn find_cell(
        &self,
        cell_id: isize,
        spare: &mut Option<Box<Segment<T>>>,
    ) -> (&Segment<T>, &Cell<T>) {
        let mut current = self;
        for i in current.id..cell_id / current.cells.len() as isize {
            let mut next = current.next.load(Ordering::Acquire);
            if next.is_null() {
                let mut temp = spare.take();
                if let Some(temp) = &mut temp {
                    temp.id = i + 1;
                } else {
                    temp = Some(Box::new(Segment::new(i + 1, current.cells.len())));
                }
                let temp = Box::into_raw(temp.unwrap());
                match current.next.compare_exchange(
                    next,
                    temp,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => next = temp,
                    Err(_) => {
                        *spare = Some(unsafe { Box::from_raw(temp) });
                        next = current.next.load(Ordering::Acquire);
                    }
                }
            }
            current = unsafe { &*next };
        }
        (
            current,
            &current.cells[cell_id as usize % current.cells.len()],
        )
    }
}

#[derive(Debug)]
struct Peer<REQ, T> {
    req: CachePadded<REQ>,
    peer: CachePadded<AtomicPtr<WFQState<T>>>,
}

#[derive(Debug)]
pub struct WFQState<T> {
    hazard_node_id: AtomicUsize,
    enqueue_segment: CachePadded<AtomicPtr<Segment<T>>>,
    enqueue_node_id: usize,
    dequeue_segment: CachePadded<AtomicPtr<Segment<T>>>,
    dequeue_node_id: usize,
    next: CachePadded<AtomicPtr<WFQState<T>>>,
    enq: Peer<EnqRequest<T>, T>,
    deq: Peer<DeqRequest, T>,
    spare: Option<Box<Segment<T>>>,
    enqueue_index: isize,
    delay: usize,
}

#[derive(Debug)]
pub struct WFQ<T> {
    head: CachePadded<AtomicPtr<Segment<T>>>,
    enqueue_index: CachePadded<AtomicIsize>,
    dequeue_index: CachePadded<AtomicIsize>,
    head_index: CachePadded<AtomicIsize>,
    handle_tail: CachePadded<AtomicPtr<WFQState<T>>>,
    handles: Box<[CachePadded<UnsafeCell<Box<WFQState<T>>>>]>,
    current_thread: AtomicUsize,
    num_threads: usize,
    ring_size: usize,
}

impl<T> WFQ<T> {
    const MAX_PATIENCE: usize = 10;
    const MAX_SPIN: usize = 100;
    const TOP_ITEM: *mut T = usize::MAX as *mut T;
    const TOP_DEQUEUE: *mut DeqRequest = usize::MAX as *mut DeqRequest;
    const TOP_ENQUEUE: *mut EnqRequest<T> = usize::MAX as *mut EnqRequest<T>;
    pub fn new(ring_size: usize, num_threads: usize) -> Self {
        let head_segment = Box::into_raw(Box::new(Segment::new(0, ring_size)));
        let mut handle_tail = ptr::null_mut();
        let handles = (0..num_threads).map(|_| CachePadded::new(UnsafeCell::new(Self::create_state(head_segment, &mut handle_tail, ring_size)))).collect();
        Self {
            head: CachePadded::new(AtomicPtr::new(head_segment)),
            enqueue_index: CachePadded::new(AtomicIsize::new(1)),
            dequeue_index: CachePadded::new(AtomicIsize::new(1)),
            head_index: CachePadded::new(AtomicIsize::new(0)),
            handle_tail: CachePadded::new(AtomicPtr::new(handle_tail)),
            handles: handles,
            current_thread: AtomicUsize::new(0),
            num_threads,
            ring_size,
        }
    }

    fn create_state(head: *mut Segment<T>, handle_tail: &mut *mut WFQState<T>, ring_size: usize) -> Box<WFQState<T>> {
        let mut handle = Box::new(WFQState {
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            hazard_node_id: AtomicUsize::new(usize::MAX),
            enqueue_segment: CachePadded::new(AtomicPtr::new(head)),
            enqueue_node_id: unsafe { (*head).id as usize },
            dequeue_segment: CachePadded::new(AtomicPtr::new(head)),
            dequeue_node_id: unsafe { (*head).id as usize },
            delay: 0,
            spare: Some(Box::new(Segment::new(0, ring_size))),
            enqueue_index: 0,
            enq: Peer {
                req: CachePadded::new(EnqRequest {
                    val: AtomicPtr::new(ptr::null_mut()),
                    state: AtomicIsize::new(0),
                }),
                peer: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            },
            deq: Peer {
                req: CachePadded::new(DeqRequest {
                    id: AtomicIsize::new(0),
                    state: AtomicIsize::new(-1),
                }),
                peer: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            },
        });
        if handle_tail.is_null() {
            handle.next.store(
                handle.as_ref() as *const WFQState<T> as *mut WFQState<T>,
                Ordering::Release,
            );
            *handle_tail = handle.as_mut();
            let next = handle.next.load(Ordering::Acquire);
            handle.enq.peer.store(next, Ordering::Release);
            handle.deq.peer.store(next, Ordering::Release);
            return handle;
        }
        let tail = unsafe { &**handle_tail };
        let next = tail.next.load(Ordering::Acquire);
        loop {
            handle.next.store(next, Ordering::Release);
            match tail.next.compare_exchange(
                next,
                handle.as_mut(),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => {}
            }
        }
        handle.enq.peer.store(next, Ordering::Release);
        handle.deq.peer.store(next, Ordering::Release);
        handle

    }

    fn max_garbage(&self) -> usize {
        self.num_threads * 2
    }

    fn spin(item: &AtomicPtr<T>) -> *mut T {
        let mut patience = Self::MAX_SPIN;
        let mut v = item.load(Ordering::Acquire);
        while v.is_null() && patience > 0 {
            v = item.load(Ordering::Acquire);
            std::hint::spin_loop();
            patience -= 1;
        }
        v
    }

    fn enqueue_fast(&self, item: *mut T, handle: &mut WFQState<T>, id: &mut isize) -> bool {
        let i = self.enqueue_index.fetch_add(1, Ordering::Acquire);
        let segment = unsafe { &*handle.enqueue_segment.load(Ordering::Acquire) };
        let (segment, c) = segment.find_cell(i, &mut handle.spare);
        handle.enqueue_segment.store(
            segment as *const Segment<T> as *mut Segment<T>,
            Ordering::Release,
        );
        match c
            .val
            .compare_exchange(ptr::null_mut(), item, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => true,
            Err(_) => {
                *id = i;
                false
            }
        }
    }

    fn enqueue_slow(&self, item: *mut T, handle: &mut WFQState<T>, mut id: isize) {
        let enq = &*handle.enq.req;
        enq.val.store(item, Ordering::Release);
        enq.state.store(id, Ordering::Release);
        let mut tail = unsafe { &*handle.enqueue_segment.load(Ordering::Acquire) };
        let mut i;

        loop {
            i = self.enqueue_index.fetch_add(1, Ordering::Acquire);
            let c;
            (tail, c) = tail.find_cell(i, &mut handle.spare);
            match c.enq.compare_exchange(
                ptr::null_mut(),
                enq as *const EnqRequest<T> as *mut EnqRequest<T>,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if c.val.load(Ordering::Acquire) != Self::TOP_ITEM {
                        _ = enq.state.compare_exchange(
                            id,
                            -1,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                        break;
                    }
                }
                Err(_) => {}
            }
            if enq.state.load(Ordering::Acquire) <= 0 {
                break;
            }
        }
        id = -enq.state.load(Ordering::Acquire);
        let c;
        let mut tail = unsafe { &*handle.enqueue_segment.load(Ordering::Acquire) };
        (tail, c) = tail.find_cell(id, &mut handle.spare);
        handle.enqueue_segment.store(
            tail as *const Segment<T> as *mut Segment<T>,
            Ordering::Release,
        );
        if id > i {
            let index = self.enqueue_index.load(Ordering::Acquire);
            while index <= id {
                match self.enqueue_index.compare_exchange(
                    index,
                    id + 1,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
        }
        c.val.store(item, Ordering::Release);
    }

    fn help_enqueue(&self, cell: &Cell<T>, handle: &mut WFQState<T>, i: isize) -> *mut T {
        let item = Self::spin(&cell.val);

        if item != Self::TOP_ITEM && !item.is_null() {
            return item;
        } else if item.is_null() {
            match cell.val.compare_exchange(
                item,
                Self::TOP_ITEM,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {}
                Err(_) => {
                    if item != Self::TOP_ITEM {
                        return item;
                    }
                }
            }
        }
        let mut enq = cell.enq.load(Ordering::Acquire);
        if enq.is_null() {
            let enqueue_handle = handle.enq.peer.load(Ordering::Acquire);
            let enqueue_request = unsafe { &(*enqueue_handle).enq.req };
            let id = enqueue_request.state.load(Ordering::Acquire);
            if handle.enqueue_index != 0 && handle.enqueue_index != id {
                handle.enqueue_index = 0;
                handle.enq.peer.store(
                    unsafe { (*enqueue_handle).next.load(Ordering::Acquire) },
                    Ordering::Release,
                );
            }
            if enq.is_null() {
                match cell.enq.compare_exchange(
                    enq,
                    Self::TOP_ENQUEUE,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        enq = Self::TOP_ENQUEUE;
                    }
                    Err(_) => {}
                }
            }
        }
        if enq == Self::TOP_ENQUEUE {
            return if self.enqueue_index.load(Ordering::Acquire) <= i {
                ptr::null_mut()
            } else {
                Self::TOP_ITEM
            };
        }
        let enq = unsafe { &*enq };
        let enqueue_id = enq.state.load(Ordering::Acquire);
        let enqueue_val = enq.val.load(Ordering::Acquire);
        if enqueue_id > i {
            if cell.val.load(Ordering::Acquire) == Self::TOP_ITEM
                && self.enqueue_index.load(Ordering::Acquire) <= i
            {
                return ptr::null_mut();
            }
        } else {
            if enqueue_id > 0 {
                match enq.state.compare_exchange(
                    enqueue_id,
                    -i,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let enqueue_index = self.enqueue_index.load(Ordering::Acquire);
                        loop {
                            if enqueue_index > i {
                                break;
                            }
                            match self.enqueue_index.compare_exchange(
                                enqueue_index,
                                i + 1,
                                Ordering::Release,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(_) => {}
                            }
                        }
                        cell.val.store(enqueue_val, Ordering::Release);
                    }
                    Err(_) => {}
                }
            }
        }
        cell.val.load(Ordering::Acquire)
    }

    fn help_dequeue2(&self, handle1: &mut WFQState<T>, handle2: &mut WFQState<T>) {
        let deq = &handle2.deq.req;
        let index = deq.state.load(Ordering::Acquire);
        let id = deq.id.load(Ordering::Acquire);
        if index < id {
            return;
        }

        handle1.hazard_node_id.store(
            handle2.hazard_node_id.load(Ordering::Acquire),
            Ordering::Release,
        );
        compiler_fence(Ordering::Acquire);
        let mut index = deq.state.load(Ordering::Acquire);
        let mut i = id + 1;
        let mut old = id;
        let mut new = 0;
        loop {
            let dequeue_segment = handle2.dequeue_segment.load(Ordering::Acquire);
            println!("{:?}", dequeue_segment);
            let mut h = unsafe { &*dequeue_segment };
            while index == old && new == 0 {
                let cell;
                (h, cell) = h.find_cell(i, &mut handle1.spare);
                let dequeue_index = self.dequeue_index.load(Ordering::Acquire);
                loop {
                    if dequeue_index > i {
                        break;
                    }
                    match self.dequeue_index.compare_exchange(
                        dequeue_index,
                        i + 1,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(_) => {}
                    }
                }
                let item = self.help_enqueue(cell, handle1, i);
                if item.is_null()
                    || (item != Self::TOP_ITEM && cell.deq.load(Ordering::Acquire).is_null())
                {
                    new = i;
                } else {
                    index = deq.state.load(Ordering::Acquire);
                }
                i += 1;
            }

            if new != 0 {
                match deq
                    .state
                    .compare_exchange(index, new, Ordering::Release, Ordering::Relaxed)
                {
                    Ok(_) => {
                        index = new;
                    }
                    Err(_) => {}
                }
                if index >= new {
                    new = 0;
                }
            }
            if index < 0 || deq.id.load(Ordering::Acquire) != id {
                break;
            }
            let dequeue_segment = unsafe { &*dequeue_segment };
            let cell;
            (_, cell) = dequeue_segment.find_cell(index, &mut handle1.spare);
            if cell.val.load(Ordering::Acquire) == Self::TOP_ITEM {
                _ = deq
                    .state
                    .compare_exchange(index, -index, Ordering::Release, Ordering::Relaxed);
                break;
            } else {
                match cell.deq.compare_exchange(
                    ptr::null_mut(),
                    &**deq as *const DeqRequest as *mut DeqRequest,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        _ = deq.state.compare_exchange(
                            index,
                            -index,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                        break;
                    }
                    Err(_) => {}
                }
            }
            old = index;
            if index >= i {
                i = index + 1;
            }
        }
    }

    fn help_dequeue(&self, handle: &mut WFQState<T>) {
        let deq = &handle.deq.req;
        let index = deq.state.load(Ordering::Acquire);
        let id = deq.id.load(Ordering::Acquire);
        if index < id {
            return;
        }

        let dequeue_segment = handle.dequeue_segment.load(Ordering::Acquire);
        handle.hazard_node_id.store(
            handle.hazard_node_id.load(Ordering::Acquire),
            Ordering::Release,
        );
        compiler_fence(Ordering::Acquire);
        let mut index = deq.state.load(Ordering::Acquire);
        let mut i = id + 1;
        let mut old = id;
        let mut new = 0;
        loop {
            let mut h = unsafe { &*dequeue_segment };
            while index == old && new == 0 {
                let cell;
                (h, cell) = h.find_cell(i, &mut handle.spare);
                let dequeue_index = self.dequeue_index.load(Ordering::Acquire);
                loop {
                    if dequeue_index > i {
                        break;
                    }
                    match self.dequeue_index.compare_exchange(
                        dequeue_index,
                        i + 1,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(_) => {}
                    }
                }
                let item = self.help_enqueue(cell, handle, i);
                let deq = &handle.deq.req;
                if item.is_null()
                    || (item != Self::TOP_ITEM && cell.deq.load(Ordering::Acquire).is_null())
                {
                    new = i;
                } else {
                    index = deq.state.load(Ordering::Acquire);
                }
                i += 1;
            }

            let deq = &handle.deq.req;
            if new != 0 {
                match deq
                    .state
                    .compare_exchange(index, new, Ordering::Release, Ordering::Relaxed)
                {
                    Ok(_) => {
                        index = new;
                    }
                    Err(_) => {}
                }
                if index >= new {
                    new = 0;
                }
            }
            if index < 0 || deq.id.load(Ordering::Acquire) != id {
                break;
            }
            let dequeue_segment = unsafe { &*dequeue_segment };
            let cell;
            (_, cell) = dequeue_segment.find_cell(index, &mut handle.spare);
            if cell.val.load(Ordering::Acquire) == Self::TOP_ITEM {
                _ = deq
                    .state
                    .compare_exchange(index, -index, Ordering::Release, Ordering::Relaxed);
                break;
            } else {
                match cell.deq.compare_exchange(
                    ptr::null_mut(),
                    &**deq as *const DeqRequest as *mut DeqRequest,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        _ = deq.state.compare_exchange(
                            index,
                            -index,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                        break;
                    }
                    Err(_) => {}
                }
            }
            old = index;
            if index >= i {
                i = index + 1;
            }
        }
    }

    fn dequeue_fast(&self, handle: &mut WFQState<T>, id: &mut isize) -> *mut T {
        let i = self.dequeue_index.fetch_add(1, Ordering::Acquire);
        let (dequeue_segment, cell) = unsafe {
            (*handle.dequeue_segment.load(Ordering::Acquire)).find_cell(i, &mut handle.spare)
        };
        handle.dequeue_segment.store(
            dequeue_segment as *const Segment<T> as *mut Segment<T>,
            Ordering::Release,
        );
        let item = self.help_enqueue(cell, handle, i);
        if item.is_null() {
            return ptr::null_mut();
        }
        if item != Self::TOP_ITEM {
            match cell.deq.compare_exchange(
                ptr::null_mut(),
                Self::TOP_DEQUEUE,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return item;
                }
                Err(_) => {}
            }
        }
        *id = i;
        Self::TOP_ITEM
    }

    fn dequeue_slow(&self, handle: &mut WFQState<T>, id: isize) -> *mut T {
        let deq = &handle.deq.req;
        deq.id.store(id, Ordering::Release);
        deq.state.store(id, Ordering::Release);

        self.help_dequeue(handle);
        let deq = &handle.deq.req;
        let i = -deq.state.load(Ordering::Acquire);
        let dequeue_segment = unsafe { &*handle.dequeue_segment.load(Ordering::Acquire) };
        let (dequeue_segment, cell) = dequeue_segment.find_cell(i, &mut handle.spare);
        handle.dequeue_segment.store(
            dequeue_segment as *const Segment<T> as *mut Segment<T>,
            Ordering::Release,
        );
        let item = cell.val.load(Ordering::Acquire);
        if item == Self::TOP_ITEM {
            ptr::null_mut()
        } else {
            item
        }
    }

    fn check(
        hazard_node_id: &AtomicUsize,
        mut current_ptr: *const Segment<T>,
        old: &Segment<T>,
    ) -> *const Segment<T> {
        let hazard_node_id = hazard_node_id.load(Ordering::Acquire) as isize;
        let current = unsafe { &*current_ptr };
        if hazard_node_id < current.id {
            let mut temp = old;
            while temp.id < hazard_node_id {
                temp = unsafe { &*temp.next.load(Ordering::Acquire) };
            }
            current_ptr = temp;
        }
        current_ptr
    }

    fn update(
        end_segment: &AtomicPtr<Segment<T>>,
        mut current_ptr: *const Segment<T>,
        hazard_node_id: &AtomicUsize,
        old: &Segment<T>,
    ) -> *const Segment<T> {
        let segment_ptr = end_segment.load(Ordering::Acquire);
        let segment = unsafe { &*segment_ptr };
        let current = unsafe { &*current_ptr };
        if segment.id < current.id {
            match end_segment.compare_exchange(
                segment_ptr,
                current_ptr as *mut Segment<T>,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {}
                Err(_) => {
                    if segment.id < current.id {
                        current_ptr = segment_ptr as *const Segment<T>;
                    }
                }
            }
            current_ptr = Self::check(hazard_node_id, current_ptr, old)
        }
        current_ptr
    }

    fn cleanup(&self, handle: &WFQState<T>) {
        let head_index = self.head_index.load(Ordering::Acquire);
        let mut new = unsafe { &*handle.dequeue_segment.load(Ordering::Acquire) };
        if head_index == -1 {
            return;
        }
        if new.id - head_index > self.max_garbage() as isize {
            return;
        }
        match self
            .head_index
            .compare_exchange(head_index, -1, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => {}
            Err(_) => return,
        }
        let mut old = unsafe { &mut *self.head.load(Ordering::Acquire) };
        let mut current_handle = handle;
        let mut i = 0;
        stackalloc(self.num_threads, ptr::null(), move |handles| {
            loop {
                new = unsafe { &*Self::check(&current_handle.hazard_node_id, new, old) };
                new = unsafe {
                    &*Self::update(
                        &current_handle.enqueue_segment,
                        new,
                        &current_handle.hazard_node_id,
                        old,
                    )
                };
                new = unsafe {
                    &*Self::update(
                        &current_handle.dequeue_segment,
                        new,
                        &current_handle.hazard_node_id,
                        old,
                    )
                };

                handles[i] = current_handle;
                i += 1;
                current_handle = unsafe { &*current_handle.next.load(Ordering::Acquire) };

                if new.id <= head_index
                    || current_handle as *const WFQState<T> == handle as *const WFQState<T>
                {
                    break;
                }
            }

            i += 1;
            while new.id > head_index {
                i -= 1;
                if i == 0 {
                    break;
                }
                new = unsafe { &*Self::check(&(*handles[i + 1]).hazard_node_id, new, old) };
            }

            let new_id = new.id;

            if new_id <= head_index {
                self.head_index.store(head_index, Ordering::Release);
            } else {
                self.head.store(
                    new as *const Segment<T> as *mut Segment<T>,
                    Ordering::Release,
                );
                self.head_index.store(new_id, Ordering::Release);

                while old as *const Segment<T> != new as *const Segment<T> {
                    let temp = old.next.load(Ordering::Acquire);
                    unsafe {
                        _ = Box::from_raw(old as *mut Segment<T>);
                    };
                    old = unsafe { &mut *temp };
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct WFQHandle {
    thread_id: usize
}

type Handle = WFQHandle;

impl<T> Queue<T, WFQHandle> for WFQ<T> {
    fn enqueue(&self, item: T, handle: &mut Handle) -> EnqueueResult<T> {
        let item = Box::into_raw(Box::new(item));
        let handle = unsafe {
            &mut *self.handles[handle.thread_id].get()
        };
        handle
            .hazard_node_id
            .store(handle.enqueue_node_id, Ordering::Release);
        let mut id = 0;
        let mut p = Self::MAX_PATIENCE + 1;
        while !self.enqueue_fast(item, handle, &mut id) && p > 0 {
            p -= 1;
        }
        if p == 0 {
            self.enqueue_slow(item, handle, id);
        }
        handle.enqueue_node_id =
            unsafe { (*handle.enqueue_segment.load(Ordering::Acquire)).id as usize };
        handle.hazard_node_id.store(usize::MAX, Ordering::Release);
        Ok(())
    }

    fn dequeue(&self, handle: &mut Handle) -> Option<T> {
        let handle = unsafe {
            &mut *self.handles[handle.thread_id].get()
        };
        handle
            .hazard_node_id
            .store(handle.dequeue_node_id, Ordering::Release);
        let mut p = Self::MAX_PATIENCE + 1;
        let mut item;
        let mut id = 0;
        loop {
            item = self.dequeue_fast(handle, &mut id);
            if item != Self::TOP_ITEM || p == 0 {
                break;
            }
            p -= 1;
        }
        if item == Self::TOP_ITEM {
            item = self.dequeue_slow(handle, id);
        }
        if !item.is_null() {
            self.help_dequeue2(handle, unsafe {
                &mut *handle.deq.peer.load(Ordering::Acquire)
            });
            handle.deq.peer.store(
                unsafe {
                    (*handle.deq.peer.load(Ordering::Acquire))
                        .next
                        .load(Ordering::Acquire)
                },
                Ordering::Release,
            );
        }
        handle.dequeue_node_id =
            unsafe { (*handle.dequeue_segment.load(Ordering::Acquire)).id as usize };
        handle.hazard_node_id.store(usize::MAX, Ordering::Release);
        if handle.spare.is_none() {
            self.cleanup(handle);
            handle.spare = Some(Box::new(Segment::new(0, self.ring_size)));
        }
        if item.is_null() {
            None
        } else {
            Some(unsafe { *Box::from_raw(item) })
        }
    }

    fn register(&self) -> HandleResult<Handle> {
        let thread_id = self.current_thread.fetch_add(1, Ordering::Acquire);
        if thread_id < self.num_threads {
            Ok(WFQHandle {
                thread_id
            })
        } else {
            Err(HandleError)
        }
    }
}

unsafe impl<T> Send for WFQ<T> {}
unsafe impl<T> Sync for WFQ<T> {}

impl<T> Drop for WFQ<T> {
    fn drop(&mut self) {
        while let Some(_) = self.dequeue(&mut WFQHandle {thread_id: 0}) {}
    }
}
