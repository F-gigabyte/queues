use std::ptr::{self, NonNull};

#[derive(Debug)]
pub struct TaggedPtr<T> {
   pub ptr: Option<NonNull<T>>,
   pub tag: u16,
}

impl<T> TaggedPtr<T> {
    pub const PTR_BITS: u32 = usize::BITS - 16;
    const PTR_MASK: usize = (1 << Self::PTR_BITS) - 1;
    pub fn new(ptr: Option<NonNull<T>>, tag: u16) -> Self {
        Self { 
            ptr, 
            tag 
        }
    }

    pub fn from_raw(ptr: *mut T, tag: u16) -> Self {
        Self { 
            ptr: NonNull::new(ptr), 
            tag 
        }
    }
}

impl<T> From<usize> for TaggedPtr<T> {
    fn from(value: usize) -> Self {
        let tag = ((value >> Self::PTR_BITS) & u16::MAX as usize) as u16;
        let ptr = (value & Self::PTR_MASK) as *mut T;
        Self::from_raw(ptr, tag)
    }
}

impl<T> From<TaggedPtr<T>> for usize {
    fn from(value: TaggedPtr<T>) -> Self {
        ((value.tag as usize) << TaggedPtr::<T>::PTR_BITS) | ((value.ptr.map(|v| v.as_ptr()).unwrap_or(ptr::null_mut()) as usize) & TaggedPtr::<T>::PTR_MASK)
    }
}

impl<T> Clone for TaggedPtr<T> {
    fn clone(&self) -> Self {
        Self { 
            ptr: self.ptr, 
            tag: self.tag 
        }
    }
}

impl<T> Copy for TaggedPtr<T> {}
