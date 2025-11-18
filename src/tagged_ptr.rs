use std::ptr::{self, NonNull};

pub struct TaggedPtr<T> {
   pub ptr: Option<NonNull<T>>,
   pub tag: u16,
}

impl<T> TaggedPtr<T> {
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

impl<T> From<u64> for TaggedPtr<T> {
    fn from(value: u64) -> Self {
        let tag = ((value >> 48) & 0xffff) as u16;
        let ptr = (value & 0xffffffffffff) as usize as *mut T;
        Self::from_raw(ptr, tag)
    }
}

impl<T> From<TaggedPtr<T>> for u64 {
    fn from(value: TaggedPtr<T>) -> Self {
        ((value.tag as u64) << 48) | ((value.ptr.map(|v| v.as_ptr()).unwrap_or(ptr::null_mut()) as u64) & 0xffffffffffff)
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
