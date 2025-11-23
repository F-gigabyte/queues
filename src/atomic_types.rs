#[cfg(target_pointer_width="16")]
use portable_atomic::{AtomicU32, AtomicI32};

#[cfg(target_pointer_width="32")]
use portable_atomic::{AtomicI64, AtomicU64};

#[cfg(target_pointer_width="64")]
use portable_atomic::{AtomicI128, AtomicU128};


#[cfg(target_pointer_width="16")]
pub type AtomicDIsize = AtomicI32;
#[cfg(target_pointer_width="16")]
pub type DIsize = i32;
#[cfg(target_pointer_width="16")]
pub type AtomicDUsize = AtomicU32;
#[cfg(target_pointer_width="16")]
pub type DUsize = u32;

#[cfg(target_pointer_width="32")]
pub type AtomicDIsize = AtomicI64;
#[cfg(target_pointer_width="32")]
pub type DIsize = i64;
#[cfg(target_pointer_width="32")]
pub type AtomicDUsize = AtomicU64;
#[cfg(target_pointer_width="32")]
pub type DUsize = u64;

#[cfg(target_pointer_width="64")]
pub type AtomicDIsize = AtomicI128;
#[cfg(target_pointer_width="64")]
pub type DIsize = i128;
#[cfg(target_pointer_width="64")]
pub type AtomicDUsize = AtomicU128;
#[cfg(target_pointer_width="64")]
pub type DUsize = u128;
