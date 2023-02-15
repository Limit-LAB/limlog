#![feature(once_cell)]
#![feature(trait_alias)]
#![allow(dead_code)]
#![allow(unused_imports)]

mod error;
pub mod formats;
mod gc;
mod util;

use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

pub use error::*;
use memmap2::MmapRaw;
use parking_lot::Mutex;

use crate::formats::log::Log;

#[cfg(test)]
mod tests;

const STACK_BUF_SIZE: usize = 256;

/// A wrapper for [`MmapRaw`]
pub struct Map {
    raw: MmapRaw,
}

/// Shared map for writing logs
pub struct SharedMap {
    map: Map,
}

/// Index map for write and read logs
pub struct IndexMap {
    map: Map,
}

pub struct Inner {
    logs: SharedMap,
    offset: AtomicUsize,
}

pub struct Appender {
    inner: Arc<Inner>,
    idx: IndexMap,
    recv: kanal::Receiver<Log>,
}

pub struct Selector {
    inner: Arc<Inner>,
}
