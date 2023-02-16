use std::{
    fs::File,
    path::Path,
    slice::SliceIndex,
    sync::atomic::{AtomicUsize, Ordering},
};

use event_listener::{Event, EventListener};
use fs2::FileExt;
use memmap2::{MmapOptions, MmapRaw};

use crate::error::Result;

/// A wrapper for [`MmapRaw`]
struct Map {
    raw: MmapRaw,
    file: File,
}

impl Map {
    pub(crate) fn new(path: &Path, size: u64) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        file.set_len(size)?;
        let raw = MmapOptions::new().map_raw(&file)?;
        Ok(Self { raw, file })
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: File held by self is locked and mmap is not modified until self is
        // dropped
        unsafe { std::slice::from_raw_parts(self.raw.as_ptr(), self.raw.len()) }
    }

    /// # Safety
    /// Caller must ensure that access to the slice is exclusive
    unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: File held by self is locked and mmap is not modified until self is
        // dropped
        unsafe { std::slice::from_raw_parts_mut(self.raw.as_mut_ptr(), self.raw.len()) }
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        self.raw.flush().unwrap();
        self.file.unlock().unwrap();
        self.file.set_len(self.raw.len() as u64).unwrap();
    }
}
/// Shared map for writing logs
pub struct LogsMap {
    map: Map,
    offset: AtomicUsize,
}

impl LogsMap {
    pub(crate) fn new(topic: &str, dir: &Path) -> Result<Self> {
        let mut p = dir.join(topic);
        p.set_extension("limlog");

        let map = Map::new(&p, 1 << 24)?;
        let offset = AtomicUsize::new(0); // TODO: use len in file header

        Ok(Self { map, offset })
    }

    /// Load the offset
    #[inline(always)]
    pub fn offset(&self) -> usize {
        self.offset.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn commit(&self, len: usize) -> usize {
        self.offset.fetch_add(len, Ordering::SeqCst) + len
    }

    /// Split the map into two slices, one immutable and one mutable, at the
    /// given index. The index must be less than or equal to the length of the
    /// map.
    ///
    /// # SAFETY
    ///
    /// Caller must guarantee that the mutable part is exclusive
    unsafe fn split_to(&self, at: usize) -> (&[u8], &mut [u8]) {
        debug_assert!(at <= self.map.raw.len());

        let ptr = self.map.raw.as_mut_ptr();

        (
            std::slice::from_raw_parts(ptr, at),
            std::slice::from_raw_parts_mut(ptr.add(at), self.map.raw.len() - at),
        )
    }

    // SAFETY: Caller must guarantee that this is exclusive
    unsafe fn write_half(&self) -> &mut [u8] {
        let (_, half) = self.split_to(self.offset());
        half
    }

    fn read_half(&self) -> &[u8] {
        // SAFETY: Only chunks greater than offset are written to
        let (half, _) = unsafe { self.split_to(self.offset()) };
        half
    }

    /// Index the underlying map
    unsafe fn index<I>(&self, index: I) -> &I::Output
    where
        I: SliceIndex<[u8]>,
    {
        &self.map.as_slice()[index]
    }
}

/// Index
pub struct IndexMap {
    map: Map,
}

impl IndexMap {
    pub(crate) fn new(topic: &str, dir: &Path) -> Result<Self> {
        let mut p = dir.join(topic);
        p.set_extension("idx");

        let map = Map::new(&p, 1 << 14)?;

        Ok(Self { map })
    }
}

pub struct Shared {
    logs: LogsMap,
    topic: String,
    event: Event,
}

impl Shared {
    #[inline]
    pub(crate) fn new(logs: LogsMap, topic: String) -> Self {
        Self {
            logs,
            topic,
            event: Event::new(),
        }
    }

    #[inline]
    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub(crate) fn subscribe(&self) -> EventListener {
        self.event.listen()
    }

    #[inline(always)]
    pub(crate) fn offset(&self) -> usize {
        self.logs.offset()
    }

    /// Index the underlying map
    #[inline]
    pub unsafe fn index<I>(&self, index: I) -> &I::Output
    where
        I: SliceIndex<[u8]>,
    {
        self.logs.index(index)
    }

    #[inline]
    pub(crate) unsafe fn mut_slice(&self) -> &mut [u8] {
        self.logs.write_half()
    }

    pub(crate) fn flush(&self, len: usize) -> Result<()> {
        self.logs.map.raw.flush()?;
        self.logs.commit(len);
        self.event.notify_additional(10);
        Ok(())
    }

    #[inline]
    pub(crate) fn ref_slice(&self, from: usize) -> &[u8] {
        let offset = self.offset();
        debug_assert!(from <= offset);

        // SAFETY: memory before `offset` are immutable and ready to be read
        unsafe { self.index(from..offset) }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.logs.map.raw.len() - self.offset()
    }
}

#[test]
fn test_map() {
    use bincode::Options;
    use uuid7::Uuid;

    use crate::{bincode_option, Log};

    let dir = tempfile::tempdir().unwrap();
    let map = LogsMap::new("123", dir.path()).unwrap();

    let (r, w) = unsafe { map.split_to(10) };

    assert_eq!(r.len(), 10);
    println!("{:?}", &w[..100]);

    let l = Log {
        uuid: Uuid::MAX,
        key: vec![114],
        value: vec![191],
    };

    bincode_option().serialize_into(&mut w[..], &l).unwrap();

    println!("{:?}", &w[..100]);
}
