use std::{
    path::Path,
    slice::SliceIndex,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bincode::Options;
use event_listener::{Event, EventListener};

use crate::{
    error::Result,
    formats::log::{Header, Log, UuidIndex},
    raw::Map,
    util::{bincode_option, BincodeOptions},
};

/// Shared map for writing logs
pub struct LogsMap {
    map: Map,
    offset: AtomicUsize,
}

impl LogsMap {
    pub(crate) fn new(name: &str, dir: &Path) -> Result<Self> {
        let map = Map::new(
            &dir.join(name).with_extension("limlog"),
            1 << 24,
            Header::LOG,
        )?;
        let offset = AtomicUsize::new(0);

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
        assert!(at <= self.map.len());

        let ptr = self.map.as_mut_ptr();

        (
            std::slice::from_raw_parts(ptr, at),
            std::slice::from_raw_parts_mut(ptr.add(at), self.map.len() - at),
        )
    }

    // SAFETY: Caller must guarantee that this is exclusive
    unsafe fn write_half(&self) -> &mut [u8] {
        let at = self.offset();
        std::slice::from_raw_parts_mut(self.map.as_mut_ptr().add(at), self.map.len() - at)
    }

    fn read_half(&self) -> &[u8] {
        let at = self.offset();
        // SAFETY: Only chunks greater than offset are written to
        unsafe { std::slice::from_raw_parts(self.map.as_ptr(), at) }
    }

    /// Index the underlying map
    unsafe fn index<I>(&self, index: I) -> &I::Output
    where
        I: SliceIndex<[u8]>,
    {
        &self.map.as_slice()[index]
    }
}

impl Drop for LogsMap {
    fn drop(&mut self) {
        self.map.close(self.offset() as _).unwrap();
    }
}

/// Index
pub struct IndexMap {
    map: Map,
    pos: usize,
}

impl IndexMap {
    pub(crate) fn new(name: &str, dir: &Path) -> Result<Self> {
        let map = Map::new(
            &dir.join(name).with_extension("idx"),
            1 << 14,
            Header::INDEX,
        )?;

        Ok(Self { map, pos: 0 })
    }

    pub(crate) fn push(&mut self, index: UuidIndex) -> Result<()> {
        if self.pos + 24 > self.map.len() {
            return Err(crate::error::ErrorType::IndexFileTooSmall);
        }
        // SAFETY: self is a mutable reference
        let slice = unsafe { &mut self.map.as_mut_slice()[self.pos..self.pos + 24] };
        index.write_to(slice.try_into().unwrap());
        self.pos += 24;
        Ok(())
    }
}

impl Drop for IndexMap {
    fn drop(&mut self) {
        self.map.close(self.pos as _).unwrap();
    }
}

pub struct Shared {
    logs: LogsMap,
    event: Event,
}

impl Shared {
    #[inline]
    pub(crate) fn new(logs: LogsMap, topic: String) -> Self {
        Self {
            logs,

            event: Event::new(),
        }
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
    pub unsafe fn mut_slice(&self) -> &mut [u8] {
        self.logs.write_half()
    }

    pub(crate) fn commit(&self, len: usize) -> Result<()> {
        self.logs.map.flush_range(self.offset(), len)?;
        self.logs.commit(len);
        self.event.notify_additional(usize::MAX);
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
        self.logs.map.len() - self.offset()
    }
}

pub(crate) struct Appender {
    inner: Arc<Shared>,
    idx: IndexMap,
    recv: kanal::AsyncReceiver<Log>,
}

impl Appender {
    pub fn new(inner: Arc<Shared>, idx: IndexMap, recv: kanal::AsyncReceiver<Log>) -> Self {
        Self { inner, idx, recv }
    }

    pub fn arc_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    pub async fn run(mut self) -> Result<()> {
        let opt: BincodeOptions = bincode_option();

        while let Ok(log) = self.recv.recv().await {
            let len = opt.serialized_size(&log)? as usize;
            if self.inner.remaining() < len {
                break;
            }
            let offset = self.inner.offset() as _;
            // SAFETY: We are the only one accessing the mutable portion of mmap
            let buf = unsafe { self.inner.mut_slice() };
            opt.serialize_into(&mut buf[..len], &log)?;
            self.inner.commit(len)?;
            // If flush failed, don't commit the index
            self.idx.push(UuidIndex {
                uuid: log.uuid,
                offset,
            })?;
        }
        Ok(())
    }

    pub fn shared(&self) -> Arc<Shared> {
        self.inner.clone()
    }
}

#[test]
fn test_map() {
    use bincode::Options;
    use uuid7::Uuid;

    use crate::Log;

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
