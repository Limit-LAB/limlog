use std::{
    path::Path,
    slice::SliceIndex,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use bincode::Options;
use event_listener::{Event, EventListener};

use crate::{
    consts::INDEX_SIZE,
    error::Result,
    formats::{Header, Log, UuidIndex},
    raw::RawMap,
    util::{bincode_option, BincodeOptions},
    TopicBuilder,
};

pub(crate) struct Shared {
    pub conf: TopicBuilder,
    event: Event,

    /// Pointer to the active map. This is rarely changed and is only changed
    /// when one map is full and a new map is created. Readers should keep a
    /// copy of the pointer to the map when created so creating new map
    /// won't interupt existing maps. When readers found EOF, they should
    /// clone this pointer and read from the new map.
    map: ArcSwap<SharedMap>,
}

impl Shared {
    pub fn new(conf: TopicBuilder, map: Arc<SharedMap>) -> Self {
        Self {
            event: Event::new(),
            conf,
            map: ArcSwap::from(map),
        }
    }

    pub fn swap_map(&self, map: Arc<SharedMap>) -> Arc<SharedMap> {
        self.map.swap(map)
    }

    pub fn map(&self) -> Arc<SharedMap> {
        self.map.load_full()
    }

    pub fn offset(&self) -> usize {
        self.map.load().offset()
    }

    pub fn subscribe(&self) -> EventListener {
        self.event.listen()
    }
}

/// Shared map for reading concurrently and writing exclusively
pub(crate) struct SharedMap {
    map: RawMap,
    offset: AtomicUsize,
    finished: AtomicBool,
}

impl SharedMap {
    pub fn new(dir: &Path, name: &str) -> Result<Self> {
        let map = RawMap::new(
            &dir.join(name).with_extension("limlog"),
            1 << 24,
            Header::LOG,
        )?;
        let offset = AtomicUsize::new(0);
        let finished = AtomicBool::new(false);

        Ok(Self {
            map,
            offset,
            finished,
        })
    }

    /// Load the offset
    #[inline(always)]
    pub fn offset(&self) -> usize {
        self.offset.load(Ordering::SeqCst)
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
    unsafe fn mut_slice(&self) -> &mut [u8] {
        let at = self.offset();
        std::slice::from_raw_parts_mut(self.map.as_mut_ptr().add(at), self.map.len() - at)
    }

    fn read_half(&self) -> &[u8] {
        let at = self.offset();
        // SAFETY: Only chunks greater than offset are written to
        unsafe { std::slice::from_raw_parts(self.map.as_ptr(), at) }
    }

    pub fn commit(&self, len: usize) -> Result<()> {
        self.map.flush_range(self.offset(), len)?;
        self.offset.fetch_add(len, Ordering::SeqCst);
        Ok(())
    }

    #[inline]
    pub fn slice(&self, from: usize) -> &[u8] {
        // SAFETY: memory before `offset` are immutable and ready to be read
        unsafe { self.map.range(from, self.offset() - from) }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.map.len() - self.offset()
    }

    #[inline]
    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        // Readers should see the file is finished after writer marks it so
        self.finished.load(Ordering::Acquire)
    }
}

impl Drop for SharedMap {
    fn drop(&mut self) {
        self.map.close(self.offset() as _).unwrap();
    }
}

/// Index map for read and write exclusively
pub(crate) struct UniqueMap {
    map: RawMap,
    pos: usize,
}

impl UniqueMap {
    pub fn new(dir: &Path, name: &str) -> Result<Self> {
        let map = RawMap::new(
            &dir.join(name).with_extension("idx"),
            1 << 14,
            Header::INDEX,
        )?;

        Ok(Self { map, pos: 0 })
    }

    /// If the index file is full. Returns true if it cannot handle one more
    /// [`UuidIndex`]
    pub fn is_full(&self) -> bool {
        self.pos + INDEX_SIZE > self.map.len()
    }

    pub fn push(&mut self, index: UuidIndex) -> Result<()> {
        debug_assert!(!self.is_full());

        // SAFETY: self is a mutable reference
        let slice = unsafe { self.map.range_mut(self.pos, INDEX_SIZE) };
        index.write_to(slice.try_into().unwrap());
        self.map.flush_range(self.pos, INDEX_SIZE)?;
        self.pos += INDEX_SIZE;
        Ok(())
    }
}

impl Drop for UniqueMap {
    fn drop(&mut self) {
        self.map.close(self.pos as _).unwrap();
    }
}

pub(crate) struct Appender {
    map: Arc<SharedMap>,
    idx: UniqueMap,
    recv: kanal::AsyncReceiver<Log>,
}

impl Appender {
    pub fn new(map: Arc<SharedMap>, idx: UniqueMap, recv: kanal::AsyncReceiver<Log>) -> Self {
        Self { map, idx, recv }
    }

    pub fn into_parts(self) -> (Arc<SharedMap>, UniqueMap, kanal::AsyncReceiver<Log>) {
        (self.map, self.idx, self.recv)
    }

    pub async fn run(&mut self) -> Result<()> {
        let opt: BincodeOptions = bincode_option();

        loop {
            let log = self.recv.recv().await?;
            let len = opt.serialized_size(&log)? as usize;

            let offset = self.map.offset() as _;
            // SAFETY: We are the only one accessing the mutable portion of mmap
            let buf = unsafe { self.map.mut_slice() };
            opt.serialize_into(&mut buf[..len], &log)?;
            self.map.commit(len)?;
            // If flush failed, don't commit the index
            self.idx.push(UuidIndex {
                uuid: log.uuid,
                offset,
            })?;
            if self.map.remaining() < len || self.idx.is_full() {
                self.map.finish();
                return Ok(());
            }
        }
    }
}

#[test]
fn test_map() {
    use bincode::Options;
    use uuid7::Uuid;

    use crate::Log;

    let dir = tempfile::tempdir().unwrap();
    let map = SharedMap::new(dir.path(), "123").unwrap();

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
