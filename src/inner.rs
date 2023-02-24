use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use bincode::Options;
use event_listener::{Event, EventListener};
use tracing::instrument;

use crate::{
    consts::{INDEX_SIZE, MIN_LOG_SIZE},
    error::Result,
    formats::{Header, Log, UuidIndex},
    raw::RawMap,
    util::{bincode_option, BincodeOptions},
    TopicBuilder,
};

#[derive(Debug)]
pub(crate) struct Shared {
    pub conf: TopicBuilder,
    pub event: Event,

    /// Pointer to the active map. This is rarely changed and is only changed
    /// when one map is full and a new map is created. Readers should keep a
    /// copy of the pointer to the map when created so creating new map
    /// won't interrupt existing maps. When readers found EOF, they should
    /// clone this pointer and read from the new map.
    map: ArcSwap<SharedMap>,
}

impl Shared {
    pub fn new(conf: TopicBuilder, event: Event, map: Arc<SharedMap>) -> Self {
        Self {
            conf,
            event,
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
#[derive(Debug)]
pub(crate) struct SharedMap {
    map: RawMap,
    offset: AtomicUsize,
    finished: AtomicBool,
}

impl SharedMap {
    pub fn new(dir: &Path, name: &str, size: u64) -> Result<Self> {
        let map = RawMap::new(&dir.join(name).with_extension("limlog"), size, Header::LOG)?;
        let offset = AtomicUsize::new(0);
        let finished = AtomicBool::new(false);

        Ok(Self {
            map,
            offset,
            finished,
        })
    }

    /// Load the offset with [`Ordering::Acquire`]
    #[inline(always)]
    pub fn offset(&self) -> usize {
        self.offset.load(Ordering::Acquire)
    }

    /// Load the offset with [`Ordering::Relaxed`]
    #[inline(always)]
    pub fn offset_relaxed(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }

    // SAFETY: Caller must guarantee that this is exclusive
    #[inline]
    pub unsafe fn mut_slice(&self) -> &mut [u8] {
        let at = self.offset();
        debug_assert!(at <= self.map.len());

        std::slice::from_raw_parts_mut(self.map.as_mut_ptr().add(at), self.map.len() - at)
    }

    /// Get the slice of the map from the given offset
    ///
    /// # Panic
    ///
    /// Panics if `from` is greater than the current offset
    #[inline]
    pub fn slice(&self, from: usize) -> &[u8] {
        let at = self.offset();
        let from = from.min(at);

        // SAFETY: memory before `offset` are immutable and ready to be read
        unsafe { self.map.range(from, at - from) }
    }

    pub fn commit(&self, len: usize) -> Result<()> {
        self.map.flush_range(self.offset(), len)?;
        self.offset.fetch_add(len, Ordering::AcqRel);
        Ok(())
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.map.len() - self.offset()
    }

    #[inline]
    pub fn finish(&self) -> Result<()> {
        self.finished.store(true, Ordering::Release);
        self.map.flush_sync()?;

        Ok(())
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
#[derive(Debug)]
pub(crate) struct UniqueMap {
    map: RawMap,
    pos: usize,
}

impl UniqueMap {
    pub fn new(dir: &Path, name: &str, size: u64) -> Result<Self> {
        let map = RawMap::new(&dir.join(name).with_extension("idx"), size, Header::INDEX)?;

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

#[derive(Debug)]
pub(crate) struct Appender {
    pub log: Arc<SharedMap>,
    pub idx: UniqueMap,
    pub recv: kanal::AsyncReceiver<Log>,
}

impl Appender {
    /// Run with the given [`Log`] and return the last [`Log`] if it
    /// cannot write it to log file due to file size.
    #[instrument(level = "trace")]
    pub async fn run(&mut self, mut rem: Option<Log>, event: &Event) -> Result<Option<Log>> {
        let opt: BincodeOptions = bincode_option();

        if let Some(log) = rem.take() {
            if let Some(rem) = self.write_one(&opt, log, event)? {
                return Ok(Some(rem));
            }
        }

        loop {
            let log = self.recv.recv().await?;
            if let Some(rem) = self.write_one(&opt, log, event)? {
                return Ok(Some(rem));
            }

            // If the map is full, return without any remaining log
            if self.log.remaining() < MIN_LOG_SIZE || self.idx.is_full() {
                return Ok(None);
            }
        }
    }

    fn write_one(&mut self, opt: &BincodeOptions, log: Log, event: &Event) -> Result<Option<Log>> {
        let len = opt.serialized_size(&log)? as usize;

        if self.log.remaining() < len || self.idx.is_full() {
            return Ok(Some(log));
        }

        let offset = self.log.offset() as _;

        // SAFETY: We are the only one accessing the mutable portion of mmap
        let buf = unsafe { self.log.mut_slice() };

        opt.serialize_into(&mut buf[..len], &log)?;

        // Commit map. If commit failed, leave index untouched
        self.log.commit(len)?;
        self.idx.push(UuidIndex {
            uuid: log.uuid,
            offset,
        })?;

        // Write successfully, notify all pending readers
        event.notify_additional(usize::MAX);

        Ok(None)
    }
}

#[test]
fn test_map() {
    use bincode::Options;
    use uuid7::Uuid;

    use crate::Log;

    let dir = tempfile::tempdir().unwrap();
    let map = SharedMap::new(dir.path(), "123", 100).unwrap();

    let (r, w) = unsafe { (map.slice(10), map.mut_slice()) };

    assert_eq!(r.len(), 0);
    assert_eq!(&[0; 100], &w[..100]);

    let l = Log {
        uuid: Uuid::MAX,
        key: vec![114],
        value: vec![191],
    };

    bincode_option().serialize_into(&mut w[..], &l).unwrap();

    let counter = [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0,
        0, 0, 0, 0, 114, 1, 0, 0, 0, 0, 0, 0, 0, 191,
    ];

    assert_eq!(&counter[..], &w[..counter.len()]);
}
