#[repr(u8)]
pub enum CacheWriteStrategy {
    /// write cache first, then write to disk
    /// if write to disk failed, the cache will be lost
    /// cache will mark as dirty if write is not preformed
    /// will be better for performance
    WriteBefore,
    /// write to disk first, then write cache
    /// if write to disk failed there will be no cache
    /// will be better for data integrity
    /// but will be slower
    WriteAfter,
}


pub enum GCStrategy {
    /// compact the file after threshold
    Compaction,
    /// delete the file after threshold
    Delete,
}
struct Config {
    folder_path: String,
    cache_write_strategy: CacheWriteStrategy,
    gc_strategy: GCStrategy,
    gc_ts_threshold: Duration,
    gc_size_threshold: usize,
    gc_worker_count: usize,
}
