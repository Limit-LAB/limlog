/// an LRU cache can be used to cache the log data in memory
pub struct CacheConfig {
    pub max_size: Option<u64>,
    pub max_time: Option<Duration>,
}

// 找个LRU的库