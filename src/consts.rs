use smallvec::SmallVec;

pub const INDEX_MAGIC: &[u8; 8] = b"LIM_IDX\0";
pub const LOG_MAGIC: &[u8; 8] = b"LIM_LOG\0";

/// `MAGIC_NUMBER` (8) + `ATTRIBUTES` (8)
pub const HEADER_SIZE: usize = 16;

/// `UUID` (16) + `OFFSET` (8)
pub const INDEX_SIZE: usize = 24;

/// `UUID` (16) + `KEY_LEN` (8) + `VALUE_LEN` (8) + `KEY` (0) + `VALUE` (0)
pub const MIN_LOG_SIZE: usize = 32;

/// Default size of the log file, 4GB.
pub const DEFAULT_LOG_SIZE: u64 = 1 << 32;

/// Default size of the index file, 16MB.
pub const DEFAULT_INDEX_SIZE: u64 = 1 << 24;

/// Default size of the channel, 16 items.
pub const DEFAULT_CHANNEL_SIZE: u32 = 1 << 4;

pub type SmallBytes = SmallVec<[u8; 62]>;
