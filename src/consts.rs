pub const INDEX_MAGIC: &[u8; 8] = b"LIM_IDX\0";
pub const LOG_MAGIC: &[u8; 8] = b"LIM_LOG\0";

/// UUID (16) + KEY_LEN (8) + VALUE_LEN (8) + KEY (0) + VALUE (0)
pub const MIN_LOG_SIZE: usize = 32;
