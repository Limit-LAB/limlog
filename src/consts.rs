pub const INDEX_MAGIC: &[u8; 7] = b"LIM_IDX";
pub const LOG_MAGIC: &[u8; 7] = b"LIM_LOG";

/// UUID (16) + KEY_LEN (8) + VALUE_LEN (8) + KEY (0) + VALUE (0)
pub const MIN_LOG_SIZE: usize = 32;
