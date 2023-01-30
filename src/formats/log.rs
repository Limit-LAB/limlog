use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

/*
log file format:
MAGIC_NUMBER 8 bytes
ATTRIBUTES 8 bytes
ENTRY_COUNT 8 bytes
LOGBATCHS👇

logbatch format:
MIN_TS 8 bytes
MAX_TS 8 bytes
logs👇

log format: OFFSET point to here
TS 8 bytes
ID 8 bytes
key_length 4 bytes
value_length 4 bytes
key N bytes
value N bytes

index file format:
MAGIC_NUMBER 8 bytes
indexs👇

index format:
ID 8 bytes
OFFSET 8 bytes

timestamp file format:
MAGIC_NUMBER 8 bytes
timestamps👇

timestamp format:
TS 8 bytes
OFFSET 8 bytes
*/
