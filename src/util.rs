use std::{
    fmt::Debug,
    fs,
    io::Cursor,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bincode::Options;
use bytes::Buf;
use serde::{de::DeserializeOwned};
use uuid7::Uuid;

pub(crate) trait ToTime {
    /// Retrieve the [SystemTime] of the object.
    fn to_system_time(&self) -> SystemTime;

    /// Retrieve the timestamp of the object.
    fn to_ts(&self) -> u64;
}

impl ToTime for Uuid {
    #[inline]
    fn to_system_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.to_ts())
    }

    #[inline]
    fn to_ts(&self) -> u64 {
        let mut bytes = [0; 8];
        bytes[2..].copy_from_slice(&self.as_bytes()[..6]);
        u64::from_be_bytes(bytes)
    }
}

#[inline]
pub(crate) fn to_uuid(ts: u64, fill: u8) -> Uuid {
    let mut uuid = [fill; 16];
    uuid[..6].copy_from_slice(&ts.to_be_bytes()[2..8]);
    Uuid::from(uuid)
}

// scan the log groups in the given path
pub(crate) fn log_groups(log_dir: impl AsRef<Path>) -> Vec<Uuid> {
    let Ok(dirs) = fs::read_dir(log_dir.as_ref()) else {
        return Vec::new();
    };

    dirs.into_iter()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();

            (path.is_file() && path.extension().unwrap_or_default().eq("limlog")).then_some(())?;

            let uuid = path.file_stem()?.to_str()?.parse::<Uuid>().ok()?;

            log_dir
                .as_ref()
                .join(format!("{uuid}.idx"))
                .is_file()
                .then_some(uuid)
        })
        .collect()
}

/// Workaround for rust resolving `BincodeOptions` to two different types
mod bincode_option_mod {
    use bincode::{DefaultOptions, Options};

    pub type BincodeOptions = impl Options + Copy;

    #[inline(always)]
    pub fn bincode_option() -> BincodeOptions {
        DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            // .reject_trailing_bytes()
            .with_limit(1 << 12)
    }
}
pub use bincode_option_mod::{bincode_option, BincodeOptions};

pub fn try_decode<T: DeserializeOwned + Debug>(
    data: &impl Buf,
) -> Result<Option<(T, u64)>, bincode::Error> {
    if data.chunk().is_empty() {
        return Ok(None);
    }

    let mut cur = Cursor::new(data.chunk());

    let res = bincode_option().deserialize_from(&mut cur);

    match res {
        Ok(val) => Ok(Some((val, cur.position() as _))),
        Err(e) => match *e {
            // Buffer is not filled (yet), not an error. Leave the cursor untouched so that
            // remaining bytes can be used in the next decode attempt.
            bincode::ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(e),
        },
    }
}
