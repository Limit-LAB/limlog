use std::{
    fmt::Debug,
    fs,
    io::Cursor,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bincode::Options;
// use bytes::Buf;
use serde::de::DeserializeOwned;
use uuid7::Uuid;

pub trait ToTime {
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
pub fn uuid_now() -> Uuid {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    to_uuid(now as _, 0)
}

#[inline]
pub fn to_uuid(ts: u64, fill: u8) -> Uuid {
    let mut uuid = [fill; 16];
    uuid[..6].copy_from_slice(&ts.to_be_bytes()[2..8]);
    Uuid::from(uuid)
}

// scan the log groups in the given path
pub fn log_groups(log_dir: impl AsRef<Path>) -> Vec<Uuid> {
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

/// Try to decode from stream of bytes with bincode.
///
/// # Retrun
///
/// - If the buffer starts with a valid `T`, return `Some((T, bytes_read))`.
/// - If the buffer does not start with a valid `T`, return `None`. This means
///   either the buffer is not filled or it's corrupted
/// - If any error happened, return `Err`.
pub fn try_decode<T: DeserializeOwned + Debug>(
    data: &[u8],
) -> Result<Option<(T, u64)>, bincode::Error> {
    if data.is_empty() {
        return Ok(None);
    }

    let mut cur = Cursor::new(data);

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

pub trait SubArray {
    const LEN: usize;
    type T;

    fn sub<const L: usize, const R: usize>(&self) -> &[Self::T; R - L]
    where
        Bool<{ Self::LEN >= R }>: True,
        Bool<{ R >= L }>: True;
}

pub trait SubArrayMut: SubArray {
    fn sub_mut<const L: usize, const R: usize>(&mut self) -> &mut [Self::T; R - L]
    where
        Bool<{ Self::LEN >= R }>: True,
        Bool<{ R >= L }>: True;
}

impl<T, const LEN: usize> SubArray for [T; LEN] {
    type T = T;

    const LEN: usize = LEN;

    fn sub<const L: usize, const R: usize>(&self) -> &[T; R - L]
    where
        Bool<{ Self::LEN >= R }>: True,
        Bool<{ R >= L }>: True,
    {
        self[L..R].try_into().unwrap()
    }
}

impl<T, const LEN: usize> SubArrayMut for [T; LEN] {
    fn sub_mut<const L: usize, const R: usize>(&mut self) -> &mut [T; R - L]
    where
        Bool<{ Self::LEN >= R }>: True,
        Bool<{ R >= L }>: True,
    {
        (&mut self[L..R]).try_into().unwrap()
    }
}

pub trait True {}

pub struct Bool<const B: bool>;

impl True for Bool<true> {}

#[test]
fn test_subarray() {
    let a = [1, 2, 3, 4, 5];
    assert_eq!(a.sub::<0, 5>(), &a);
    assert_eq!(a.sub::<1, 3>(), &[2, 3]);

    // This will fail
    // a.sub::<0, 12>();

    // This will fail too
    // a.sub::<13, 12>();
}
