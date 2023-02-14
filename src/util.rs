use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
        (&mut bytes[2..]).copy_from_slice(&self.as_bytes()[..6]);
        u64::from_be_bytes(bytes)
    }
}

#[inline]
pub(crate) fn to_uuid(ts: u64, fill: u8) -> Uuid {
    let mut uuid = [fill; 16];
    (&mut uuid[..6]).copy_from_slice(&ts.to_be_bytes()[2..8]);
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
