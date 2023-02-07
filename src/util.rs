use std::{
    fs::{self, File},
    io::{Read, Result, Seek, Write},
    path::Path,
};

use positioned_io::{ReadAt, WriteAt};
use serde::{Deserialize, Serialize};

pub trait LogItem = Clone + Serialize + for<'a> Deserialize<'a> + Default + Send + Sync + 'static;

pub trait BlockIODevice: Read + Write + ReadAt + WriteAt + Seek + Sync + Send + 'static {
    fn len(&self) -> Result<u64>;
    fn sync_data(&self) -> Result<()>;
}

impl BlockIODevice for File {
    fn len(&self) -> Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn sync_data(&self) -> Result<()> {
        self.sync_data()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct LogGroup {
    pub id: u64,
    pub ts: u64,
}

pub fn log_groups(log_dir: impl AsRef<Path>) -> Vec<LogGroup> {
    let Ok(dirs) = fs::read_dir(log_dir.as_ref()) else {
        return Vec::new();
    };

    dirs.into_iter()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();

            (path.is_file() && path.extension().unwrap_or_default().eq("limlog")).then_some(())?;

            let name = path.file_name()?.to_str()?;
            let ret = name.split_once('_').and_then(|(id, ts)| {
                Some(LogGroup {
                    id: id.parse::<u64>().ok()?,
                    ts: ts.parse::<u64>().ok()?,
                })
            })?;

            (log_dir.as_ref().join(format!("{name}.idx")).is_file()
                && log_dir.as_ref().join(format!("{name}.ts.idx")).is_file())
            .then_some(ret)
        })
        .collect()
}
