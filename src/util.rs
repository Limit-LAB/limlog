use std::{
    fs::{self, File},
    io::{Read, Result, Seek, Write},
    path::Path,
};

use positioned_io::{ReadAt, WriteAt};
use serde::{Deserialize, Serialize};

use crate::formats::log::{Index, Timestamp};

pub(crate) trait IndexItem:
    Copy + Clone + Serialize + for<'a> Deserialize<'a> + Default + Send + Sync + 'static
{
}

impl IndexItem for Index {}
impl IndexItem for Timestamp {}

pub(crate) trait BlockIODevice:
    Read + Write + ReadAt + WriteAt + Seek + Sync + Send + 'static
{
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
pub(crate) struct LogGroup {
    pub id: u64,
    pub ts: u64,
}

// scan the log groups in the given path
pub(crate) fn log_groups(log_dir: impl AsRef<Path>) -> Vec<LogGroup> {
    let Ok(dirs) = fs::read_dir(log_dir.as_ref()) else {
        return Vec::new();
    };

    dirs.into_iter()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();

            (path.is_file() && path.extension().unwrap_or_default().eq("limlog")).then_some(())?;

            let name = path.file_stem()?.to_str()?;
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
