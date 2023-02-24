#![feature(cursor_remaining)]

use std::io::{Cursor, Seek, SeekFrom};

use bincode::Options;
use limlog::{
    bincode_option,
    consts::{HEADER_SIZE, INDEX_SIZE},
    formats::{Log, UuidIndex},
    Result,
};
use rand::{thread_rng, Rng};
use tap::{Pipe, Tap};
use tokio::{
    fs,
    io::{AsyncReadExt, BufReader},
};
use tracing::{info, warn};

mod_use::mod_use!(common);

#[tokio::test]
async fn test_index() {
    init();
    test_index_impl().await.unwrap();
}

async fn test_index_impl() -> Result<()> {
    let (_tmp, dir) = write_several(thread_rng().gen_range(10000..100000)).await?;

    let mut read_dir = fs::read_dir(&dir).await?;

    while let Some(dir) = read_dir.next_entry().await? {
        if !dir.file_type().await?.is_file() {
            continue;
        }

        if !dir.file_name().to_str().unwrap().ends_with(".idx") {
            continue;
        }

        let idx = {
            let mut buf = Vec::with_capacity(1 << 24);
            fs::File::open(dir.path())
                .await?
                .pipe(BufReader::new)
                .read_to_end(&mut buf)
                .await?;
            Cursor::new(buf).tap_mut(|c| c.set_position(HEADER_SIZE as _))
        };
        let log = {
            let mut buf = Vec::with_capacity(1 << 16);
            fs::File::open(dir.path().with_extension("limlog"))
                .await?
                .pipe(BufReader::new)
                .read_to_end(&mut buf)
                .await?;
            Cursor::new(buf).tap_mut(|c| c.set_position(HEADER_SIZE as _))
        };

        if !validate(idx, log)? {
            warn!(dir = ?dir.file_name(), "Invalid");
        } else {
            info!(dir = ?dir.file_name(), "Valid");
        }
    }

    Ok(())
}

fn validate(mut idx: Cursor<Vec<u8>>, mut log: Cursor<Vec<u8>>) -> Result<bool> {
    let opt = bincode_option();
    loop {
        let slice = idx.remaining_slice();
        if slice.len() < INDEX_SIZE {
            if slice.is_empty() {
                return Ok(true);
            }

            return Ok(false);
        }
        let index = UuidIndex::from_bytes(&slice[..INDEX_SIZE].try_into().unwrap());
        idx.seek(SeekFrom::Current(INDEX_SIZE as _))?;

        let log = match opt.deserialize_from::<_, Log>(&mut log) {
            Ok(t) => t,
            Err(e) => match *e {
                bincode::ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                _ => Err(e)?,
            },
        };
        assert_eq!(log.uuid, index.uuid);
    }
    Ok(false)
}
