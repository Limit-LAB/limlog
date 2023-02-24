use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::PathBuf,
};

use futures::StreamExt;
use tap::{Pipe, Tap};
use tempfile::TempDir;
use tokio::signal::ctrl_c;
use tracing::info;

use crate::{
    consts::{HEADER_SIZE, INDEX_SIZE},
    formats::{Log, UuidIndex},
    util::bincode_option,
    TopicBuilder,
};

mod format;

#[tokio::test]
async fn test_run() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "DEBUG");
    }

    tracing_subscriber::fmt::try_init().pipe(|_| {});

    // let dir = TempDir::new().unwrap();
    let topic = TopicBuilder::new_with_dir("test", "/home/pop/Dev/Projects/limlog/data")
        .unwrap()
        .with_log_size(1 << 14)
        .build()
        .await
        .unwrap();

    info!("{:?}", topic.config());

    let w = topic.writer();
    let mut r = topic.reader();

    info!("{}", r.cursor());

    tokio::spawn(async move {
        loop {
            w.write("hello", "world").await.unwrap();
        }
    });

    tokio::spawn(async move { while let Some(_) = r.next().await {} });

    ctrl_c().await.unwrap();

    info!("Stop");

    topic.abort();
}

// #[tokio::test]
// async fn test_index() {
//     use bincode::Options;
//     "01867c01-af03-7c38-9e71-4423f3e08451";
//     let id = "01867c01-af03-7c38-9e71-4423f3e08451";
//     let dir = PathBuf::from("/tmp/.tmpjBpCDn/123");

//     let idx = {
//         let mut buf = Vec::with_capacity(1 << 16);
//         File::open(dir.join(id).with_extension("idx"))
//             .unwrap()
//             .pipe(BufReader::new)
//             .read_to_end(&mut buf)
//             .unwrap();
//         buf
//     };
//     let mut log = {
//         let mut buf = Vec::with_capacity(1 << 16);
//         File::open(dir.join(id).with_extension("limlog"))
//             .unwrap()
//             .pipe(BufReader::new)
//             .read_to_end(&mut buf)
//             .unwrap();
//         Cursor::new(buf).tap_mut(|c| c.set_position(HEADER_SIZE as _))
//     };

//     let opt = bincode_option();
//     let mut idx_cur = HEADER_SIZE;

//     loop {
//         if idx_cur + INDEX_SIZE >= idx.len() {
//             break;
//         }
//         let idx = UuidIndex::from_bytes(&idx[idx_cur..idx_cur +
// INDEX_SIZE].try_into().unwrap());         println!("{}", idx.uuid.encode());
//         idx_cur += INDEX_SIZE;

//         let t = match opt.deserialize_from::<_, Log>(&mut log) {
//             Ok(t) => t,
//             Err(e) => match *e {
//                 bincode::ErrorKind::Io(e) if e.kind() ==
// std::io::ErrorKind::UnexpectedEof => break,                 _ =>
// Err(e).unwrap(),             },
//         };
//         assert_eq!(t.uuid, idx.uuid);
//     }
// }

#[test]
fn test_aa() {
    println!("{:x?}", 14280u64.to_le_bytes());
}
