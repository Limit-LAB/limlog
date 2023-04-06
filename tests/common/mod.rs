use std::path::PathBuf;

use limlog::{Result, TopicBuilder};
use tap::Pipe;
use tempfile::TempDir;
use uuid7::Uuid;

pub fn init() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "DEBUG");
    }

    tracing_subscriber::fmt::try_init().pipe(|_| {});
}

#[inline]
pub fn to_uuid(ts: u64, fill: u8) -> Uuid {
    let mut uuid = [fill; 16];
    uuid[..6].copy_from_slice(&ts.to_be_bytes()[2..8]);
    Uuid::from(uuid)
}

pub async fn write_several(n: usize) -> Result<(TempDir, PathBuf)> {
    let dir = TempDir::new()?;

    let topic = TopicBuilder::new_with_dir("test", dir.path())
        .unwrap()
        .build()
        .await
        .unwrap();

    let w = topic.writer();

    for _ in 0..n {
        w.write("hello".as_bytes()).await.unwrap();
    }

    Ok((dir, topic.config().topic_dir()))
}
