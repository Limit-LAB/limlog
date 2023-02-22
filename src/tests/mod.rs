use std::future::IntoFuture;

use tempfile::TempDir;
use uuid7::uuid7;

use crate::{formats::log::Log, Topic};

mod log_format_test;

#[tokio::test]
async fn test_run() {
    let dir = TempDir::new().unwrap();
    let topic = Topic::new("test", dir.path()).unwrap();

    let w = topic.writer();
    let h = tokio::spawn(topic.into_future());

    for i in 0..100u32 {
        w.send
            .send(Log {
                uuid: uuid7(),
                key: vec![],
                value: i.to_be_bytes().to_vec(),
            })
            .await
            .unwrap();
    }
    h.abort();
}
