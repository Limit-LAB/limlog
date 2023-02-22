use std::time::Duration;

use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

use crate::TopicBuilder;

mod log_format_test;

#[tokio::test]
async fn test_run() {
    let dir = TempDir::new().unwrap();
    let topic = TopicBuilder::new("123", dir.path())
        .unwrap()
        .build()
        .await
        .unwrap();

    eprintln!("{:?}", topic.config());

    let w = topic.writer();
    let mut r = topic.reader();

    for i in 0..100u32 {
        w.write(vec![], i.to_be_bytes()).await.unwrap();
    }

    while let Some(e) = r.next().await {
        eprintln!("{e:?}");
    }

    sleep(Duration::from_secs(1)).await;

    topic.abort();
}
