use std::time::Duration;

use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

use crate::TopicBuilder;

mod format;

#[tokio::test]
async fn test_run() {
    let dir = TempDir::new().unwrap();
    let topic = TopicBuilder::new_with_dir("123", dir.path())
        .unwrap()
        .build()
        .await
        .unwrap();

    eprintln!("{:?}", topic.config());

    let w = topic.writer();
    let mut r = topic.reader();

    r.next().await.unwrap().unwrap();

    while let Some(e) = r.next().await {
        eprintln!("{e:?}");
    }

    sleep(Duration::from_secs(1)).await;

    topic.abort();
}
