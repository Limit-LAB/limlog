use std::pin::pin;

use futures::{future::select, StreamExt};
use limlog::TopicBuilder;
use tempfile::TempDir;
use tokio::signal::ctrl_c;
use tracing::info;

mod_use::mod_use!(common);

#[tokio::test]
async fn test_run() {
    init();

    let dir = TempDir::new().unwrap();
    let topic = TopicBuilder::new_with_dir("test", dir.path())
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

    select(
        pin!(ctrl_c()),
        pin!(tokio::time::sleep(tokio::time::Duration::from_secs(1))),
    )
    .await;

    info!("Stop");

    topic.abort();
}
