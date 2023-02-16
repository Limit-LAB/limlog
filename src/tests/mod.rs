use futures::StreamExt;
use uuid7::uuid7;

use crate::{formats::log::Log, Topic};

mod log_format_test;

#[tokio::test]
async fn test_miri() {
    use tempdir::TempDir;

    let dir = TempDir::new("test").unwrap();
    let topic = Topic::new("test", dir).unwrap();

    let (mut r, w) = (topic.reader(), topic.writer());

    tokio::spawn(topic.start_append());

    w.send
        .send(Log {
            uuid: uuid7(),
            key: "key".into(),
            value: "value".into(),
        })
        .unwrap();

    let item = r.next().await.unwrap().unwrap();

    println!("{:?}", item);
}
