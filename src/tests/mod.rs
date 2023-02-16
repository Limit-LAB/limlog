use futures::{task::SpawnExt, StreamExt};
use tempfile::TempDir;
use uuid7::uuid7;

use crate::{formats::log::Log, Topic};

mod log_format_test;

#[test]
fn test_run() {
    use futures_time::{task::sleep, time::Duration};

    let dir = TempDir::new().unwrap();
    let topic = Topic::new("test", dir.path()).unwrap();

    let (mut r, w) = (topic.reader(), topic.writer());

    let mut pool = futures_executor::LocalPool::new();

    pool.spawner()
        .spawn(async { topic.start_append().await.unwrap() })
        .unwrap();

    pool.spawner()
        .spawn(async move {
            let mut counter = 0;
            loop {
                w.send
                    .send(Log {
                        uuid: uuid7(),
                        key: vec![counter % u8::MAX],
                        value: vec![],
                    })
                    .await
                    .unwrap();
                counter += 1;
                sleep(Duration::from_secs(1)).await;
            }
        })
        .unwrap();

    pool.run_until(async move {
        loop {
            let item = r.next().await.unwrap().unwrap();
            eprintln!("Woohoo {item:?}");
        }
    });
}
