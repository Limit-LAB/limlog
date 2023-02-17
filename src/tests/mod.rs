use std::{
    future::IntoFuture,
    time::{Duration, Instant},
};

use futures::StreamExt;
use rand::Rng;
use tempfile::TempDir;
use tokio::{select, time::sleep};
use uuid7::uuid7;

use crate::{formats::log::Log, Topic};

mod log_format_test;

#[tokio::test]
async fn test_run() {
    let dir = TempDir::new().unwrap();
    let topic = Topic::new("test", dir.path()).unwrap();

    for i in 0..10 {
        tokio::spawn({
            let w = topic.writer().clone();
            async move {
                let mut counter = 0;
                loop {
                    w.send
                        .send(Log {
                            uuid: uuid7(),
                            key: vec![i],
                            value: vec![counter % u8::MAX],
                        })
                        .await
                        .unwrap();
                    counter += 1;
                    let secs = { rand::thread_rng().gen_range(0.5..1.5) };
                    sleep(Duration::from_secs_f32(secs)).await;
                }
            }
        });
    }

    for i in 0..10 {
        tokio::spawn({
            let mut r = topic.reader();
            async move {
                loop {
                    let now = Instant::now();
                    let item = r.next().await.unwrap().unwrap();
                    eprintln!("#{i} {:?}: {:?}", item.key, item.value);
                    eprintln!("Passed {}", now.elapsed().as_secs_f32());
                }
            }
        });
    }

    select! {
        _ = topic.into_future() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
