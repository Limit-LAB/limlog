use std::{fs, thread, time::Duration};

use limlog::{Log, LogAppender};

fn main() {
    _ = fs::create_dir(".logs");

    let appender = LogAppender::builder(".logs").build().unwrap();
    let mut count = 0;

    for _ in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for _ in 0..100 {
            batch.push(Log {
                ts: count,
                id: count,
                key: vec![b'H', b'i'],
                value: vec![b'H', b'e', b'l', b'l', b'o', b'w', b'o', b'r', b'l', b'd'],
            });
            count += 1;
        }

        appender.insert_batch(batch).unwrap();
    }

    appender.flush().unwrap();
    // wait for finished
    thread::sleep(Duration::from_millis(1000));
}
