use std::{fs, thread, time::{Duration, SystemTime, UNIX_EPOCH}};

use limlog::{Log, LogAppender};
use uuid7::gen7::Generator;

fn main() {
    _ = fs::create_dir(".logs");

    let appender = LogAppender::builder(".logs").build().unwrap();
    let mut gen = Generator::new(rand::thread_rng());

    for _ in 0..100 {
        let mut batch = Vec::with_capacity(100);
        for _ in 0..100 {
            let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            batch.push(Log {
                uuid: gen.generate_core(now_ts).0,
                key: vec![b'H', b'i'],
                value: vec![b'H', b'e', b'l', b'l', b'o', b'w', b'o', b'r', b'l', b'd'],
            });
        }

        appender.insert_batch(batch).unwrap();
    }

    appender.flush().unwrap();
    // wait for finished
    thread::sleep(Duration::from_millis(2000));
}
