use std::{fs, thread, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use limlog::{Log, LogAppender};

pub fn criterion_benchmark(c: &mut Criterion) {
    fs::create_dir("bench_dir").unwrap();

    let appender = LogAppender::builder("bench_dir").build().unwrap();
    let mut count = 0;

    c.bench_function("appender", |b| {
        b.iter(|| {
            let mut batch = Vec::with_capacity(1000);
            for _ in 0..1000 {
                batch.push(Log {
                    ts: count,
                    id: count,
                    key: vec![b'H', b'i'],
                    value: vec![b'H', b'e', b'l', b'l', b'o', b'w', b'o', b'r', b'l', b'd'],
                });
                count += 1;
            }

            appender.insert_batch(batch).unwrap();
        })
    });

    thread::sleep(Duration::from_millis(5000));

    fs::remove_dir_all("bench_dir").unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
