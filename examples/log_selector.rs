use std::{fs, time::{SystemTime, UNIX_EPOCH}};

use limlog::selector::{LogSelector, SelectRange};

fn main() {
    _ = fs::create_dir(".logs");

    let selector = LogSelector::new(".logs").unwrap();
    let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut results = Vec::new();
    //for _ in 0..100 {
        results.push(
            selector
                .select_range(SelectRange::Timestamp(0, now_ts))
                .unwrap(),
        );
    //}

    for res in results {
        let len = res.recv().unwrap().len();
        println!("{len}");
    }
}
