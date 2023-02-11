use std::fs;

use limlog::selector::{LogSelector, SelectRange};
use rand::Rng;

fn main() {
    _ = fs::create_dir(".logs");

    let mut rng = rand::thread_rng();

    let selector = LogSelector::new(".logs").unwrap();
    let mut results = Vec::new();
    for _ in 0..1000 {
        let start = rng.gen_range(0..500);
        results.push(
            selector
                .select_range(SelectRange::Timestamp(start, start + 5))
                .unwrap(),
        );
    }

    for res in results {
        _ = res.recv().unwrap().is_empty();
    }
}
