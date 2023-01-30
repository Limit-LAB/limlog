use anyhow::Result;

pub trait Insert {
    type Key;
    type Value;
    fn insert(&mut self, index: u64, ts: u64, key: Self::Key, value: Self::Value) -> Result<()>;
    fn insert_batch(&mut self, batch: Vec<(u64, u64, Self::Key, Self::Value)>) -> Result<()>;
}

pub enum IndexType {
    Timestamp(u64),
    Id(u64),
}

pub trait Select {
    type Key;
    type Value;
    fn select(&self, index: IndexType) -> Result<Self::Value>;
    fn select_range(&self, start: IndexType, end: IndexType) -> Result<Vec<Self::Value>>;
}
