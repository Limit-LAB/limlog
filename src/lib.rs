// Features
#![feature(io_error_more)]
#![feature(type_alias_impl_trait)]

pub mod consts;
pub mod formats;

mod error;
mod inner;
mod raw;
mod util;

#[cfg(test)]
mod tests;

use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::EventListener;
use futures::{ready, Future, Stream};
use kanal::SendFuture;
use tap::Pipe;
use tokio::{fs, task::JoinHandle};
use uuid7::uuid7;

use crate::{
    consts::MIN_LOG_SIZE,
    error::{ErrorType, Result},
    formats::log::Log,
    inner::{Appender, Shared, SharedMap, UniqueMap},
    util::try_decode,
};

#[derive(Debug, Clone)]
pub struct TopicBuilder {
    topic: String,
    dir: PathBuf,
    log_size: usize,
    index_size: usize,
}

impl TopicBuilder {
    pub fn new(topic: impl Into<String>, dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if !dir.is_dir() {
            return Err(ErrorType::Io(IoError::new(
                IoErrorKind::NotADirectory,
                dir.display().to_string(),
            )));
        }
        let topic = topic.into();
        let dir = dir.join(&topic);

        Ok(Self {
            topic: topic.into(),
            dir: dir.into(),
            log_size: 1 << 24,
            index_size: 1 << 16,
        })
    }

    pub fn with_log_size(mut self, log_size: usize) -> Self {
        self.log_size = log_size;
        self
    }

    pub fn with_index_size(mut self, index_size: usize) -> Self {
        self.index_size = index_size;
        self
    }

    pub fn topic_dir(&self) -> PathBuf {
        self.dir.join(&self.topic)
    }

    pub async fn build(self) -> Result<Topic> {
        Topic::new(self).await
    }
}

pub struct Topic {
    shared: Arc<Shared>,
    handle: JoinHandle<Result<()>>,
    send: kanal::AsyncSender<Log>,
}

impl Topic {
    pub fn builder(topic: impl Into<String>, dir: impl Into<PathBuf>) -> Result<TopicBuilder> {
        TopicBuilder::new(topic, dir)
    }

    pub async fn new(conf: TopicBuilder) -> Result<Self> {
        let (send, recv) = kanal::bounded_async(1 << 4);

        let dir = conf.topic_dir();
        fs::create_dir_all(&dir).await?;

        let (log_map, appender) = Self::make_maps(&conf, recv.clone())?;
        let shared = Arc::new(Shared::new(conf, log_map));
        let handle = tokio::spawn(Self::background(shared.clone(), appender, recv));

        Ok(Self {
            shared,
            handle,
            send,
        })
    }

    pub fn config(&self) -> &TopicBuilder {
        &self.shared.conf
    }

    fn make_maps(
        conf: &TopicBuilder,
        recv: kanal::AsyncReceiver<Log>,
    ) -> Result<(Arc<SharedMap>, Appender)> {
        let filename = util::uuid_now().encode();
        let dir = conf.topic_dir();

        let log_map = SharedMap::new(&dir, filename.as_str())?.pipe(Arc::new);
        let idx_map = UniqueMap::new(&dir, filename.as_str())?;
        let appender = Appender::new(log_map.clone(), idx_map, recv);

        Ok((log_map, appender))
    }

    async fn background(
        shared: Arc<Shared>,
        mut appender: Appender,
        recv: kanal::AsyncReceiver<Log>,
    ) -> Result<()> {
        'o: loop {
            eprintln!("Start");
            // Start receiving and save logs
            loop {
                match appender.run().await {
                    Ok(_) => unreachable!("Appender should never stop with Ok"),
                    // Log file is full, create a new one
                    Err(e) if matches!(e, ErrorType::LogFileFull) => break,
                    // Unexpected error, stop the background task
                    Err(e) => break 'o Err(e),
                }
            }
            let (map, app) = Self::make_maps(&shared.conf, recv.clone())?;

            appender = app;
            shared.store_map(map);
        }
    }

    pub fn writer(&self) -> Writer {
        Writer {
            send: self.send.clone(),
        }
    }

    pub fn reader(&self) -> Reader {
        let shared = self.shared.clone();
        let map = shared.map();

        Reader {
            read_at: map.offset(),
            notify: shared.subscribe(),
            map,
            shared,
        }
    }

    pub fn reader_at(&self, read_at: usize) -> Result<Reader> {
        let shared = self.shared.clone();
        let map = self.shared.map();
        let offset = map.offset();
        if read_at > offset {
            return Err(ErrorType::InvalidOffset {
                maximum: offset,
                got: read_at,
            });
        }

        Ok(Reader {
            read_at,
            notify: shared.subscribe(),
            map,
            shared,
        })
    }

    pub fn abort(&self) {
        self.handle.abort();
    }

    pub async fn wait_for_bg(self) -> Result<()> {
        self.handle.await.unwrap()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Writer {
    send: kanal::AsyncSender<Log>,
}

impl Writer {
    pub fn write(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> SendFuture<'_, Log> {
        self.send.send(Log {
            uuid: uuid7(),
            key: key.into(),
            value: value.into(),
        })
    }
}

pin_project_lite::pin_project! {
    pub struct Reader {
        #[pin]
        notify: EventListener,
        read_at: usize,
        map: Arc<SharedMap>,
        shared: Arc<Shared>
        // raw: MmapInner
    }
}

impl Reader {
    pub fn as_slice(&self) -> &[u8] {
        self.map.slice(self.read_at)
    }
}

impl Stream for Reader {
    type Item = Result<Log>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let (inner, mut notify) = (this.map, this.notify);

        loop {
            if inner.offset() - *this.read_at < MIN_LOG_SIZE {
                ready!(notify.as_mut().poll(cx));
            }

            let slice = inner.slice(*this.read_at);

            match try_decode::<Log>(&slice) {
                Ok(Some((log, read))) => {
                    *this.read_at += read as usize;
                    return Poll::Ready(Some(Ok(log)));
                }
                Err(e) => return Poll::Ready(Some(Err(ErrorType::Bincode(e)))),
                Ok(None) => {
                    std::mem::replace(&mut *notify, this.shared.subscribe()).discard();
                    ready!(notify.as_mut().poll(cx));
                }
            }
        }
    }
}
