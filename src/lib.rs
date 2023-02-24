#![doc = include_str!("../README.md")]
// Features
#![feature(type_alias_impl_trait, generic_const_exprs, io_error_more)]
// Lints
#![warn(clippy::nursery, clippy::pedantic)]
#![allow(
    incomplete_features,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::module_name_repetitions,
    clippy::must_use_candidate
)]

pub mod consts;
pub mod formats;

mod_use::mod_use![error, inner, raw, util];

use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::{Event, EventListener};
use futures_core::{ready, Future, Stream};
use kanal::SendFuture;
use serde::{Deserialize, Serialize};
use tap::{Conv, Pipe};
use tokio::{fs, task::JoinHandle};
use tracing::{instrument, trace};
pub use util::{bincode_option, BincodeOptions};
use uuid7::uuid7;

use crate::{consts::MIN_LOG_SIZE, formats::Log};

#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TopicBuilder {
    topic: String,
    dir: PathBuf,
    log_size: u64,
    index_size: u64,
    channel_size: u32,
}

impl TopicBuilder {
    pub fn new(topic: impl Into<String>) -> Result<Self> {
        Self::new_with_dir(topic, std::env::current_dir()?)
    }

    pub fn new_with_dir(topic: impl Into<String>, dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if !dir.is_dir() {
            return IoError::new(IoErrorKind::NotADirectory, dir.display().to_string())
                .conv::<ErrorType>()
                .pipe(Err);
        }

        Ok(Self {
            topic: topic.into(),
            dir,
            log_size: 1 << 24,    // 16M
            index_size: 1 << 16,  // 64k
            channel_size: 1 << 4, // 16
        })
    }

    pub fn with_directory(mut self, dir: impl Into<PathBuf>) -> Result<Self> {
        self.dir = dir.into();
        if !self.dir.is_dir() {
            return IoError::new(IoErrorKind::NotADirectory, self.dir.display().to_string())
                .conv::<ErrorType>()
                .pipe(Err);
        }
        Ok(self)
    }

    pub const fn with_log_size(mut self, log_size: u64) -> Self {
        self.log_size = log_size;
        self
    }

    pub const fn with_index_size(mut self, index_size: u64) -> Self {
        self.index_size = index_size;
        self
    }

    pub const fn with_channel_size(mut self, channel_size: u32) -> Self {
        self.channel_size = channel_size;
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
    pub fn builder(topic: impl Into<String>) -> Result<TopicBuilder> {
        TopicBuilder::new(topic)
    }

    pub async fn new(conf: TopicBuilder) -> Result<Self> {
        let (send, recv) = kanal::bounded_async(conf.channel_size as _);

        let dir = conf.topic_dir();
        fs::create_dir_all(&dir).await?;

        let event = Event::new();
        let (log_map, appender) = Self::make(&conf, recv)?;
        let shared = Arc::new(Shared::new(conf, event, log_map));
        let handle = tokio::spawn(Self::background(shared.clone(), appender));

        Ok(Self {
            shared,
            handle,
            send,
        })
    }

    pub fn config(&self) -> &TopicBuilder {
        &self.shared.conf
    }

    fn make(
        conf: &TopicBuilder,
        recv: kanal::AsyncReceiver<Log>,
    ) -> Result<(Arc<SharedMap>, Appender)> {
        let filename = uuid7().encode();

        let dir = conf.topic_dir();

        trace!(?dir, id = %filename, "Rolling");

        let log_map = SharedMap::new(&dir, filename.as_str(), conf.log_size)?.pipe(Arc::new);
        let idx_map = UniqueMap::new(&dir, filename.as_str(), conf.index_size)?;
        let appender = Appender {
            log: log_map.clone(),
            idx: idx_map,
            recv,
        };

        Ok((log_map, appender))
    }

    #[instrument(level = "trace")]
    async fn background(shared: Arc<Shared>, mut appender: Appender) -> Result<()> {
        // Remaining log that wasn't saved due to lack of file space. Will be written to
        // the next file.
        let mut rem = None;
        loop {
            // Start receiving and save logs
            rem = appender.run(rem, &shared.event).await?;
            let Appender { log, recv, idx } = appender;

            // Close the log file and flush to disk
            idx.drop();

            log.finish()?;

            // Log file is full, create a new one
            let (map, app) = Self::make(&shared.conf, recv)?;

            appender = app;
            shared.swap_map(map);
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
        let offset = self.shared.offset();
        if read_at > offset {
            return Err(ErrorType::InvalidOffset {
                maximum: offset,
                got: read_at,
            });
        }

        let shared = self.shared.clone();
        let map = self.shared.map();

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

    /// # Panics
    /// Panics if the background task panicked
    pub async fn join(self) -> Result<()> {
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
        // Current read position
        read_at: usize,
        // Map being reading, may not be the latest one
        map: Arc<SharedMap>,
        // Up to date shared info
        shared: Arc<Shared>
    }
}

impl Reader {
    // Get the unread bytes. This will start at the log boundary. (i.e. followed
    // by a valid log or nothing)
    pub fn as_slice(&self) -> &[u8] {
        self.map.slice(self.read_at)
    }

    pub const fn cursor(&self) -> usize {
        self.read_at
    }
}

impl Stream for Reader {
    type Item = Result<Log>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let (map, mut notify) = (this.map, this.notify);

        loop {
            // We don't have enough data to decode a log. Check if the map is closed and if
            // any event has been emitted.
            if map.offset() - *this.read_at < MIN_LOG_SIZE {
                // Current map is obsolete
                if map.is_finished() {
                    let current = this.shared.map();

                    // If the active map is finished, the background task is not making more
                    // progress (swap the map). Mark the reader as finished.
                    if current.is_finished() {
                        return Poll::Ready(None);
                    }

                    // Otherwise, swap it with the active one and reset the read
                    // pointer.
                    *map = current;
                    *this.read_at = 0;
                }

                // Poll the event listener. If no event has been emitted, return
                // `Poll::Pending`. If it's `Poll::Ready`, new data is available after
                // last `offset` call, continue to decoding.
                ready!(notify.as_mut().poll(cx));
                std::mem::replace(&mut *notify, this.shared.subscribe()).discard();
            }

            let slice = map.slice(*this.read_at);

            match try_decode::<Log>(slice) {
                // Successfully decoded a log. Advance the read pointer.
                Ok(Some((log, read))) => {
                    *this.read_at += read as usize;
                    return Poll::Ready(Some(Ok(log)));
                }

                // Error while decoding.
                Err(e) => {
                    return Poll::Ready(Some(Err(ErrorType::Bincode(e))));
                }

                // This should not happen. If it does, there's some problem with the writer, we need
                // to wait for the next chunk of data to be written. This behavior maybe changed to
                // return an error in future.
                Ok(None) => {
                    std::mem::replace(&mut *notify, this.shared.subscribe()).discard();
                    ready!(notify.as_mut().poll(cx));
                }
            }
        }
    }
}
