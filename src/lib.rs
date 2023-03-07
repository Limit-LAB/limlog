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

mod_use::mod_use![error];

mod inner;
mod raw;
mod util;

use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::{Event, EventListener};
use futures_core::{ready, Future, Stream};
use inner::{Appender, Shared, SharedMap};
use kanal::SendFuture;
use serde::{Deserialize, Serialize};
use tap::{Conv, Pipe};
use tokio::{fs, task::JoinHandle};
use tracing::{instrument, trace};
use uuid7::uuid7;

pub use crate::util::{bincode_option, try_decode, BincodeOptions};
use crate::{
    consts::{DEFAULT_CHANNEL_SIZE, DEFAULT_INDEX_SIZE, DEFAULT_LOG_SIZE, MIN_LOG_SIZE},
    formats::Log,
    inner::UniqueMap,
    util::Discard,
};

/// Builds [`Topic`] with custom configuration values.
///
/// Methods can be chained in order to set the configuration values.
/// The [`Topic`] is constructed by calling [`TopicBuilder::build`].
///
/// New instances of [`TopicBuilder`] are obtained via [`TopicBuilder::new`],
/// [`TopicBuilder::new_with_dir`] or [`Topic::builder`].
///
/// ```ignore
/// let _topic = Topic::builder("test")
///     .unwrap()
///     .with_log_size(1 << 32)
///     .with_index_size(1 << 24)
///     .with_channel_size(16)
///     .build()
///     .await
///     .unwrap();
/// ```
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
    /// Returns a new [`TopicBuilder`] with `topic` and current working directory.
    ///
    /// Configuration methods can be chained on the return value.
    pub fn new(topic: impl Into<String>) -> Result<Self> {
        Self::new_with_dir(topic, std::env::current_dir()?)
    }

    /// Returns a new [`TopicBuilder`] with `topic` and `dir`.
    ///
    /// Configuration methods can be chained on the return value.
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
            log_size: DEFAULT_LOG_SIZE,
            index_size: DEFAULT_INDEX_SIZE,
            channel_size: DEFAULT_CHANNEL_SIZE,
        })
    }

    /// Specify the topic working directory.
    pub fn with_directory(mut self, dir: impl Into<PathBuf>) -> Result<Self> {
        self.dir = dir.into();
        if !self.dir.is_dir() {
            return IoError::new(IoErrorKind::NotADirectory, self.dir.display().to_string())
                .conv::<ErrorType>()
                .pipe(Err);
        }
        Ok(self)
    }

    /// Set log file max size.
    pub const fn with_log_size(mut self, log_size: u64) -> Self {
        self.log_size = log_size;
        self
    }

    /// Set index file max size.
    pub const fn with_index_size(mut self, index_size: u64) -> Self {
        self.index_size = index_size;
        self
    }

    /// Set channel max size.
    /// 
    /// The [Writer] will block if the channel is full until write request is consumed.
    pub const fn with_channel_size(mut self, channel_size: u32) -> Self {
        self.channel_size = channel_size;
        self
    }

    /// Returns the topic directory where the `.limlog` and `.idx` files placed.
    pub fn topic_dir(&self) -> PathBuf {
        self.dir.join(&self.topic)
    }

    /// Construct a [`Topic`] instant if configurations is valid.
    pub async fn build(self) -> Result<Topic> {
        Topic::new(self).await
    }
}

/// The topic which is used to read and write logs.
#[derive(Debug)]
pub struct Topic {
    shared: Arc<Shared>,
    handle: JoinHandle<Result<()>>,
    send: kanal::AsyncSender<Log>,
}

impl Topic {
    /// Returns a new [`TopicBuilder`] with `topic` and current working directory.
    ///
    /// Configuration methods can be chained on the return value.
    pub fn builder(topic: impl Into<String>) -> Result<TopicBuilder> {
        TopicBuilder::new(topic)
    }

    /// Create a new [`Topic`] with [`TopicBuilder`].
    /// 
    /// Equivalent to [`TopicBuilder::build`].
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

    /// Returns the topic configurations.
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

    /// Write a [`Log`] asynchronous.
    pub async fn write_one(&self, log: Log) -> Result<()> {
        self.send.send(log).await?;
        Ok(())
    }

    /// Returns the [`Writer`] to write logs.
    /// 
    /// ```ignore
    /// let w = topic.writer();
    /// loop {
    ///     // topic.write_one(Log { uuid: uuid7(), body: "hello".into() }).await.unwrap();
    ///     w.write("hello").await.unwrap();
    /// }
    /// ```
    pub fn writer(&self) -> Writer {
        Writer {
            send: self.send.clone(),
        }
    }

    /// Returns the [`Reader`] to read logs.
    /// 
    /// ```ignore
    /// use futures::StreamExt;
    /// let r = topic.reader();
    /// let log = r.next().await.unwrap().unwrap();
    /// ```
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

    /// Create a [`Reader`] by given offset.
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

    /// Abort background task which is writing logs.
    /// After this operation there should be no more writes.
    /// 
    /// Notice that background task may not abort immediately.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Wait for background task to complete.
    /// 
    /// # Panics
    /// Panics if the background task panicked.
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
    /// Write log with `body` and generated UUID.
    pub fn write(&self, body: impl Into<Vec<u8>>) -> SendFuture<'_, Log> {
        self.send.send(Log {
            uuid: uuid7(),
            body: body.into(),
        })
    }
}

pin_project_lite::pin_project! {
    #[derive(Debug)]
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

    /// Returns the current cursor.
    pub const fn cursor(&self) -> usize {
        self.read_at
    }
}

impl Clone for Reader {
    /// Clone the reader which will have the same read position and map. For
    /// fresh map, use [`Topic::reader`] or [`Topic::reader_at`] instead.
    fn clone(&self) -> Self {
        Self {
            notify: self.shared.subscribe(),
            read_at: self.read_at,
            map: self.map.clone(),
            shared: self.shared.clone(),
        }
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
