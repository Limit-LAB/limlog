// Features
#![feature(io_error_more)]
#![feature(type_alias_impl_trait)]

pub mod consts;
pub mod formats;

mod error;
mod gc;
mod inner;
mod raw;
mod util;

#[cfg(test)]
mod tests;

use std::{
    fs,
    future::IntoFuture,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::EventListener;
use futures::{ready, Future, Stream};
use kanal::SendFuture;
use tokio::task::JoinHandle;
use uuid7::uuid7;

use crate::{
    consts::MIN_LOG_SIZE,
    error::{ErrorType, Result},
    formats::log::Log,
    inner::{Appender, IndexMap, LogsMap, Shared},
    util::try_decode,
};

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
}
pub struct Topic {
    conf: TopicBuilder,
    handle: JoinHandle<Result<()>>,
    send: kanal::AsyncSender<Log>,
}

impl Topic {
    pub fn new(conf: TopicBuilder) -> Result<Self> {
        let (send, recv) = kanal::bounded_async(1 << 4);

        let handle = tokio::spawn(Self::background(conf.topic.clone(), conf.dir.clone(), recv));

        Ok(Self { conf, handle, send })
    }

    pub async fn background(
        topic: String,
        dir: PathBuf,
        recv: kanal::AsyncReceiver<Log>,
    ) -> Result<()> {
        let dir = dir.join(&topic);
        fs::create_dir_all(&dir)?;

        loop {
            let id = util::uuid_now().encode();
            let logs = LogsMap::new(id.as_str(), &dir)?;
            let idx = IndexMap::new(id.as_str(), &dir)?;

            let inner = Arc::new(Shared::new(logs, topic.clone()));

            let appender = Appender::new(inner.clone(), idx, recv.clone());

            match appender.run().await {
                Ok(_) => todo!(),
                Err(e) if matches!(e, ErrorType::LogFileFull) => todo!(),
                Err(_) => break,
            }
        }
        Ok(())
    }

    pub fn writer(&self) -> Writer {
        Writer {
            send: self.send.clone(),
        }
    }

    pub fn reader(&self) -> Reader {
        let inner = self.appender.shared();

        Reader {
            read_at: inner.offset(),
            notify: inner.subscribe(),
            inner,
        }
    }

    pub fn reader_at(&self, read_at: usize) -> Result<Reader> {
        let inner = self.appender.shared();
        let offset = inner.offset();
        if read_at > offset {
            return Err(ErrorType::InvalidOffset {
                maximum: offset,
                got: read_at,
            });
        }

        Ok(Reader {
            read_at,
            notify: inner.subscribe(),
            inner,
        })
    }
}

impl IntoFuture for Topic {
    type Output = Result<()>;

    type IntoFuture = impl Future<Output = Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        self.appender.start()
    }
}

#[derive(Clone, Debug)]
pub struct Writer {
    send: kanal::AsyncSender<Log>,
}

impl Writer {
    pub fn send(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> SendFuture<'_, Log> {
        self.send.send(Log {
            uuid: uuid7(),
            key: key.into(),
            value: value.into(),
        })
    }
}

impl Deref for Writer {
    type Target = kanal::AsyncSender<Log>;

    fn deref(&self) -> &Self::Target {
        &self.send
    }
}

impl DerefMut for Writer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.send
    }
}

pin_project_lite::pin_project! {
    pub struct Reader {
        #[pin]
        notify: EventListener,
        read_at: usize,
        inner: Arc<Shared>,
        // raw: MmapInner
    }
}

impl Reader {
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: memory before `offset` are immutable and ready to be read
        unsafe { self.inner.index(self.read_at..self.inner.offset()) }
    }
}

impl Stream for Reader {
    type Item = Result<Log>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let (inner, mut notify) = (this.inner, this.notify);

        loop {
            if inner.offset() - *this.read_at < MIN_LOG_SIZE {
                ready!(notify.as_mut().poll(cx));
            }

            let slice = inner.ref_slice(*this.read_at);

            match try_decode::<Log>(&slice) {
                Ok(Some((log, read))) => {
                    *this.read_at += read as usize;
                    return Poll::Ready(Some(Ok(log)));
                }
                Err(e) => return Poll::Ready(Some(Err(ErrorType::Bincode(e)))),
                Ok(None) => {
                    std::mem::replace(&mut *notify, inner.subscribe()).discard();
                    ready!(notify.as_mut().poll(cx));
                }
            }
        }
    }
}
