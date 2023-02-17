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
    future::IntoFuture,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bincode::Options;
use event_listener::EventListener;
use futures::{ready, Future, Stream};

use crate::{
    consts::MIN_LOG_SIZE,
    error::{ErrorType, Result},
    formats::log::Log,
    inner::{IndexMap, LogsMap, Shared},
    util::{bincode_option, try_decode, BincodeOptions},
};

/// The long-live future which the appender start to receive logs and append to
/// the mmap. This should normally be spawned as a task.
pub type Appending = impl Future<Output = Result<()>>;

pub struct Appender {
    inner: Arc<Shared>,
    idx: IndexMap,
    recv: kanal::AsyncReceiver<Log>,
}

impl Appender {
    async fn start_append(self) -> Result<()> {
        let opt: BincodeOptions = bincode_option();

        while let Ok(log) = self.recv.recv().await {
            let _offset = self.inner.offset();
            let len = opt.serialized_size(&log)? as usize;
            if self.inner.remaining() < len {
                break;
            }
            // SAFETY: We are the only one accessing the mutable portion of mmap
            let buf = unsafe { self.inner.mut_slice() };
            opt.serialize_into(&mut buf[..len], &log)?;
            self.inner.flush(len)?;
        }
        Ok(())
    }
}

pub struct Topic {
    appender: Appender,
    send: kanal::AsyncSender<Log>,
}

impl Topic {
    pub fn new(topic: impl Into<String>, dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            return Err(ErrorType::Io(IoError::new(
                IoErrorKind::NotADirectory,
                dir.display().to_string(),
            )));
        }
        let topic = topic.into();

        let logs = LogsMap::new(&topic, dir)?;
        let idx = IndexMap::new(&topic, dir)?;

        let inner = Arc::new(Shared::new(logs, topic));

        let (send, recv) = kanal::bounded_async(1 << 4);
        let appender = Appender { inner, idx, recv };

        Ok(Self { send, appender })
    }

    pub fn writer(&self) -> Writer {
        Writer {
            send: self.send.clone(),
        }
    }

    pub fn reader(&self) -> Reader {
        let inner = self.appender.inner.clone();

        Reader {
            read_at: inner.offset(),
            notify: inner.subscribe(),
            inner,
        }
    }

    pub fn reader_at(&self, read_at: usize) -> Result<Reader> {
        let inner = self.appender.inner.clone();
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
    type IntoFuture = Appending;
    type Output = Result<()>;

    fn into_future(self) -> Self::IntoFuture {
        self.appender.start_append()
    }
}

#[derive(Clone, Debug)]
pub struct Writer {
    send: kanal::AsyncSender<Log>,
}

pin_project_lite::pin_project! {
    pub struct Reader {
        #[pin]
        notify: EventListener,
        read_at: usize,
        inner: Arc<Shared>,
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
                std::mem::replace(&mut *notify, inner.subscribe()).discard();
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
