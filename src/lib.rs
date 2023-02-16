// Features
#![feature(once_cell)]
#![feature(trait_alias)]
#![feature(io_error_more)]
#![feature(type_alias_impl_trait)]
// POC
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod consts;
pub mod formats;

mod error;
mod gc;
mod inner;
mod util;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    fs::File,
    io::{Error as IoError, ErrorKind as IoErrorKind},
    marker::PhantomData,
    ops::{Index, RangeBounds},
    path::Path,
    pin::Pin,
    slice::SliceIndex,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use bincode::Options;
use event_listener::{Event, EventListener};
use futures::{ready, Future, Stream};

use crate::{
    consts::MIN_LOG_SIZE,
    error::{ErrorType, Result},
    formats::log::Log,
    inner::{IndexMap, LogsMap, Shared},
    util::{bincode_option, try_decode, BincodeOptions},
};

pub struct Appender {
    inner: Arc<Shared>,
    idx: IndexMap,
    recv: kanal::Receiver<Log>,
}

impl Appender {
    fn start_append(self) -> Result<()> {
        let opt: BincodeOptions = bincode_option();
        while let Ok(log) = self.recv.recv() {
            let offset = self.inner.offset();
            let len = opt.serialized_size(&log)?;
            if (self.inner.remaining() as u64) < len {
                break;
            }
            // SAFETY: We are the only one accessing the mutable portion of mmap
            let mut buf = unsafe { self.inner.mut_slice() };
            opt.serialize_into(&mut buf, &Some(log))?;
            self.inner.flush(len as usize)?;
        }
        Ok(())
    }
}

pub struct Topic {
    appender: Appender,
    send: kanal::Sender<Log>,
}

impl Topic {
    fn new(topic: impl Into<String>, dir: impl AsRef<Path>) -> Result<Self> {
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

        let (send, recv) = kanal::bounded(1 << 8);
        let appender = Appender { inner, idx, recv };

        Ok(Self { send, appender })
    }

    pub fn writer(&self) -> Writer {
        Writer {
            send: self.send.clone(),
        }
    }

    pub fn reader(&self) -> Reader {
        let read_to = self.appender.inner.offset();
        Reader {
            read_to,
            inner: self.appender.inner.clone(),
            notify: self.appender.inner.subscribe(),
        }
    }

    async fn start_append(self) -> Result<()> {
        self.appender.start_append()
    }
}

pub struct Writer {
    send: kanal::Sender<Log>,
}

pin_project_lite::pin_project! {
    pub struct Reader {
        #[pin]
        notify: EventListener,
        read_to: usize,
        inner: Arc<Shared>,
    }
}

impl Reader {
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: memory before `offset` are immutable and ready to be read
        unsafe { self.inner.index(self.read_to..self.inner.offset()) }
    }
}

impl Stream for Reader {
    type Item = Result<Log>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let (inner, mut notify) = (this.inner, this.notify);

        loop {
            ready!(notify.as_mut().poll(cx));

            // Event emitted but has no new logs
            if inner.offset() - *this.read_to < MIN_LOG_SIZE {
                return Poll::Ready(None);
            }

            notify.set(inner.subscribe());

            let slice = inner.ref_slice(*this.read_to);

            match try_decode::<Log>(&slice, Some(bincode_option())) {
                Ok(Some((log, read))) => {
                    *this.read_to += read as usize;
                    return Poll::Ready(Some(Ok(log)));
                }
                Err(e) => return Poll::Ready(Some(Err(ErrorType::Bincode(e)))),
                Ok(None) => {
                    // Not ready yet, let the new EventListener to be polled
                    // again
                    continue;
                }
            }
        }
    }
}
