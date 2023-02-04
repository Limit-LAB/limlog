#![feature(once_cell)]
#![feature(trait_alias)]

pub mod appender;
pub(crate) mod util;

pub mod formats;

pub use appender::LogAppender;
pub use formats::log::Log;

#[cfg(test)]
mod tests;
