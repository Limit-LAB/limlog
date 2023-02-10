#![feature(once_cell)]
#![feature(trait_alias)]
#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

pub mod appender;
pub mod selector;

pub mod formats;

pub use appender::LogAppender;
pub use formats::log::Log;
pub use selector::{LogSelector, SelectRange, SelectResult};

mod checker;
mod util;

#[cfg(test)]
mod tests;

const STACK_BUF_SIZE: usize = 256;
