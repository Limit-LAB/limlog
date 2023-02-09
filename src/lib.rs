#![feature(once_cell)]
#![feature(trait_alias)]
#![feature(generic_const_exprs)]

pub mod appender;
pub mod selector;

pub mod formats;

pub use appender::LogAppender;
pub use formats::log::Log;

mod checker;
mod util;

#[cfg(test)]
mod tests;
