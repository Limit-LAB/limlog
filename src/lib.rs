#![feature(once_cell)]
#![feature(trait_alias)]
#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

pub mod appender;
pub mod formats;
pub mod selector;

mod checker;
mod error;
mod util;

pub use error::*;

#[cfg(test)]
mod tests;

const STACK_BUF_SIZE: usize = 256;
