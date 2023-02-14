#![feature(once_cell)]
#![feature(trait_alias)]

mod error;
pub mod formats;
mod gc;
mod util;

pub use error::*;

#[cfg(test)]
mod tests;

const STACK_BUF_SIZE: usize = 256;
