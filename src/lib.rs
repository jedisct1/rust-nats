#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate serde;
extern crate serde_json;

pub use client::*;
pub use errors::*;

mod client;
mod errors;
mod stream;
