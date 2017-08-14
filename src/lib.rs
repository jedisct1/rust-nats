#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate serde;
extern crate serde_json;
pub extern crate openssl;

pub use client::*;
pub use errors::*;
pub use tls_config::*;

mod client;
mod errors;
mod stream;
mod tls_config;
