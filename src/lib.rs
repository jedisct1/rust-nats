#![feature(custom_derive, plugin, vec_push_all)]
#![plugin(serde_macros)]

extern crate serde;

pub use client::*;
pub use errors::*;

mod client;
mod errors;
