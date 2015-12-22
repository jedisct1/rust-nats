Rust-NATS
=========

Rust-NATS is a Rust client library for the [NATS](https://nats.io) message
queue.

The crate is called `nats` and can be added as a dependency using Cargo:

```rust
[dependencies]
nats = "*"
```

It was written for Rust-nightly, but support for Rust 1.2 will be added later.

The library was designed to be robust. It doesn't use any usafe code, it never
calls `panic!()` and failed commands are automatically retried on different
cluster nodes.

It provides a simple low-level interface that makes it easy to send/receive
messages over Rust channels if needed.

Connecting
==========

Single-node client connection:

```rust
extern crate nats;
use nats::*;

let client = Client::new("nats://user:password@127.0.0.1").unwrap();
```

The username and password are optional.

Connecting to a cluster:

```rust
let cluster = vec!("nats://user:password@127.0.0.1", "nats://127.0.0.2");
let mut client = nats::Client::new(cluster).unwrap();
```

By default, commands are sent in fire-and-forget mode. In order to wait for an
acknowledgment after each command, the synchronous ("verbose") mode can be
turned on:

```rust
client.set_synchronous(true);
```

The client name can also be customized:

```rust
client.set_name("app");
```

Publishing messages
===================

```rust
client.publish("subject.test", "test".as_bytes()).unwrap();
```

In order to use NATS for RPC, the `Client.make_request()` function creates an
ephemeral subject ("inbox"), subscribes to it, schedules the removal of the
subscription after the first received message, publishes the initial request,
and returns the inbox subject name:

```rust
let inbox = client.make_request("subject.rpc", "test".as_bytes()).unwrap();
```

Subscribing to subjects
=======================

`Client.subscribe()` adds a subscription to a subject, with an optional group:

```rust
let s1 = client.subscribe("subject", None).unwrap();
let s2 = client.subscribe("subject.*", Some("app")).unwrap();
```

With group membership, a given message will be only delivered to one client in
the group.

`Client.unsubscribe()` removes a subscription:

```rust
client.unsubscribe(s1).unwrap();
```

Or to remove it after `n` messages have been received:

```rust
client.unsubscribe_after(s1, n).unwrap();
```

Receiving events
================

`Client.wait()` waits for a new event, and transparently responds to server
`PING` requests.

```rust
let event = client.wait();
```

This returns an `Event` structure:
```rust
pub struct Event {
    pub subject: String,
    pub channel: Channel,
    pub msg: Vec<u8>,
    pub inbox: Option<String>
}
```

Alternatively, events can be received using an iterator:

```rust
for event in client.events() {
    ...
}
```
