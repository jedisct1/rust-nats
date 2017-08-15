extern crate openssl;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate url;

use errors::*;
use errors::ErrorKind::*;
use stream;
use tls_config::TlsConfig;
use self::rand::{thread_rng, Rng};
use self::serde_json::de;
use self::serde_json::value::Value;
use self::url::Url;
use self::openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod};
use std::cmp;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::error::Error;
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

const CIRCUIT_BREAKER_WAIT_AFTER_BREAKING_MS: u64 = 2000;
const CIRCUIT_BREAKER_WAIT_BETWEEN_ROUNDS_MS: u64 = 250;
const CIRCUIT_BREAKER_ROUNDS_BEFORE_BREAKING: u32 = 4;
const DEFAULT_NAME: &'static str = "#rustlang";
const DEFAULT_PORT: u16 = 4222;
const URI_SCHEME: &'static str = "nats";
const RETRIES_MAX: u32 = 10;

#[derive(Clone, Debug)]
struct Credentials {
    username: String,
    password: String,
}

#[derive(Clone, Debug)]
struct ServerInfo {
    host: String,
    port: u16,
    credentials: Option<Credentials>,
    max_payload: usize,
    tls_required: bool,
}

#[derive(Debug)]
struct ClientState {
    stream_writer: stream::Stream,
    buf_reader: BufReader<stream::Stream>,
    max_payload: usize,
}

#[derive(Debug)]
pub struct Client {
    servers_info: Vec<ServerInfo>,
    server_idx: usize,
    verbose: bool,
    pedantic: bool,
    name: String,
    state: Option<ClientState>,
    circuit_breaker: Option<Instant>,
    sid: u64,
    tls_config: Option<TlsConfig>,
}

#[derive(Debug)]
struct ConnectNoCredentials {
    verbose: bool,
    pedantic: bool,
    name: String,
}

impl ConnectNoCredentials {
    pub fn into_json(self) -> serde_json::Result<String> {
        let mut map = serde_json::Map::new();
        map.insert("verbose", Value::Bool(self.verbose));
        map.insert("pedantic", Value::Bool(self.pedantic));
        map.insert("name", Value::String(self.name));
        serde_json::to_string(&map)
    }
}

#[derive(Debug)]
struct ConnectWithCredentials {
    verbose: bool,
    pedantic: bool,
    name: String,
    user: String,
    pass: String,
}

impl ConnectWithCredentials {
    pub fn into_json(self) -> serde_json::Result<String> {
        let mut map = serde_json::Map::new();
        map.insert("verbose", Value::Bool(self.verbose));
        map.insert("pedantic", Value::Bool(self.pedantic));
        map.insert("name", Value::String(self.name));
        map.insert("user", Value::String(self.user));
        map.insert("pass", Value::String(self.pass));
        serde_json::to_string(&map)
    }
}

#[derive(Debug)]
pub struct Channel {
    pub sid: u64,
}

#[derive(Debug)]
pub struct Event {
    pub subject: String,
    pub channel: Channel,
    pub msg: Vec<u8>,
    pub inbox: Option<String>,
}

pub struct Events<'t> {
    client: &'t mut Client,
}

impl Client {
    pub fn new<T: ToStringVec>(uris: T) -> Result<Client, NatsError> {
        let mut servers_info = Vec::new();
        for uri in uris.to_string_vec() {
            let parsed = parse_nats_uri(&uri)?;
            let host = parsed
                .host_str()
                .ok_or((InvalidClientConfig, "Missing host name"))?
                .to_owned();
            let port = parsed.port().unwrap_or(DEFAULT_PORT);
            let credentials = match (parsed.username(), parsed.password()) {
                ("", None) => None,
                ("", Some(_)) => {
                    return Err(NatsError::from(
                        (InvalidClientConfig, "Username can't be empty"),
                    ))
                }
                (_, None) => {
                    return Err(NatsError::from(
                        (InvalidClientConfig, "Password can't be empty"),
                    ))
                }
                (username, Some(password)) => Some(Credentials {
                    username: username.to_owned(),
                    password: password.to_owned(),
                }),
            };
            servers_info.push(ServerInfo {
                host: host,
                port: port,
                credentials: credentials,
                max_payload: 0,
                tls_required: false,
            })
        }
        thread_rng().shuffle(&mut servers_info);
        Ok(Client {
            servers_info: servers_info,
            server_idx: 0,
            verbose: false,
            pedantic: false,
            name: DEFAULT_NAME.to_owned(),
            state: None,
            sid: 1,
            circuit_breaker: None,
            tls_config: None,
        })
    }

    pub fn set_synchronous(&mut self, synchronous: bool) {
        self.verbose = synchronous;
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_owned();
    }

    pub fn set_tls_config(&mut self, config: TlsConfig) {
        self.tls_config = Some(config);
    }

    pub fn subscribe(&mut self, subject: &str, queue: Option<&str>) -> Result<Channel, NatsError> {
        subject_check(subject)?;
        let sid = self.sid;
        let cmd = match queue {
            None => format!("SUB {} {}\r\n", subject, sid),
            Some(queue) => {
                queue_check(queue)?;
                format!("SUB {} {} {}\r\n", subject, queue, sid)
            }
        };
        let verbose = self.verbose;
        self.maybe_connect()?;
        let res = self.with_reconnect(|mut state| -> Result<Channel, NatsError> {
            state.stream_writer.write_all(cmd.as_bytes())?;
            wait_ok(&mut state, verbose)?;
            Ok(Channel { sid: sid })
        });
        if res.is_ok() {
            self.sid = self.sid.wrapping_add(1);
        }
        res
    }

    pub fn unsubscribe(&mut self, channel: Channel) -> Result<(), NatsError> {
        let cmd = format!("UNSUB {}\r\n", channel.sid);
        let verbose = self.verbose;
        self.maybe_connect()?;
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            state.stream_writer.write_all(cmd.as_bytes())?;
            wait_ok(&mut state, verbose)?;
            Ok(())
        })
    }

    pub fn unsubscribe_after(&mut self, channel: Channel, max: u64) -> Result<(), NatsError> {
        let cmd = format!("UNSUB {} {}\r\n", channel.sid, max);
        let verbose = self.verbose;
        self.maybe_connect()?;
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            state.stream_writer.write_all(cmd.as_bytes())?;
            wait_ok(&mut state, verbose)?;
            Ok(())
        })
    }

    pub fn publish(&mut self, subject: &str, msg: &[u8]) -> Result<(), NatsError> {
        self.publish_with_optional_inbox(subject, msg, None)
    }

    pub fn make_request(&mut self, subject: &str, msg: &[u8]) -> Result<String, NatsError> {
        let mut rng = rand::thread_rng();
        let inbox: String = rng.gen_ascii_chars().take(16).collect();
        let sid = self.subscribe(&inbox, None)?;
        self.unsubscribe_after(sid, 1)?;
        self.publish_with_optional_inbox(subject, msg, Some(&inbox))?;
        Ok(inbox)
    }

    pub fn wait(&mut self) -> Result<Event, NatsError> {
        self.maybe_connect()?;
        self.with_reconnect(|state| -> Result<Event, NatsError> {
            let buf_reader = &mut state.buf_reader;
            loop {
                let mut line = String::new();
                match buf_reader.read_line(&mut line) {
                    Ok(line_len) if line_len < "PING\r\n".len() => {
                        return Err(NatsError::from((
                            ErrorKind::ServerProtocolError,
                            "Incomplete server response",
                        )))
                    }
                    Err(e) => return Err(NatsError::from(e)),
                    Ok(_) => {}
                };
                if line.starts_with("MSG ") {
                    return wait_read_msg(&line, buf_reader);
                }
                if line != "PING\r\n" {
                    return Err(NatsError::from((
                        ErrorKind::ServerProtocolError,
                        "Server sent an unexpected response",
                        line,
                    )));
                }
                let cmd = "PONG\r\n";
                state.stream_writer.write_all(cmd.as_bytes())?;
            }
        })
    }

    pub fn events(&mut self) -> Events {
        Events { client: self }
    }

    fn try_connect(&mut self) -> Result<(), NatsError> {
        let server_info = &mut self.servers_info[self.server_idx];
        let stream_reader = TcpStream::connect((&server_info.host as &str, server_info.port))
            .map(stream::Stream::Tcp)?;
        let mut stream_writer = stream_reader.try_clone()?;
        let mut buf_reader = BufReader::new(stream_reader);
        let mut line = String::new();
        match buf_reader.read_line(&mut line) {
            Ok(line_len) if line_len < "INFO {}".len() => {
                return Err(NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unexpected EOF",
                )))
            }
            Err(e) => return Err(NatsError::from(e)),
            Ok(_) => {}
        };
        if !line.starts_with("INFO ") {
            return Err(NatsError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Server INFO not received",
            )));
        }
        let obj: Value = de::from_str(&line[5..]).or_else(|_| {
            Err(NatsError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid JSON object sent by the server",
            )))
        })?;
        let obj = obj.as_object().ok_or_else(|| {
            NatsError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid JSON object sent by the \
                 server",
            ))
        })?;
        let max_payload = obj.get("max_payload")
            .ok_or_else(|| {
                NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Server didn't send the max payload size",
                ))
            })?
            .as_u64()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Received payload size is not a u64",
                )
            })?;
        if max_payload < 1 {
            return Err(NatsError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid max payload size received",
            )));
        }
        server_info.max_payload = max_payload as usize;
        server_info.tls_required = obj.get("tls_required")
            .ok_or_else(|| {
                NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Server didn't send tls_required",
                ))
            })?
            .as_bool()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Received tls_required is not a boolean",
                )
            })?;
        if server_info.tls_required {
            // Wrap connection with TLS
            let connector = self.tls_config
                .as_ref()
                .map_or(default_tls_connector()?, |c| c.clone().into_connector());
            stream_writer = connector
                .connect(&server_info.host, stream_writer.as_tcp()?)
                .map(|conn| stream::Stream::Ssl(stream::SslStream::new(conn)))
                .map_err(|e| {
                    NatsError::from((
                        TlsError,
                        "Failed to establish TLS connection",
                        e.description().to_owned(),
                    ))
                })?;
            buf_reader = BufReader::new(stream_writer.try_clone()?);
        }
        let auth_required = obj.get("auth_required")
            .ok_or_else(|| {
                NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Server didn't send auth_required",
                ))
            })?
            .as_bool()
            .ok_or_else(|| {
                NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Received auth_required is not a boolean",
                ))
            })?;
        let connect_json = match (auth_required, &server_info.credentials) {
            (true, &Some(ref credentials)) => {
                let connect = ConnectWithCredentials {
                    verbose: self.verbose,
                    pedantic: self.pedantic,
                    name: self.name.clone(),
                    user: credentials.username.clone(),
                    pass: credentials.password.clone(),
                };
                connect.into_json().or_else(|_| {
                    Err(NatsError::from(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Received auth_required is not a boolean",
                    )))
                })?
            }
            (false, _) | (_, &None) => {
                let connect = ConnectNoCredentials {
                    verbose: self.verbose,
                    pedantic: self.pedantic,
                    name: self.name.clone(),
                };
                connect.into_json().unwrap()
            }
        };
        let connect_string = format!("CONNECT {}\nPING\n", connect_json);
        let connect_bytes = connect_string.as_bytes();
        stream_writer.write_all(connect_bytes).unwrap();
        if self.verbose {
            let mut line = String::new();
            match buf_reader.read_line(&mut line) {
                Ok(line_len) if line_len != "+OK\r\n".len() => {
                    return Err(NatsError::from(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Unexpected EOF",
                    )))
                }
                Err(e) => return Err(NatsError::from(e)),
                Ok(_) => {}
            };
            if line != "+OK\r\n" {
                return Err(NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Server +OK not received",
                )));
            }
        }
        let mut line = String::new();
        match buf_reader.read_line(&mut line) {
            Ok(line_len) if line_len != "PONG\r\n".len() => {
                return Err(NatsError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unexpected EOF",
                )))
            }
            Err(e) => return Err(NatsError::from(e)),
            Ok(_) => {}
        };
        if line != "PONG\r\n" {
            return Err(NatsError::from(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Server PONG not received",
            )));
        }
        let state = ClientState {
            stream_writer: stream_writer,
            buf_reader: buf_reader,
            max_payload: max_payload as usize,
        };
        self.state = Some(state);
        Ok(())
    }

    fn connect(&mut self) -> Result<(), NatsError> {
        if let Some(circuit_breaker) = self.circuit_breaker {
            if circuit_breaker.elapsed() <
                Duration::from_millis(CIRCUIT_BREAKER_WAIT_AFTER_BREAKING_MS)
            {
                return Err(NatsError::from((
                    ErrorKind::ServerProtocolError,
                    "Cluster down - Connections are temporarily \
                     suspended",
                )));
            }
            self.circuit_breaker = None;
        }
        self.state = None;
        let servers_count = self.servers_info.len();
        for _ in 0..CIRCUIT_BREAKER_ROUNDS_BEFORE_BREAKING {
            for _ in 0..servers_count {
                let result = self.try_connect();
                if let Err(true) = result.as_ref().map_err(|e| e.kind() == TlsError) {
                    return result;
                }
                if result.is_ok() {
                    if self.state.is_none() {
                        panic!("Inconsistent state");
                    }
                    return Ok(());
                }
                self.server_idx = (self.server_idx + 1) % servers_count;
            }
            thread::sleep(Duration::from_millis(
                CIRCUIT_BREAKER_WAIT_BETWEEN_ROUNDS_MS,
            ));
        }
        self.circuit_breaker = Some(Instant::now());
        Err(NatsError::from((
            ErrorKind::ServerProtocolError,
            "The entire cluster is down or unreachable",
        )))
    }

    fn reconnect(&mut self) -> Result<(), NatsError> {
        if let Some(mut state) = self.state.take() {
            let _ = state.stream_writer.flush();
        }
        self.connect()
    }

    fn maybe_connect(&mut self) -> Result<(), NatsError> {
        if self.state.is_none() {
            return self.connect();
        }
        Ok(())
    }

    fn with_reconnect<F, T>(&mut self, f: F) -> Result<T, NatsError>
    where
        F: Fn(&mut ClientState) -> Result<T, NatsError>,
    {
        let mut res: Result<T, NatsError> = Err(NatsError::from((ErrorKind::IoError, "I/O error")));
        for _ in 0..RETRIES_MAX {
            let mut state = self.state.take().unwrap();
            res = match f(&mut state) {
                e @ Err(_) => match self.reconnect() {
                    Err(e) => return Err(e),
                    Ok(_) => e,
                },
                res @ Ok(_) => {
                    self.state = Some(state);
                    return res;
                }
            };
        }
        res
    }

    fn publish_with_optional_inbox(
        &mut self,
        subject: &str,
        msg: &[u8],
        inbox: Option<&str>,
    ) -> Result<(), NatsError> {
        subject_check(subject)?;
        let msg_len = msg.len();
        let cmd = match inbox {
            None => format!("PUB {} {}\r\n", subject, msg_len),
            Some(inbox) => {
                inbox_check(inbox)?;
                format!("PUB {} {} {}\r\n", subject, inbox, msg_len)
            }
        };
        let mut cmd: Vec<u8> = cmd.as_bytes().to_owned();
        let cmd_len0 = cmd.len();
        cmd.reserve(cmd_len0 + msg_len + 2);
        cmd.extend_from_slice(msg);
        cmd.push(0x0d);
        cmd.push(0x0a);
        let verbose = self.verbose;
        self.maybe_connect()?;
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            let max_payload = state.max_payload;
            if cmd.len() > max_payload {
                return Err(NatsError::from((
                    ErrorKind::ClientProtocolError,
                    "Message too large",
                    format!("Maximum payload size is {} bytes", max_payload),
                )));
            }
            state.stream_writer.write_all(&cmd)?;
            wait_ok(&mut state, verbose)?;
            Ok(())
        })
    }
}

impl<'t> Iterator for Events<'t> {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let client = &mut self.client;
        match client.wait() {
            Ok(event) => Some(event),
            Err(_) => None,
        }
    }
}

pub trait ToStringVec {
    fn to_string_vec(self) -> Vec<String>;
}

impl ToStringVec for String {
    fn to_string_vec(self) -> Vec<String> {
        vec![self]
    }
}

impl<'t> ToStringVec for &'t str {
    fn to_string_vec(self) -> Vec<String> {
        vec![self.to_owned()]
    }
}

impl ToStringVec for Vec<String> {
    fn to_string_vec(self) -> Vec<String> {
        self
    }
}

impl<'t> ToStringVec for Vec<&'t str> {
    fn to_string_vec(self) -> Vec<String> {
        self.iter().map(|&x| x.to_owned()).collect()
    }
}

fn space_check(name: &str, errmsg: &'static str) -> Result<(), NatsError> {
    if name.contains(' ') {
        return Err(NatsError::from((ErrorKind::ClientProtocolError, errmsg)));
    }
    Ok(())
}

fn subject_check(subject: &str) -> Result<(), NatsError> {
    space_check(subject, "A subject cannot contain spaces")
}

fn inbox_check(inbox: &str) -> Result<(), NatsError> {
    space_check(inbox, "An inbox name cannot contain spaces")
}

fn queue_check(queue: &str) -> Result<(), NatsError> {
    space_check(queue, "A queue name cannot contain spaces")
}

fn parse_nats_uri(uri: &str) -> Result<Url, NatsError> {
    let url = Url::parse(uri)?;
    if url.scheme() != URI_SCHEME {
        Err(NatsError::from(
            (ErrorKind::InvalidSchemeError, "Unsupported scheme"),
        ))
    } else {
        Ok(url)
    }
}

fn read_exact<R: BufRead + ?Sized>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<usize> {
    let len = buf.len();
    let mut to_read = len;
    buf.clear();
    while to_read > 0 {
        let used = {
            let buffer = match reader.fill_buf() {
                Ok(buffer) => buffer,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            let used = cmp::min(buffer.len(), to_read);
            buf.extend_from_slice(&buffer[..used]);
            used
        };
        reader.consume(used);
        to_read -= used;
    }
    Ok(len)
}

fn wait_ok(state: &mut ClientState, verbose: bool) -> Result<(), NatsError> {
    if !verbose {
        return Ok(());
    }
    let buf_reader = &mut state.buf_reader;
    let mut line = String::new();
    match buf_reader.read_line(&mut line) {
        Ok(line_len) if line_len < "OK\r\n".len() => {
            return Err(NatsError::from((
                ErrorKind::ServerProtocolError,
                "Incomplete server response",
            )))
        }
        Err(e) => return Err(NatsError::from(e)),
        Ok(_) => {}
    };
    match line.as_ref() {
        "+OK\r\n" => {}
        "PING\r\n" => {
            let pong = b"PONG\r\n";
            state.stream_writer.write_all(pong)?;
        }
        _ => {
            return Err(NatsError::from((
                ErrorKind::ServerProtocolError,
                "Received unexpected response from the server",
                line,
            )))
        }
    }
    Ok(())
}

fn wait_read_msg(
    line: &str,
    buf_reader: &mut BufReader<stream::Stream>,
) -> Result<Event, NatsError> {
    if line.len() < "MSG _ _ _\r\n".len() {
        return Err(NatsError::from((
            ErrorKind::ServerProtocolError,
            "Incomplete server response",
            line.to_owned(),
        )));
    }
    let line = line.trim_right();
    let mut parts = line[4..].split(' ');
    let subject = parts.next().ok_or_else(|| {
        NatsError::from((
            ErrorKind::ServerProtocolError,
            "Unsupported server response",
            line.to_owned(),
        ))
    })?;
    let sid: u64 = parts
        .next()
        .ok_or_else(|| {
            NatsError::from((
                ErrorKind::ServerProtocolError,
                "Unsupported server response",
                line.to_owned(),
            ))
        })?
        .parse()
        .unwrap_or(0);
    let inbox_or_len_s = parts.next().ok_or_else(|| {
        NatsError::from((
            ErrorKind::ServerProtocolError,
            "Unsupported server response",
            line.to_owned(),
        ))
    })?;
    let mut inbox: Option<String> = None;
    let len_s = match parts.next() {
        None => inbox_or_len_s,
        Some(len_s) => {
            inbox = Some(inbox_or_len_s.to_owned());
            len_s
        }
    };
    let len: usize = len_s.parse().ok().ok_or_else(|| {
        NatsError::from((
            ErrorKind::ServerProtocolError,
            "Suspicous message length",
            format!("{} (len: [{}])", line, len_s),
        ))
    })?;
    let mut msg: Vec<u8> = vec![0; len];
    read_exact(buf_reader, &mut msg)?;
    let mut crlf: Vec<u8> = vec![0; 2];
    read_exact(buf_reader, &mut crlf)?;
    if crlf[0] != 0x0d || crlf[1] != 0x0a {
        return Err(NatsError::from((
            ErrorKind::ServerProtocolError,
            "Missing CRLF after a message",
            line.to_owned(),
        )));
    }
    let event = Event {
        subject: subject.to_owned(),
        channel: Channel { sid: sid },
        msg: msg,
        inbox: inbox,
    };
    Ok(event)
}

fn default_tls_connector() -> Result<SslConnector, NatsError> {
    Ok(SslConnectorBuilder::new(SslMethod::tls())?.build())
}

#[test]
fn client_test() {
    let mut client = Client::new(vec!["nats://user:password@127.0.0.1"]).unwrap();
    client.set_synchronous(false);
    client.set_name("test");
    client.subscribe("chan", None).unwrap();
    client.publish("chan", b"test").unwrap();
    client.wait().unwrap();
    let s = client.subscribe("chan2", Some("queue")).unwrap();
    client.unsubscribe(s).unwrap();
    client.make_request("chan", b"test").unwrap();
    client.wait().unwrap();
    client.subscribe("chan.*", None).unwrap();
    client.publish("chan", b"test1").unwrap();
    client.publish("chan", b"test2").unwrap();
    client.publish("chan", b"test3").unwrap();
    client.publish("chan.last", b"test4").unwrap();
    client
        .events()
        .find(|event| event.subject == "chan.last")
        .unwrap();
}
