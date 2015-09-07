extern crate rand;
extern crate url;
extern crate serde;
extern crate serde_json;
extern crate time;

use errors::*;
use errors::ErrorKind::*;
use self::rand::{thread_rng, Rng};
use self::serde_json::de;
use self::serde_json::value::Value;
use self::time::{Duration, SteadyTime};
use self::url::{ParseError, ParseResult, SchemeType, Url, UrlParser};
use std::cmp;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;

const CIRCUIT_BREAKER_WAIT_AFTER_BREAKING_MS: i64 = 2000;
const CIRCUIT_BREAKER_WAIT_BETWEEN_ROUNDS_MS: u32 = 250;
const CIRCUIT_BREAKER_ROUNDS_BEFORE_BREAKING: u32 = 4;
const DEFAULT_NAME: &'static str = "#rustlang";
const DEFAULT_PORT: u16 = 4222;
const URI_SCHEME: &'static str = "nats";
const RETRIES_MAX: u32 = 10;

#[derive(Clone, Debug)]
struct Credentials {
    username: String,
    password: String
}

#[derive(Clone, Debug)]
struct ServerInfo {
    host: String,
    port: u16,
    credentials: Option<Credentials>,
    max_payload: usize
}

#[derive(Debug)]
struct ClientState {
    stream_writer: TcpStream,
    buf_reader: BufReader<TcpStream>,
    max_payload: usize
}

#[derive(Debug)]
pub struct Client {
    servers_info: Vec<ServerInfo>,
    server_idx: usize,
    verbose: bool,
    pedantic: bool,
    name: String,
    state: Option<ClientState>,
    circuit_breaker: Option<SteadyTime>,
    sid: u64
}

#[derive(Serialize, Debug)]
struct ConnectNoCredentials {
    verbose: bool,
    pedantic: bool,
    name: String
}

#[derive(Serialize, Debug)]
struct ConnectWithCredentials {
    verbose: bool,
    pedantic: bool,
    name: String,
    user: String,
    pass: String
}

#[derive(Clone, Debug)]
pub struct Channel {
    sid: u64
}

#[derive(Clone, Debug)]
pub struct Event {
    subject: String,
    channel: Channel,
    msg: Vec<u8>,
    inbox: Option<String>
}

impl Client {
    pub fn new<T: ToStringVec>(uris: T) -> Result<Client, NatsError> {
        let mut servers_info = Vec::new();
        for uri in uris.to_string_vec() {
            let parsed = try!(parse_nats_uri(&uri));
            let host = try!(parsed.serialize_host().ok_or((InvalidClientConfig, "Missing host name")));
            let port = try!(parsed.port_or_default().ok_or((InvalidClientConfig, "Invalid port number")));
            let credentials = match (parsed.username(), parsed.password()) {
                (None, None) | (Some(""), None) => None,
                (Some(username), Some(password)) => Some(Credentials {
                    username: username.to_owned(), password: password.to_owned()
                }),
                (None, Some(_)) => return Err(NatsError::from((InvalidClientConfig, "Username can't be empty"))),
                (Some(_), None) => return Err(NatsError::from((InvalidClientConfig, "Password can't be empty"))),
            };
            servers_info.push(ServerInfo {
                host: host,
                port: port,
                credentials: credentials,
                max_payload: 0
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
            circuit_breaker: None
        })
    }

    pub fn set_synchronous(&mut self, synchronous: bool) {
        self.verbose = synchronous;
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn subscribe(&mut self, subject: &str, queue: Option<&str>) -> Result<Channel, NatsError> {
        try!(subject_check(subject));
        let sid = self.sid;
        let cmd = match queue {
            None => format!("SUB {} {}\r\n", subject, sid),
            Some(queue) => {
                try!(queue_check(queue));
                format!("SUB {} {} {}\r\n", subject, queue, sid)
            }
        };
        let verbose = self.verbose;
        try!(self.maybe_connect());
        let res = self.with_reconnect(|mut state| -> Result<Channel, NatsError> {
            try!(state.stream_writer.write_all(cmd.as_bytes()));
            try!(wait_ok(&mut state, verbose));
            Ok(Channel {
                sid: sid
            })
        });
        if res.is_ok() {
            self.sid = self.sid.wrapping_add(1);
        }
        res
    }

    pub fn unsubscribe(&mut self, channel: Channel) -> Result<(), NatsError> {
        let cmd = format!("UNSUB {}\r\n", channel.sid);
        let verbose = self.verbose;
        try!(self.maybe_connect());
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            try!(state.stream_writer.write_all(cmd.as_bytes()));
            try!(wait_ok(&mut state, verbose));
            Ok(())
        })
    }

    pub fn unsubscribe_after(&mut self, channel: Channel, max: u64) -> Result<(), NatsError> {
        let cmd = format!("UNSUB {} {}\r\n", channel.sid, max);
        let verbose = self.verbose;
        try!(self.maybe_connect());
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            try!(state.stream_writer.write_all(cmd.as_bytes()));
            try!(wait_ok(&mut state, verbose));
            Ok(())
        })
    }

    pub fn publish(&mut self, subject: &str, msg: &[u8]) -> Result<(), NatsError> {
        self.publish_with_optional_inbox(subject, msg, None)
    }

    pub fn make_request(&mut self, subject: &str, msg: &[u8]) -> Result<String, NatsError> {
        let mut rng = rand::thread_rng();
        let inbox: String = rng.gen_ascii_chars().take(16).collect();
        let sid = try!(self.subscribe(&inbox, None));
        try!(self.unsubscribe_after(sid, 1));
        try!(self.publish_with_optional_inbox(subject, msg, Some(&inbox)));
        Ok(inbox)
    }

    pub fn wait(&mut self) -> Result<Option<Event>, NatsError> {
        try!(self.maybe_connect());
        self.with_reconnect(|mut state| -> Result<Option<Event>, NatsError> {
            let mut buf_reader = &mut state.buf_reader;
            loop {
                let mut line = String::new();
                match buf_reader.read_line(&mut line) {
                    Ok(line_len) if line_len < "PING\r\n".len() =>
                        return Err(NatsError::from((ErrorKind::ServerProtocolError, "Incomplete server response"))),
                    Err(e) => return Err(NatsError::from(e)),
                    Ok(_) => { }
                };
                if line.starts_with("MSG ") {
                    return wait_read_msg(line, buf_reader)
                }
                if line != "PING\r\n" {
                    return Err(NatsError::from((ErrorKind::ServerProtocolError, "Server sent an unexpected response", line)));
                }
                let cmd = "PONG\r\n";
                try!(state.stream_writer.write_all(cmd.as_bytes()));
            }
        })
    }

    fn try_connect(&mut self) -> io::Result<()> {
        let server_info = &mut self.servers_info[self.server_idx];
        let stream_reader = try!(TcpStream::connect((&server_info.host as &str, server_info.port)));
        let mut stream_writer = try!(stream_reader.try_clone());
        let mut buf_reader = BufReader::new(stream_reader);
        let mut line = String::new();
        match buf_reader.read_line(&mut line) {
            Ok(line_len) if line_len < "INFO {}".len() =>
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unexpected EOF")),
            Err(e) => return Err(e),
            Ok(_) => { }
        };
        if line.starts_with("INFO ") == false {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Server INFO not received"));
        }
        let obj: Value = try!(de::from_str(&line[5..]).
            or(Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid JSON object sent by the server"))));
        let obj = try!(obj.as_object().
            ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Invalid JSON object sent by the server")));
        let max_payload = try!(try!(obj.get("max_payload").
            ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Server didn't send the max payload size"))).
            as_u64().ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Received payload size is not a u64")));
        if max_payload < 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid max payload size received"));
        }
        server_info.max_payload = max_payload as usize;
        let auth_required = try!(try!(obj.get("auth_required").
            ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Server didn't send auth_required"))).
            as_boolean().ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Received auth_required is not a boolean")));
        let connect_json = match (auth_required, &server_info.credentials) {
            (true, &Some(ref credentials)) => {
                let connect = ConnectWithCredentials {
                    verbose: self.verbose,
                    pedantic: self.pedantic,
                    name: self.name.clone(),
                    user: credentials.username.clone(),
                    pass: credentials.password.clone()
                };
                try!(serde_json::to_string(&connect).or(Err(io::Error::new(io::ErrorKind::InvalidInput, "Received auth_required is not a boolean"))))
            }
            (false, _) | (_, &None) => {
                let connect = ConnectNoCredentials {
                    verbose: self.verbose,
                    pedantic: self.pedantic,
                    name: self.name.clone()
                };
                serde_json::to_string(&connect).unwrap()
            }
        };
        let connect_string = format!("CONNECT {}\nPING\n", connect_json);
        let connect_bytes = connect_string.as_bytes();
        stream_writer.write_all(connect_bytes).unwrap();
        if self.verbose {
            let mut line = String::new();
            match buf_reader.read_line(&mut line) {
                Ok(line_len) if line_len != "+OK\r\n".len() =>
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unexpected EOF")),
                Err(e) => return Err(e),
                Ok(_) => { }
            };
            if line != "+OK\r\n" {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Server +OK not received"));
            }
        }
        let mut line = String::new();
        match buf_reader.read_line(&mut line) {
            Ok(line_len) if line_len != "PONG\r\n".len() =>
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Unexpected EOF")),
            Err(e) => return Err(e),
            Ok(_) => { }
        };
        if line != "PONG\r\n" {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Server PONG not received"));
        }
        let state = ClientState {
            stream_writer: stream_writer,
            buf_reader: buf_reader,
            max_payload: max_payload as usize
        };
        self.state = Some(state);
        Ok(())
    }

    fn connect(&mut self) -> Result<(), NatsError> {
        if let Some(circuit_breaker) = self.circuit_breaker {
            if SteadyTime::now() - circuit_breaker < Duration::milliseconds(CIRCUIT_BREAKER_WAIT_AFTER_BREAKING_MS) {
                return Err(NatsError::from((ErrorKind::ServerProtocolError, "Cluster down - Connections are temporarily suspended")));
            }
            self.circuit_breaker = None;
        }
        self.state = None;
        let servers_count = self.servers_info.len();
        for _ in (0..CIRCUIT_BREAKER_ROUNDS_BEFORE_BREAKING) {
            for _ in (0..servers_count) {
                if self.try_connect().is_ok() {
                    if self.state.is_none() {
                        panic!("Inconsistent state");
                    }
                    return Ok(());
                }
                self.server_idx = (self.server_idx + 1) % servers_count;
            }
            thread::sleep_ms(CIRCUIT_BREAKER_WAIT_BETWEEN_ROUNDS_MS);
        }
        self.circuit_breaker = Some(SteadyTime::now());
        Err(NatsError::from((ErrorKind::ServerProtocolError,
            "The entire cluster is down or unreachable")))
    }

    fn reconnect(&mut self) -> Result<(), NatsError> {
        if let Some(mut state) = self.state.take() {
            let _ = state.stream_writer.flush();
        }
        self.connect()
    }

    fn maybe_connect(&mut self) -> Result<(), NatsError> {
        if self.state.is_none() {
            return self.connect()
        }
        Ok(())
    }

    fn with_reconnect<F, T>(&mut self, f: F) -> Result<T, NatsError> where F: Fn(&mut ClientState) -> Result<T, NatsError> {
        let mut res: Result<T, NatsError> = Err(NatsError::from((ErrorKind::IoError, "I/O error")));
        for _ in (0..RETRIES_MAX) {
            let mut state = self.state.take().unwrap();
            res = match f(&mut state) {
                e @ Err(_) => match self.reconnect() {
                    Err(e) => return Err(e),
                    Ok(_) => e
                },
                res @ Ok(_) => {
                    self.state = Some(state);
                    return res;
                }
            };
        }
        res
    }

    fn publish_with_optional_inbox(&mut self, subject: &str, msg: &[u8], inbox: Option<&str>) -> Result<(), NatsError> {
        try!(subject_check(subject));
        let msg_len = msg.len();
        let cmd = match inbox {
            None => format!("PUB {} {}\r\n", subject, msg_len),
            Some(inbox) => {
                try!(inbox_check(inbox));
                format!("PUB {} {} {}\r\n", subject, inbox, msg_len)
            }
        };
        let mut cmd: Vec<u8> = cmd.as_bytes().to_owned();
        let cmd_len0 = cmd.len();
        cmd.reserve(cmd_len0 + msg_len + 2);
        cmd.push_all(msg);
        cmd.push(0x0d);
        cmd.push(0x0a);
        let verbose = self.verbose;
        try!(self.maybe_connect());
        self.with_reconnect(|mut state| -> Result<(), NatsError> {
            let max_payload = state.max_payload;
            if cmd.len() > max_payload {
                return Err(NatsError::from((ErrorKind::ClientProtocolError, "Message too large",
                    format!("Maximum payload size is {} bytes", max_payload))));
            }
            try!(state.stream_writer.write_all(&cmd));
            try!(wait_ok(&mut state, verbose));
            Ok(())
        })
    }
}

pub trait ToStringVec {
    fn to_string_vec(self) -> Vec<String>;
}

impl ToStringVec for String {
    fn to_string_vec(self) -> Vec<String> {
        vec!(self)
    }
}

impl<'t> ToStringVec for &'t str {
    fn to_string_vec(self) -> Vec<String> {
        vec!(self.to_owned())
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

fn nats_scheme_type_mapper(scheme: &str) -> SchemeType {
    match scheme {
        URI_SCHEME => SchemeType::Relative(DEFAULT_PORT),
        _ => SchemeType::NonRelative
    }
}

fn parse_nats_uri(uri: &str) -> ParseResult<Url> {
    let mut parser = UrlParser::new();
    parser.scheme_type_mapper(nats_scheme_type_mapper);
    match parser.parse(uri) {
        Ok(res) => {
            if res.scheme == URI_SCHEME {
                Ok(res)
            } else {
                Err(ParseError::InvalidScheme)
            }
        },
        e => e
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
                Err(e) => return Err(e)
            };
            let used = cmp::min(buffer.len(), to_read);
            buf.push_all(&buffer[..used]);
            used
        };
        reader.consume(used);
        to_read -= used;
    }
    Ok(len)
}

fn wait_ok(state: &mut ClientState, verbose: bool) -> Result<(), NatsError> {
    if verbose == false {
        return Ok(());
    }
    let mut buf_reader = &mut state.buf_reader;
    let mut line = String::new();
    match buf_reader.read_line(&mut line) {
        Ok(line_len) if line_len < "OK\r\n".len() =>
            return Err(NatsError::from((ErrorKind::ServerProtocolError, "Incomplete server response"))),
        Err(e) => return Err(NatsError::from(e)),
        Ok(_) => { }
    };
    match line.as_ref() {
        "+OK\r\n" => { },
        "PING\r\n" => {
            let pong = "PONG\r\n".as_bytes();
            try!(state.stream_writer.write_all(pong));
        },
        _ => return Err(NatsError::from((ErrorKind::ServerProtocolError,
                        "Received unexpected response from the server", line)))
    }
    Ok(())
}

fn wait_read_msg(line: String, buf_reader: &mut BufReader<TcpStream>) -> Result<Option<Event>, NatsError> {
    if line.len() < "MSG _ _ _\r\n".len() {
        return Err(NatsError::from((ErrorKind::ServerProtocolError, "Incomplete server response", line.clone())));
    }
    let line = line.trim_right();
    let mut parts = line[4..].split(' ');
    let subject = try!(parts.next().
        ok_or(NatsError::from((ErrorKind::ServerProtocolError, "Unsupported server response", line.to_owned()))));
    let sid: u64 = try!(parts.next().
        ok_or(NatsError::from((ErrorKind::ServerProtocolError, "Unsupported server response", line.to_owned())))).
        parse().unwrap_or(0);
    let inbox_or_len_s = try!(parts.next().
        ok_or(NatsError::from((ErrorKind::ServerProtocolError, "Unsupported server response", line.to_owned()))));
    let mut inbox: Option<String> = None;
    let len_s = match parts.next() {
        None => inbox_or_len_s,
        Some(len_s) => {
            inbox = Some(inbox_or_len_s.to_owned());
            len_s
        }
    };
    let len: usize = try!(len_s.parse().ok().
        ok_or(NatsError::from((ErrorKind::ServerProtocolError, "Suspicous message length",
        format!("{} (len: [{}])", line, len_s)))));
    let mut msg: Vec<u8> = vec![0; len];
    try!(read_exact(buf_reader, &mut msg));
    let mut crlf: Vec<u8> = vec![0; 2];
    try!(read_exact(buf_reader, &mut crlf));
    if crlf[0] != 0x0d || crlf[1] != 0x0a {
        return Err(NatsError::from((ErrorKind::ServerProtocolError, "Missing CRLF after a message", line.to_owned())))
    }
    let event = Event {
        subject: subject.to_owned(),
        channel: Channel {
            sid: sid
        },
        msg: msg,
        inbox: inbox
    };
    Ok(Some(event))
}

#[test]
fn client_test() {
    let mut client = Client::new(vec!("nats://user:password@127.0.0.1")).unwrap();
    client.set_synchronous(false);
    client.set_name("test".to_owned());
    client.subscribe("chan", None).unwrap();
    client.publish("chan", "test".as_bytes()).unwrap();
    client.wait().unwrap();
    let s = client.subscribe("chan2", Some("queue")).unwrap();
    client.unsubscribe(s).unwrap();
    client.make_request("chan", "test".as_bytes()).unwrap();
    client.wait().unwrap();
}
