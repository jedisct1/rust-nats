extern crate openssl;

use self::openssl::ssl;
use std::io;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use self::Stream::{Ssl, Tcp};

#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Ssl(SslStream),
}

impl Stream {
    pub fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Tcp(ref s) => Ok(Tcp(s.try_clone()?)),
            Ssl(ref s) => Ok(Ssl(s.clone())),
        }
    }

    pub fn as_tcp(&self) -> io::Result<TcpStream> {
        match *self {
            Tcp(ref s) => s.try_clone(),
            Ssl(ref s) => s.as_tcp(),
        }
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Tcp(ref mut s) => s.read(buf),
            Ssl(ref mut s) => s.read(buf),
        }
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Tcp(ref mut s) => s.write(buf),
            Ssl(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Tcp(ref mut s) => s.flush(),
            Ssl(ref mut s) => s.flush(),
        }
    }
}

// Clonable TLS Stream
#[derive(Debug, Clone)]
pub struct SslStream(Arc<Mutex<ssl::SslStream<TcpStream>>>);

impl SslStream {
    pub fn new(stream: ssl::SslStream<TcpStream>) -> SslStream {
        SslStream(Arc::new(Mutex::new(stream)))
    }

    pub fn as_tcp(&self) -> io::Result<TcpStream> {
        self.0.lock().unwrap().get_ref().try_clone()
    }
}

impl io::Read for SslStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.lock().unwrap().read(buf)
    }
}

impl io::Write for SslStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}
