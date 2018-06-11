#![allow(dead_code)]

extern crate openssl;
extern crate url;

use std::error::Error;
use std::fmt;
use std::io;
use std::str::Utf8Error;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum ErrorKind {
    ClientProtocolError,
    InvalidClientConfig,
    IoError,
    InvalidSchemeError,
    ServerProtocolError,
    TypeError,
    TlsError,
}

#[derive(Debug)]
enum ErrorRepr {
    WithDescription(ErrorKind, &'static str),
    WithDescriptionAndDetail(ErrorKind, &'static str, String),
    IoError(io::Error),
    UrlParseError(url::ParseError),
}

#[derive(Debug)]
pub struct NatsError {
    repr: ErrorRepr,
}

impl NatsError {
    pub fn kind(&self) -> ErrorKind {
        match self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => kind,
            ErrorRepr::IoError(_) => ErrorKind::IoError,
            ErrorRepr::UrlParseError(_) => ErrorKind::InvalidSchemeError,
        }
    }
}

impl Error for NatsError {
    fn description(&self) -> &str {
        match self.repr {
            ErrorRepr::WithDescription(_, description)
            | ErrorRepr::WithDescriptionAndDetail(_, description, _) => description,
            ErrorRepr::IoError(ref e) => e.description(),
            ErrorRepr::UrlParseError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self.repr {
            ErrorRepr::IoError(ref e) => Some(e as &Error),
            _ => None,
        }
    }
}

impl fmt::Display for NatsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.repr {
            ErrorRepr::WithDescription(_, description) => description.fmt(f),
            ErrorRepr::WithDescriptionAndDetail(_, description, ref detail) => {
                description.fmt(f)?;
                f.write_str(": ")?;
                detail.fmt(f)
            }
            ErrorRepr::IoError(ref e) => e.fmt(f),
            ErrorRepr::UrlParseError(ref e) => e.fmt(f),
        }
    }
}

impl From<Utf8Error> for NatsError {
    fn from(_: Utf8Error) -> NatsError {
        NatsError {
            repr: ErrorRepr::WithDescription(ErrorKind::TypeError, "Invalid UTF-8"),
        }
    }
}

impl From<(ErrorKind, &'static str)> for NatsError {
    fn from((kind, description): (ErrorKind, &'static str)) -> NatsError {
        NatsError {
            repr: ErrorRepr::WithDescription(kind, description),
        }
    }
}

impl From<(ErrorKind, &'static str, String)> for NatsError {
    fn from((kind, description, detail): (ErrorKind, &'static str, String)) -> NatsError {
        NatsError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, description, detail),
        }
    }
}

impl From<io::Error> for NatsError {
    fn from(e: io::Error) -> NatsError {
        NatsError {
            repr: ErrorRepr::IoError(e),
        }
    }
}

impl From<openssl::error::ErrorStack> for NatsError {
    fn from(e: openssl::error::ErrorStack) -> NatsError {
        NatsError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::TlsError,
                "",
                e.description().to_owned(),
            ),
        }
    }
}

impl From<url::ParseError> for NatsError {
    fn from(e: url::ParseError) -> NatsError {
        NatsError {
            repr: ErrorRepr::UrlParseError(e),
        }
    }
}
