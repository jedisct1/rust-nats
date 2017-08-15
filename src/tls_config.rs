extern crate openssl;

use errors::*;
use std::fmt;
use self::openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod};
use self::openssl::x509::X509;
use self::openssl::pkey::PKey;

#[derive(Clone)]
pub struct TlsConfig(SslConnector);

pub struct TlsConfigBuilder(SslConnectorBuilder);

impl TlsConfigBuilder {
    pub fn new() -> Result<TlsConfigBuilder, NatsError> {
        Ok(TlsConfigBuilder(
            SslConnectorBuilder::new(SslMethod::tls())?,
        ))
    }

    pub fn add_root_certificate(&mut self, cert: X509) -> Result<&mut Self, NatsError> {
        self.0.builder_mut().cert_store_mut().add_cert(cert)?;
        Ok(self)
    }

    pub fn add_client_certificate(
        &mut self,
        cert: X509,
        key: PKey,
    ) -> Result<&mut Self, NatsError> {
        {
            let ctx = self.0.builder_mut();
            ctx.set_certificate(&cert)?;
            ctx.set_private_key(&key)?;
            ctx.check_private_key()?;
        }
        Ok(self)
    }

    pub fn build(self) -> TlsConfig {
        TlsConfig(self.0.build())
    }
}

impl TlsConfig {
    pub fn into_connector(self) -> SslConnector {
        self.0
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TlsConfig {{}}")
    }
}
