extern crate openssl;

use self::openssl::{
    pkey::{PKey, Private},
    ssl::{SslConnector, SslConnectorBuilder, SslMethod},
    x509::X509,
};
use errors::*;
use std::fmt;

#[derive(Clone)]
pub struct TlsConfig(SslConnector);

pub struct TlsConfigBuilder(SslConnectorBuilder);

impl TlsConfigBuilder {
    pub fn new() -> Result<TlsConfigBuilder, NatsError> {
        Ok(TlsConfigBuilder(SslConnector::builder(SslMethod::tls())?))
    }

    pub fn add_root_certificate(&mut self, cert: X509) -> Result<&mut Self, NatsError> {
        self.0.cert_store_mut().add_cert(cert)?;
        Ok(self)
    }

    pub fn add_client_certificate(
        &mut self,
        cert: X509,
        key: PKey<Private>,
    ) -> Result<&mut Self, NatsError> {
        {
            let ctx = &mut self.0;
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
