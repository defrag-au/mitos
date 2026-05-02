//! Direction-agnostic WebSocket transport.
//!
//! `IndexerHandle::run_subscriber` runs the same protocol logic in
//! both directions — server-side (`/replicate/{indexer}` accepting
//! upgrades from synthetic test clients) and client-side
//! (`Replicator` dialing outbound to CF DOs for production use).
//! Object-safe so `dyn IndexerHandle` can take `Box<dyn WsTransport>`.

use async_trait::async_trait;

/// One frame of binary data on a WebSocket. We only use Binary frames
/// (CBOR-encoded envelopes) — text/ping/pong are handled inside each
/// transport impl as appropriate (e.g. tungstenite auto-replies to
/// pings).
#[async_trait]
pub trait WsTransport: Send {
    /// Receive the next binary frame. Returns `Ok(None)` when the peer
    /// has closed cleanly. Non-binary frames are silently skipped.
    async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>>;

    /// Send one binary frame.
    async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()>;
}

/// `axum::extract::ws::WebSocket` impl — used by the server-side
/// `/replicate/{indexer}` endpoint.
mod axum_impl {
    use super::WsTransport;

    use async_trait::async_trait;
    use axum::extract::ws::{Message, WebSocket};
    use futures_util::StreamExt;

    pub struct AxumWs(pub WebSocket);

    #[async_trait]
    impl WsTransport for AxumWs {
        async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
            loop {
                match self.0.next().await {
                    Some(Ok(Message::Binary(b))) => return Ok(Some(b.into())),
                    Some(Ok(Message::Close(_))) | None => return Ok(None),
                    // Skip text/ping/pong; axum auto-handles ping.
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Err(anyhow::anyhow!("ws recv: {e}")),
                }
            }
        }

        async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
            self.0
                .send(Message::Binary(bytes.into()))
                .await
                .map_err(|e| anyhow::anyhow!("ws send: {e}"))
        }
    }
}

pub use axum_impl::AxumWs;

/// `tokio_tungstenite::WebSocketStream` impl — used by the
/// client-side `Replicator` dialing outbound.
mod tungstenite_impl {
    use super::WsTransport;

    use async_trait::async_trait;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio_tungstenite::{
        MaybeTlsStream, WebSocketStream,
        tungstenite::Message,
    };

    /// A connected outbound stream. The two TLS variants are unified
    /// behind `MaybeTlsStream<TcpStream>`.
    pub struct TungsteniteWs(pub WebSocketStream<MaybeTlsStream<TcpStream>>);

    #[async_trait]
    impl WsTransport for TungsteniteWs {
        async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
            loop {
                match self.0.next().await {
                    Some(Ok(Message::Binary(b))) => return Ok(Some(b.into())),
                    Some(Ok(Message::Close(_))) | None => return Ok(None),
                    // Skip text frames; tungstenite auto-handles ping/pong.
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Err(anyhow::anyhow!("ws recv: {e}")),
                }
            }
        }

        async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
            self.0
                .send(Message::Binary(bytes.into()))
                .await
                .map_err(|e| anyhow::anyhow!("ws send: {e}"))
        }
    }
}

pub use tungstenite_impl::TungsteniteWs;
