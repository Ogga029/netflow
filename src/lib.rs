use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use futures::SinkExt;

pub mod client;
pub mod server;

pub use client::Client;
pub use server::Server;

#[derive(Clone)]
pub enum Protocol {
    Tcp,
    Udp,
    WebSocket,
}

pub struct Packet {
    pub data: Vec<u8>,
    pub protocol: Protocol,
    responder: Responder,
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let protocol_str = match self.protocol {
            Protocol::Tcp => "TCP",
            Protocol::Udp => "UDP",
            Protocol::WebSocket => "WebSocket",
        };
        f.debug_struct("Packet")
            .field("data", &self.data)
            .field("data_string", &String::from_utf8_lossy(&self.data))
            .field("protocol", &protocol_str)
            .finish()
    }
}

impl Packet {
    pub async fn reply(&mut self, data: &[u8]) {
        self.responder.send(data).await;
    }
}

pub(crate) enum Responder {
    Tcp(Arc<Mutex<TcpStream>>),
    Udp(Arc<UdpSocket>, SocketAddr),
    WebSocket(Arc<Mutex<tokio_tungstenite::WebSocketStream<TcpStream>>>),
}

impl Responder {
    async fn send(&mut self, data: &[u8]) {
        match self {
            Responder::Tcp(stream) => {
                let mut locked = stream.lock().await;
                let _ = locked.write_all(data).await;
            }
            Responder::Udp(socket, addr) => {
                let _ = socket.send_to(data, *addr).await;
            }
            Responder::WebSocket(ws) => {
                let mut locked = ws.lock().await;
                let msg = tokio_tungstenite::tungstenite::Message::Binary(
                    tokio_tungstenite::tungstenite::Bytes::copy_from_slice(data)
                );
                let _ = locked.send(msg).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_clone_tcp() {
        let p1 = Protocol::Tcp;
        let p2 = p1.clone();
        matches!(p2, Protocol::Tcp);
    }

    #[test]
    fn protocol_clone_udp() {
        let p1 = Protocol::Udp;
        let p2 = p1.clone();
        matches!(p2, Protocol::Udp);
    }

    #[test]
    fn protocol_clone_websocket() {
        let p1 = Protocol::WebSocket;
        let p2 = p1.clone();
        matches!(p2, Protocol::WebSocket);
    }
}
