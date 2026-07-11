use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_tungstenite::tungstenite::Message;

pub mod client;
pub mod server;

pub use client::Client;
pub use server::{Server, TcpMode};

pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    pub async fn reply(&self, data: &[u8]) -> io::Result<()> {
        self.responder.send(data).await
    }
}

use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum Responder {
    Tcp(mpsc::UnboundedSender<Vec<u8>>),
    Udp(Arc<UdpSocket>, SocketAddr),
    WebSocket(mpsc::UnboundedSender<Message>),
}

impl Responder {
    pub async fn send(&self, data: &[u8]) -> io::Result<()> {
        match self {
            Responder::Tcp(tx) => {
                tx.send(data.to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "tcp writer closed"))?;
                Ok(())
            }
            Responder::Udp(socket, addr) => {
                socket.send_to(data, addr).await?;
                Ok(())
            }
            Responder::WebSocket(tx) => {
                tx.send(Message::Binary(data.to_vec().into()))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "websocket writer closed")
                    })?;
                Ok(())
            }
        }
    }
}
