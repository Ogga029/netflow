use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;

pub mod client;
pub mod server;

pub use client::Client;
pub use server::Server;

#[derive(Clone)]
pub enum Protocol {
    Tcp,
    Udp,
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
}

impl Responder {
    async fn send(&mut self, data: &[u8]) {
        match self {
            Responder::Tcp(stream) => {
                if let Ok(mut locked) = stream.try_lock() {
                    let _ = locked.write_all(data).await;
                }
            }
            Responder::Udp(socket, addr) => {
                let _ = socket.send_to(data, *addr).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server() {
            let _ = Server::new()
                .bind("127.0.0.1:9001", Protocol::Tcp)
                .on_connect(|addr| async move {
                    println!("Client connecting from: {}", addr);
                    true
                })
                .on_packet(|mut packet, addr| async move {
                    println!("Received from {}: {:?}", addr, packet);
                    packet.reply(b"OK").await;
                })
                .run()
                .await;
    }

    #[tokio::test]
    async fn server_tcp_reject_connections() {
        let server = Server::new()
            .bind("127.0.0.1:9002", Protocol::Tcp)
            .on_connect(|_addr| async move { false })
            .on_packet(|_packet, _addr| async move {});

        assert_eq!(server.listeners.len(), 1);
    }
}
