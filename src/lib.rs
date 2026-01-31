use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;

pub mod client;
pub mod server;


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
    pub async fn reply(&self, data: &[u8]) {
        self.responder.send(data).await;
    }
}

pub(crate) enum Responder {
    Tcp(Arc<Mutex<TcpStream>>),
    Udp(Arc<UdpSocket>, SocketAddr),
    WebSocket(
        Arc<
            Mutex<
                futures::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<TcpStream>,
                    Message,
                >,
            >,
        >,
    ),
}

impl Responder {
    async fn send(&self, data: &[u8]) {
        match self {
            Responder::Tcp(stream) => {
                let mut locked = stream.lock().await;
                let _ = locked.write_all(data).await;
            }
            Responder::Udp(socket, addr) => {
                let _ = socket.send_to(data, *addr).await;
            }
            Responder::WebSocket(writer) => {
                let mut locked = writer.lock().await;
                let msg = Message::binary(data.to_vec());
                let _ = locked.send(msg).await;
            }
        }
    }
}
