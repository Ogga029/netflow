use tokio::net::{TcpStream, UdpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::Protocol;

type MessageHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

enum Connection {
    Tcp(Arc<Mutex<TcpStream>>),
    Udp(Arc<UdpSocket>),
}

pub struct Client {
    connection: Connection,
    on_message: Option<MessageHandler>,
}

impl Client {
    pub async fn connect(
        addr: &str,
        protocol: Protocol,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = match protocol {
            Protocol::Tcp => {
                let stream = TcpStream::connect(addr).await?;
                Connection::Tcp(Arc::new(Mutex::new(stream)))
            }
            Protocol::Udp => {
                let socket = UdpSocket::bind("0.0.0.0:0").await?;
                socket.connect(addr).await?;
                Connection::Udp(Arc::new(socket))
            }
            Protocol::WebSocket => {
                return Err("WebSocket client not fully supported yet".into());
            }
        };

        Ok(Self {
            connection,
            on_message: None,
        })
    }

    pub fn on_message<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_message = Some(Arc::new(move |data| {
            Box::pin(func(data))
        }));
        self
    }

    pub async fn send(&self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        match &self.connection {
            Connection::Tcp(stream) => {
                let mut locked = stream.lock().await;
                locked.write_all(data).await?;
            }
            Connection::Udp(socket) => {
                socket.send(data).await?;
            }
        }
        Ok(())
    }

    pub async fn listen(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = self.on_message.ok_or("Message handler not set")?;

        match self.connection {
            Connection::Tcp(stream) => {
                let mut buffer = [0u8; 1024];
                loop {
                    let size = {
                        let mut locked = stream.lock().await;
                        match locked.read(&mut buffer).await {
                            Ok(0) => return Ok(()),
                            Ok(n) => n,
                            Err(e) => return Err(e.into()),
                        }
                    };

                    handler(buffer[..size].to_vec()).await;
                }
            }
            Connection::Udp(socket) => {
                let mut buffer = [0u8; 1024];
                loop {
                    match socket.recv(&mut buffer).await {
                        Ok(0) => return Ok(()),
                        Ok(n) => {
                            handler(buffer[..n].to_vec()).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }
}
