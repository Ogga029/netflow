use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::io::{AsyncReadExt};
use tokio::sync::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::net::SocketAddr;
use crate::{Packet, Protocol, Responder};

type PacketHandler = Arc<
    dyn Fn(Packet, SocketAddr) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

type ConnectionValidator = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = bool> + Send>>
        + Send
        + Sync,
>;

pub struct Server {
    pub listeners: Vec<(String, Protocol)>,
    on_packet: Option<PacketHandler>,
    on_connect: Option<ConnectionValidator>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
            on_packet: None,
            on_connect: None,
        }
    }

    pub fn bind(mut self, addr: &str, protocol: Protocol) -> Self {
        self.listeners.push((addr.to_string(), protocol));
        self
    }

    pub fn on_packet<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(Packet, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_packet = Some(Arc::new(move |packet, addr| {
            Box::pin(func(packet, addr))
        }));
        self
    }

    pub fn on_connect<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.on_connect = Some(Arc::new(move |addr| {
            Box::pin(func(addr))
        }));
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let on_packet = self.on_packet.ok_or("Packet handler not set")?;
        let on_connect = self.on_connect.clone();

        for (addr, protocol) in self.listeners {
            match protocol {
                Protocol::Tcp => {
                    Self::spawn_tcp_listener(addr, on_packet.clone(), on_connect.clone());
                }
                Protocol::Udp => {
                    Self::spawn_udp_listener(addr, on_packet.clone());
                }
            }
        }

        std::future::pending::<()>().await;
        Ok(())
    }

    fn spawn_tcp_listener(
        addr: String,
        handler: PacketHandler,
        validator: Option<ConnectionValidator>,
    ) {
        tokio::spawn(async move {
            match TcpListener::bind(&addr).await {
                Ok(listener) => loop {
                    match listener.accept().await {
                        Ok((stream, client_addr)) => {
                            if let Some(ref validator) = validator {
                                if !validator(client_addr).await {
                                    continue;
                                }
                            }

                            let handler = handler.clone();
                            let stream = Arc::new(Mutex::new(stream));

                            tokio::spawn(async move {
                                Self::handle_tcp_connection(stream, client_addr, handler).await;
                            });
                        }
                        Err(e) => {
                            eprintln!("Error accepting connection: {}", e);
                        }
                    }
                },
                Err(e) => eprintln!("Failed to bind TCP listener on {}: {}", addr, e),
            }
        });
    }

    fn spawn_udp_listener(addr: String, handler: PacketHandler) {
        tokio::spawn(async move {
            match UdpSocket::bind(&addr).await {
                Ok(socket) => {
                    let socket = Arc::new(socket);
                    let mut buffer = [0u8; 1024];

                    loop {
                        match socket.recv_from(&mut buffer).await {
                            Ok((size, client_addr)) => {
                                let packet = Packet {
                                    data: buffer[..size].to_vec(),
                                    protocol: Protocol::Udp,
                                    responder: Responder::Udp(socket.clone(), client_addr),
                                };

                                let handler = handler.clone();
                                tokio::spawn(async move {
                                    handler(packet, client_addr).await;
                                });
                            }
                            Err(e) => {
                                eprintln!("Error receiving UDP packet: {}", e);
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Failed to bind UDP listener on {}: {}", addr, e),
            }
        });
    }

    async fn handle_tcp_connection(
        stream: Arc<Mutex<TcpStream>>,
        addr: SocketAddr,
        handler: PacketHandler,
    ) {
        let mut buffer = [0u8; 1024];

        loop {
            let size = {
                let mut locked = stream.lock().await;
                match locked.read(&mut buffer).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                }
            };

            let packet = Packet {
                data: buffer[..size].to_vec(),
                protocol: Protocol::Tcp,
                responder: Responder::Tcp(stream.clone()),
            };

            handler(packet, addr).await;
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}
