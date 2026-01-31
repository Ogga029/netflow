use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, RwLock, broadcast};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use crate::{Packet, Protocol, Responder};
use tokio_tungstenite::tungstenite::Message;
use futures::{StreamExt};

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct ConnectionManager {
    broadcast_tx: broadcast::Sender<BroadcastMessage>,
    connections: Arc<RwLock<HashMap<SocketAddr, broadcast::Receiver<BroadcastMessage>>>>,
}

impl ConnectionManager {
    pub fn new(capacity: usize) -> Self {
        let (broadcast_tx, _) = broadcast::channel(capacity);
        Self {
            broadcast_tx,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, addr: SocketAddr) {
        let rx = self.broadcast_tx.subscribe();
        let mut conns = self.connections.write().await;
        conns.insert(addr, rx);
    }

    pub async fn unregister(&self, addr: SocketAddr) {
        let mut conns = self.connections.write().await;
        conns.remove(&addr);
    }

    pub async fn send_to(&self, _addr: SocketAddr, data: Vec<u8>) -> Result<(), String> {
        let _ = self.broadcast_tx.send(BroadcastMessage { data });
        Ok(())
    }

    pub fn broadcast(&self, data: Vec<u8>) {
        let _ = self.broadcast_tx.send(BroadcastMessage { data });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastMessage> {
        self.broadcast_tx.subscribe()
    }

    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
}

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
    pub connection_manager: ConnectionManager,
}

impl Server {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
            on_packet: None,
            on_connect: None,
            connection_manager: ConnectionManager::new(1000),
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
        let manager = self.connection_manager.clone();

        for (addr, protocol) in self.listeners {
            match protocol {
                Protocol::Tcp => {
                    Self::spawn_tcp_listener(addr, on_packet.clone(), on_connect.clone(), manager.clone());
                }
                Protocol::Udp => {
                    Self::spawn_udp_listener(addr, on_packet.clone());
                }
                Protocol::WebSocket => {
                    Self::spawn_websocket_listener(addr, on_packet.clone(), on_connect.clone(), manager.clone());
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
        manager: ConnectionManager,
    ) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                let (stream, client_addr) = listener.accept().await.unwrap();

                if let Some(ref validator) = validator {
                    if !validator(client_addr).await {
                        continue;
                    }
                }

                let handler = handler.clone();
                let stream = Arc::new(Mutex::new(stream));
                let manager = manager.clone();

                tokio::spawn(async move {
                    manager.register(client_addr).await;
                    Self::handle_tcp_connection(stream, client_addr, handler, manager).await;
                });
            }
        });
    }

    fn spawn_udp_listener(addr: String, handler: PacketHandler) {
        tokio::spawn(async move {
            let socket = Arc::new(UdpSocket::bind(&addr).await.unwrap());
            let mut buffer = [0u8; 1024];

            loop {
                let (size, client_addr) = socket.recv_from(&mut buffer).await.unwrap();

                let packet = Packet {
                    data: buffer[..size].to_vec(),
                    protocol: Protocol::Udp,
                    responder: Responder::Udp(socket.clone(), client_addr),
                };

                handler(packet, client_addr).await;
            }
        });
    }

    async fn handle_tcp_connection(
        stream: Arc<Mutex<TcpStream>>,
        addr: SocketAddr,
        handler: PacketHandler,
        manager: ConnectionManager,
    ) {
        let mut buffer = [0u8; 1024];

        loop {
            let size = {
                let mut locked = stream.lock().await;
                match locked.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                }
            };

            let packet = Packet {
                data: buffer[..size].to_vec(),
                protocol: Protocol::Tcp,
                responder: Responder::Tcp(stream.clone()),
            };

            handler(packet, addr).await;
        }

        manager.unregister(addr).await;
    }

    fn spawn_websocket_listener(
        addr: String,
        handler: PacketHandler,
        validator: Option<ConnectionValidator>,
        manager: ConnectionManager,
    ) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                let (stream, client_addr) = listener.accept().await.unwrap();

                if let Some(ref validator) = validator {
                    if !validator(client_addr).await {
                        continue;
                    }
                }

                let handler = handler.clone();
                let manager = manager.clone();

                tokio::spawn(async move {
                    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };

                    manager.register(client_addr).await;

                    let (write, mut read) = ws_stream.split();
                    let writer = Arc::new(Mutex::new(write));

                    while let Some(msg) = read.next().await {
                        let msg = match msg {
                            Ok(m) => m,
                            Err(_) => break,
                        };

                        let data = match msg {
                            Message::Binary(d) => d.to_vec(),
                            Message::Text(t) => t.as_bytes().to_vec(),
                            _ => continue,
                        };

                        let packet = Packet {
                            data,
                            protocol: Protocol::WebSocket,
                            responder: Responder::WebSocket(writer.clone()),
                        };

                        handler(packet, client_addr).await;
                    }

                    manager.unregister(client_addr).await;
                });
            }
        });
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}
