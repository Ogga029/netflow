use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::io::{AsyncReadExt};
use tokio::sync::{Mutex, RwLock, broadcast};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use crate::{Packet, Protocol, Responder};
use tokio_tungstenite::tungstenite::Message;
use futures::stream::StreamExt;

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
                            let manager = manager.clone();

                            tokio::spawn(async move {
                                manager.register(client_addr).await;
                                Self::handle_tcp_connection(stream, client_addr, handler, manager).await;
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
        manager: ConnectionManager,
    ) {
        let mut buffer = [0u8; 1024];

        loop {
            let size = {
                let mut locked = stream.lock().await;
                match locked.read(&mut buffer).await {
                    Ok(0) => {
                        manager.unregister(addr).await;
                        return;
                    }
                    Ok(n) => n,
                    Err(_) => {
                        manager.unregister(addr).await;
                        return;
                    }
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

    fn spawn_websocket_listener(
        addr: String,
        handler: PacketHandler,
        validator: Option<ConnectionValidator>,
        manager: ConnectionManager,
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
                            let manager = manager.clone();

                            tokio::spawn(async move {
                                match tokio_tungstenite::accept_async(stream).await {
                                    Ok(ws_stream) => {
                                        manager.register(client_addr).await;
                                        Self::handle_websocket_connection(
                                            Arc::new(Mutex::new(ws_stream)),
                                            client_addr,
                                            handler,
                                            manager,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        eprintln!("WebSocket handshake error: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("Error accepting connection: {}", e);
                        }
                    }
                },
                Err(e) => eprintln!("Failed to bind WebSocket listener on {}: {}", addr, e),
            }
        });
    }

    async fn handle_websocket_connection(
        ws: Arc<Mutex<tokio_tungstenite::WebSocketStream<TcpStream>>>,
        addr: SocketAddr,
        handler: PacketHandler,
        manager: ConnectionManager,
    ) {
        loop {
            let msg = {
                let mut locked = ws.lock().await;
                locked.next().await
            };

            match msg {
                Some(Ok(Message::Binary(data))) => {
                    let packet = Packet {
                        data: data.to_vec(),
                        protocol: Protocol::WebSocket,
                        responder: Responder::WebSocket(ws.clone()),
                    };

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        handler(packet, addr).await;
                    });
                }
                Some(Ok(Message::Text(text))) => {
                    let packet = Packet {
                        data: text.as_bytes().to_vec(),
                        protocol: Protocol::WebSocket,
                        responder: Responder::WebSocket(ws.clone()),
                    };

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        handler(packet, addr).await;
                    });
                }
                _ => {
                    // Connection closed or error
                    manager.unregister(addr).await;
                    break;
                }
            }
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_manager_new() {
        let manager = ConnectionManager::new(100);
        assert_eq!(manager.broadcast_tx.receiver_count(), 0);
    }

    #[tokio::test]
    async fn connection_manager_register_unregister() {
        let manager = ConnectionManager::new(100);
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        manager.register(addr).await;
        assert_eq!(manager.connection_count().await, 1);

        manager.unregister(addr).await;
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn connection_manager_broadcast() {
        let manager = ConnectionManager::new(100);
        manager.broadcast(vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn connection_manager_subscribe() {
        let manager = ConnectionManager::new(100);
        let _rx = manager.subscribe();
        assert!(manager.broadcast_tx.receiver_count() > 0);
    }

    #[tokio::test]
    async fn connection_manager_send_to() {
        let manager = ConnectionManager::new(100);
        let addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let result = manager.send_to(addr, vec![1, 2, 3]).await;
        assert!(result.is_ok());
    }

    #[test]
    fn server_new() {
        let server = Server::new();
        assert_eq!(server.listeners.len(), 0);
        assert!(server.on_packet.is_none());
        assert!(server.on_connect.is_none());
    }

    #[test]
    fn server_default() {
        let server = Server::default();
        assert_eq!(server.listeners.len(), 0);
    }

    #[test]
    fn server_bind() {
        let server = Server::new()
            .bind("127.0.0.1:9001", Protocol::Tcp)
            .bind("127.0.0.1:9002", Protocol::WebSocket);
        assert_eq!(server.listeners.len(), 2);
    }

    #[test]
    fn server_bind_multiple_same_protocol() {
        let server = Server::new()
            .bind("127.0.0.1:9003", Protocol::Tcp)
            .bind("127.0.0.1:9004", Protocol::Tcp)
            .bind("127.0.0.1:9005", Protocol::Tcp);
        assert_eq!(server.listeners.len(), 3);
    }

    #[test]
    fn broadcast_message_clone() {
        let msg = BroadcastMessage {
            data: vec![1, 2, 3],
        };
        let msg2 = msg.clone();
        assert_eq!(msg.data, msg2.data);
    }

    #[test]
    fn broadcast_message_debug() {
        let msg = BroadcastMessage {
            data: vec![255],
        };
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("BroadcastMessage"));
    }
}
