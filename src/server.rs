use crate::{Packet, Protocol, Responder};
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
pub struct ConnectionManager {
    connections: Arc<RwLock<HashMap<SocketAddr, Responder>>>,
    pub(crate) on_disconnect: Option<DisconnectHandler>,
}

impl std::fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("connections", &self.connections)
            .finish()
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            on_disconnect: None,
        }
    }

    pub async fn register(&self, addr: SocketAddr, responder: Responder) {
        let mut conns = self.connections.write().await;
        conns.insert(addr, responder);
    }

    pub async fn unregister(&self, addr: SocketAddr) {
        let mut conns = self.connections.write().await;
        conns.remove(&addr);

        if let Some(disconnect_handler) = &self.on_disconnect {
            disconnect_handler(addr).await;
        }
    }

    pub async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<(), String> {
        let responder = {
            let conns = self.connections.read().await;
            conns.get(&addr).cloned()
        };

        if let Some(responder) = responder {
            responder.send(data).await;
            Ok(())
        } else {
            Err("Client not found".to_string())
        }
    }

    pub async fn broadcast(&self, data: &[u8]) {
        let conns = self.connections.read().await;
        for responder in conns.values() {
            responder.send(data).await;
        }
    }

    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
}

type PacketHandler =
    Arc<dyn Fn(Packet, SocketAddr) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

type ConnectionValidator =
    Arc<dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

type DisconnectHandler =
    Arc<dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct Server {
    pub listeners: Vec<(String, Protocol)>,
    on_packet: Option<PacketHandler>,
    on_connect: Option<ConnectionValidator>,
    on_disconnect: Option<DisconnectHandler>,
    pub connection_manager: ConnectionManager,
}

impl Server {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
            on_packet: None,
            on_connect: None,
            on_disconnect: None,
            connection_manager: ConnectionManager::new(),
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
        self.on_packet = Some(Arc::new(move |packet, addr| Box::pin(func(packet, addr))));
        self
    }

    pub fn on_connect<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.on_connect = Some(Arc::new(move |addr| Box::pin(func(addr))));
        self
    }

    pub fn on_disconnect<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_disconnect = Some(Arc::new(move |addr| Box::pin(func(addr))));
        self.connection_manager.on_disconnect = self.on_disconnect.clone();
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let on_packet = self.on_packet.ok_or("Packet handler not set")?;
        let on_connect = self.on_connect.clone();
        let manager = self.connection_manager.clone();

        for (addr, protocol) in self.listeners {
            match protocol {
                Protocol::Tcp => {
                    Self::spawn_tcp_listener(
                        addr,
                        on_packet.clone(),
                        on_connect.clone(),
                        manager.clone(),
                    );
                }
                Protocol::Udp => {
                    Self::spawn_udp_listener(addr, on_packet.clone());
                }
                Protocol::WebSocket => {
                    Self::spawn_websocket_listener(
                        addr,
                        on_packet.clone(),
                        on_connect.clone(),
                        manager.clone(),
                    );
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
                let manager_clone = manager.clone();

                tokio::spawn(async move {
                    Self::handle_tcp_connection(stream, client_addr, handler, manager_clone).await;
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
        stream: TcpStream,
        addr: SocketAddr,
        handler: PacketHandler,
        manager: ConnectionManager,
    ) {
        let (mut reader, mut writer) = stream.into_split();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        manager.register(addr, Responder::Tcp(tx.clone())).await;

        let write_task = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            while let Some(data) = rx.recv().await {
                if writer.write_all(&data).await.is_err() {
                    break;
                }
            }
        });

        let mut buffer = [0u8; 1024];

        loop {
            let size = match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };

            let packet = Packet {
                data: buffer[..size].to_vec(),
                protocol: Protocol::Tcp,
                responder: Responder::Tcp(tx.clone()),
            };

            handler(packet, addr).await;
        }

        manager.unregister(addr).await;
        write_task.abort();
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
                let manager_clone = manager.clone();

                tokio::spawn(async move {
                    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };

                    let (mut write, mut read) = ws_stream.split();

                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

                    manager_clone
                        .register(client_addr, Responder::WebSocket(tx.clone()))
                        .await;

                    let write_task = tokio::spawn(async move {
                        use futures::SinkExt;

                        while let Some(msg) = rx.recv().await {
                            if write.send(msg).await.is_err() {
                                break;
                            }
                        }
                    });

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
                            responder: Responder::WebSocket(tx.clone()),
                        };

                        handler(packet, client_addr).await;
                    }

                    manager_clone.unregister(client_addr).await;
                    write_task.abort();
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
