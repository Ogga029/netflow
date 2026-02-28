use crate::{Packet, Protocol, Responder};
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;

// TODO: MAKE ConnectionValidator work with UDP if possible

// ---- ConnectionManager ----
pub struct ConnectionManager<S> {
    connections: Arc<RwLock<HashMap<SocketAddr, Responder>>>,
    pub(crate) on_disconnect: Option<DisconnectHandler<S>>,
}

impl<S> ConnectionManager<S> {
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

    pub async fn unregister(&self, addr: SocketAddr, state: Arc<S>) {
        let mut conns = self.connections.write().await;
        conns.remove(&addr);

        if let Some(handler) = &self.on_disconnect {
            handler(addr, state, self.clone()).await;
        }
    }

    pub async fn send_to(&self, addr: SocketAddr, data: &[u8]) -> Result<(), String> {
        if let Some(responder) = self.connections.read().await.get(&addr).cloned() {
            responder.send(data).await;
            Ok(())
        } else {
            Err("Client not found".to_string())
        }
    }

    pub async fn broadcast(&self, data: &[u8]) {
        for responder in self.connections.read().await.values() {
            responder.send(data).await;
        }
    }

    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
}

impl<S> Clone for ConnectionManager<S> {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }
}

impl<S> std::fmt::Debug for ConnectionManager<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("connections", &self.connections)
            .finish()
    }
}

// ---- Types ----
type PacketHandler<S> = Arc<
    dyn Fn(
            Packet,
            SocketAddr,
            Arc<S>,
            ConnectionManager<S>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;
type ConnectionValidator<S> = Arc<
    dyn Fn(SocketAddr, Arc<S>, ConnectionManager<S>) -> Pin<Box<dyn Future<Output = bool> + Send>>
        + Send
        + Sync,
>;
type DisconnectHandler<S> = Arc<
    dyn Fn(SocketAddr, Arc<S>, ConnectionManager<S>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

// ---- Server ----
pub struct Server<S> {
    pub listeners: Vec<(String, Protocol)>,
    on_packet: Option<PacketHandler<S>>,
    on_connect: Option<ConnectionValidator<S>>,
    on_disconnect: Option<DisconnectHandler<S>>,
    pub connection_manager: ConnectionManager<S>,
    state: Arc<S>,
}

impl<S> Server<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(state: Arc<S>) -> Self {
        Self {
            listeners: Vec::new(),
            on_packet: None,
            on_connect: None,
            on_disconnect: None,
            connection_manager: ConnectionManager::new(),
            state,
        }
    }

    pub fn bind(mut self, addr: &str, protocol: Protocol) -> Self {
        self.listeners.push((addr.to_string(), protocol));
        self
    }

    pub fn on_packet<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(Packet, SocketAddr, Arc<S>, ConnectionManager<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_packet = Some(Arc::new(move |packet, addr, state, cm| {
            Box::pin(func(packet, addr, state, cm))
        }));
        self
    }

    pub fn on_connect<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(SocketAddr, Arc<S>, ConnectionManager<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.on_connect = Some(Arc::new(move |addr, state, cm| {
            Box::pin(func(addr, state, cm))
        }));
        self
    }

    pub fn on_disconnect<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(SocketAddr, Arc<S>, ConnectionManager<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_disconnect = Some(Arc::new(move |addr, state, cm| {
            Box::pin(func(addr, state, cm))
        }));
        self.connection_manager.on_disconnect = self.on_disconnect.clone();
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let on_packet = self.on_packet.ok_or("Packet handler not set")?;
        let on_connect = self.on_connect.clone();
        let manager = self.connection_manager.clone();
        let state = self.state.clone();

        for (addr, protocol) in self.listeners {
            let on_packet = on_packet.clone();
            let on_connect = on_connect.clone();
            let manager = manager.clone();
            let state = state.clone();

            match protocol {
                Protocol::Tcp => {
                    Self::spawn_tcp_listener(addr, on_packet, on_connect, manager, state);
                }
                Protocol::Udp => {
                    Self::spawn_udp_listener(addr, on_packet, state, manager);
                }
                Protocol::WebSocket => {
                    Self::spawn_websocket_listener(addr, on_packet, on_connect, manager, state);
                }
            }
        }

        std::future::pending::<()>().await;
        Ok(())
    }

    fn spawn_tcp_listener(
        addr: String,
        handler: PacketHandler<S>,
        validator: Option<ConnectionValidator<S>>,
        manager: ConnectionManager<S>,
        state: Arc<S>,
    ) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                let (stream, client_addr) = listener.accept().await.unwrap();
                let handler = handler.clone();
                let manager = manager.clone();
                let state = state.clone();

                if let Some(ref validator) = validator {
                    let state_clone = state.clone();
                    if !validator(client_addr, state_clone, manager.clone()).await {
                        continue;
                    }
                }

                tokio::spawn(Self::handle_tcp_connection(
                    stream,
                    client_addr,
                    handler,
                    manager,
                    state,
                ));
            }
        });
    }

    async fn handle_tcp_connection(
        stream: TcpStream,
        addr: SocketAddr,
        handler: PacketHandler<S>,
        manager: ConnectionManager<S>,
        state: Arc<S>,
    ) {
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        manager.register(addr, Responder::Tcp(tx.clone())).await;

        // Writer task
        let write_task = tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                let len_bytes = (data.len() as u32).to_be_bytes();
                if writer.write_all(&len_bytes).await.is_err() {
                    break;
                }
                if writer.write_all(&data).await.is_err() {
                    break;
                }
            }
        });

        let mut len_buf = [0u8; 4];

        loop {
            if let Err(_) = reader.read_exact(&mut len_buf).await {
                break;
            }

            let packet_len = u32::from_be_bytes(len_buf) as usize;
            let mut packet_buf = vec![0u8; packet_len];

            if let Err(_) = reader.read_exact(&mut packet_buf).await {
                break;
            }

            let packet = Packet {
                data: packet_buf,
                protocol: Protocol::Tcp,
                responder: Responder::Tcp(tx.clone()),
            };

            handler(packet, addr, state.clone(), manager.clone()).await;
        }

        manager.unregister(addr, state).await;
        write_task.abort();
    }

    fn spawn_udp_listener(
        addr: String,
        handler: PacketHandler<S>,
        state: Arc<S>,
        manager: ConnectionManager<S>,
    ) {
        tokio::spawn(async move {
            let socket = Arc::new(UdpSocket::bind(&addr).await.unwrap());

            loop {
                // Allocate buffer for max UDP packet size (65535 bytes)
                let mut buffer = vec![0u8; 65535];

                let (size, client_addr) = match socket.recv_from(&mut buffer).await {
                    Ok(res) => res,
                    Err(_) => continue,
                };

                // Shrink buffer to actual received size
                buffer.truncate(size);

                let packet = Packet {
                    data: buffer,
                    protocol: Protocol::Udp,
                    responder: Responder::Udp(socket.clone(), client_addr),
                };

                let handler = handler.clone();
                let state = state.clone();
                let mg = manager.clone();

                tokio::spawn(async move {
                    handler(packet, client_addr, state, mg).await;
                });
            }
        });
    }

    fn spawn_websocket_listener(
        addr: String,
        handler: PacketHandler<S>,
        validator: Option<ConnectionValidator<S>>,
        manager: ConnectionManager<S>,
        state: Arc<S>,
    ) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                let (stream, client_addr) = listener.accept().await.unwrap();
                let handler = handler.clone();
                let manager = manager.clone();
                let state = state.clone();

                if let Some(ref validator) = validator {
                    let state_clone = state.clone();
                    if !validator(client_addr, state_clone, manager.clone()).await {
                        continue;
                    }
                }

                tokio::spawn(async move {
                    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };
                    let (mut write, mut read) = ws_stream.split();
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

                    manager
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

                    let handler = handler.clone();
                    let state = state.clone();
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

                        let handler = handler.clone();
                        let state = state.clone();
                        let mg = manager.clone();
                        tokio::spawn(async move {
                            handler(packet, client_addr, state, mg).await;
                        });
                    }

                    manager.unregister(client_addr, state).await;
                    write_task.abort();
                });
            }
        });
    }
}
