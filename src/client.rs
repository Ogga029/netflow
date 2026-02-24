use tokio::net::{TcpStream, UdpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage, WebSocketStream};
use tokio_tungstenite::MaybeTlsStream;
use futures::{SinkExt, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::Protocol;

type MessageHandler<S> = Arc<
    dyn Fn(Vec<u8>, Arc<S>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

#[derive(Clone)]
enum Connection {
    Tcp(Arc<Mutex<TcpStream>>),
    Udp(Arc<UdpSocket>),
    WebSocket {
        tx: Arc<mpsc::UnboundedSender<WsMessage>>,
        reader: Arc<Mutex<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    },
}

#[derive(Clone)]
pub struct Client<S> {
    connection: Connection,
    on_message: Option<MessageHandler<S>>,
    state: Option<Arc<S>>,
}

impl<S> Client<S>
where
    S: Send + Sync + 'static,
{
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
                // `addr` should be a ws:// or wss:// URL for WebSocket
                let (ws_stream, _) = connect_async(addr).await?;
                let (write, read) = ws_stream.split();

                let (tx, mut rx) = mpsc::unbounded_channel::<WsMessage>();
                // spawn writer task
                tokio::spawn(async move {
                    let mut write = write;
                    while let Some(msg) = rx.recv().await {
                        if write.send(msg).await.is_err() {
                            break;
                        }
                    }
                });

                Connection::WebSocket {
                    tx: Arc::new(tx),
                    reader: Arc::new(Mutex::new(read)),
                }
            }
        };

        Ok(Self {
            connection,
            on_message: None,
            state: None,
        })
    }

    pub fn with_state(mut self, state: Arc<S>) -> Self {
        self.state = Some(state);
        self
    }

    pub fn on_message<F, Fut>(mut self, func: F) -> Self
    where
        F: Fn(Vec<u8>, Arc<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_message = Some(Arc::new(move |data, state| Box::pin(func(data, state))));
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
            Connection::WebSocket { tx, .. } => {
                let _ = tx.send(WsMessage::binary(data.to_vec()));
            }
        }
        Ok(())
    }

    pub async fn listen(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = self.on_message.ok_or("Message handler not set")?;
        let state = self.state.ok_or("State not set")?;

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

                    handler(buffer[..size].to_vec(), state.clone()).await;
                }
            }
            Connection::Udp(socket) => {
                let mut buffer = [0u8; 1024];
                loop {
                    match socket.recv(&mut buffer).await {
                        Ok(0) => return Ok(()),
                        Ok(n) => {
                            handler(buffer[..n].to_vec(), state.clone()).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            Connection::WebSocket { reader, .. } => {
                loop {
                    let mut guard = reader.lock().await;
                    match guard.next().await {
                        Some(Ok(msg)) => {
                            let data = match msg {
                                WsMessage::Binary(d) => d.to_vec(),
                                WsMessage::Text(t) => t.as_bytes().to_vec(),
                                _ => continue,
                            };

                            handler(data, state.clone()).await;
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}
