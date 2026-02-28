use crate::Protocol;
use futures::{SinkExt, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message as WsMessage};

type MessageHandler<S> =
    Arc<dyn Fn(Vec<u8>, Arc<S>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Clone)]
enum Connection {
    Tcp(Arc<Mutex<TcpStream>>),
    Udp(Arc<UdpSocket>),
    WebSocket {
        tx: Arc<mpsc::UnboundedSender<WsMessage>>,
        reader:
            Arc<Mutex<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
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

                let len_bytes = (data.len() as u32).to_be_bytes();
                locked.write_all(&len_bytes).await?;

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
                let mut locked = stream.lock().await;

                loop {
                    let mut len_buf = [0u8; 4];
                    locked.read_exact(&mut len_buf).await?;

                    let packet_len = u32::from_be_bytes(len_buf) as usize;
                    let mut packet_buf = vec![0u8; packet_len];

                    locked.read_exact(&mut packet_buf).await?;

                    handler(packet_buf, state.clone()).await;
                }
            }
            Connection::Udp(socket) => {
                loop {
                    // Allocate buffer for max UDP packet size
                    let mut buffer = vec![0u8; 65535];

                    match socket.recv(&mut buffer).await {
                        Ok(0) => return Ok(()), // connection closed
                        Ok(n) => {
                            buffer.truncate(n); // shrink to actual size
                            handler(buffer, state.clone()).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            Connection::WebSocket { reader, .. } => loop {
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
            },
        }
    }
}
