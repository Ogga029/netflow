use crate::Protocol;
use futures::{SinkExt, StreamExt};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message as WsMessage};

type MessageHandler<S> =
    Arc<dyn Fn(Vec<u8>, Arc<S>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Clone)]
enum Connection {
    Tcp {
        tx: mpsc::UnboundedSender<Vec<u8>>,
        reader: Arc<Mutex<Option<OwnedReadHalf>>>,
    },
    Udp(Arc<UdpSocket>),
    WebSocket {
        tx: mpsc::UnboundedSender<WsMessage>,
        reader: Arc<
            Mutex<Option<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
        >,
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
                let (reader, mut writer) = stream.into_split();

                let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                tokio::spawn(async move {
                    while let Some(data) = rx.recv().await {
                        let len = match u32::try_from(data.len()) {
                            Ok(len) => len,
                            Err(_) => break,
                        };

                        if writer.write_all(&len.to_be_bytes()).await.is_err() {
                            break;
                        }

                        if writer.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                });

                Connection::Tcp {
                    tx,
                    reader: Arc::new(Mutex::new(Some(reader))),
                }
            }
            Protocol::Udp => {
                let socket = UdpSocket::bind("0.0.0.0:0").await?;
                socket.connect(addr).await?;
                Connection::Udp(Arc::new(socket))
            }
            Protocol::WebSocket => {
                let (ws_stream, _) = connect_async(addr).await?;
                let (mut write, read) = ws_stream.split();

                let (tx, mut rx) = mpsc::unbounded_channel::<WsMessage>();
                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        if write.send(msg).await.is_err() {
                            break;
                        }
                    }
                });

                Connection::WebSocket {
                    tx,
                    reader: Arc::new(Mutex::new(Some(read))),
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
            Connection::Tcp { tx, .. } => {
                u32::try_from(data.len())
                    .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "TCP frame too large"))?;
                tx.send(data.to_vec())
                    .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "tcp writer closed"))?;
            }
            Connection::Udp(socket) => {
                socket.send(data).await?;
            }
            Connection::WebSocket { tx, .. } => {
                tx.send(WsMessage::binary(data.to_vec())).map_err(|_| {
                    io::Error::new(ErrorKind::BrokenPipe, "websocket writer closed")
                })?;
            }
        }

        Ok(())
    }

    pub async fn listen(self) -> Result<(), Box<dyn std::error::Error>> {
        let handler = self
            .on_message
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Message handler not set"))?;
        let state = self
            .state
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "State not set"))?;

        match self.connection {
            Connection::Tcp { reader, .. } => {
                let mut reader = {
                    let mut guard = reader.lock().await;
                    guard.take().ok_or_else(|| {
                        io::Error::new(ErrorKind::Other, "TCP reader already taken")
                    })?
                };

                loop {
                    let mut len_buf = [0u8; 4];
                    match reader.read_exact(&mut len_buf).await {
                        Ok(_) => {}
                        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(()),
                        Err(err) => return Err(err.into()),
                    }

                    let packet_len = u32::from_be_bytes(len_buf) as usize;
                    if packet_len > crate::MAX_FRAME_SIZE {
                        return Err(
                            io::Error::new(ErrorKind::InvalidData, "TCP frame too large").into(),
                        );
                    }

                    let mut packet_buf = vec![0u8; packet_len];
                    reader.read_exact(&mut packet_buf).await?;

                    let handler = handler.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        handler(packet_buf, state).await;
                    });
                }
            }
            Connection::Udp(socket) => {
                let mut buffer = vec![0u8; 65535];

                loop {
                    let size = socket.recv(&mut buffer).await?;
                    let data = buffer[..size].to_vec();

                    let handler = handler.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        handler(data, state).await;
                    });
                }
            }
            Connection::WebSocket { reader, .. } => {
                let mut reader = {
                    let mut guard = reader.lock().await;
                    guard.take().ok_or_else(|| {
                        io::Error::new(ErrorKind::Other, "WebSocket reader already taken")
                    })?
                };

                while let Some(msg_result) = reader.next().await {
                    let msg = msg_result?;

                    let data = match msg {
                        WsMessage::Binary(d) => d.to_vec(),
                        WsMessage::Text(t) => t.as_bytes().to_vec(),
                        _ => continue,
                    };

                    let handler = handler.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        handler(data, state).await;
                    });
                }

                Ok(())
            }
        }
    }
}
