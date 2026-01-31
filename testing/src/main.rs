use netflow;

#[tokio::main]
async fn main() {
        netflow::Netflow::new()
            .bind("127.0.0.1:1234", netflow::Protocol::Tcp)
            .on_packet(|mut packet, addr| async move {
                println!("GOT PACKET FROM {}", addr);
                let content_str = String::from_utf8_lossy(&packet.data);
                if content_str.starts_with("GET HTTP/1.1") {
                    packet.reply(b"Nah Nah Nah, HTTP!").await;
                }
            })
            .run()
            .await
            .unwrap();
}
