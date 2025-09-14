use kxkdb::ipc::*;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::StreamExt;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to KDB
    let mut kdb_socket = QStream::connect(ConnectionMethod::TCP, "127.0.0.1", 5001_u16, "").await?;
    println!("Connected to KDB");
    
    // Connect to Binance WebSocket
    let binance_url = "wss://fstream.binance.com/ws/btcusdt@miniTicker";
    let (ws_stream, _) = connect_async(binance_url).await.expect("Failed to connect to Binance");
    println!("Connected to Binance WebSocket");
    
    let (mut _write, mut read) = ws_stream.split();
    
    // Stream data
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(data)) => {
                println!("Received: {}", data);
                
                // Parse JSON in Rust
                if let Ok(json) = serde_json::from_str::<Value>(&data) {
                    let symbol = json["s"].as_str().unwrap_or("UNKNOWN");
                    let close = json["c"].as_str().unwrap_or("0");
                    let open = json["o"].as_str().unwrap_or("0");
                    let high = json["h"].as_str().unwrap_or("0");
                    let low = json["l"].as_str().unwrap_or("0");
                    let volume = json["v"].as_str().unwrap_or("0");
                    
                    // Send parsed data to KDB
                    let cmd = format!("addTicker[`{};{};{};{};{};{}]", 
                                    symbol, close, open, high, low, volume);
                    kdb_socket.send_async_message(&cmd.as_str()).await?;
                }
            }
            Err(e) => {
                println!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }
    
    Ok(())
}