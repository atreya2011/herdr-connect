use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use herdr_connect_rs::drive_gateway;
use tokio::net::TcpListener;
use tokio_websockets::{Message, ServerBuilder};

const MESSAGE_CREATE_DATA: &str = r#"{"id":"200","type":0,"channel_id":"100","guild_id":"1","content":"hello from thread","timestamp":"2024-01-01T00:00:00.000000+00:00","edited_timestamp":null,"tts":false,"mention_everyone":false,"mentions":[],"mention_roles":[],"attachments":[],"embeds":[],"pinned":false,"call":null,"author":{"id":"9","username":"owner","discriminator":"0001","avatar":null,"accent_color":null,"avatar_decoration":null,"avatar_decoration_data":null,"banner":null}}"#;

struct GatewayStand {
    url: String,
    opcodes: Arc<Mutex<Vec<u64>>>,
}

async fn scripted_gateway() -> GatewayStand {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let opcodes = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&opcodes);
    let resume = url.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            let resume = resume.clone();
            tokio::spawn(async move {
                let Ok((_, mut socket)) = ServerBuilder::new().accept(stream).await else {
                    return;
                };
                let hello = r#"{"op":10,"d":{"heartbeat_interval":500}}"#;
                if socket.send(Message::text(hello)).await.is_err() {
                    return;
                }
                let mut identified = false;
                while let Some(Ok(frame)) = socket.next().await {
                    let Some(text) = frame.as_text() else {
                        continue;
                    };
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
                        continue;
                    };
                    let Some(opcode) = value.get("op").and_then(serde_json::Value::as_u64) else {
                        continue;
                    };
                    seen.lock().unwrap().push(opcode);
                    if opcode == 1 {
                        let _ = socket.send(Message::text(r#"{"op":11}"#)).await;
                    }
                    if opcode == 2 && !identified {
                        identified = true;
                        let ready = format!(
                            r#"{{"op":0,"s":1,"t":"READY","d":{{"session_id":"test-session","resume_gateway_url":"{resume}"}}}}"#
                        );
                        if socket.send(Message::text(ready)).await.is_err() {
                            return;
                        }
                        let create = format!(
                            r#"{{"op":0,"s":2,"t":"MESSAGE_CREATE","d":{MESSAGE_CREATE_DATA}}}"#
                        );
                        if socket.send(Message::text(create)).await.is_err() {
                            return;
                        }
                        let _ = socket.send(Message::text("not-a-gateway-frame")).await;
                    }
                }
            });
        }
    });
    GatewayStand { url, opcodes }
}

#[tokio::test]
async fn gateway_identifies_heartbeats_receives_message_and_reports_errors_without_exiting() {
    let _: twilight_model::channel::Message =
        serde_json::from_str(MESSAGE_CREATE_DATA).expect("fixture must be a real Message");
    let stand = scripted_gateway().await;
    let (tx, rx) = mpsc::channel();
    let task = tokio::spawn(drive_gateway("test-token".into(), Some(stand.url), tx));
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut notices = Vec::new();
    loop {
        while let Ok(notice) = rx.try_recv() {
            notices.push(notice);
        }
        let opcodes = stand.opcodes.lock().unwrap().clone();
        let identified = opcodes.contains(&2);
        let heartbeated = opcodes.contains(&1);
        let saw_message = notices
            .iter()
            .any(|notice| notice.contains("MESSAGE_CREATE"));
        let saw_error = notices
            .iter()
            .any(|notice| notice.contains("discord gateway error"));
        if identified && heartbeated && saw_message && saw_error && !task.is_finished() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "gateway runtime did not identify, heartbeat, receive MESSAGE_CREATE, and report an error without exiting; opcodes={opcodes:?} notices={notices:?} finished={}",
            task.is_finished()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !task.is_finished(),
        "gateway task exited after reporting an error"
    );
    task.abort();
}
