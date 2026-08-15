use std::sync::{Arc, Mutex};

use herdr_connect_rs::{deliver_transition, sync_topology, update_live_status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use twilight_http::Client;
use twilight_model::id::Id;

const CHANNEL: &str = r#"{"id":"100","type":0,"guild_id":"1","name":"herdr-workspace-ws","topic":"herdr workspace [ws]"}"#;
const DUPLICATE_CHANNELS: &str = concat!(
    r#"{"id":"100","type":0,"guild_id":"1","name":"first","topic":"herdr workspace [ws]"},"#,
    r#"{"id":"101","type":0,"guild_id":"1","name":"second","topic":"herdr workspace [ws]"}"#,
);
const MESSAGE: &str = r#"{"id":"200","type":0,"channel_id":"100","content":"x","timestamp":"2024-01-01T00:00:00.000000+00:00","author":{"id":"9","username":"bot","discriminator":"0001","avatar":null},"attachments":[],"embeds":[],"mentions":[],"mention_roles":[],"mention_everyone":false,"pinned":false,"tts":false,"edited_timestamp":null}"#;
const EMPTY_THREADS: &str = r#"{"threads":[],"members":[]}"#;
const ARCHIVED_TAB: &str = r#"{"threads":[{"id":"101","type":11,"guild_id":"1","parent_id":"100","name":"previous-label [tab-7]","thread_metadata":{"archived":true,"auto_archive_duration":1440,"archive_timestamp":"2024-01-01T00:00:00.000000+00:00","locked":false}}],"members":[],"has_more":false}"#;
const DUPLICATE_TAB_THREADS: &str = r#"{"threads":[{"id":"101","type":11,"guild_id":"1","parent_id":"100","name":"previous-label [tab-7]","thread_metadata":{"archived":true,"auto_archive_duration":1440,"archive_timestamp":"2024-01-01T00:00:00.000000+00:00","locked":false}},{"id":"102","type":11,"guild_id":"1","parent_id":"100","name":"another-label [tab-7]","thread_metadata":{"archived":true,"auto_archive_duration":1440,"archive_timestamp":"2024-01-01T00:00:00.000000+00:00","locked":false}}],"members":[],"has_more":false}"#;
const STALLED_ARCHIVED_TAB: &str = r#"{"threads":[{"id":"101","type":11,"guild_id":"1","parent_id":"100","name":"other [tab-8]","thread_metadata":{"archived":true,"auto_archive_duration":1440,"archive_timestamp":"2024-01-01T00:00:00.000000+00:00","locked":false}}],"members":[],"has_more":true}"#;
const EMPTY_ARCHIVED_PAGE_WITH_MORE: &str = r#"{"threads":[],"members":[],"has_more":true}"#;

/// Records every call the code under test makes and answers it with a canned Discord payload.
struct Stand {
    address: String,
    calls: Arc<Mutex<Vec<String>>>,
    bodies: Arc<Mutex<Vec<String>>>,
}

async fn stand(existing_channels: &'static str) -> Stand {
    stand_with_threads(existing_channels, EMPTY_THREADS, EMPTY_THREADS).await
}

async fn stand_with_threads(
    existing_channels: &'static str,
    active_threads: &'static str,
    archived_threads: &'static str,
) -> Stand {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let (seen, sent) = (Arc::clone(&calls), Arc::clone(&bodies));
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let (seen, sent) = (Arc::clone(&seen), Arc::clone(&sent));
            tokio::spawn(async move {
                let mut raw = Vec::new();
                loop {
                    let mut buffer = [0_u8; 4096];
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buffer[..read]);
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    let Some(head_end) = text.find("\r\n\r\n") else {
                        continue;
                    };
                    let head = &text[..head_end];
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")
                                .or_else(|| line.strip_prefix("Content-Length: "))
                        })
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let body = &text[head_end + 4..];
                    if body.len() < length {
                        continue;
                    }
                    let request = head.lines().next().unwrap_or_default();
                    let mut parts = request.split_whitespace();
                    let method = parts.next().unwrap_or_default().to_owned();
                    let path = parts.next().unwrap_or_default().to_owned();
                    seen.lock().unwrap().push(format!("{method} {path}"));
                    sent.lock().unwrap().push(body[..length].to_owned());
                    let payload = if path.ends_with("/messages") {
                        MESSAGE.to_owned()
                    } else if method == "GET" && path.ends_with("/channels") {
                        format!("[{existing_channels}]")
                    } else if path.contains("/threads/active") {
                        active_threads.to_owned()
                    } else if path.contains("/threads/archived/") {
                        archived_threads.to_owned()
                    } else {
                        CHANNEL.to_owned()
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                    return;
                }
            });
        }
    });
    Stand {
        address,
        calls,
        bodies,
    }
}

// W5: A renamed tab must reuse its archived thread through the stable tab-id suffix.
#[tokio::test]
async fn w5_reuses_and_unarchives_a_thread_by_tab_id_suffix() {
    let stand = stand_with_threads(CHANNEL, EMPTY_THREADS, ARCHIVED_TAB).await;
    let _ = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "workspace-ws",
        "current-label [tab-7]",
        "tab-7",
    )
    .await;
    let calls = stand.calls.lock().unwrap().clone();
    let bodies = stand.bodies.lock().unwrap().clone();
    assert!(
        !calls
            .iter()
            .any(|call| call == "POST /api/v10/channels/100/threads"),
        "created a duplicate tab thread; calls: {calls:?}"
    );
    assert!(
        calls.iter().zip(&bodies).any(|(call, body)| {
            call == "PATCH /api/v10/channels/101" && body.contains("\"archived\":false")
        }),
        "did not unarchive the matched tab thread; calls: {calls:?} bodies: {bodies:?}"
    );
}

#[tokio::test]
async fn w6_refuses_duplicate_discord_threads_for_one_tab_id() {
    let stand = stand_with_threads(CHANNEL, DUPLICATE_TAB_THREADS, EMPTY_THREADS).await;
    let result = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "workspace-ws",
        "current-label [tab-7]",
        "tab-7",
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        "Discord topology has duplicate threads for tab tab-7"
    );
}

#[tokio::test]
async fn archived_thread_pagination_must_advance() {
    let cases = [
        (
            STALLED_ARCHIVED_TAB,
            "Discord returned archived threads without an advancing pagination cursor",
        ),
        (
            EMPTY_ARCHIVED_PAGE_WITH_MORE,
            "Discord returned an empty archived-thread page with has_more",
        ),
    ];
    for (page, expected) in cases {
        let stand = stand_with_threads(CHANNEL, EMPTY_THREADS, page).await;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            sync_topology(
                &client(&stand.address),
                Id::new(1),
                "ws",
                "workspace-ws",
                "tab [tab-7]",
                "tab-7",
            ),
        )
        .await
        .expect("archived-thread pagination did not fail fast");
        assert_eq!(result.unwrap_err(), expected);
    }
}

fn client(address: &str) -> Client {
    // The shipped binary omits this line; see finding 1. Installed here so the wire defects are visible.
    let _ = rustls::crypto::ring::default_provider().install_default();
    Client::builder()
        .token("Bot local".to_owned())
        .proxy(address.to_owned(), true)
        .build()
}

// W1: src/lib.rs:760 — PARITY 18/19: the workspace channel is matched by name, not by its topic marker.
#[tokio::test]
async fn w1_reuses_the_channel_carrying_the_workspace_topic_marker() {
    let stand = stand(CHANNEL).await;
    let _ = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "workspace-ws",
        "tab [tab-7]",
        "tab-7",
    )
    .await;
    let calls = stand.calls.lock().unwrap().clone();
    let bodies = stand.bodies.lock().unwrap().clone();
    assert!(
        !calls
            .iter()
            .zip(&bodies)
            .any(|(call, body)| call == "POST /api/v10/guilds/1/channels"
                && body.contains("herdr workspace [ws]")),
        "recreated a workspace channel that already exists; calls: {calls:?} bodies: {bodies:?}"
    );
}

#[tokio::test]
async fn refuses_duplicate_channels_carrying_one_workspace_topic_marker() {
    let stand = stand(DUPLICATE_CHANNELS).await;
    let result = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "workspace-ws",
        "tab [tab-7]",
        "tab-7",
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        "Discord topology has duplicate channels for workspace ws"
    );
    let calls = stand.calls.lock().unwrap().clone();
    assert_eq!(calls, ["GET /api/v10/guilds/1/channels"]);
}

#[tokio::test]
async fn keeps_the_name_of_the_channel_identified_by_topic() {
    let stand = stand(CHANNEL).await;
    let _ = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "changed-workspace-ws",
        "tab [tab-7]",
        "tab-7",
    )
    .await;
    let calls = stand.calls.lock().unwrap().clone();
    assert!(
        !calls
            .iter()
            .any(|call| call == "PATCH /api/v10/channels/100"),
        "renamed a workspace channel identified by its topic; calls: {calls:?}"
    );
}

// W2: src/lib.rs:770 — PARITY 20/21/22: the tab is created as a guild channel, never as a thread.
#[tokio::test]
async fn w2_creates_the_tab_as_a_thread() {
    let stand = stand(CHANNEL).await;
    let _ = sync_topology(
        &client(&stand.address),
        Id::new(1),
        "ws",
        "workspace-ws",
        "tab [tab-7]",
        "tab-7",
    )
    .await;
    let calls = stand.calls.lock().unwrap().clone();
    assert!(
        calls.iter().any(|call| call.contains("/threads")),
        "no thread endpoint was touched; calls seen = {calls:?}"
    );
}

// W3: src/lib.rs:808 — PARITY 41/42/43: live status is recreated each poll and never edited.
#[tokio::test]
async fn w3_live_status_is_edited_not_recreated() {
    let stand = stand(CHANNEL).await;
    let client = client(&stand.address);
    let _ = update_live_status(&client, Id::new(100), "t1", None).await;
    let _ = update_live_status(&client, Id::new(100), "t1", None).await;
    let calls = stand.calls.lock().unwrap().clone();
    let posts = calls
        .iter()
        .filter(|call| call.starts_with("POST /api/v10/channels/100/messages"))
        .count();
    assert_eq!(
        posts, 1,
        "the second update created another message instead of editing; calls: {calls:?}"
    );
}

// W4: src/lib.rs:786 — PARITY 27/30: the card is delivered as bare content, without embed or mention.
#[tokio::test]
async fn w4_delivery_carries_the_embed_and_owner_mention() {
    let stand = stand(CHANNEL).await;
    let _ = deliver_transition(
        &client(&stand.address),
        Id::new(100),
        "needs input",
        "t1-blocked-1",
    )
    .await;
    let bodies = stand.bodies.lock().unwrap().clone();
    let body = bodies.join("");
    assert!(
        body.contains("embeds") && body.contains("allowed_mentions"),
        "delivered body was: {body}"
    );
}
