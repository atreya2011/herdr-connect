use twilight_http::Client;
use twilight_model::id::Id;

// C1: src/main.rs:56 — the binary builds its Discord client without installing a rustls provider.
#[tokio::test]
async fn c1_main_client_construction_does_not_panic() {
    // Same construction as src/main.rs:56. The proxy only keeps the request off the real network;
    // it does not change the panic, because twilight builds a TLS connector either way.
    let client = Client::builder()
        .token("Bot local".to_owned())
        .proxy("127.0.0.1:1".to_owned(), true)
        .build();
    let _ = client.guild_channels(Id::new(1)).await;
}
