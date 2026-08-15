use herdr_connect_rs::{load_config, load_discord_config};

#[test]
fn config_loading() {
    let cases = [
        (
            vec![("HERDR_SOCKET_PATH", "/run/herdr.sock")],
            "/run/herdr.sock",
        ),
        (
            vec![("OTHER", "ignored")],
            "/home/u/.config/herdr/herdr.sock",
        ),
    ];
    for (environment, socket) in cases {
        let actual = load_config(&environment, "/home/u");
        assert_eq!(actual.herdr_socket_path, socket);
        assert_eq!(actual.poll_interval_ms, 1_500);
    }
    let discord = [
        (
            vec![
                ("DISCORD_TOKEN", "token"),
                ("DISCORD_GUILD_ID", "guild"),
                ("DISCORD_OWNER_ID", "owner"),
            ],
            true,
        ),
        (vec![("DISCORD_GUILD_ID", "guild")], false),
        (
            vec![
                ("DISCORD_TOKEN", "token"),
                ("DISCORD_GUILD_ID", "guild"),
                ("DISCORD_OWNER_ID", "   "),
            ],
            false,
        ),
    ];
    for (environment, valid) in discord {
        assert_eq!(load_discord_config(&environment).is_ok(), valid);
    }
}
