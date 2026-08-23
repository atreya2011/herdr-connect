use herdr_connect_rs::load_discord_config;

#[test]
fn config_loading() {
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
