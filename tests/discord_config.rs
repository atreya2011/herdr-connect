use herdr_connect_rs::load_discord_config;

#[test]
fn reads_required_values() {
    let actual = load_discord_config(&[
        ("DISCORD_GUILD_ID", "guild-123"),
        ("DISCORD_OWNER_ID", "owner-123"),
        ("DISCORD_TOKEN", "token-123"),
    ])
    .unwrap();
    assert_eq!(actual.guild_id, "guild-123");
    assert_eq!(actual.owner_id, "owner-123");
    assert_eq!(actual.token, "token-123");
}

#[test]
fn rejects_reference_invalid_environment_table() {
    let cases = [
        (
            &[][..],
            "Missing required environment variables: DISCORD_TOKEN, DISCORD_GUILD_ID, DISCORD_OWNER_ID",
        ),
        (
            &[
                ("DISCORD_GUILD_ID", "guild-123"),
                ("DISCORD_OWNER_ID", "owner-123"),
                ("DISCORD_TOKEN", "   "),
            ][..],
            "Missing required environment variables: DISCORD_TOKEN",
        ),
    ];
    for (environment, expected) in cases {
        assert_eq!(load_discord_config(environment).unwrap_err(), expected);
    }
}
