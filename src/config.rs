pub const ENV_DISCORD_TOKEN: &str = "DISCORD_TOKEN";
pub const ENV_DISCORD_GUILD_ID: &str = "DISCORD_GUILD_ID";
pub const ENV_DISCORD_OWNER_ID: &str = "DISCORD_OWNER_ID";
pub const ENV_HOME: &str = "HOME";

#[derive(Debug, PartialEq, Eq)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub owner_id: String,
    pub token: String,
}

/// Loads and validates Discord configuration.
///
/// # Errors
///
/// Returns all missing or blank required variables.
pub fn load_discord_config(environment: &[(&str, &str)]) -> Result<DiscordConfig, String> {
    let value = |n: &str| environment.iter().find(|(k, _)| *k == n).map(|(_, v)| *v);
    let names = [
        ENV_DISCORD_TOKEN,
        ENV_DISCORD_GUILD_ID,
        ENV_DISCORD_OWNER_ID,
    ];
    let resolved = names.map(|name| value(name).map(str::trim).filter(|v| !v.is_empty()));
    let [Some(token), Some(guild_id), Some(owner_id)] = resolved else {
        let missing: Vec<_> = names
            .into_iter()
            .zip(resolved)
            .filter_map(|(name, value)| value.is_none().then_some(name))
            .collect();
        return Err(format!(
            "Missing required environment variables: {}",
            missing.join(", ")
        ));
    };
    Ok(DiscordConfig {
        guild_id: guild_id.into(),
        owner_id: owner_id.into(),
        token: token.into(),
    })
}
