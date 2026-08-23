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
    let names = ["DISCORD_TOKEN", "DISCORD_GUILD_ID", "DISCORD_OWNER_ID"];
    let missing: Vec<_> = names
        .into_iter()
        .filter(|n| value(n).is_none_or(|v| v.trim().is_empty()))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "Missing required environment variables: {}",
            missing.join(", ")
        ));
    }
    Ok(DiscordConfig {
        guild_id: value("DISCORD_GUILD_ID")
            .ok_or_else(|| "DISCORD_GUILD_ID missing".to_owned())?
            .trim()
            .into(),
        owner_id: value("DISCORD_OWNER_ID")
            .ok_or_else(|| "DISCORD_OWNER_ID missing".to_owned())?
            .trim()
            .into(),
        token: value("DISCORD_TOKEN")
            .ok_or_else(|| "DISCORD_TOKEN missing".to_owned())?
            .trim()
            .into(),
    })
}
