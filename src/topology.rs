/// Synchronizes the Discord workspace topology.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    workspace: &str,
    tab: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>, String> {
    let channels = client
        .guild_channels(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let topic = format!("herdr workspace [{workspace}]");
    let workspace_channel = if let Some(channel) = channels
        .iter()
        .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
    {
        channel.id
    } else {
        client
            .create_guild_channel(guild, workspace)
            .topic(&topic)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .id
    };
    let tab_name = format!("{tab} [{tab}]");
    let mut existing = match client.active_threads(guild).await {
        Ok(response) => response
            .model()
            .await
            .map_or_else(|_| Vec::new(), |listing| listing.threads),
        Err(_) => Vec::new(),
    };
    if let Ok(response) = client
        .public_archived_threads(workspace_channel)
        .limit(100)
        .await
        && let Ok(listing) = response.model().await
    {
        existing.extend(listing.threads);
    }
    if let Some(thread) = existing.into_iter().find(|thread| {
        thread.parent_id == Some(workspace_channel)
            && thread.name.as_deref() == Some(tab_name.as_str())
    }) {
        if thread
            .thread_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.archived)
        {
            client
                .update_thread(thread.id)
                .archived(false)
                .await
                .map_err(|error| error.to_string())?;
        }
        return Ok(thread.id);
    }
    Ok(client
        .create_thread(
            workspace_channel,
            &tab_name,
            twilight_model::channel::ChannelType::PublicThread,
        )
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?
        .id)
}
