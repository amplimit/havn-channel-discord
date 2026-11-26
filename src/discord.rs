//! Discord's REST side, over twilight-http: sending text, files and
//! components, editing and deleting the messages of a reply being
//! written, opening threads and private channels, the typing
//! indicator, the command menu, answers to interactions, and fetching
//! what people attach. No mention in what the bot sends pings anyone.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use twilight_http::Client;
use twilight_model::application::command::{
    Command, CommandOption, CommandOptionType, CommandType,
};
use twilight_model::application::interaction::InteractionContextType;
use twilight_model::channel::message::AllowedMentions;
use twilight_model::channel::message::component::Component;
use twilight_model::http::attachment::Attachment;
use twilight_model::http::interaction::InteractionResponse;
use twilight_model::id::Id;
use twilight_model::id::marker::{
    ApplicationMarker, ChannelMarker, InteractionMarker, MessageMarker, UserMarker,
};
use twilight_model::oauth::ApplicationIntegrationType;

use crate::text;
use crate::types::ChannelCommand;

/// The largest file the bot uploads: Discord's limit for a server without
/// boosts.
pub const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;

/// The option every slash command takes: what follows the command.
pub const COMMAND_TEXT: &str = "text";

/// Discord's error codes the adapter tells apart.
pub mod code {
    /// The bot may not do it here.
    pub const MISSING_PERMISSIONS: u64 = 50013;
    /// A thread was opened from that message already.
    pub const THREAD_EXISTS: u64 = 160_004;
}

#[derive(Clone)]
pub struct Discord {
    pub http: Arc<Client>,
    pub app: Id<ApplicationMarker>,
    pub me: Id<UserMarker>,
    cdn: reqwest::Client,
    /// Each person's private channel with the bot, as opened.
    dms: Arc<Mutex<HashMap<Id<UserMarker>, Id<ChannelMarker>>>>,
}

/// Mentions in what the bot sends ping no one: the agent's `@everyone` is
/// text.
fn silent() -> AllowedMentions {
    AllowedMentions::default()
}

/// Whether Discord refused a request as malformed (400).
fn bad_request(e: &twilight_http::Error) -> bool {
    matches!(
        e.kind(),
        twilight_http::error::ErrorType::Response { status, .. } if status.get() == 400
    )
}

/// Discord's error code of a failed request, if it gave one.
pub fn api_code(e: &twilight_http::Error) -> Option<u64> {
    match e.kind() {
        twilight_http::error::ErrorType::Response {
            error: twilight_http::api_error::ApiError::General(general),
            ..
        } => Some(general.code),
        _ => None,
    }
}

impl Discord {
    pub fn new(http: Arc<Client>, app: Id<ApplicationMarker>, me: Id<UserMarker>) -> Result<Self> {
        let cdn = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build the CDN client")?;
        Ok(Self {
            http,
            app,
            me,
            cdn,
            dms: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The private channel with `user`, as one of their messages named it
    /// or as opened now.
    pub async fn dm(&self, user: Id<UserMarker>) -> Result<Id<ChannelMarker>> {
        if let Some(found) = self.dms.lock().ok().and_then(|d| d.get(&user).copied()) {
            return Ok(found);
        }
        let channel = self
            .http
            .create_private_channel(user)
            .await
            .context("opening a private channel")?
            .model()
            .await
            .context("reading the private channel")?;
        self.note_dm(user, channel.id);
        Ok(channel.id)
    }

    /// `user` wrote in private channel `channel`.
    pub fn note_dm(&self, user: Id<UserMarker>, channel: Id<ChannelMarker>) {
        if let Ok(mut dms) = self.dms.lock() {
            if dms.len() >= 10_000 {
                dms.clear();
            }
            dms.insert(user, channel);
        }
    }

    /// Send `text` to `channel` in parts, the first a reply to `reply`
    /// (when it is in that channel), the last with `components` under it.
    pub async fn send_text(
        &self,
        channel: Id<ChannelMarker>,
        text: &str,
        reply: Option<Id<MessageMarker>>,
        components: &[Component],
    ) -> Result<()> {
        let mut parts = text::split(text);
        if parts.is_empty() && !components.is_empty() {
            parts.push("\u{200b}".to_string());
        }
        let last = parts.len().saturating_sub(1);
        let mentions = silent();
        for (n, part) in parts.iter().enumerate() {
            let mut request = self
                .http
                .create_message(channel)
                .content(part)
                .allowed_mentions(Some(&mentions));
            if n == 0
                && let Some(reply) = reply
            {
                request = request.reply(reply).fail_if_not_exists(false);
            }
            let with_components = n == last && !components.is_empty();
            if with_components {
                request = request.components(components);
            }
            let mut sent = request.await;
            // Components Discord refuses (400) do not take the text with them.
            if with_components
                && let Err(e) = &sent
                && bad_request(e)
            {
                tracing::warn!(error = %e, "discord refused the buttons; sending the text alone");
                let mut again = self
                    .http
                    .create_message(channel)
                    .content(part)
                    .allowed_mentions(Some(&mentions));
                if n == 0
                    && let Some(reply) = reply
                {
                    again = again.reply(reply).fail_if_not_exists(false);
                }
                sent = again.await;
            }
            if let Err(e) = sent {
                if n == 0 {
                    return Err(e).context("sending a message");
                }
                // The person is told the rest did not come.
                let _ = self
                    .http
                    .create_message(channel)
                    .content("(The rest of this reply could not be sent.)")
                    .allowed_mentions(Some(&mentions))
                    .await;
                return Err(e).context("sending a part of a message");
            }
        }
        Ok(())
    }

    /// Send `part`, text of one message, to `channel`, a reply to `reply`,
    /// with `components` under it (when Discord refuses them, the text
    /// goes alone): its id.
    pub async fn send_part(
        &self,
        channel: Id<ChannelMarker>,
        part: &str,
        reply: Option<Id<MessageMarker>>,
        components: &[Component],
    ) -> Result<Id<MessageMarker>> {
        let mentions = silent();
        let mut sent = None;
        for under in [components, &[]] {
            let mut request = self
                .http
                .create_message(channel)
                .content(part)
                .allowed_mentions(Some(&mentions));
            if let Some(reply) = reply {
                request = request.reply(reply).fail_if_not_exists(false);
            }
            if !under.is_empty() {
                request = request.components(under);
            }
            match request.await {
                Err(e) if !under.is_empty() && bad_request(&e) => {
                    tracing::warn!(error = %e, "discord refused the buttons; sending the text alone");
                }
                done => {
                    sent = Some(done);
                    break;
                }
            }
        }
        let Some(sent) = sent else {
            bail!("sending a message: nothing was sent");
        };
        let message = sent
            .context("sending a message")?
            .model()
            .await
            .context("reading the message sent")?;
        Ok(message.id)
    }

    /// Show `part` in message `id` instead, with `components` under it
    /// (none takes them off).
    pub async fn edit_part(
        &self,
        channel: Id<ChannelMarker>,
        id: Id<MessageMarker>,
        part: &str,
        components: &[Component],
    ) -> Result<()> {
        let mentions = silent();
        self.http
            .update_message(channel, id)
            .content(Some(part))
            .components(Some(components))
            .allowed_mentions(Some(&mentions))
            .await
            .context("editing a message")?;
        Ok(())
    }

    /// Delete the bot's message `id`. Best-effort.
    pub async fn delete(&self, channel: Id<ChannelMarker>, id: Id<MessageMarker>) {
        if let Err(e) = self.http.delete_message(channel, id).await {
            tracing::debug!(error = %e, "discord: a message could not be deleted");
        }
    }

    /// Send file `filename` to `channel`, with `caption` (its first part;
    /// the rest goes after it).
    pub async fn send_file(
        &self,
        channel: Id<ChannelMarker>,
        filename: &str,
        bytes: Vec<u8>,
        caption: Option<&str>,
        reply: Option<Id<MessageMarker>>,
    ) -> Result<()> {
        if bytes.len() > MAX_UPLOAD_BYTES {
            bail!(
                "the file is larger than Discord's {} MB",
                MAX_UPLOAD_BYTES / (1024 * 1024)
            );
        }
        let parts = caption.map(text::split).unwrap_or_default();
        let files = [Attachment::from_bytes(filename.to_string(), bytes, 0)];
        let mentions = silent();
        let mut request = self
            .http
            .create_message(channel)
            .attachments(&files)
            .allowed_mentions(Some(&mentions));
        if let Some(first) = parts.first() {
            request = request.content(first);
        }
        if let Some(reply) = reply {
            request = request.reply(reply).fail_if_not_exists(false);
        }
        request.await.context("sending a file")?;
        for part in parts.iter().skip(1) {
            self.send_text(channel, part, None, &[]).await?;
        }
        Ok(())
    }

    /// Show that the bot is typing in `channel`, for about 10 seconds.
    pub async fn typing(&self, channel: Id<ChannelMarker>) {
        if let Err(e) = self.http.create_typing_trigger(channel).await {
            tracing::debug!(error = %e, "discord typing failed");
        }
    }

    /// Open a thread named `name` from message `message` in `channel`: its
    /// id is the message's. One opened already is taken as it is.
    pub async fn open_thread(
        &self,
        channel: Id<ChannelMarker>,
        message: Id<MessageMarker>,
        name: &str,
    ) -> Result<Id<ChannelMarker>> {
        match self
            .http
            .create_thread_from_message(channel, message, name)
            .await
        {
            Ok(_) => Ok(message.cast()),
            Err(e) if api_code(&e) == Some(code::THREAD_EXISTS) => Ok(message.cast()),
            Err(e) => Err(anyhow::Error::new(e).context("opening a thread")),
        }
    }

    /// Set the bot's slash commands to the gateway's `commands`: how many.
    pub async fn set_commands(&self, commands: &[ChannelCommand]) -> Result<usize> {
        // A name twice fails the whole list: the first is kept.
        let mut seen = std::collections::HashSet::new();
        let list: Vec<Command> = commands
            .iter()
            .filter_map(slash_command)
            .filter(|c| seen.insert(c.name.clone()))
            .take(100)
            .collect();
        self.http
            .interaction(self.app)
            .set_global_commands(&list)
            .await
            .context("setting the slash commands")?;
        Ok(list.len())
    }

    /// Answer interaction `id`.
    pub async fn respond(
        &self,
        id: Id<InteractionMarker>,
        token: &str,
        response: &InteractionResponse,
    ) -> Result<()> {
        self.http
            .interaction(self.app)
            .create_response(id, token, response)
            .await
            .context("answering an interaction")?;
        Ok(())
    }

    /// The bytes of an attachment at `url`, from Discord's file hosts
    /// only, at most `max` of them.
    pub async fn download(&self, url: &str, max: usize) -> Result<Vec<u8>> {
        let parsed = file_host_url(url)?;
        let mut resp = self
            .cdn
            .get(parsed)
            .send()
            .await
            .context("fetching an attachment")?;
        if !resp.status().is_success() {
            bail!("Discord did not give the attachment ({})", resp.status());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.context("reading an attachment")? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > max {
                bail!("the attachment is larger than {} MB", max / (1024 * 1024));
            }
        }
        Ok(bytes)
    }
}

/// `url` when it is on one of Discord's file hosts, over https.
fn file_host_url(url: &str) -> Result<url::Url> {
    let parsed = url::Url::parse(url).context("an attachment URL")?;
    let host = parsed.host_str().unwrap_or_default();
    if parsed.scheme() != "https" || !matches!(host, "cdn.discordapp.com" | "media.discordapp.net")
    {
        bail!("not one of Discord's file hosts: {host}");
    }
    Ok(parsed)
}

/// The slash command for a gateway command, or `None` for a name Discord
/// does not take: lowercase letters, digits, `-` and `_`, at most 32.
fn slash_command(c: &ChannelCommand) -> Option<Command> {
    let name = c.name.trim();
    let valid = !name.is_empty()
        && name.chars().count() <= 32
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_');
    if !valid {
        return None;
    }
    let mut description: String = c.description.trim().chars().take(100).collect();
    if description.is_empty() {
        description = name.to_string();
    }
    // A command that takes nothing has no field for it (havn issue #294);
    // one that takes something says what.
    let options = match c.args.as_deref().map(str::trim) {
        Some("") => Vec::new(),
        args => vec![text_option(args)],
    };
    Some(Command {
        application_id: None,
        contexts: Some(vec![
            InteractionContextType::Guild,
            InteractionContextType::BotDm,
        ]),
        default_member_permissions: None,
        description,
        description_localizations: None,
        #[allow(deprecated)]
        dm_permission: None,
        guild_id: None,
        id: None,
        integration_types: Some(vec![ApplicationIntegrationType::GuildInstall]),
        kind: CommandType::ChatInput,
        name: name.to_string(),
        name_localizations: None,
        nsfw: None,
        options,
        version: Id::new(1),
    })
}

/// The optional [`COMMAND_TEXT`] of a command, described by what it takes,
/// as `[name | number]`, when that is known.
fn text_option(args: Option<&str>) -> CommandOption {
    let description = match args {
        Some(args) => args.chars().take(100).collect(),
        None => "What follows the command".into(),
    };
    CommandOption {
        autocomplete: None,
        channel_types: None,
        choices: None,
        description,
        description_localizations: None,
        kind: CommandOptionType::String,
        max_length: Some(1900),
        max_value: None,
        min_length: None,
        min_value: None,
        name: COMMAND_TEXT.into(),
        name_localizations: None,
        options: None,
        file_types: None,
        required: Some(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_discord_does_not_take_are_left_out() {
        let c = |name: &str, description: &str| ChannelCommand {
            name: name.into(),
            description: description.into(),
            args: None,
        };
        let made = slash_command(&c("model", "Choose the model")).expect("valid");
        assert_eq!(made.name, "model");
        assert_eq!(made.options[0].name, COMMAND_TEXT);
        assert_eq!(made.options[0].required, Some(false));
        assert_eq!(
            slash_command(&c("new", "")).expect("valid").description,
            "new"
        );
        assert_eq!(
            slash_command(&c("x", &"d".repeat(300)))
                .expect("valid")
                .description
                .chars()
                .count(),
            100
        );
        for bad in ["", "Model", "a b", &"x".repeat(33), "café"] {
            assert!(slash_command(&c(bad, "d")).is_none(), "{bad}");
        }
        assert_eq!(made.options[0].max_length, Some(1900));
    }

    #[test]
    fn a_command_has_a_text_field_only_when_it_takes_something() {
        let with = |args: Option<&str>| {
            slash_command(&ChannelCommand {
                name: "model".into(),
                description: "Choose the model".into(),
                args: args.map(Into::into),
            })
            .expect("valid")
            .options
        };
        assert!(with(Some("")).is_empty(), "/new takes nothing");
        assert!(with(Some("  ")).is_empty());
        let named = with(Some("[name | number | default]"));
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].description, "[name | number | default]");
        assert_eq!(
            with(None)[0].description,
            "What follows the command",
            "an older gateway's may take anything"
        );
        assert_eq!(
            with(Some(&"a".repeat(150)))[0].description.chars().count(),
            100
        );
    }

    #[test]
    fn files_come_only_from_discords_hosts_over_https() {
        assert!(file_host_url("https://cdn.discordapp.com/attachments/1/2/a.png").is_ok());
        assert!(file_host_url("https://media.discordapp.net/x").is_ok());
        for bad in [
            "http://cdn.discordapp.com/a",
            "https://cdn.discordapp.com.evil.example/a",
            "https://evil.example/cdn.discordapp.com",
            "file:///etc/passwd",
            "not a url",
        ] {
            assert!(file_host_url(bad).is_err(), "{bad}");
        }
    }
}
