//! `havn-channel-discord`: bridges a Discord bot to havn's
//! `/api/v1/channel` WebSocket.
//!
//! - **Discord to havn.** Discord's gateway, over twilight. A private
//!   message goes on as it is. In a server a message goes on with its
//!   place (havn issue #285): the channel, as the group let in; the server,
//!   which may be let in as a whole; and the thread, the conversation.
//!   - A message that mentions the bot or replies to it in a channel
//!     starts a thread, named from it, with the message's id. The thread
//!     is opened only when the first reply goes, so a channel not let in
//!     is left as it was.
//!   - Every message in a thread the bot opened is to it.
//!   - Slash commands, button presses and choices in a menu go on as text
//!     from the person who used them.
//! - **havn to Discord.** Replies as text (split under Discord's limit),
//!   files, buttons and select menus, in the private channel, the
//!   channel or the thread they are for. No mention pings anyone.

#![forbid(unsafe_code)]

mod config;
mod discord;
mod gateway;
mod havn_ws;
mod text;
mod types;

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::Parser;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;
use twilight_cache_inmemory::{DefaultInMemoryCache, ResourceType};
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_model::application::interaction::{Interaction, InteractionData, InteractionType};
use twilight_model::channel::message::component::{
    ActionRow, Button, ButtonStyle, Component, SelectMenu, SelectMenuOption, SelectMenuType,
};
use twilight_model::channel::message::{AllowedMentions, Message, MessageFlags, MessageType};
use twilight_model::channel::{Channel, ChannelType};
use twilight_model::gateway::payload::incoming::MessageCreate;
use twilight_model::http::interaction::{
    InteractionResponse, InteractionResponseData, InteractionResponseType,
};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker};
use twilight_model::oauth::ApplicationFlags;

use crate::config::Config;
use crate::discord::Discord;
use crate::gateway::Gateway;
use crate::havn_ws::{WsClient, WsConfig};
use crate::types::{
    Attachment, ChannelFrame, ChannelGroup, ChannelInbound, ChannelMessageContent, ChannelOutbound,
    ChannelPlace,
};

#[derive(Debug, Parser)]
#[command(
    name = "havn-channel-discord",
    about = "havn channel adapter for Discord",
    version
)]
struct Cli {
    /// The daemon's TOML config file (see README).
    #[arg(
        long,
        env = "HAVN_DC_CONFIG",
        default_value = "/etc/havn-channel-discord/config.toml"
    )]
    config: PathBuf,
    /// Where the daemon keeps the last reply it delivered, to resume from
    /// after a restart. Defaults to `<account_id>.state.json` in systemd's
    /// `StateDirectory`, else next to the config file.
    #[arg(long, env = "HAVN_DC_STATE")]
    state: Option<PathBuf>,
}

/// The id prefix of everything this adapter names: `discord:`.
const PREFIX: &str = "discord:";

/// Whether the bot sees the text of every message: Discord gave it the
/// Message Content intent. Without it, a message's text reaches the bot
/// only when it mentions the bot.
static SEES_ALL: AtomicBool = AtomicBool::new(false);

/// Whether the gateway takes server messages (havn issue #263): until it
/// says so they are not passed on, as an older one would take them for
/// private ones.
static GATEWAY_GROUPS: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;
    info!(
        config = %cli.config.display(),
        gateway = %cfg.havn_gateway,
        channel = %cfg.channel_id,
        account = %cfg.account_id,
        "havn-channel-discord starting"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let http = Arc::new(twilight_http::Client::new(cfg.bot_token.clone()));
    let dc = identify(&http).await?;
    let sees_all = SEES_ALL.load(Ordering::Relaxed);

    let gateway = Gateway::new(
        &cfg.havn_gateway,
        &cfg.adapter_token,
        &cfg.channel_id,
        &cfg.account_id,
    )
    .context("havn gateway client init")?;
    let state = cli.state.clone().or_else(|| {
        let dir = std::env::var("STATE_DIRECTORY")
            .ok()
            .and_then(|dirs| dirs.split(':').next().map(PathBuf::from))
            .filter(|dir| !dir.as_os_str().is_empty())
            .or_else(|| cli.config.parent().map(std::path::Path::to_path_buf))?;
        Some(havn_ws::Delivered::file_in(&dir, &cfg.account_id))
    });
    let delivered = Arc::new(havn_ws::Delivered::load(state, &cfg.account_id));
    let WsClient {
        outbound_tx,
        inbound_rx,
    } = WsClient::spawn(WsConfig {
        gateway_url: cfg.havn_gateway.clone(),
        adapter_token: cfg.adapter_token.clone(),
        channel_id: cfg.channel_id.clone(),
        account_id: cfg.account_id.clone(),
        delivered: Arc::clone(&delivered),
    });

    let cache = Arc::new(
        DefaultInMemoryCache::builder()
            .resource_types(ResourceType::CHANNEL | ResourceType::GUILD | ResourceType::ROLE)
            .build(),
    );
    let typing = {
        let dc = dc.clone();
        Typing::new(move |to: String| {
            let dc = dc.clone();
            Box::pin(async move {
                if let Ok(channel) = typing_channel(&dc, &to).await {
                    dc.typing(channel).await;
                }
            })
        })
    };

    let mut intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::DIRECT_MESSAGES;
    if sees_all {
        intents |= Intents::MESSAGE_CONTENT;
    }
    let shard = Shard::new(ShardId::ONE, cfg.bot_token.clone(), intents);
    let ctx = Ctx {
        dc: dc.clone(),
        gateway: gateway.clone(),
        cache: Arc::clone(&cache),
        typing: typing.clone(),
        account_id: cfg.account_id.clone(),
        to_havn: outbound_tx.clone(),
        seq: Arc::new(AtomicU64::new(0)),
        workers: Workers::default(),
    };
    let inbound_task = tokio::spawn(discord_inbound_loop(shard, ctx));
    let outbound_task = tokio::spawn(havn_outbound_loop(
        dc, gateway, inbound_rx, typing, delivered,
    ));
    // A loop that ends is an error, so systemd restarts the daemon.
    let ended = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl-C, shutting down");
            Ok(())
        }
        r = inbound_task => Err(anyhow::anyhow!("the Discord connection ended: {r:?}")),
        r = outbound_task => Err(anyhow::anyhow!("the havn outbound loop ended: {r:?}")),
    };
    drop(outbound_tx);
    ended
}

/// Who the bot is, and whether it may read every message ([`SEES_ALL`]);
/// tried again until Discord answers, but for a token it refuses.
async fn identify(http: &Arc<twilight_http::Client>) -> Result<Discord> {
    let (app, me) = loop {
        let found = async {
            let app = http.current_user_application().await?.model().await?;
            let me = http.current_user().await?.model().await?;
            anyhow::Ok((app, me))
        }
        .await;
        match found {
            Ok(found) => break found,
            Err(e) if unauthorized(&e) => {
                return Err(e).context("Discord refused the bot token");
            }
            Err(e) => {
                warn!(error = %e, "discord: could not identify the bot; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    let flags = app.flags.unwrap_or_else(ApplicationFlags::empty);
    let sees_all = flags.intersects(
        ApplicationFlags::GATEWAY_MESSAGE_CONTENT
            | ApplicationFlags::GATEWAY_MESSAGE_CONTENT_LIMITED,
    );
    SEES_ALL.store(sees_all, Ordering::Relaxed);
    info!(bot = %me.name, sees_all, "discord bot identified");
    if !sees_all {
        warn!(
            "the Message Content intent is off: in a server the bot reads only messages that \
             mention it, so a thread it opened needs a mention in each message; turn it on in \
             the Developer Portal (Bot, Privileged Gateway Intents) and restart the daemon"
        );
    }
    Discord::new(Arc::clone(http), app.id, me.id)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,havn_channel_discord=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

/// Whether Discord refused the token (401).
fn unauthorized(e: &anyhow::Error) -> bool {
    e.downcast_ref::<twilight_http::Error>()
        .is_some_and(|e| matches!(e.kind(), twilight_http::error::ErrorType::Unauthorized))
}

/// What the Discord side shares.
#[derive(Clone)]
struct Ctx {
    dc: Discord,
    gateway: Gateway,
    cache: Arc<DefaultInMemoryCache>,
    typing: Typing,
    account_id: String,
    to_havn: tokio::sync::mpsc::Sender<ChannelFrame>,
    seq: Arc<AtomicU64>,
    workers: Workers,
}

/// A worker for each channel with messages to pass on, so a channel's
/// messages go on in order while one's file does not hold up another's.
#[derive(Clone, Default)]
struct Workers(Arc<Mutex<HashMap<Id<ChannelMarker>, Queue>>>);

type Queue = tokio::sync::mpsc::UnboundedSender<Box<MessageCreate>>;

/// A worker with nothing to do for this long ends.
const WORKER_IDLE: Duration = Duration::from_secs(300);

impl Workers {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Id<ChannelMarker>, Queue>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `message` to its channel's worker, started when there is none (or
    /// the one there just ended).
    fn pass(&self, ctx: &Ctx, mut message: Box<MessageCreate>) {
        let channel = message.channel_id;
        // Sent under the lock (an unbounded send does not wait), so a worker
        // that ends, which takes it too, has nothing left behind.
        let mut workers = self.lock();
        loop {
            let queue = workers
                .entry(channel)
                .or_insert_with(|| self.start(ctx, channel));
            match queue.send(message) {
                Ok(()) => return,
                Err(back) => {
                    message = back.0;
                    workers.remove(&channel);
                }
            }
        }
    }

    fn start(&self, ctx: &Ctx, channel: Id<ChannelMarker>) -> Queue {
        let (queue, mut messages) = tokio::sync::mpsc::unbounded_channel::<Box<MessageCreate>>();
        let ctx = ctx.clone();
        let workers = self.clone();
        let own = queue.clone();
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(WORKER_IDLE, messages.recv()).await {
                    Ok(Some(message)) => on_message(&ctx, &message).await,
                    Ok(None) => return,
                    // Idle: it ends only when nothing came meanwhile, as the
                    // map's lock is held.
                    Err(_) => {
                        let mut all = workers.lock();
                        if messages.is_empty() {
                            if all.get(&channel).is_some_and(|q| q.same_channel(&own)) {
                                all.remove(&channel);
                            }
                            return;
                        }
                    }
                }
            }
        });
        queue
    }
}

/// Files fetched from Discord and given to the gateway at once, at most.
static TRANSFERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// Discord to havn: every event updates the cache; messages and
/// interactions go on. The shard reconnects and resumes on its own; it
/// ends only when Discord closes it for good (a token refused, an intent
/// not allowed).
async fn discord_inbound_loop(mut shard: Shard, ctx: Ctx) -> Result<()> {
    let wanted = EventTypeFlags::READY
        | EventTypeFlags::GUILD_CREATE
        | EventTypeFlags::GUILD_UPDATE
        | EventTypeFlags::GUILD_DELETE
        | EventTypeFlags::CHANNEL_CREATE
        | EventTypeFlags::CHANNEL_UPDATE
        | EventTypeFlags::CHANNEL_DELETE
        | EventTypeFlags::THREAD_CREATE
        | EventTypeFlags::THREAD_UPDATE
        | EventTypeFlags::THREAD_DELETE
        | EventTypeFlags::THREAD_LIST_SYNC
        | EventTypeFlags::ROLE_CREATE
        | EventTypeFlags::ROLE_UPDATE
        | EventTypeFlags::ROLE_DELETE
        | EventTypeFlags::MESSAGE_CREATE
        | EventTypeFlags::INTERACTION_CREATE;
    while let Some(item) = shard.next_event(wanted).await {
        let event = match item {
            Ok(event) => event,
            Err(e) => {
                // An event not read is not shown: it may hold a message or an
                // interaction's token.
                if matches!(
                    e.kind(),
                    twilight_gateway::error::ReceiveMessageErrorType::Deserializing { .. }
                ) {
                    warn!("discord gateway: an event could not be read");
                } else {
                    warn!(error = %e, "discord gateway: could not read an event");
                }
                continue;
            }
        };
        ctx.cache.update(&event);
        match event {
            Event::Ready(ready) => info!(guilds = ready.guilds.len(), "discord gateway ready"),
            Event::MessageCreate(m) => ctx.workers.pass(&ctx, m),
            Event::InteractionCreate(i) => {
                let ctx = ctx.clone();
                tokio::spawn(async move { on_interaction(&ctx, &i).await });
            }
            _ => {}
        }
    }
    anyhow::bail!("Discord closed the gateway connection for good (a token or intent refused)")
}

/// Where a server message was written, as the gateway names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Place {
    guild: Id<GuildMarker>,
    guild_name: Option<String>,
    /// The channel: the thread's parent, or where it was written.
    channel: Id<ChannelMarker>,
    channel_name: Option<String>,
    /// The channel's kind, when known: threads open from messages only in
    /// some.
    channel_kind: Option<ChannelType>,
    /// The thread it was written in, if any.
    thread: Option<Thread>,
}

/// Whether a thread can be opened from a message in a channel of `kind`:
/// a text or an announcement channel's (not a voice channel's chat).
fn opens_threads(kind: Option<ChannelType>) -> bool {
    matches!(
        kind,
        Some(ChannelType::GuildText | ChannelType::GuildAnnouncement)
    )
}

/// A thread a message was written in.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Thread {
    id: Id<ChannelMarker>,
    name: Option<String>,
    /// Who opened it.
    owner: Option<Id<UserMarker>>,
}

fn is_thread(kind: ChannelType) -> bool {
    matches!(
        kind,
        ChannelType::PublicThread | ChannelType::PrivateThread | ChannelType::AnnouncementThread
    )
}

/// A channel, from the cache or else from Discord (then kept).
async fn channel_of(ctx: &Ctx, id: Id<ChannelMarker>) -> Option<Channel> {
    if let Some(found) = ctx.cache.channel(id) {
        return Some(found.value().clone());
    }
    let channel = ctx.dc.http.channel(id).await.ok()?.model().await.ok()?;
    ctx.cache
        .update(&twilight_model::gateway::payload::incoming::ChannelUpdate(
            channel.clone(),
        ));
    Some(channel)
}

/// Where a message in channel `id` of `guild` was written.
async fn place_of(ctx: &Ctx, guild: Id<GuildMarker>, id: Id<ChannelMarker>) -> Option<Place> {
    let here = channel_of(ctx, id).await?;
    let guild_name = ctx.cache.guild(guild).map(|g| g.name().to_string());
    if is_thread(here.kind) {
        let parent_id = here.parent_id?;
        let parent = channel_of(ctx, parent_id).await;
        Some(Place {
            guild,
            guild_name,
            channel: parent_id,
            channel_kind: parent.as_ref().map(|p| p.kind),
            channel_name: parent.and_then(|p| p.name),
            thread: Some(Thread {
                id: here.id,
                name: here.name,
                owner: here.owner_id,
            }),
        })
    } else {
        Some(Place {
            guild,
            guild_name,
            channel: here.id,
            channel_kind: Some(here.kind),
            channel_name: here.name,
            thread: None,
        })
    }
}

fn user_id(id: Id<UserMarker>) -> String {
    format!("{PREFIX}{id}")
}

fn channel_group(id: Id<ChannelMarker>) -> String {
    format!("{PREFIX}c{id}")
}

fn thread_place(id: Id<ChannelMarker>) -> String {
    format!("{PREFIX}t{id}")
}

fn guild_space(id: Id<GuildMarker>) -> String {
    format!("{PREFIX}s{id}")
}

/// `discord:<channel>:<message>`: what a reply answers.
fn message_id(channel: Id<ChannelMarker>, message: u64) -> String {
    format!("{PREFIX}{channel}:{message}")
}

/// The channel and message of a [`message_id`].
fn parse_message_id(s: &str) -> Option<(Id<ChannelMarker>, Id<MessageMarker>)> {
    let (channel, message) = s.strip_prefix(PREFIX)?.split_once(':')?;
    Some((
        Id::new_checked(channel.parse().ok()?)?,
        Id::new_checked(message.parse().ok()?)?,
    ))
}

/// Where a reply `to` goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// A person, in their private channel with the bot.
    Person(Id<UserMarker>),
    Channel(Id<ChannelMarker>),
    Thread(Id<ChannelMarker>),
}

/// The target of `to`; `None` for anything else, a server included, which
/// is not a place to send to.
fn target_of(to: &str) -> Option<Target> {
    let rest = to.strip_prefix(PREFIX)?;
    let id = |s: &str| s.parse::<u64>().ok().and_then(Id::new_checked);
    if let Some(c) = rest.strip_prefix('c') {
        return id(c).map(Target::Channel);
    }
    if let Some(t) = rest.strip_prefix('t') {
        return id(t).map(Target::Thread);
    }
    id(rest).map(|u| Target::Person(u.cast()))
}

/// Threads named for a mention, not opened yet (they are when their
/// first reply goes): their channel and name, by thread id.
static WAITING_THREADS: std::sync::LazyLock<Mutex<WaitingThreads>> =
    std::sync::LazyLock::new(|| Mutex::new(Bounded::default()));

type WaitingThreads = Bounded<(Id<ChannelMarker>, String)>;

/// Threads that could not be opened: their channel, which their replies
/// go to.
static UNOPENED_THREADS: std::sync::LazyLock<Mutex<Bounded<Id<ChannelMarker>>>> =
    std::sync::LazyLock::new(|| Mutex::new(Bounded::default()));

/// Channels where a thread could not be opened (no right to): a mention
/// there is answered in the channel, as one conversation.
static NO_THREADS: std::sync::LazyLock<Mutex<Bounded<()>>> =
    std::sync::LazyLock::new(|| Mutex::new(Bounded::default()));

fn threads_fail_in(channel: Id<ChannelMarker>) -> bool {
    NO_THREADS
        .lock()
        .ok()
        .is_some_and(|n| n.get(channel.get()).is_some())
}

/// A map that forgets its oldest past [`Bounded::KEPT`].
struct Bounded<V> {
    map: HashMap<u64, V>,
    order: VecDeque<u64>,
}

impl<V> Default for Bounded<V> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<V> Bounded<V> {
    const KEPT: usize = 1024;

    fn put(&mut self, key: u64, value: V) {
        if self.map.insert(key, value).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > Self::KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
    }

    fn take(&mut self, key: u64) -> Option<V> {
        let value = self.map.remove(&key)?;
        self.order.retain(|k| *k != key);
        Some(value)
    }

    fn get(&self, key: u64) -> Option<&V> {
        self.map.get(&key)
    }
}

/// The channel a reply `to` shows typing in: a thread not opened yet shows
/// it in its channel.
async fn typing_channel(dc: &Discord, to: &str) -> Result<Id<ChannelMarker>> {
    match target_of(to).context("not a place to type in")? {
        Target::Person(user) => dc.dm(user).await,
        Target::Channel(c) => Ok(c),
        Target::Thread(t) => Ok(WAITING_THREADS
            .lock()
            .ok()
            .and_then(|w| w.get(t.get()).map(|(channel, _)| *channel))
            .unwrap_or(t)),
    }
}

/// The name a server member goes by: their nickname there, else their
/// display name, else their username.
fn shown_name(message: &Message) -> String {
    message
        .member
        .as_ref()
        .and_then(|m| m.nick.clone())
        .or_else(|| message.author.global_name.clone())
        .unwrap_or_else(|| message.author.name.clone())
}

/// Whether `message` is to the bot: it mentions it (or a role that is the
/// bot's), or replies to one of its messages.
fn mentions_bot(ctx: &Ctx, message: &Message) -> bool {
    let me = ctx.dc.me;
    message.mentions.iter().any(|m| m.id == me)
        || message.mention_roles.iter().any(|r| {
            ctx.cache.role(*r).is_some_and(|role| {
                role.resource().tags.as_ref().and_then(|t| t.bot_id) == Some(me)
            })
        })
        || message
            .referenced_message
            .as_ref()
            .is_some_and(|replied| replied.author.id == me)
}

/// `message`'s text without the mentions of the bot (and its role).
fn without_bot(ctx: &Ctx, message: &Message) -> String {
    let mut content = message
        .content
        .replace(&format!("<@{}>", ctx.dc.me), "")
        .replace(&format!("<@!{}>", ctx.dc.me), "");
    for role in &message.mention_roles {
        let bots = ctx
            .cache
            .role(*role)
            .is_some_and(|r| r.resource().tags.as_ref().and_then(|t| t.bot_id) == Some(ctx.dc.me));
        if bots {
            content = content.replace(&format!("<@&{role}>"), "");
        }
    }
    content
}

/// The names of what `message` refers to by id.
fn names_in(ctx: &Ctx, message: &Message) -> text::Names {
    let mut names = text::Names::default();
    for m in &message.mentions {
        let shown = m
            .member
            .as_ref()
            .and_then(|member| member.nick.clone())
            .unwrap_or_else(|| m.name.clone());
        names.users.insert(m.id.get(), shown);
    }
    for r in &message.mention_roles {
        if let Some(role) = ctx.cache.role(*r) {
            names.roles.insert(r.get(), role.resource().name.clone());
        }
    }
    for c in channel_refs(&message.content) {
        if let Some(name) = ctx.cache.channel(c).and_then(|ch| ch.name.clone()) {
            names.channels.insert(c.get(), name);
        }
    }
    names
}

/// The channels `content` names as `<#id>`.
fn channel_refs(content: &str) -> Vec<Id<ChannelMarker>> {
    content
        .split("<#")
        .skip(1)
        .filter_map(|rest| rest.split_once('>'))
        .filter_map(|(id, _)| id.parse().ok().and_then(Id::new_checked))
        .collect()
}

/// Characters of a replied-to message the agent is shown.
const REPLIED_CHARS: usize = 500;

/// `text` cut to [`REPLIED_CHARS`], as a quoted, escaped string.
fn quoted(text: &str) -> String {
    let mut cut: String = text.chars().take(REPLIED_CHARS).collect();
    if text.chars().count() > REPLIED_CHARS {
        cut.push('…');
    }
    serde_json::to_string(&cut).unwrap_or_default()
}

/// What `message` replies to, for the agent: a marked line, what the
/// sender supplied quoted and escaped.
fn context_lines(ctx: &Ctx, message: &Message) -> Vec<String> {
    let Some(replied) = &message.referenced_message else {
        return Vec::new();
    };
    let whose = if replied.author.id == ctx.dc.me {
        "the bot's message".to_string()
    } else if replied.author.id == message.author.id {
        "their own earlier message".to_string()
    } else {
        format!("a message by {}", quoted(&shown_name(replied)))
    };
    let what = if replied.content.is_empty() {
        if replied.attachments.is_empty() {
            "a message".to_string()
        } else {
            "a file".to_string()
        }
    } else {
        quoted(&text::readable(&replied.content, &names_in(ctx, replied)))
    };
    vec![format!(
        "[Replying to {whose} (user-supplied, untrusted): {what}]"
    )]
}

/// The group `place` is, as the gateway names it, for a message from
/// `sender_name`, addressed or not: and where its reply goes.
fn group_at(
    place: &Place,
    thread: Option<ChannelPlace>,
    sender_name: String,
    addressed: bool,
) -> (ChannelGroup, String) {
    let to = thread
        .as_ref()
        .map_or_else(|| channel_group(place.channel), |t| t.id.clone());
    let group = ChannelGroup {
        id: channel_group(place.channel),
        title: place.channel_name.clone(),
        sender_name: Some(sender_name),
        addressed,
        sees_all: Some(SEES_ALL.load(Ordering::Relaxed)),
        space: Some(ChannelPlace {
            id: guild_space(place.guild),
            title: place.guild_name.clone(),
        }),
        thread,
    };
    (group, to)
}

/// The thread of `place` as the gateway names it.
fn thread_of(place: &Place) -> Option<ChannelPlace> {
    place.thread.as_ref().map(|t| ChannelPlace {
        id: thread_place(t.id),
        title: t.name.clone(),
    })
}

/// The group of a message in a server, and where its reply goes; `None`
/// for one not passed on.
async fn server_group(
    ctx: &Ctx,
    guild: Id<GuildMarker>,
    message: &Message,
) -> Option<(ChannelGroup, String)> {
    if !GATEWAY_GROUPS.load(Ordering::Relaxed) {
        debug!("dropping a server message: the gateway has not said it takes them");
        return None;
    }
    let Some(place) = place_of(ctx, guild, message.channel_id).await else {
        warn!(channel = %message.channel_id, "discord: could not tell where a message was written; dropping");
        return None;
    };
    let in_own_thread = place
        .thread
        .as_ref()
        .is_some_and(|t| t.owner == Some(ctx.dc.me));
    let addressed = in_own_thread || mentions_bot(ctx, message);
    let thread = if place.thread.is_some() {
        thread_of(&place)
    } else if addressed && opens_threads(place.channel_kind) && !threads_fail_in(place.channel) {
        // A mention in a channel starts a conversation of its own, named
        // from it without the bot's mention.
        let name = text::thread_name(&text::readable(
            &without_bot(ctx, message),
            &names_in(ctx, message),
        ));
        let id: Id<ChannelMarker> = message.id.cast();
        if let Ok(mut waiting) = WAITING_THREADS.lock() {
            waiting.put(id.get(), (place.channel, name.clone()));
        }
        Some(ChannelPlace {
            id: thread_place(id),
            title: Some(name),
        })
    } else {
        None
    };
    Some(group_at(&place, thread, shown_name(message), addressed))
}

/// The files of `message` on to the gateway, for one it answers; for one
/// it does not, or a file that could not go, a line in `said` names them.
async fn message_files(
    ctx: &Ctx,
    sender: &str,
    group: Option<&ChannelGroup>,
    message: &Message,
    said: &mut String,
) -> Vec<Attachment> {
    let mut attachments = Vec::new();
    let named = |s: &str| serde_json::to_string(s).unwrap_or_default();
    if group.is_some_and(|g| !g.addressed) {
        for file in &message.attachments {
            let _ = write!(said, "\n«a file: {}»", named(&file.filename));
        }
        return attachments;
    }
    let voice = message
        .flags
        .is_some_and(|f| f.contains(MessageFlags::IS_VOICE_MESSAGE));
    for file in &message.attachments {
        let name = if voice {
            "voice.ogg".to_string()
        } else {
            file.filename.clone()
        };
        match receive(ctx, sender, group, file, &name).await {
            Ok(a) => attachments.push(a),
            Err(e) => {
                warn!(file = %file.filename, error = %format!("{e:#}"), "discord: a file could not be passed on");
                let _ = write!(
                    said,
                    "\n(A file, {}, could not be passed on.)",
                    named(&file.filename)
                );
            }
        }
    }
    attachments
}

/// A message from Discord, on to the gateway if it is one to pass on.
async fn on_message(ctx: &Ctx, message: &Message) {
    if message.author.bot
        || message.webhook_id.is_some()
        || !matches!(message.kind, MessageType::Regular | MessageType::Reply)
    {
        return;
    }
    let sender = user_id(message.author.id);
    let (group, to) = match message.guild_id {
        None => {
            ctx.dc.note_dm(message.author.id, message.channel_id);
            (None, sender.clone())
        }
        Some(guild) => match server_group(ctx, guild, message).await {
            Some((group, to)) => (Some(group), to),
            None => return,
        },
    };
    let mut said = text::readable(&message.content, &names_in(ctx, message));
    for sticker in &message.sticker_items {
        let _ = write!(
            said,
            "\n«a sticker: {}»",
            serde_json::to_string(&sticker.name).unwrap_or_default()
        );
    }
    let attachments = message_files(ctx, &sender, group.as_ref(), message, &mut said).await;
    let said_trimmed = said.trim_start().len();
    said.replace_range(..said.len() - said_trimmed, "");
    let lines = context_lines(ctx, message);
    if !lines.is_empty() {
        said = if said.is_empty() {
            lines.join("\n")
        } else {
            format!("{}\n{said}", lines.join("\n"))
        };
    }
    if said.trim().is_empty() && attachments.is_empty() {
        // Without the Message Content intent, the text of a message that
        // does not mention the bot does not come.
        return;
    }
    // In a server a message may get no reply (a channel not let in), so
    // typing shows only in private chats.
    if group.is_none() {
        ctx.typing.start(&to);
    }
    send_inbound(
        ctx,
        &sender,
        message_id(message.channel_id, message.id.get()),
        said,
        attachments,
        group,
    )
    .await;
}

/// File `file` on to the gateway, named `name`, as `sender` sent it in
/// `group` (a server's channel, with its server and thread) or privately.
async fn receive(
    ctx: &Ctx,
    sender: &str,
    group: Option<&ChannelGroup>,
    file: &twilight_model::channel::Attachment,
    name: &str,
) -> Result<Attachment> {
    if usize::try_from(file.size).unwrap_or(usize::MAX) > gateway::MAX_FILE_BYTES {
        anyhow::bail!("larger than {} MB", gateway::MAX_FILE_BYTES / (1024 * 1024));
    }
    let _turn = TRANSFERS
        .acquire()
        .await
        .context("file transfers stopped")?;
    // One sender's slow file does not hold up everyone's for long.
    tokio::time::timeout(TRANSFER_FOR, async {
        let bytes = ctx.dc.download(&file.url, gateway::MAX_FILE_BYTES).await?;
        let media_type = file
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into());
        ctx.gateway
            .upload(sender, group, name, &media_type, bytes)
            .await
    })
    .await
    .unwrap_or_else(|_| anyhow::bail!("the transfer took over {} s", TRANSFER_FOR.as_secs()))
}

/// The longest a file takes, from Discord to the gateway.
const TRANSFER_FOR: Duration = Duration::from_secs(150);

async fn send_inbound(
    ctx: &Ctx,
    sender: &str,
    channel_message_id: String,
    text: String,
    attachments: Vec<Attachment>,
    group: Option<ChannelGroup>,
) {
    let frame = ChannelFrame::Inbound(ChannelInbound {
        seq: ctx.seq.fetch_add(1, Ordering::Relaxed) + 1,
        account_id: ctx.account_id.clone(),
        channel_message_id,
        sender_id: sender.to_string(),
        content: ChannelMessageContent::Text { text },
        timestamp: chrono::Utc::now(),
        raw: None,
        attachments,
        group,
    });
    if ctx.to_havn.send(frame).await.is_err() {
        warn!("havn WS closed; dropping a message");
    }
}

/// What an interaction asks for.
#[derive(Debug, PartialEq)]
enum Asked {
    /// Text to pass on as typed.
    Said(String),
    /// A choice in a question kept by number: its answer, then `numbers`.
    Numbered { menu: u64, numbers: String },
}

/// What a slash command, a button pressed or a choice made asks, and how
/// Discord is answered; `None` for another interaction.
fn parse_interaction(interaction: &Interaction) -> Option<(Asked, InteractionResponse)> {
    use twilight_model::application::interaction::application_command::CommandOptionValue;
    match (&interaction.kind, &interaction.data) {
        (InteractionType::ApplicationCommand, Some(InteractionData::ApplicationCommand(data))) => {
            let rest = data
                .options
                .iter()
                .find(|o| o.name == discord::COMMAND_TEXT)
                .and_then(|o| match &o.value {
                    CommandOptionValue::String(s) => Some(s.trim().to_string()),
                    _ => None,
                })
                .filter(|s| !s.is_empty());
            let said = match rest {
                Some(rest) => format!("/{} {rest}", data.name),
                None => format!("/{}", data.name),
            };
            // What was asked shows in the chat, as a typed command would.
            let mut shown: String = said.replace('`', "'").chars().take(1900).collect();
            shown = format!("`{shown}`");
            Some((
                Asked::Said(said),
                InteractionResponse {
                    kind: InteractionResponseType::ChannelMessageWithSource,
                    data: Some(InteractionResponseData {
                        content: Some(shown),
                        allowed_mentions: Some(AllowedMentions::default()),
                        ..Default::default()
                    }),
                },
            ))
        }
        (InteractionType::MessageComponent, Some(InteractionData::MessageComponent(data))) => {
            let numbers = || {
                data.values
                    .iter()
                    .filter_map(|v| v.parse::<usize>().ok())
                    .map(|n| (n + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let asked = if let Some(answer) = data.custom_id.strip_prefix(MENU_PREFIX) {
                Asked::Said(format!("{answer} {}", numbers()))
            } else if let Some(menu) = data.custom_id.strip_prefix(MENU_KEPT_PREFIX) {
                Asked::Numbered {
                    menu: menu.parse().unwrap_or(0),
                    numbers: numbers(),
                }
            } else {
                Asked::Said(data.custom_id.clone())
            };
            // The components come off, so it is answered once.
            Some((
                asked,
                InteractionResponse {
                    kind: InteractionResponseType::UpdateMessage,
                    data: Some(InteractionResponseData {
                        components: Some(Vec::new()),
                        ..Default::default()
                    }),
                },
            ))
        }
        _ => None,
    }
}

/// A slash command, a button pressed or a choice made, on to the gateway
/// as text from the person who used it. Discord is answered first, as it
/// waits 3 seconds at most.
async fn on_interaction(ctx: &Ctx, interaction: &Interaction) {
    let Some(user) = interaction
        .member
        .as_ref()
        .and_then(|m| m.user.clone())
        .or_else(|| interaction.user.clone())
    else {
        return;
    };
    if user.bot {
        return;
    }
    let Some(channel) = interaction.channel.as_ref().map(|c| c.id) else {
        return;
    };
    let Some((asked, response)) = parse_interaction(interaction) else {
        return;
    };
    let answer = |response: InteractionResponse| async move {
        if let Err(e) = ctx
            .dc
            .respond(interaction.id, &interaction.token, &response)
            .await
        {
            warn!(error = %format!("{e:#}"), "discord: could not answer an interaction");
        }
    };
    if interaction.guild_id.is_some() && !GATEWAY_GROUPS.load(Ordering::Relaxed) {
        answer(ephemeral("This bot is not used in servers yet.")).await;
        return;
    }
    let said = match asked {
        Asked::Said(said) => said,
        Asked::Numbered { menu, numbers } => {
            let kept = MENUS.lock().ok().and_then(|mut m| m.take(menu));
            let Some(answer_text) = kept else {
                answer(ephemeral("This question is no longer open.")).await;
                return;
            };
            format!("{answer_text} {numbers}")
        }
    };
    answer(response).await;
    let sender = user_id(user.id);
    let (group, to) = match interaction.guild_id {
        None => {
            ctx.dc.note_dm(user.id, channel);
            (None, sender.clone())
        }
        Some(guild) => {
            let Some(place) = place_of(ctx, guild, channel).await else {
                warn!(%channel, "discord: could not tell where an interaction was used; dropping");
                return;
            };
            let name = interaction
                .member
                .as_ref()
                .and_then(|m| m.nick.clone())
                .or_else(|| user.global_name.clone())
                .unwrap_or_else(|| user.name.clone());
            let (group, to) = group_at(&place, thread_of(&place), name, true);
            (Some(group), to)
        }
    };
    if group.is_none() {
        ctx.typing.start(&to);
    }
    send_inbound(
        ctx,
        &sender,
        message_id(channel, interaction.id.get()),
        said,
        Vec::new(),
        group,
    )
    .await;
}

/// A reply only the person who asked sees.
fn ephemeral(text: &str) -> InteractionResponse {
    InteractionResponse {
        kind: InteractionResponseType::ChannelMessageWithSource,
        data: Some(InteractionResponseData {
            content: Some(text.to_string()),
            flags: Some(MessageFlags::EPHEMERAL),
            ..Default::default()
        }),
    }
}

/// The custom id of a question's menu: this and the answer a choice sends
/// back, when that fits in Discord's 100 characters.
const MENU_PREFIX: &str = "choose:";
/// Else this and its number, with the answer kept while the daemon runs.
const MENU_KEPT_PREFIX: &str = "choose#";

/// Questions whose answer does not fit in their menu (havn issue #204), by
/// number: what a choice sends back.
static MENUS: std::sync::LazyLock<Mutex<Bounded<String>>> =
    std::sync::LazyLock::new(|| Mutex::new(Bounded::default()));
static NEXT_MENU: AtomicU64 = AtomicU64::new(1);

/// The rows of buttons for `buttons`, each one's data what a press sends
/// back: their rows when Discord takes them (5 rows of 5), else all of
/// them flowed into 5 rows of 5. A button Discord would refuse (no label,
/// data over 100 characters, the same data as another) is left out; its
/// message says how to answer by typing.
fn button_rows(buttons: &[Vec<types::Button>]) -> Vec<Component> {
    let mut seen = std::collections::HashSet::new();
    let rows: Vec<Vec<&types::Button>> = buttons
        .iter()
        .map(|row| {
            row.iter()
                .filter(|b| {
                    !b.label.trim().is_empty()
                        && !b.data.is_empty()
                        && b.data.chars().count() <= 100
                        && seen.insert(b.data.as_str())
                })
                .collect::<Vec<_>>()
        })
        .filter(|row| !row.is_empty())
        .collect();
    let rows: Vec<Vec<&types::Button>> = if rows.len() <= 5 && rows.iter().all(|r| r.len() <= 5) {
        rows
    } else {
        rows.concat().chunks(5).take(5).map(<[_]>::to_vec).collect()
    };
    rows.into_iter()
        .map(|row| {
            let components = row
                .into_iter()
                .map(|b| {
                    Component::Button(Button {
                        custom_id: Some(b.data.clone()),
                        disabled: false,
                        emoji: None,
                        id: None,
                        label: Some(b.label.trim().chars().take(80).collect()),
                        sku_id: None,
                        style: ButtonStyle::Secondary,
                        url: None,
                    })
                })
                .collect();
            Component::ActionRow(ActionRow {
                components,
                id: None,
            })
        })
        .collect()
}

/// Whether Discord takes `poll` as a menu: 1 to 25 options.
fn menu_fits(poll: &types::Poll) -> bool {
    (1..=25).contains(&poll.options.len())
}

/// A question of several answers as a menu that takes several, under its
/// text; `None` when Discord would not take it (over 25 options).
fn question_menu(poll: &types::Poll) -> Option<(String, Vec<Component>)> {
    if !menu_fits(poll) {
        return None;
    }
    let custom_id = if MENU_PREFIX.len() + poll.answer.len() <= 100 {
        format!("{MENU_PREFIX}{}", poll.answer)
    } else {
        let n = NEXT_MENU.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut menus) = MENUS.lock() {
            menus.put(n, poll.answer.clone());
        }
        format!("{MENU_KEPT_PREFIX}{n}")
    };
    let options = poll
        .options
        .iter()
        .enumerate()
        .map(|(i, label)| SelectMenuOption {
            default: false,
            description: None,
            emoji: None,
            label: label.chars().take(100).collect(),
            value: i.to_string(),
        })
        .collect::<Vec<_>>();
    let count = u8::try_from(options.len()).unwrap_or(25);
    let menu = Component::SelectMenu(SelectMenu {
        channel_types: None,
        custom_id,
        default_values: None,
        disabled: false,
        id: None,
        kind: SelectMenuType::Text,
        max_values: Some(count),
        min_values: Some(1),
        options: Some(options),
        placeholder: Some("Choose".into()),
        required: None,
    });
    Some((
        poll.question.clone(),
        vec![Component::ActionRow(ActionRow {
            components: vec![menu],
            id: None,
        })],
    ))
}

/// The channel a reply `to` goes to, a waiting thread opened first.
async fn reply_channel(
    dc: &Discord,
    to: &str,
    in_reply_to: Option<(Id<ChannelMarker>, Id<MessageMarker>)>,
) -> Result<Id<ChannelMarker>> {
    match target_of(to).with_context(|| format!("not a place to send to: {to:?}"))? {
        Target::Person(user) => dc.dm(user).await,
        Target::Channel(c) => {
            // The answer to a message in a thread of the channel (a refusal,
            // havn issue #288) goes to that thread.
            if let Some((written, _)) = in_reply_to
                && written != c
                && let Ok(found) = dc.http.channel(written).await
                && let Ok(thread) = found.model().await
                && is_thread(thread.kind)
                && thread.parent_id == Some(c)
            {
                return Ok(written);
            }
            Ok(c)
        }
        Target::Thread(t) => {
            if let Some(channel) = UNOPENED_THREADS
                .lock()
                .ok()
                .and_then(|u| u.get(t.get()).copied())
            {
                return Ok(channel);
            }
            let waiting = WAITING_THREADS
                .lock()
                .ok()
                .and_then(|mut w| w.take(t.get()));
            let named_now = waiting.is_some();
            // One named before a restart is opened from the message it
            // answers, which has the thread's id.
            let opening = waiting.or_else(|| {
                in_reply_to
                    .filter(|(channel, message)| message.get() == t.get() && *channel != t)
                    .map(|(channel, _)| (channel, "conversation".to_string()))
            });
            let Some((channel, name)) = opening else {
                return Ok(t);
            };
            if named_now || dc.http.channel(t).await.is_err() {
                match dc.open_thread(channel, t.cast(), &name).await {
                    Ok(thread) => Ok(thread),
                    // Not opened (no right to, too many): the reply goes to
                    // the channel, as a reply to the message.
                    // Its next replies go there too, and so do the next
                    // mentions' when it had no right to.
                    Err(e) => {
                        warn!(error = %format!("{e:#}"), "discord: a thread could not be opened; replying in its channel");
                        if let Ok(mut unopened) = UNOPENED_THREADS.lock() {
                            unopened.put(t.get(), channel);
                        }
                        if e.downcast_ref::<twilight_http::Error>()
                            .and_then(discord::api_code)
                            == Some(discord::code::MISSING_PERMISSIONS)
                            && let Ok(mut none) = NO_THREADS.lock()
                        {
                            none.put(channel.get(), ());
                        }
                        Ok(channel)
                    }
                }
            } else {
                Ok(t)
            }
        }
    }
}

/// havn to Discord: each reply is delivered, the menu set from the
/// gateway's commands.
async fn havn_outbound_loop(
    dc: Discord,
    gateway: Gateway,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
    typing: Typing,
    delivered: Arc<havn_ws::Delivered>,
) -> Result<()> {
    let mut menu: Option<Vec<types::ChannelCommand>> = None;
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Commands(c) => {
                GATEWAY_GROUPS.store(c.groups, Ordering::Relaxed);
                if menu.as_ref() == Some(&c.commands) {
                    continue;
                }
                match dc.set_commands(&c.commands).await {
                    Ok(n) => {
                        info!(commands = n, "discord slash commands set");
                        menu = Some(c.commands);
                    }
                    Err(e) => {
                        warn!(error = %format!("{e:#}"), "discord: could not set the slash commands");
                    }
                }
            }
            ChannelFrame::Outbound(o) => {
                if let Err(e) = deliver(&dc, &gateway, &o).await {
                    warn!(error = %format!("{e:#}"), seq = o.seq, "discord: a reply could not be delivered");
                }
                delivered.mark(o.seq);
                if o.partial {
                    typing.more(&o.to);
                } else {
                    typing.replied(&o.to);
                }
            }
            ChannelFrame::Pong => {}
            ChannelFrame::Inbound(_) | ChannelFrame::Resume(_) | ChannelFrame::Ping => {
                debug!("dropping a frame the gateway does not send");
            }
        }
    }
    info!("havn outbound channel closed; discord outbound loop exiting");
    Ok(())
}

/// Whether reply `o` has nothing to send: the end of one whose parts went
/// already, or an empty answer. It opens no thread (issue #2).
fn says_nothing(o: &ChannelOutbound) -> bool {
    !o.poll.as_ref().is_some_and(menu_fits)
        && button_rows(&o.buttons).is_empty()
        && matches!(&o.content, ChannelMessageContent::Text { text } if text.trim().is_empty())
}

async fn deliver(dc: &Discord, gateway: &Gateway, o: &ChannelOutbound) -> Result<()> {
    if says_nothing(o) {
        return Ok(());
    }
    let answers = o.in_reply_to.as_deref().and_then(parse_message_id);
    let channel = reply_channel(dc, &o.to, answers).await?;
    // A reply answers its message where that is in the same channel.
    let reply = answers.and_then(|(c, m)| (c == channel).then_some(m));

    if let Some(poll) = &o.poll
        && let Some((question, components)) = question_menu(poll)
    {
        return dc.send_text(channel, &question, reply, &components).await;
    }
    let text = match &o.content {
        ChannelMessageContent::Text { text } => text.clone(),
        ChannelMessageContent::Image { url, caption } => match caption {
            Some(c) => format!("[image: {url}]\n{c}"),
            None => format!("[image: {url}]"),
        },
        ChannelMessageContent::File {
            url,
            filename,
            caption,
        } => {
            let sent = match gateway.fetch(url).await {
                Ok(bytes) => {
                    dc.send_file(channel, filename, bytes, caption.as_deref(), reply)
                        .await
                }
                Err(e) => Err(e),
            };
            match sent {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(filename, error = %format!("{e:#}"), "discord: a file could not be sent");
                    format!(
                        "(A file, {filename}, could not be delivered: {}.)",
                        failure(&e)
                    )
                }
            }
        }
        ChannelMessageContent::Audio {
            transcript,
            duration_seconds,
            ..
        } => match duration_seconds {
            Some(d) => format!("[voice, {d}s] {transcript}"),
            None => format!("[voice] {transcript}"),
        },
        ChannelMessageContent::Location {
            latitude,
            longitude,
            name,
            ..
        } => {
            let coords = format!("{latitude:.6},{longitude:.6}");
            match name {
                Some(n) if !n.is_empty() => format!("[location: {coords} ({n})]"),
                _ => format!("[location: {coords}]"),
            }
        }
        ChannelMessageContent::Contact {
            display_name,
            phone_number,
            ..
        } => match phone_number {
            Some(ph) => format!("[contact: {display_name} {ph}]"),
            None => format!("[contact: {display_name}]"),
        },
    };
    dc.send_text(channel, &text, reply, &button_rows(&o.buttons))
        .await
}

/// Why a file was not sent, for the person.
fn failure(e: &anyhow::Error) -> &'static str {
    let said = format!("{e:#}");
    if said.contains("larger than") {
        "it is too large for Discord"
    } else {
        "Discord did not take it"
    }
}

/// The typing indicator, by where a reply is due: shown from a message to
/// its reply, again every [`TYPING_EVERY`] (Discord shows it for about
/// 10 seconds), for at most [`TYPING_FOR`].
#[derive(Clone)]
struct Typing {
    send: Arc<SendTyping>,
    places: Arc<Mutex<HashMap<String, Waiting>>>,
    next: Arc<AtomicU64>,
}

struct Waiting {
    replies: u32,
    task: (u64, tokio::task::AbortHandle),
}

type SendTyping = dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

const TYPING_EVERY: Duration = Duration::from_secs(8);
const TYPING_FOR: Duration = Duration::from_secs(300);

impl Typing {
    fn new(
        send: impl Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            send: Arc::new(send),
            places: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    fn places(&self) -> std::sync::MutexGuard<'_, HashMap<String, Waiting>> {
        self.places
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A message came for `to`: one more reply is due.
    fn start(&self, to: &str) {
        let task = self.spawn(to);
        let mut places = self.places();
        let replies = places.get(to).map_or(0, |w| w.replies) + 1;
        if let Some(earlier) = places.insert(to.to_string(), Waiting { replies, task }) {
            earlier.task.1.abort();
        }
    }

    /// A reply was sent to `to`: one fewer is due.
    fn replied(&self, to: &str) {
        let mut places = self.places();
        let Some(waiting) = places.get_mut(to) else {
            return;
        };
        waiting.replies = waiting.replies.saturating_sub(1);
        if waiting.replies == 0 {
            if let Some(done) = places.remove(to) {
                done.task.1.abort();
            }
            return;
        }
        drop(places);
        self.again(to);
    }

    /// A part of a reply went to `to`, with more to come.
    fn more(&self, to: &str) {
        if self.places().contains_key(to) {
            self.again(to);
        }
    }

    fn again(&self, to: &str) {
        let task = self.spawn(to);
        let mut places = self.places();
        match places.get_mut(to) {
            Some(waiting) => std::mem::replace(&mut waiting.task, task).1.abort(),
            None => task.1.abort(),
        }
    }

    fn spawn(&self, to: &str) -> (u64, tokio::task::AbortHandle) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let send = Arc::clone(&self.send);
        let places = Arc::clone(&self.places);
        let to = to.to_string();
        let task = tokio::spawn(async move {
            let until = tokio::time::Instant::now() + TYPING_FOR;
            loop {
                send(to.clone()).await;
                if tokio::time::Instant::now() + TYPING_EVERY > until {
                    break;
                }
                tokio::time::sleep(TYPING_EVERY).await;
            }
            let mut places = places
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if places.get(&to).is_some_and(|w| w.task.0 == id) {
                places.remove(&to);
            }
        });
        (id, task.abort_handle())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn places_are_named_apart_and_a_server_is_not_sent_to() {
        let c: Id<ChannelMarker> = Id::new(5);
        assert_eq!(channel_group(c), "discord:c5");
        assert_eq!(thread_place(c), "discord:t5");
        assert_eq!(guild_space(Id::new(7)), "discord:s7");
        assert_eq!(user_id(Id::new(9)), "discord:9");
        assert_eq!(target_of("discord:c5"), Some(Target::Channel(c)));
        assert_eq!(target_of("discord:t5"), Some(Target::Thread(c)));
        assert_eq!(target_of("discord:9"), Some(Target::Person(Id::new(9))));
        for not in [
            "discord:s7",
            "discord:",
            "discord:c",
            "discord:c0",
            "tg:5",
            "discord:x5",
        ] {
            assert_eq!(target_of(not), None, "{not}");
        }
    }

    #[test]
    fn a_reply_with_nothing_to_send_is_told_apart() {
        let reply = |v: serde_json::Value| -> ChannelOutbound {
            let mut frame = serde_json::json!({
                "seq": 1, "account_id": "a", "to": "discord:t5",
                "content": {"type": "text", "text": ""}
            });
            if let (Some(frame), Some(more)) = (frame.as_object_mut(), v.as_object()) {
                frame.extend(more.clone());
            }
            serde_json::from_value(frame).expect("frame")
        };
        assert!(says_nothing(&reply(serde_json::json!({}))));
        assert!(says_nothing(&reply(serde_json::json!({
            "content": {"type": "text", "text": " \n "}
        }))));
        assert!(!says_nothing(&reply(serde_json::json!({
            "content": {"type": "text", "text": "hi"}
        }))));
        assert!(!says_nothing(&reply(serde_json::json!({
            "buttons": [[{"label": "Approve", "data": "/approve"}]]
        }))));
        assert!(!says_nothing(&reply(serde_json::json!({
            "poll": {"question": "Which?", "options": ["a", "b"], "answer": "/choose q"}
        }))));
        // Buttons Discord would not take, and a menu that cannot be made,
        // are nothing either.
        assert!(says_nothing(&reply(serde_json::json!({
            "buttons": [[], [{"label": " ", "data": "/approve"}]]
        }))));
        assert!(says_nothing(&reply(serde_json::json!({
            "poll": {"question": "Which?", "options": [], "answer": "/choose q"}
        }))));
    }

    #[test]
    fn a_message_id_names_its_channel_and_message() {
        let id = message_id(Id::new(5), 42);
        assert_eq!(id, "discord:5:42");
        assert_eq!(parse_message_id(&id), Some((Id::new(5), Id::new(42))));
        for not in ["discord:5", "discord:x:1", "tg:5:1", "discord:0:1"] {
            assert_eq!(parse_message_id(not), None, "{not}");
        }
    }

    #[test]
    fn channels_named_by_id_are_found() {
        assert_eq!(
            channel_refs("see <#12> and <#x> and <#34>"),
            [Id::new(12), Id::new(34)]
        );
    }

    #[test]
    fn buttons_discord_takes_go_in_rows_of_five() {
        let b = |data: &str| types::Button {
            label: "L".into(),
            data: data.into(),
        };
        let rows = button_rows(&[
            (0..7).map(|i| b(&format!("/a{i}"))).collect(),
            vec![b(&"x".repeat(101))],
            vec![b("/ok")],
        ]);
        assert_eq!(rows.len(), 2, "the row of buttons too long goes");
        let Component::ActionRow(first) = &rows[0] else {
            panic!("a row");
        };
        assert_eq!(first.components.len(), 5);
    }

    #[test]
    fn ten_choices_flow_into_rows_and_buttons_discord_refuses_go() {
        let b = |label: &str, data: &str| types::Button {
            label: label.into(),
            data: data.into(),
        };
        let one_per_row: Vec<Vec<types::Button>> = (0..10)
            .map(|i| vec![b("L", &format!("/choose q {i}"))])
            .collect();
        let rows = button_rows(&one_per_row);
        let sizes: Vec<usize> = rows
            .iter()
            .map(|r| match r {
                Component::ActionRow(row) => row.components.len(),
                _ => 0,
            })
            .collect();
        assert_eq!(sizes, [5, 5], "all ten kept");
        let rows = button_rows(&[vec![
            b("A", "/x"),
            b("B", "/x"),
            b("  ", "/y"),
            b("C", "/z"),
        ]]);
        let Component::ActionRow(row) = &rows[0] else {
            panic!("a row");
        };
        assert_eq!(row.components.len(), 2, "a second /x and an empty label go");
    }

    #[test]
    fn a_question_becomes_a_menu_and_its_choice_its_answer() {
        let poll = types::Poll {
            question: "Which?".into(),
            options: vec!["a".into(), "b".into()],
            answer: "/answer q1".into(),
        };
        let (text, rows) = question_menu(&poll).expect("menu");
        assert_eq!(text, "Which?");
        let Component::ActionRow(row) = &rows[0] else {
            panic!("a row");
        };
        let Component::SelectMenu(menu) = &row.components[0] else {
            panic!("a menu");
        };
        assert_eq!(menu.max_values, Some(2));
        assert_eq!(menu.custom_id, "choose:/answer q1", "the answer is in it");
        let long = types::Poll {
            answer: format!("/answer {}", "q".repeat(100)),
            ..poll.clone()
        };
        let (_, rows) = question_menu(&long).expect("menu");
        let Component::ActionRow(row) = &rows[0] else {
            panic!("a row");
        };
        let Component::SelectMenu(menu) = &row.components[0] else {
            panic!("a menu");
        };
        let n: u64 = menu
            .custom_id
            .strip_prefix(MENU_KEPT_PREFIX)
            .expect("kept")
            .parse()
            .expect("n");
        assert_eq!(MENUS.lock().expect("menus").take(n), Some(long.answer));
        let many = types::Poll {
            options: (0..26).map(|i| i.to_string()).collect(),
            ..poll
        };
        assert!(question_menu(&many).is_none());
    }

    #[test]
    fn a_bounded_map_forgets_its_oldest() {
        let mut m: Bounded<u64> = Bounded::default();
        for i in 0..(Bounded::<u64>::KEPT as u64 + 3) {
            m.put(i, i);
        }
        assert!(m.get(0).is_none() && m.get(2).is_none());
        assert_eq!(m.take(3), Some(3));
        assert!(m.take(3).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn typing_shows_until_each_reply_came() {
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let typing = {
            let sent = Arc::clone(&sent);
            Typing::new(move |to| {
                let sent = Arc::clone(&sent);
                Box::pin(async move { sent.lock().expect("sent").push(to) })
            })
        };
        typing.start("discord:c1");
        typing.start("discord:c1");
        tokio::time::sleep(Duration::from_secs(9)).await;
        typing.replied("discord:c1");
        assert!(
            typing.places().contains_key("discord:c1"),
            "one more is due"
        );
        typing.replied("discord:c1");
        assert!(!typing.places().contains_key("discord:c1"));
        assert!(sent.lock().expect("sent").len() >= 2);
    }
}
