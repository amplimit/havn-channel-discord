# havn-channel-discord

A daemon that connects a Discord bot to a [Havn](https://github.com/amplimit/havn) gateway: people message the bot in Discord, privately or in a server's channels, and the Havn agent bound to it answers there.

It runs as its own process, apart from the gateway, and talks to it over the gateway's channel protocol (a WebSocket at `/api/v1/channel`). It holds the bot token and an adapter token; it cannot read the gateway's credentials or reach the agents. Stopping it stops the bot and nothing else.

## What it does

- **Private messages** with the bot, both ways.
- **Server channels.** A message that mentions the bot (or a role that is the bot's), or replies to one of its messages, opens a **thread** from it, named from its first line. The thread is a conversation of its own: every message in it is to the bot, with no mention needed, and the reply goes there. The thread is opened when the first reply goes, so a channel that is not let in keeps no empty thread. In a thread someone else opened, a mention or a reply is needed, and that thread is a conversation of its own too.
- **Letting a server in.** The gateway answers only channels it has let in: the owner is told of a channel turned away, with **Allow the whole server** and **Allow this channel** (see [Group chats](https://github.com/amplimit/havn#group-chats) in the Havn README).
- **Slash commands.** The gateway's commands (`/new`, `/stop`, `/model`, ...) are the bot's slash commands, each with an optional `text` for what follows it. A command shows in the chat as it was asked, and goes to the gateway as if typed.
- **Buttons** the gateway puts under a message (such as **Approve** and **Deny**) are shown under it. A press goes to the gateway as the button's command from the person who pressed it, and the buttons come off, so it is pressed once.
- **Questions of several answers** (the agent's `ask_choice`) are a menu that takes several choices; a choice goes back as the person's answer and the menu comes off. A menu carries its answer, so it outlives a restart; one whose answer does not fit Discord's 100 characters is remembered while the daemon runs (the last 1024).
- **Files both ways.** Files someone attaches to a message to the bot go to the gateway (up to 25 MB each), which saves them in the agent's inbox and reads the text of PDFs, Word documents and spreadsheets; a voice message goes as `voice.ogg`, which the gateway transcribes when `[transcription]` is set. A file the agent sends is posted with its caption (up to 10 MB, Discord's limit for a server without boosts); one that cannot be sent is named in a message instead.
- **Replies and mentions.** A message that replies to another starts with a line saying what it answers (the bot's message, the person's own, or someone else's), its text cut to 500 characters, quoted and marked as user-supplied. Mentions, channels and custom emoji are written as their names (`@ann`, `#general`, `:wave:`).
- **Formatted replies.** Discord shows the agent's Markdown as it is written. A long reply is split under Discord's 2000 characters at paragraph, line or word breaks; a code block cut in two is closed and opened again.
- **No pings.** Nothing the bot sends mentions anyone: an `@everyone` the agent writes is text.
- **A typing indicator** in private chats, from a message until its reply (in a server a message may get no reply, so none shows there).
- **Replies missed while disconnected.** When the daemon reconnects to the gateway, it gets the replies sent meanwhile that the gateway kept (the last 5 minutes), in order, before any new one. It keeps the last reply it delivered in `<account_id>.state.json` in the unit's `StateDirectory` (or next to the config file, or at `--state`), so a restart resumes too.

Who may use the bot, what the commands do and how chats work is decided by the gateway: see [Channel Adapters](https://github.com/amplimit/havn#channel-adapters), [Chat commands](https://github.com/amplimit/havn#chat-commands) and [Invite codes](https://github.com/amplimit/havn#invite-codes) in the Havn README.

## Requirements

- A Havn gateway, 0.2.11 or later (`havn --version`).
- A Discord application with a bot (below).
- Linux on x86_64 or aarch64 for the release binaries; elsewhere, Rust 1.94 or later to build the daemon.

## Setup

### 1. Make the Discord bot

In the [Developer Portal](https://discord.com/developers/applications), **New Application**, then:

1. **Bot**: **Reset Token** and keep the token. Under **Privileged Gateway Intents**, turn on **Message Content Intent**: without it the bot reads only messages that mention it, so in a thread each message needs a mention. The daemon reads the setting when it starts, and says in its log when it is off.
2. **Installation**: for **Guild Install**, the scopes `bot` and `applications.commands`, and the permissions **View Channels**, **Send Messages**, **Send Messages in Threads**, **Create Public Threads**, **Read Message History** and **Attach Files**. Open the install link to add the bot to a server.

### 2. Declare the bot's account in the gateway

In the gateway's `config.toml`, add the account and bind it to an agent:

```toml
[[channels.discord.accounts]]
id                = "acme-discord"
adapter_token_ref = "secret:channel:discord:acme-discord"
dm_policy         = "allowlist"   # or "open": anyone who finds the bot
group_policy      = "allowlist"   # servers: "off" (the default), "allowlist" or "open"

[[bindings]]
agent_id   = "<id of the agent that answers, from `havn agent list`>"
channel    = "discord"
account_id = "acme-discord"
```

A person is `discord:` and their user id (with Developer Mode on in Discord's settings, **Copy User ID**); a server is `discord:s<id>` and a channel `discord:c<id>`, for `allow_from` and `allow_groups`. Changes to `[[channels]]` and `[[bindings]]` apply when the gateway restarts.

### 3. Give the gateway and the daemon a shared adapter token

```bash
ADAPTER_TOKEN="$(openssl rand -base64 32 | tr -d '\n')"
havn credential add channel:discord "$ADAPTER_TOKEN" --name acme-discord
```

### 4. Install the daemon

```bash
VERSION=0.2.11
asset="havn-channel-discord-$(uname -m)-unknown-linux-musl.tar.gz"
base="https://github.com/amplimit/havn-channel-discord/releases/download/v$VERSION"
curl -fL -O "$base/$asset" -O "$base/$asset.sha256"
sha256sum -c "$asset.sha256"
tar -xzf "$asset"
sudo install -m 755 havn-channel-discord /usr/local/bin/
```

Or build it from source: `cargo build --release --locked`, then install `target/release/havn-channel-discord`.

### 5. Configure the daemon

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin havn-dc
sudo install -d -m 755 /etc/havn-channel-discord
sudo install -m 600 -o havn-dc -g havn-dc config.example.toml /etc/havn-channel-discord/config.toml
sudoedit /etc/havn-channel-discord/config.toml
```

```toml
bot_token     = "MTIz..."                   # from the Developer Portal
havn_gateway  = "ws://127.0.0.1:8080"       # the gateway's `listen` address; wss:// behind TLS
adapter_token = "<the ADAPTER_TOKEN above>"
channel_id    = "discord"                   # as in [[channels.discord.accounts]]
account_id    = "acme-discord"              # the account's id
```

### 6. Run it under systemd

```ini
# /etc/systemd/system/havn-channel-discord.service
[Unit]
Description=havn channel adapter for Discord
After=network.target havn.service
Wants=havn.service

[Service]
ExecStart=/usr/local/bin/havn-channel-discord --config /etc/havn-channel-discord/config.toml
Restart=always
RestartSec=5
User=havn-dc
StateDirectory=havn-channel-discord
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now havn-channel-discord
sudo journalctl -u havn-channel-discord -f
```

### 7. Let people in, and connect your own account

With `dm_policy = "allowlist"`, only people in `allow_from` and those admitted with an invite code may write to the bot privately (`havn invite create --channel discord --account acme-discord`; they send the bot `/start <code>`). To be answered as yourself, the agent's owner, connect your Discord account: on the agent's settings page, **chat apps** > **connect**, then send the bot `/start <code>` privately.

A server is let in by its `group_policy`: with `allowlist`, the first mention in a channel tells you, in your connected chat, with **Allow the whole server** and **Allow this channel**; the agent's settings page and `allow_groups` let them in too. Letting a server in lets in all of it, channels made later included, for everyone in it; Discord's channel permissions decide where the bot can read and write.

## Check that it works

The log shows the bot and the connection to the gateway:

```
INFO havn_channel_discord: discord bot identified bot=acme sees_all=true
INFO havn_channel_discord::havn_ws: havn WS connected
INFO havn_channel_discord: discord slash commands set commands=16
```

Then message the bot privately; the bound agent answers, and typing `/` shows its commands (Discord may take a minute to show new ones).

## Limitations

- A mention in a voice or stage channel's chat, where Discord opens no thread, is answered in that chat; so is one where the bot may not open threads (it lacks **Create Public Threads** or **Send Messages in Threads**). A slash command in a channel is answered in the channel, not a thread.
- A forwarded message is not passed on.
- Server messages go on only once the gateway has said, when the daemon connects, that it takes them; until then (the first moment after a start) they are dropped.
- An edited message, a reaction and an embed are not passed on.
- Stickers are passed on as their names, not their pictures.
- A thread named before a restart and not opened yet is opened with the name "conversation".
- One shard: enough for a bot in up to 2,500 servers.

## License

Apache-2.0
