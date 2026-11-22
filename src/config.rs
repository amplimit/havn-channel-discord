//! Operator-supplied configuration, loaded from a TOML file at startup. It
//! mirrors the operator's `[[channels.discord.accounts]]` entry in havn's
//! config: the same `account_id` on both sides, and the same
//! `adapter_token`, which havn keeps encrypted and this file keeps plain,
//! so it is to be readable only by the daemon's user.

use std::path::Path;

use anyhow::{Context as _, Result};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    /// The Discord bot's token, from the Developer Portal. Held only in
    /// this daemon: havn never sees it.
    pub bot_token: String,

    /// The havn gateway's WebSocket URL, such as `ws://127.0.0.1:8080` or
    /// `wss://havn.example.com`; the daemon adds
    /// `/api/v1/channel?channel=<channel_id>&account=<account_id>`.
    pub havn_gateway: String,

    /// The random string the operator stored in havn for this account,
    /// compared in constant time when the WebSocket opens.
    pub adapter_token: String,

    /// The channel's key in havn's `[channels]`: `discord` unless the
    /// operator named it otherwise.
    #[serde(default = "default_channel_id")]
    pub channel_id: String,

    /// The account's `id` in havn's `[[channels.<channel_id>.accounts]]`.
    pub account_id: String,
}

// The tokens are not shown, in a log or a panic.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("havn_gateway", &self.havn_gateway)
            .field("channel_id", &self.channel_id)
            .field("account_id", &self.account_id)
            .finish_non_exhaustive()
    }
}

fn default_channel_id() -> String {
    "discord".into()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        // The parser's message only: its excerpt of the file may show a token.
        let mut cfg: Self = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("parsing {} as TOML: {}", path.display(), e.message()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&mut self) -> Result<()> {
        self.bot_token = self.bot_token.trim().to_string();
        // A token pasted with its "Bot " prefix is the same token.
        if let Some(rest) = self.bot_token.strip_prefix("Bot ") {
            self.bot_token = rest.trim().to_string();
        }
        if self.bot_token.is_empty() {
            anyhow::bail!("bot_token must not be empty");
        }
        if self.havn_gateway.is_empty() {
            anyhow::bail!("havn_gateway must not be empty");
        }
        if self.adapter_token.is_empty() {
            anyhow::bail!("adapter_token must not be empty");
        }
        if self.channel_id.is_empty() {
            anyhow::bail!("channel_id must not be empty");
        }
        if self.account_id.is_empty() {
            anyhow::bail!("account_id must not be empty");
        }
        // havn's channel_id codec refuses its `|` separator in any part of a
        // channel id: refused here, at start, rather than at the first message.
        for (label, value) in [
            ("channel_id", &self.channel_id),
            ("account_id", &self.account_id),
        ] {
            if value.contains('|') {
                anyhow::bail!(
                    "{label} must not contain `|` (channel_id codec separator); got {value:?}"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::io::Write;

    fn write_temp(contents: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "havn-channel-discord-test-{}.toml",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        p
    }

    #[test]
    fn loads_a_minimal_config_without_showing_the_tokens() {
        let p = write_temp(
            r#"
            bot_token = "Bot MTIz.abc.def"
            havn_gateway = "ws://127.0.0.1:8100"
            adapter_token = "tok"
            account_id = "acme-discord"
            "#,
        );
        let c = Config::load(&p).unwrap();
        assert_eq!(c.account_id, "acme-discord");
        assert_eq!(c.channel_id, "discord");
        assert_eq!(
            c.bot_token, "MTIz.abc.def",
            "the Bot prefix is not part of it"
        );
        let shown = format!("{c:?}");
        assert!(!shown.contains("MTIz") && !shown.contains("tok"), "{shown}");
    }

    #[test]
    fn refuses_a_pipe_in_account_id_and_empty_fields() {
        for body in [
            r#"
            bot_token = "x"
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice|bot"
            "#,
            r#"
            bot_token = "  "
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice"
            "#,
        ] {
            assert!(Config::load(&write_temp(body)).is_err(), "{body}");
        }
    }
}
