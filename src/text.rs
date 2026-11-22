//! Text both ways. Discord shows Markdown as the agent writes it, so a
//! reply is only split under Discord's limit, its code blocks closed and
//! opened again across parts; and what a person wrote has its mentions,
//! channels and emoji written as names, for the agent.

use std::collections::HashMap;

/// Characters in one message part, under Discord's 2000.
pub const PART_CHARS: usize = 1900;

/// Characters of a thread's name taken from the message that opened it.
const THREAD_NAME_CHARS: usize = 80;

/// `text` as message parts of at most [`PART_CHARS`]: cut at a paragraph,
/// else a line, else a word, else anywhere; a code block cut in two is
/// closed in one part and opened again, with its language, in the next.
pub fn split(text: &str) -> Vec<String> {
    let text = text.trim_end();
    let mut parts = Vec::new();
    let mut rest = text;
    // The fence line that opened a code block still open, as ```rust.
    let mut fence: Option<String> = None;
    while !rest.is_empty() {
        let head = fence.as_ref().map(|f| format!("{f}\n")).unwrap_or_default();
        // Room for the head, and for closing a block left open.
        let budget = PART_CHARS
            .saturating_sub(head.chars().count() + "\n```".len())
            .max(1);
        if rest.chars().count() <= budget {
            parts.push(format!("{head}{rest}"));
            break;
        }
        let cut = cut_at(rest, budget);
        let (part, remaining) = rest.split_at(cut);
        let open = fence_after(fence.clone(), part);
        let mut piece = format!("{head}{}", part.trim_end());
        if open.is_some() {
            piece.push_str("\n```");
        }
        parts.push(piece);
        fence = open;
        rest = remaining.trim_start_matches(['\n', '\r']);
        // Inside a code block the spaces lead a line of code; outside
        // they would start the next part with a space.
        if fence.is_none() {
            rest = rest.trim_start_matches(' ');
        }
    }
    parts
}

/// The byte index to cut `text` at so that at most `budget` characters
/// come before it: after the last blank line, else line, else space, in
/// the second half of the part; else at the budget.
fn cut_at(text: &str, budget: usize) -> usize {
    let limit = text
        .char_indices()
        .nth(budget)
        .map_or(text.len(), |(i, _)| i);
    let window = &text[..limit];
    let half = window.len() / 2;
    for sep in ["\n\n", "\n", " "] {
        if let Some(at) = window.rfind(sep)
            && at >= half
        {
            return at + sep.len();
        }
    }
    limit
}

/// Whether a code block is open after `part`, which began with `open`:
/// the fence line that opened it, if so.
fn fence_after(mut open: Option<String>, part: &str) -> Option<String> {
    for line in part.lines() {
        let line = line.trim();
        if let Some(info) = line.strip_prefix("```") {
            open = match open {
                Some(_) => None,
                // A fence that closes on its own line, as ```x```, opens none.
                None if info.contains("```") => None,
                // Opened again with its language only, short.
                None => {
                    let language: String = info
                        .split_whitespace()
                        .next()
                        .unwrap_or_default()
                        .chars()
                        .take(20)
                        .collect();
                    Some(format!("```{language}"))
                }
            };
        }
    }
    open
}

/// The names of what a message refers to by id, for [`readable`].
#[derive(Debug, Default)]
pub struct Names {
    pub users: HashMap<u64, String>,
    pub roles: HashMap<u64, String>,
    pub channels: HashMap<u64, String>,
}

/// `content` as the agent reads it: `<@id>` and `<@!id>` as `@name`,
/// `<@&id>` as `@role`, `<#id>` as `#channel`, `<:name:id>` and
/// `<a:name:id>` as `:name:`, and `<t:…>` timestamps as their number. An
/// id without a name keeps its id.
pub fn readable(content: &str, names: &Names) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(start) = rest.find('<') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        let Some(end) = after.find('>') else {
            out.push_str(after);
            return out;
        };
        let inner = &after[1..end];
        if let Some(shown) = token(inner, names) {
            out.push_str(&shown);
            rest = &after[end + 1..];
        } else {
            // A `<` that opens no token is text; what follows is read on.
            out.push('<');
            rest = &after[1..];
        }
    }
    out.push_str(rest);
    out
}

/// What one `<…>` token reads as, or `None` for anything else.
fn token(inner: &str, names: &Names) -> Option<String> {
    let id = |s: &str| s.parse::<u64>().ok();
    if let Some(role) = inner.strip_prefix("@&") {
        let n = id(role)?;
        return Some(format!(
            "@{}",
            names
                .roles
                .get(&n)
                .cloned()
                .unwrap_or_else(|| n.to_string())
        ));
    }
    if let Some(user) = inner.strip_prefix('@') {
        let n = id(user.strip_prefix('!').unwrap_or(user))?;
        return Some(format!(
            "@{}",
            names
                .users
                .get(&n)
                .cloned()
                .unwrap_or_else(|| n.to_string())
        ));
    }
    if let Some(channel) = inner.strip_prefix('#') {
        let n = id(channel)?;
        return Some(format!(
            "#{}",
            names
                .channels
                .get(&n)
                .cloned()
                .unwrap_or_else(|| n.to_string())
        ));
    }
    if let Some(emoji) = inner.strip_prefix("a:").or_else(|| inner.strip_prefix(':')) {
        let (name, n) = emoji.rsplit_once(':')?;
        id(n)?;
        return Some(format!(":{name}:"));
    }
    if let Some(time) = inner.strip_prefix("t:") {
        let n = time.split(':').next()?;
        id(n)?;
        return Some(n.to_string());
    }
    None
}

/// A thread's name from the message that opened it (its text without the
/// bot's mention): its first line, cut to a word under
/// [`THREAD_NAME_CHARS`]; "conversation" when nothing is left.
pub fn thread_name(content: &str) -> String {
    let line = content
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or_default();
    let mut name = String::new();
    for word in line.split_whitespace() {
        let next = if name.is_empty() {
            word.to_string()
        } else {
            format!("{name} {word}")
        };
        if next.chars().count() > THREAD_NAME_CHARS {
            if name.is_empty() {
                name = word.chars().take(THREAD_NAME_CHARS).collect();
            }
            break;
        }
        name = next;
    }
    if name.is_empty() {
        "conversation".to_string()
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn a_short_reply_is_one_part() {
        assert_eq!(split("hello\n"), ["hello"]);
        assert!(split("   \n").is_empty());
    }

    #[test]
    fn a_long_reply_is_cut_at_paragraphs_under_the_limit() {
        let para = "word ".repeat(150);
        let text = [para.trim(), para.trim(), para.trim(), para.trim()].join("\n\n");
        let parts = split(&text);
        assert!(parts.len() >= 2, "{}", parts.len());
        for p in &parts {
            assert!(p.chars().count() <= 2000, "{}", p.chars().count());
            assert!(!p.starts_with(' ') && !p.ends_with('\n'));
        }
        assert_eq!(
            parts.join(" ").split_whitespace().count(),
            text.split_whitespace().count(),
            "no word lost"
        );
    }

    #[test]
    fn a_code_block_cut_in_two_is_closed_and_opened_again() {
        let code = (0..400).fold(String::new(), |mut code, i| {
            let _ = writeln!(code, "    let x{i} = {i};");
            code
        });
        let text = format!("Here:\n```rust\n{code}```\nDone.");
        let parts = split(&text);
        assert!(parts.len() >= 2);
        for p in &parts {
            assert!(p.chars().count() <= 2000);
            assert_eq!(p.matches("```").count() % 2, 0, "balanced fences: {p}");
        }
        assert!(
            parts[1].starts_with("```rust\n    let"),
            "{}",
            &parts[1][..40]
        );
        assert!(parts.last().is_some_and(|p| p.ends_with("Done.")));
    }

    #[test]
    fn a_fence_with_a_long_line_still_ends() {
        let text = format!("```{}\n{}\n```", "x".repeat(3000), "y\n".repeat(2000));
        let parts = split(&text);
        assert!(parts.len() < 20, "{}", parts.len());
        for p in &parts {
            assert!(p.chars().count() <= 2000, "{}", p.chars().count());
        }
    }

    #[test]
    fn a_word_longer_than_a_part_is_cut_anywhere() {
        let parts = split(&"x".repeat(4500));
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.concat().len(), 4500);
    }

    #[test]
    fn mentions_channels_and_emoji_read_as_names() {
        let mut names = Names::default();
        names.users.insert(1, "ann".into());
        names.roles.insert(2, "mods".into());
        names.channels.insert(3, "general".into());
        assert_eq!(
            readable(
                "<@1> <@!1> <@&2> in <#3> <:wave:44> <a:dance:45> <t:1700000000:R>",
                &names
            ),
            "@ann @ann @mods in #general :wave: :dance: 1700000000"
        );
        assert_eq!(readable("<@9> a < b > c <x", &names), "@9 a < b > c <x");
        assert_eq!(
            readable("a < b <@1>", &names),
            "a < b @ann",
            "a stray < keeps the mention"
        );
    }

    #[test]
    fn a_thread_is_named_from_the_first_line_without_the_mention() {
        assert_eq!(thread_name(" what is the plan?\nmore"), "what is the plan?");
        assert_eq!(thread_name("hey @ann, the plan?"), "hey @ann, the plan?");
        assert_eq!(thread_name("  "), "conversation");
        assert_eq!(thread_name(""), "conversation");
        let long = thread_name(&"word ".repeat(40));
        assert!(long.chars().count() <= 80 && !long.ends_with(' '), "{long}");
        assert_eq!(thread_name(&"y".repeat(200)).chars().count(), 80);
    }
}
