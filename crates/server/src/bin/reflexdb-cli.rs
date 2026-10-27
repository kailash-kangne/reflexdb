//! `reflexdb-cli [-h host] [-p port] [command ...]` — a small first-party
//! RESP client for manual testing, built on `reflexdb_server::resp_client`
//! (the same minimal client Sentinel uses to talk to the nodes it
//! monitors). Not a full `redis-cli` replacement: no RESP3 push types, no
//! pipelining, no `--csv`/`--eval`-style flags — just enough to type a
//! command and see the reply, interactively or as a one-shot.

use std::process::ExitCode;

use reflexdb_server::resp_client::{Reply, RespClient};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[tokio::main]
async fn main() -> ExitCode {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 6379;
    let mut rest: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--host" => match args.next() {
                Some(v) => host = v,
                None => return usage_error("missing value for -h"),
            },
            "-p" | "--port" => match args.next() {
                Some(v) => match v.parse() {
                    Ok(p) => port = p,
                    Err(_) => return usage_error(&format!("invalid port: {v}")),
                },
                None => return usage_error("missing value for -p"),
            },
            "--help" => {
                print_usage();
                return ExitCode::SUCCESS;
            }
            other => rest.push(other.to_string()),
        }
    }

    let mut client = match RespClient::connect(&host, port).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Could not connect to reflexdb at {host}:{port}: {e}");
            return ExitCode::FAILURE;
        }
    };

    if !rest.is_empty() {
        // One-shot mode: run exactly the command given on the command line.
        let words: Vec<&str> = rest.iter().map(String::as_str).collect();
        return match client.call(&words).await {
            Ok(reply) => {
                print_reply(&reply, 0);
                // `LIVE`/`SUBSCRIBE`/`PSUBSCRIBE` as a one-shot still block
                // and print pushes (Ctrl+C to stop), same as real
                // `redis-cli SUBSCRIBE channel` would — that's the whole
                // point of running them from a plain terminal.
                if let Some(extra_initial) = push_mode_extra_frames(&rest) {
                    if read_extra_initial_frames(&mut client, extra_initial).await {
                        run_push_mode(&mut client).await;
                    }
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        };
    }

    run_repl(&mut client, &host, port).await
}

async fn run_repl(client: &mut RespClient, host: &str, port: u16) -> ExitCode {
    let history_path = std::env::var("HOME").ok().map(|h| format!("{h}/.reflexdb-cli_history"));

    let mut rl = match DefaultEditor::new() {
        Ok(rl) => rl,
        Err(e) => {
            eprintln!("could not start line editor: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(path) = &history_path {
        let _ = rl.load_history(path);
    }

    let prompt = format!("{host}:{port}> ");
    loop {
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
                    break;
                }
                let words = match split_words(line) {
                    Ok(words) => words,
                    Err(e) => {
                        println!("{e}");
                        continue;
                    }
                };
                if words.is_empty() {
                    continue;
                }
                let args: Vec<&str> = words.iter().map(String::as_str).collect();
                match client.call(&args).await {
                    Ok(reply) => {
                        print_reply(&reply, 0);
                        if let Some(extra_initial) = push_mode_extra_frames(&words) {
                            if !read_extra_initial_frames(client, extra_initial).await {
                                break; // connection died mid-subscribe
                            }
                            if !run_push_mode(client).await {
                                break; // connection died while waiting for pushes
                            }
                        }
                    }
                    Err(e) => println!("error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted) => continue, // Ctrl+C: match redis-cli, just clear the line
            Err(ReadlineError::Eof) => break,            // Ctrl+D
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }

    if let Some(path) = &history_path {
        let _ = rl.save_history(path);
    }
    ExitCode::SUCCESS
}

/// `LIVE`/`SUBSCRIBE`/`PSUBSCRIBE` don't follow the plain "one command, one
/// reply" shape `call()` assumes: `SUBSCRIBE`/`PSUBSCRIBE` send one
/// confirmation frame *per channel/pattern argument* (only the first of
/// which `call()`'s single read already consumed), and all three then keep
/// pushing unsolicited frames later, whenever a watched key/channel
/// changes — see `docs/04-reactivity-live.md` and `crates/commands/src/
/// pubsub.rs`. Returns the number of *additional* initial confirmation
/// frames still to read (0 for `LIVE`, which only ever sends the one),
/// or `None` if `words` isn't one of these commands.
fn push_mode_extra_frames(words: &[String]) -> Option<usize> {
    match words.first()?.to_ascii_uppercase().as_str() {
        "LIVE" => Some(0),
        "SUBSCRIBE" | "PSUBSCRIBE" => Some(words.len().saturating_sub(2)),
        _ => None,
    }
}

/// Reads and prints `n` more initial confirmation frames (see
/// `push_mode_extra_frames`). Returns `false` if the connection died.
async fn read_extra_initial_frames(client: &mut RespClient, n: usize) -> bool {
    for _ in 0..n {
        match client.read_next().await {
            Ok(reply) => print_reply(&reply, 0),
            Err(e) => {
                println!("connection closed: {e}");
                return false;
            }
        }
    }
    true
}

/// After `LIVE`/`SUBSCRIBE`/`PSUBSCRIBE`'s initial reply(ies), the server
/// can push more frames at any time with nothing further sent from here —
/// so instead of going back to the prompt, keep reading and printing them
/// (like `redis-cli`'s own subscribe-mode) until the user presses Ctrl+C.
/// Returns `false` if the connection died instead.
async fn run_push_mode(client: &mut RespClient) -> bool {
    println!("(reading pushed updates — press Ctrl+C to stop and return to the prompt)");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!();
                return true;
            }
            result = client.read_next() => {
                match result {
                    Ok(reply) => print_reply(&reply, 0),
                    Err(e) => {
                        println!("connection closed: {e}");
                        return false;
                    }
                }
            }
        }
    }
}

/// Splits a line into words the way a shell would: whitespace-separated,
/// `"..."` allows embedded spaces (with `\"`/`\\` escapes), `'...'` is a
/// literal (no escapes) — enough for `SET key "hello world"`-style manual
/// testing. Returns an error string for an unterminated quote.
fn split_words(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut in_word = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' if !in_word => continue,
            ' ' | '\t' => {
                words.push(std::mem::take(&mut current));
                in_word = false;
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('"') => current.push('"'),
                            Some('\\') => current.push('\\'),
                            Some('n') => current.push('\n'),
                            Some(other) => {
                                current.push('\\');
                                current.push(other);
                            }
                            None => return Err("ERR unterminated quote".to_string()),
                        },
                        Some(other) => current.push(other),
                        None => return Err("ERR unterminated quote".to_string()),
                    }
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(other) => current.push(other),
                        None => return Err("ERR unterminated quote".to_string()),
                    }
                }
            }
            other => {
                in_word = true;
                current.push(other);
            }
        }
    }
    if in_word {
        words.push(current);
    }
    Ok(words)
}

/// Mimics `redis-cli`'s plain (non-`--no-raw`) reply formatting closely
/// enough for manual testing: `OK`, `(integer) N`, `(nil)`, a quoted bulk
/// string, or a numbered (and, for nested arrays, indented) list.
fn print_reply(reply: &Reply, indent: usize) {
    let pad = "   ".repeat(indent);
    match reply {
        Reply::Simple(s) => println!("{pad}{s}"),
        Reply::Error(e) => println!("{pad}(error) {e}"),
        Reply::Integer(n) => println!("{pad}(integer) {n}"),
        Reply::Bulk(None) => println!("{pad}(nil)"),
        Reply::Bulk(Some(b)) => println!("{pad}\"{}\"", String::from_utf8_lossy(b)),
        Reply::Array(None) => println!("{pad}(nil)"),
        Reply::Array(Some(items)) if items.is_empty() => println!("{pad}(empty array)"),
        Reply::Array(Some(items)) => {
            for (i, item) in items.iter().enumerate() {
                match item {
                    Reply::Array(_) => {
                        println!("{pad}{}) ", i + 1);
                        print_reply(item, indent + 1);
                    }
                    _ => {
                        print!("{pad}{}) ", i + 1);
                        print_reply(item, 0);
                    }
                }
            }
        }
    }
}

fn print_usage() {
    eprintln!("Usage: reflexdb-cli [-h host] [-p port] [command ...]");
    eprintln!("  No command given: starts an interactive prompt (type `exit` or Ctrl+D to leave).");
    eprintln!("  Command given: runs it once, prints the reply, and exits.");
}

fn usage_error(msg: &str) -> ExitCode {
    eprintln!("reflexdb-cli: {msg}");
    print_usage();
    ExitCode::FAILURE
}
