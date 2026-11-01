//! End-to-end partial resync checks: a replica whose link drops and
//! reconnects must catch up incrementally (`PSYNC` → `+CONTINUE`) when
//! its offset is still covered by the primary's backlog, and must fall
//! back to `FULLRESYNC` when it isn't.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_reflexdb-server");

#[derive(Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Client {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => {
                    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                    let reader = BufReader::new(stream.try_clone().unwrap());
                    return Client { reader, writer: stream };
                }
                Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("server never came up on port {port}: {e}"),
            }
        }
    }

    fn cmd(&mut self, args: &[&str]) -> Reply {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        self.writer.write_all(&out).unwrap();
        self.read_reply()
    }

    fn read_reply(&mut self) -> Reply {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Reply::Bulk(None);
                }
                let mut buf = vec![0u8; len as usize];
                self.reader.read_exact(&mut buf).unwrap();
                let mut crlf = [0u8; 2];
                self.reader.read_exact(&mut crlf).unwrap();
                Reply::Bulk(Some(buf))
            }
            "*" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Reply::Bulk(None);
                }
                Reply::Array((0..len).map(|_| self.read_reply()).collect())
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

fn free_port() -> u16 {
    // Atomic counter, not bind-and-release: bind(:0) releases the port
    // before the server claims it, so two parallel tests could probe the
    // same free port and race for it — the loser exits and the winner's
    // client gets a connection reset from the wrong server.
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(33_000);
    loop {
        let port = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("reflexdb-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn(port: u16, dir: &Path, extra: &[&str]) -> Child {
    Command::new(BIN)
        .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn reflexdb-server")
}

fn sigkill(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

/// These tests spawn real server processes and poll asynchronous
/// replication state — parallel runs starve the waits and flake.
/// Serialize the file; each test is only a couple of seconds.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(Some(s.as_bytes().to_vec()))
}

/// Poll `GET key` on `client` until it equals `want` — replication is
/// asynchronous, so "has the write arrived yet" is a wait, not an assert.
fn wait_replicated(client: &mut Client, key: &str, want: &Reply) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if client.cmd(&["GET", key]) == *want {
            return;
        }
        assert!(Instant::now() < deadline, "replica never caught up on {key}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Poll `INFO` until `field` equals `want` — resync counters tick when
/// the replica's new link completes its handshake, which is asynchronous.
fn wait_info_field(client: &mut Client, field: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if info_field(client, field) == want {
            return;
        }
        assert!(Instant::now() < deadline, "INFO {field} never became {want}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn info_field(client: &mut Client, field: &str) -> String {
    match client.cmd(&["INFO"]) {
        Reply::Bulk(Some(body)) => String::from_utf8_lossy(&body)
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("INFO missing {field}")),
        other => panic!("unexpected INFO reply: {other:?}"),
    }
}

/// A replica that reconnects with a still-covered offset catches up
/// incrementally — `REPLICAOF` to the same primary bounces the link, and
/// the resume cursor makes the next `PSYNC` a `+CONTINUE`.
#[test]
fn replica_resumes_with_partial_resync_after_link_bounce() {
    let _serial = serial();
    let dir_a = temp_dir("psync-primary");
    let dir_b = temp_dir("psync-replica");
    let (port_a, port_b) = (free_port(), free_port());
    let mut primary = spawn(port_a, &dir_a, &[]);
    let mut replica = spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}")]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["SET", "seed", "1"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "seed", &bulk("1"));

    // Bounce the link, then write while the replica is mid-reconnect — the
    // write lands in the backlog and arrives via catch-up.
    assert_eq!(rc.cmd(&["REPLICAOF", "127.0.0.1", &port_a.to_string()]), Reply::Simple("OK".into()));
    assert_eq!(pc.cmd(&["SET", "after", "2"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "after", &bulk("2"));

    wait_info_field(&mut pc, "sync_partial_ok", "1");
    assert_eq!(info_field(&mut pc, "sync_full"), "1");

    sigkill(&mut primary);
    sigkill(&mut replica);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// `partial-resync-backlog 0`: every reconnect is a full resync, and the
/// replica still ends up correct.
#[test]
fn replica_falls_back_to_full_resync_when_backlog_disabled() {
    let _serial = serial();
    let dir_a = temp_dir("psyncoff-primary");
    let dir_b = temp_dir("psyncoff-replica");
    let (port_a, port_b) = (free_port(), free_port());
    let mut primary = spawn(port_a, &dir_a, &["--partial-resync-backlog", "0"]);
    let mut replica = spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}")]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["SET", "seed", "1"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "seed", &bulk("1"));

    assert_eq!(rc.cmd(&["REPLICAOF", "127.0.0.1", &port_a.to_string()]), Reply::Simple("OK".into()));
    assert_eq!(pc.cmd(&["SET", "after", "2"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "after", &bulk("2"));

    wait_info_field(&mut pc, "sync_full", "2");
    assert_eq!(info_field(&mut pc, "sync_partial_ok"), "0");

    sigkill(&mut primary);
    sigkill(&mut replica);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// `kill -9` on the primary while a replica is attached: the link drops,
/// the replica must refuse writes (still read-only), then reconnect on
/// its own once the primary is back — with a new replid the resync is a
/// FULLRESYNC, whose snapshot must also wipe keys the replica replicated
/// from the pre-crash primary but that no longer exist.
#[test]
fn primary_sigkill_recovers_and_replica_converges() {
    let _serial = serial();
    let dir_a = temp_dir("crash-primary");
    let dir_b = temp_dir("crash-replica");
    let (port_a, port_b) = (free_port(), free_port());
    // No persistence: the restarted primary comes back EMPTY — the
    // strongest test of whether the replica converges instead of keeping
    // stale replicated data.
    let mut primary = spawn(port_a, &dir_a, &[]);
    let mut replica = spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}")]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["SET", "seed", "1"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "seed", &bulk("1"));
    sigkill(&mut primary);

    // While the primary is down the replica must stay read-only — a write
    // here would diverge permanently once the empty primary resyncs.
    assert!(matches!(rc.cmd(&["SET", "local", "x"]), Reply::Error(_)));

    let mut primary = spawn(port_a, &dir_a, &[]);
    let mut pc = Client::connect(port_a);
    assert_eq!(pc.cmd(&["SET", "after", "2"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "after", &bulk("2"));

    // New replid -> full resync -> replica flushed -> stale "seed" gone.
    // The restarted primary counts from zero, so this is its first sync.
    assert_eq!(rc.cmd(&["GET", "seed"]), Reply::Bulk(None));
    wait_info_field(&mut pc, "sync_full", "1");
    assert_eq!(info_field(&mut pc, "sync_partial_ok"), "0");

    sigkill(&mut primary);
    sigkill(&mut replica);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// `REPLICAOF NO ONE` clears the resume cursor — a promoted replica's
/// data may have diverged, so re-attaching must be a full resync.
#[test]
fn replicaof_no_one_clears_resume_cursor() {
    let _serial = serial();
    let dir_a = temp_dir("psyncpromote-primary");
    let dir_b = temp_dir("psyncpromote-replica");
    let (port_a, port_b) = (free_port(), free_port());
    let mut primary = spawn(port_a, &dir_a, &[]);
    let mut replica = spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}")]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["SET", "seed", "1"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "seed", &bulk("1"));

    assert_eq!(rc.cmd(&["REPLICAOF", "NO", "ONE"]), Reply::Simple("OK".into()));
    assert_eq!(rc.cmd(&["SET", "local", "x"]), Reply::Simple("OK".into()));
    assert_eq!(rc.cmd(&["REPLICAOF", "127.0.0.1", &port_a.to_string()]), Reply::Simple("OK".into()));
    assert_eq!(pc.cmd(&["SET", "after", "2"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "after", &bulk("2"));

    wait_info_field(&mut pc, "sync_full", "2");
    assert_eq!(info_field(&mut pc, "sync_partial_ok"), "0");

    sigkill(&mut primary);
    sigkill(&mut replica);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// A password-protected primary NOAUTHs a bare `PSYNC` — the link must
/// AUTH with `masterauth` first. Without the flag replication can't
/// connect at all; with it, the replica syncs normally.
#[test]
fn masterauth_authenticates_the_replication_link() {
    let _serial = serial();
    let dir_a = temp_dir("masterauth-primary");
    let dir_b = temp_dir("masterauth-replica");
    let (port_a, port_b) = (free_port(), free_port());
    let mut primary = spawn(port_a, &dir_a, &["--requirepass", "pw"]);
    let mut replica =
        spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}"), "--masterauth", "pw"]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["AUTH", "pw"]), Reply::Simple("OK".into()));
    assert_eq!(pc.cmd(&["SET", "k", "v"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "k", &bulk("v"));

    sigkill(&mut primary);
    sigkill(&mut replica);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// Automatic failover: a sentinel watching the primary must promote the
/// replica (`REPLICAOF NO ONE`) once the primary stays dead past
/// `down-after-milliseconds`. The promoted replica then accepts writes
/// and holds everything replicated before the kill.
#[test]
fn sentinel_promotes_replica_on_primary_death() {
    let _serial = serial();
    const SENTINEL_BIN: &str = env!("CARGO_BIN_EXE_reflexdb-sentinel");
    let dir_a = temp_dir("sentinel-primary");
    let dir_b = temp_dir("sentinel-replica");
    let dir_s = temp_dir("sentinel-conf");
    let (port_a, port_b, port_s) = (free_port(), free_port(), free_port());
    let mut primary = spawn(port_a, &dir_a, &[]);
    let mut replica = spawn(port_b, &dir_b, &["--replicaof", &format!("127.0.0.1 {port_a}")]);

    let mut pc = Client::connect(port_a);
    let mut rc = Client::connect(port_b);
    assert_eq!(pc.cmd(&["SET", "k", "v"]), Reply::Simple("OK".into()));
    wait_replicated(&mut rc, "k", &bulk("v"));
    // Sanity: the replica refuses client writes until promoted.
    assert!(matches!(rc.cmd(&["SET", "w", "x"]), Reply::Error(_)));

    let conf = dir_s.join("sentinel.conf");
    std::fs::write(
        &conf,
        format!(
            "port {port_s}\n\
             sentinel monitor mymaster 127.0.0.1 {port_a} 1\n\
             sentinel down-after-milliseconds mymaster 500\n\
             sentinel known-replica mymaster 127.0.0.1 {port_b}\n"
        ),
    )
    .unwrap();
    let mut sentinel = Command::new(SENTINEL_BIN)
        .arg(&conf)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn reflexdb-sentinel");

    sigkill(&mut primary);
    wait_info_field(&mut rc, "role", "master");

    // Promoted: writable, and everything replicated pre-kill survived.
    assert_eq!(rc.cmd(&["SET", "w", "x"]), Reply::Simple("OK".into()));
    assert_eq!(rc.cmd(&["GET", "k"]), bulk("v"));

    sigkill(&mut sentinel);
    sigkill(&mut replica);
    for d in [dir_a, dir_b, dir_s] {
        let _ = std::fs::remove_dir_all(&d);
    }
}
