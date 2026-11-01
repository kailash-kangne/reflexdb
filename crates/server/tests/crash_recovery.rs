//! End-to-end durability + RESP compat checks against a real spawned
//! `reflexdb-server` binary. The unit tests cover dispatch internals;
//! these cover the process boundary: spawn → write → `kill -9` → restart
//! → replay must return every write that was ever acknowledged under the
//! configured durability contract.

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

/// Minimal blocking RESP2 client — just enough to drive the tests
/// (connections are local and synchronous by nature here).
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

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim_end().to_string()
    }

    fn read_reply(&mut self) -> Reply {
        let line = self.read_line();
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

    /// Like `cmd`, but args may be arbitrary bytes (needed to send a large
    /// non-UTF-8 payload).
    fn cmd_bytes(&mut self, args: &[&[u8]]) -> Reply {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&out).unwrap();
        self.read_reply()
    }
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(Some(s.as_bytes().to_vec()))
}

fn free_port() -> u16 {
    // Atomic counter, not bind-and-release: bind(:0) releases the port
    // before the server claims it, so two parallel tests could probe the
    // same free port and race for it — the loser exits and the winner's
    // client gets a connection reset from the wrong server. The base is
    // pid-seeded so concurrently-running test binaries use disjoint ranges.
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    if NEXT.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        let base = 20_000 + (std::process::id() as u16 % 20_000);
        let _ = NEXT.compare_exchange(
            0,
            base,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    loop {
        let port = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if port >= 1024 && TcpListener::bind(("127.0.0.1", port)).is_ok() {
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

/// Spawn with a per-file size cap (`ulimit -f`, in 512-byte blocks) so
/// writes past the cap fail like a full disk. `SIGXFSZ` is ignored first —
/// otherwise the default disposition kills the process instead of
/// returning EFBIG to the writer.
#[cfg(unix)]
fn spawn_capped_file_size(port: u16, dir: &Path, blocks_512: u32) -> Child {
    Command::new("sh")
        .arg("-c")
        .arg(format!(
            "trap '' XFSZ; ulimit -f {blocks_512}; exec '{}' --port {port} --dir '{}' \
             --appendonly yes --appendfsync always",
            BIN,
            dir.display()
        ))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn reflexdb-server")
}

/// `kill -9` — no graceful shutdown, exactly the crash scenario the AOF
/// and snapshot paths exist for.
fn sigkill(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

/// These tests spawn real server processes and poll timing-sensitive
/// async behavior (eviction ticks, flag propagation, marker cleanup) —
/// run in parallel they starve each other of CPU and flake. Serialize
/// the file; each test is only a couple of seconds.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// `appendfsync always` — the `profile durable` contract: every write the
/// client saw acknowledged must survive `kill -9`.
#[test]
fn sigkill_loses_nothing_under_appendfsync_always() {
    let _serial = serial();
    let dir = temp_dir("aof-always");
    let port = free_port();
    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "always"]);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["SET", "s", "v1"]), Reply::Simple("OK".into()));
        assert_eq!(c.cmd(&["INCR", "n"]), Reply::Int(1));
        assert_eq!(c.cmd(&["RPUSH", "l", "a", "b"]), Reply::Int(2));
        assert_eq!(c.cmd(&["HSET", "h", "f", "v"]), Reply::Int(1));
        // An error reply must not corrupt the log either.
        assert!(matches!(c.cmd(&["LPUSH", "h", "x"]), Reply::Error(_)));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "always"]);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["GET", "s"]), bulk("v1"));
    assert_eq!(c.cmd(&["GET", "n"]), bulk("1"));
    assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), Reply::Array(vec![bulk("a"), bulk("b")]));
    assert_eq!(c.cmd(&["HGET", "h", "f"]), bulk("v"));
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// No AOF: a `SAVE`d snapshot is what restart must recover from.
#[test]
fn rdb_survives_sigkill_after_save() {
    let _serial = serial();
    let dir = temp_dir("rdb");
    let port = free_port();
    let mut server = spawn(port, &dir, &[]);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["SET", "persist", "me"]), Reply::Simple("OK".into()));
        assert_eq!(c.cmd(&["SAVE"]), Reply::Simple("OK".into()));
        // Written after SAVE — must NOT come back.
        assert_eq!(c.cmd(&["SET", "ephemeral", "gone"]), Reply::Simple("OK".into()));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &[]);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["GET", "persist"]), bulk("me"));
    assert_eq!(c.cmd(&["GET", "ephemeral"]), Reply::Bulk(None));
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `appendfsync no` + a burst of writes killed mid-flight: the torn tail
/// must not stop startup — replay keeps the complete prefix and carries on.
#[test]
fn torn_aof_tail_still_boots() {
    let _serial = serial();
    let dir = temp_dir("aof-torn");
    let port = free_port();
    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "no"]);
    {
        let mut c = Client::connect(port);
        for i in 0..50 {
            assert_eq!(c.cmd(&["SET", &format!("k{i}"), "v"]), Reply::Simple("OK".into()));
        }
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "no"]);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    // Whatever replay recovered must be consistent — no panics, no
    // half-decoded garbage keys.
    match c.cmd(&["DBSIZE"]) {
        Reply::Int(n) => assert!(n <= 50, "replay invented keys: dbsize {n}"),
        other => panic!("unexpected DBSIZE reply: {other:?}"),
    }
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `RUN.*` bookkeeping and `IDEMPOTENT` markers are ordinary keyspace
/// data: under `appendfsync always` they must come back after `kill -9`
/// like any other write — that's the whole "durable workflow recovery"
/// contract.
#[test]
fn workflow_primitives_survive_sigkill() {
    let _serial = serial();
    let dir = temp_dir("run-recovery");
    let port = free_port();
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let mut server = spawn(port, &dir, &aof);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["RUN", "BEGIN", "job1"]), Reply::Simple("OK".into()));
        assert_eq!(c.cmd(&["SET", "run:job1:counter", "5"]), Reply::Simple("OK".into()));
        assert_eq!(c.cmd(&["RUN", "STEP", "job1", "phase-a"]), Reply::Simple("OK".into()));
        assert_eq!(c.cmd(&["RUN", "CHECKPOINT", "job1", "v1"]), Reply::Int(1));
        assert_eq!(c.cmd(&["IDEMPOTENT", "idk1", "60000", "INCR", "run:job1:n"]), Reply::Int(1));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &aof);
    let mut c = Client::connect(port);

    // Run record, last_step and checkpoint label all survived.
    fn collect_bulks(r: &Reply, out: &mut Vec<String>) {
        match r {
            Reply::Bulk(Some(b)) => out.push(String::from_utf8_lossy(b).into_owned()),
            Reply::Array(items) => items.iter().for_each(|i| collect_bulks(i, out)),
            _ => {}
        }
    }
    match c.cmd(&["RUN", "STATUS", "job1"]) {
        Reply::Array(fields) => {
            let mut flat = Vec::new();
            collect_bulks(&Reply::Array(fields), &mut flat);
            for want in ["running", "phase-a", "v1"] {
                assert!(flat.iter().any(|s| s == want), "status missing {want}: {flat:?}");
            }
        }
        other => panic!("expected RUN STATUS array, got {other:?}"),
    }
    // The audit trail survived too.
    match c.cmd(&["RUN", "AUDIT", "job1"]) {
        Reply::Array(entries) => assert!(entries.len() >= 3, "audit lost entries: {entries:?}"),
        other => panic!("expected RUN AUDIT array, got {other:?}"),
    }
    // The IDKAPPLY record landed marker *and* effect atomically — a retry
    // replays the stored reply, it must NOT re-run the INCR.
    assert_eq!(c.cmd(&["IDEMPOTENT", "idk1", "60000", "INCR", "run:job1:n"]), Reply::Int(1));
    assert_eq!(c.cmd(&["GET", "run:job1:n"]), bulk("1"));
    // And a checkpoint restore still works post-crash.
    assert_eq!(c.cmd(&["SET", "run:job1:counter", "99"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["RUN", "RESTORE", "job1", "v1"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["GET", "run:job1:counter"]), bulk("5"));

    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `SCHEDULE` persists through its `~sched:<id>` marker key: a command
/// whose fire time passed while the server was down fires on the first
/// tick after boot, and a future one stays listed (at-least-once).
#[test]
fn scheduled_commands_survive_sigkill() {
    let _serial = serial();
    let dir = temp_dir("schedule");
    let port = free_port();
    let aof = ["--appendonly", "yes", "--appendfsync", "always"];
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

    let mut server = spawn(port, &dir, &aof);
    {
        let mut c = Client::connect(port);
        // Due during the downtime — must fire right after the restart.
        let past = (now_ms + 500).to_string();
        assert!(matches!(c.cmd(&["SCHEDULE", "AT", &past, "SET", "sched:fired", "yes"]), Reply::Int(_)));
        // Far future — must still be armed afterwards.
        let future = (now_ms + 3_600_000).to_string();
        assert!(matches!(c.cmd(&["SCHEDULE", "AT", &future, "SET", "sched:future", "yes"]), Reply::Int(_)));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &aof);
    let mut c = Client::connect(port);

    // Past-due marker fires on a maintenance tick (~100ms); give it room.
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.cmd(&["GET", "sched:fired"]) != bulk("yes") {
        assert!(Instant::now() < deadline, "sched:fired never executed after restart");
        std::thread::sleep(Duration::from_millis(50));
    }
    // If the kill landed between the firing and the marker's `DEL` (the
    // documented at-least-once window), replay restores the marker and
    // the task re-arms — it drains on the next tick once the marker's
    // gone, so poll until LIST settles to just the future task.
    loop {
        match c.cmd(&["SCHEDULE", "LIST"]) {
            Reply::Array(items) if items.len() == 1 => break,
            _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            other => panic!("SCHEDULE LIST never settled to just the future task: {other:?}"),
        }
    }
    assert_eq!(c.cmd(&["GET", "sched:future"]), Reply::Bulk(None));

    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two servers sharing one `--dir` would interleave AOF/RDB writes and
/// corrupt both. The second must refuse to start (dir flock), and the
/// first must be unaffected. Restart-after-SIGKILL is already exercised
/// by every other test here — they all respawn on the same dir.
#[cfg(unix)]
#[test]
fn second_server_on_same_dir_refuses() {
    let _serial = serial();
    let dir = temp_dir("dir-lock");
    let port = free_port();
    let mut server = spawn(port, &dir, &[]);
    let mut c = Client::connect(port);

    let mut second = spawn(free_port(), &dir, &[]);
    let status = second.wait().expect("second server should have exited");
    assert!(!status.success(), "second server on a locked dir must fail");

    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A full disk: the AOF writer's write fails asynchronously, so the write
/// that tripped it is still ACKed (fire-and-forget) but every write after
/// must be refused with MISCONF — reads stay up. After `kill -9`, the
/// torn bytes left by the failed write must not stop the next boot or
/// corrupt the recovered prefix.
#[cfg(unix)]
#[test]
fn full_disk_refuses_writes_and_recovers() {
    let _serial = serial();
    let dir = temp_dir("full-disk");
    let port = free_port();
    // 512 KiB cap — enough for startup files, too small for the frame below.
    let mut server = spawn_capped_file_size(port, &dir, 1024);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["SET", "small", "v"]), Reply::Simple("OK".into()));
        // Incompressible payload: the compressed AOF frame still exceeds
        // the cap, so this is the write whose logging fails.
        let mut x = 0x9E3779B97F4A7C15u64;
        let big: Vec<u8> = (0..1_500_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        assert_eq!(c.cmd_bytes(&[b"SET", b"rnd", &big]), Reply::Simple("OK".into()));

        // The writer thread flags the failure asynchronously; wait for it.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match c.cmd(&["SET", "s1", "v"]) {
                Reply::Error(e) => {
                    assert!(e.starts_with("MISCONF"), "{e}");
                    break;
                }
                _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
                other => panic!("writes never refused on full disk: {other:?}"),
            }
        }
        // Refused writes must not be queued either — verify reads are fine.
        assert_eq!(c.cmd(&["GET", "small"]), bulk("v"));
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "always"]);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["GET", "small"]), bulk("v"));
    assert_eq!(c.cmd(&["GET", "s1"]), Reply::Bulk(None));
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `--tiered-storage`: a key spilled to the disk tier is fsync'd before
/// the eviction completes (TIERED.md), so `kill -9` after the spill must
/// not lose it — restart, and the tier must still serve every key even
/// though RAM started empty (no AOF in this config).
#[test]
fn tiered_keys_survive_sigkill() {
    let _serial = serial();
    let dir = temp_dir("tiered");
    let port = free_port();
    let flags = ["--tiered-storage", "yes", "--tiered-memory-limit", "1", "--eviction-tick-ms", "25"];
    let mut server = spawn(port, &dir, &flags);
    {
        let mut c = Client::connect(port);
        for i in 0..10 {
            let k = format!("t{i}");
            let v = format!("v{i}");
            assert_eq!(c.cmd(&["SET", &k, &v]), Reply::Simple("OK".into()));
        }
        // Wait for the maintenance tick to actually spill all keys.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match c.cmd(&["INFO"]) {
                Reply::Bulk(Some(body))
                    if String::from_utf8_lossy(&body).lines().any(|l| l == "tiered_evictions:10") =>
                {
                    break;
                }
                _ => {}
            }
            assert!(Instant::now() < deadline, "keys never spilled to the tier");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &flags);
    let mut c = Client::connect(port);
    for i in 0..10 {
        assert_eq!(c.cmd(&["GET", &format!("t{i}")]), bulk(&format!("v{i}")));
    }
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reading a tiered key fetches it into RAM as a read cache — the tier
/// keeps its fsync'd copy, so a `kill -9` before the next eviction tick
/// must still recover the key. This test uses a long eviction tick so
/// the fetched copy is never re-spilled before the crash.
#[test]
fn promoted_tiered_key_survives_sigkill() {
    let _serial = serial();
    let dir = temp_dir("tiered-promote");
    let port = free_port();
    let fast_tick = ["--tiered-storage", "yes", "--tiered-memory-limit", "1", "--eviction-tick-ms", "25"];
    let slow_tick = ["--tiered-storage", "yes", "--tiered-memory-limit", "1", "--eviction-tick-ms", "60000"];

    let mut server = spawn(port, &dir, &fast_tick);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["SET", "t0", "v0"]), Reply::Simple("OK".into()));
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match c.cmd(&["INFO"]) {
                Reply::Bulk(Some(body))
                    if String::from_utf8_lossy(&body).lines().any(|l| l == "tiered_evictions:1") =>
                {
                    break;
                }
                _ => {}
            }
            assert!(Instant::now() < deadline, "key never spilled to the tier");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    sigkill(&mut server);

    // Long tick: after the GET promotes t0 into RAM, nothing re-spills it.
    let mut server = spawn(port, &dir, &slow_tick);
    {
        let mut c = Client::connect(port);
        assert_eq!(c.cmd(&["GET", "t0"]), bulk("v0"));
    }
    sigkill(&mut server);

    let mut server = spawn(port, &dir, &slow_tick);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["GET", "t0"]), bulk("v0"), "promoted key was lost to the crash");
    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Wire-level RESP compatibility spot-checks against a live server —
/// complements `benchmarks/smoke.sh` (which is manual) with something CI runs.
#[test]
fn resp_command_compat() {
    let _serial = serial();
    let dir = temp_dir("compat");
    let port = free_port();
    let mut server = spawn(port, &dir, &[]);
    let mut c = Client::connect(port);

    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(c.cmd(&["SET", "k", "v"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["GET", "k"]), bulk("v"));
    assert_eq!(c.cmd(&["GET", "missing"]), Reply::Bulk(None));
    assert_eq!(c.cmd(&["TYPE", "k"]), Reply::Simple("string".into()));
    assert_eq!(c.cmd(&["EXISTS", "k", "missing"]), Reply::Int(1));
    assert_eq!(c.cmd(&["DEL", "k", "missing"]), Reply::Int(1));
    assert_eq!(c.cmd(&["EXISTS", "k"]), Reply::Int(0));

    assert_eq!(c.cmd(&["INCR", "n"]), Reply::Int(1));
    assert_eq!(c.cmd(&["INCRBY", "n", "5"]), Reply::Int(6));

    assert_eq!(c.cmd(&["RPUSH", "l", "a", "b", "c"]), Reply::Int(3));
    assert_eq!(c.cmd(&["LRANGE", "l", "0", "-1"]), Reply::Array(vec![bulk("a"), bulk("b"), bulk("c")]));
    assert_eq!(c.cmd(&["HSET", "h", "f1", "v1", "f2", "v2"]), Reply::Int(2));
    assert_eq!(c.cmd(&["HGET", "h", "f2"]), bulk("v2"));

    // Wrong type and unknown command must error, not crash.
    assert!(matches!(c.cmd(&["GET", "l"]), Reply::Error(_)));
    assert!(matches!(c.cmd(&["NOSUCHCMD", "x"]), Reply::Error(_)));

    // SELECT isolation: a key in db 1 is invisible from db 0.
    assert_eq!(c.cmd(&["SELECT", "1"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["SET", "db1key", "x"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["SELECT", "0"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["GET", "db1key"]), Reply::Bulk(None));

    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `protected-mode` (on by default): with no password configured, a
/// listener bound to a non-loopback address refuses off-loopback clients
/// while loopback keeps working. The probe connects through one of this
/// machine's real interfaces — the server's `peer.ip()` check sees it as
/// remote. Denial shows up as a `-DENIED` line or a dead socket (an
/// endpoint filter can RST the conn before our reply lands); either way
/// the contract is "commands are not served".
#[cfg(unix)]
#[test]
fn protected_mode_denies_non_loopback_clients() {
    let _serial = serial();
    let dir = temp_dir("protected");
    let port = free_port();
    let mut server = spawn(port, &dir, &["--bind", "0.0.0.0"]);

    // Every non-loopback IPv4 on this box (en0, bridges, …); point-to-
    // point tunnel addrs connect but route away from the local listener,
    // so candidates that don't answer are just skipped.
    let mut refused_seen = false;
    for ip in non_loopback_ipv4s() {
        let Ok(mut sock) =
            TcpStream::connect_timeout(&std::net::SocketAddr::new(ip, port), Duration::from_secs(1))
        else {
            continue;
        };
        sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        sock.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
        let _ = sock.write_all(b"PING\r\n");
        let mut buf = [0u8; 512];
        match sock.read(&mut buf) {
            Ok(n) => {
                let reply = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    reply.starts_with("-DENIED") || reply.is_empty(),
                    "non-loopback client was served via {ip}: {reply:?}"
                );
                refused_seen = true;
            }
            Err(_) => refused_seen = true, // reset/timeout = refused
        }
    }
    assert!(refused_seen, "no non-loopback interface answered; can't verify the refusal");

    // Loopback is always allowed.
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));

    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Non-loopback IPv4s from `ifconfig` (or `ip` on minimal systems) —
/// `std::net` can't enumerate interfaces. Both formats print `inet
/// <addr[/len]>`, so the parse is the same.
#[cfg(unix)]
fn non_loopback_ipv4s() -> Vec<std::net::IpAddr> {
    for cmd in [("ifconfig", vec![]), ("ip", vec!["-o", "-4", "addr", "show"])] {
        let Ok(out) = Command::new(cmd.0).args(&cmd.1).output() else { continue };
        let text = String::from_utf8_lossy(&out.stdout);
        let mut tokens = text.split_whitespace().peekable();
        let mut ips = Vec::new();
        while let Some(tok) = tokens.next() {
            if tok != "inet" {
                continue;
            }
            let Some(addr) = tokens.next() else { break };
            let addr = addr.split('/').next().unwrap_or(addr);
            if let Ok(ip) = addr.parse::<std::net::Ipv4Addr>() {
                if !ip.is_loopback() && !ip.is_link_local() {
                    ips.push(std::net::IpAddr::V4(ip));
                }
            }
        }
        if !ips.is_empty() {
            return ips;
        }
    }
    Vec::new()
}

/// Mini soak: several client threads hammer mixed traffic for a bounded
/// wall-clock window (default 2s; `REFLEXDB_SOAK_SECS` lengthens it for
/// manual long runs). Per-thread `INCR` sequences must arrive gap-free —
/// a lost update shows up as a wrong count — and under `appendfsync
/// always` every acknowledged count must still be there after SIGKILL.
#[cfg(unix)]
#[test]
fn soak_mixed_load_then_sigkill_recovers() {
    let _serial = serial();
    let dir = temp_dir("soak");
    let port = free_port();
    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "always"]);

    let secs: u64 = std::env::var("REFLEXDB_SOAK_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut joins = Vec::new();
    for t in 0..4u64 {
        joins.push(std::thread::spawn(move || {
            let mut c = Client::connect(port);
            let counter = format!("soak-{t}");
            let scratch = format!("soak-scratch-{t}");
            let mut n = 0i64;
            while Instant::now() < deadline {
                n += 1;
                match c.cmd(&["INCR", &counter]) {
                    Reply::Int(v) => assert_eq!(v, n, "lost update on {counter}"),
                    other => panic!("INCR {counter} -> {other:?}"),
                }
                c.cmd(&["SET", &scratch, "x"]);
                c.cmd(&["DEL", &scratch]);
            }
            n
        }));
    }
    let counts: Vec<i64> = joins.into_iter().map(|j| j.join().unwrap()).collect();

    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));

    sigkill(&mut server);
    let mut server = spawn(port, &dir, &["--appendonly", "yes", "--appendfsync", "always"]);
    let mut c = Client::connect(port);
    for (t, n) in counts.iter().enumerate() {
        assert_eq!(c.cmd(&["GET", &format!("soak-{t}")]), bulk(&n.to_string()));
    }

    sigkill(&mut server);
    let _ = std::fs::remove_dir_all(&dir);
}
