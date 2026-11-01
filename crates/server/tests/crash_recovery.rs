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
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(Some(s.as_bytes().to_vec()))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
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

/// `kill -9` — no graceful shutdown, exactly the crash scenario the AOF
/// and snapshot paths exist for.
fn sigkill(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

/// `appendfsync always` — the `profile durable` contract: every write the
/// client saw acknowledged must survive `kill -9`.
#[test]
fn sigkill_loses_nothing_under_appendfsync_always() {
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
    // The future one is still armed, and its marker still exists.
    match c.cmd(&["SCHEDULE", "LIST"]) {
        Reply::Array(items) => assert_eq!(items.len(), 1, "expected exactly the future task: {items:?}"),
        other => panic!("expected SCHEDULE LIST array, got {other:?}"),
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

/// Wire-level RESP compatibility spot-checks against a live server —
/// complements `benchmarks/smoke.sh` (which is manual) with something CI runs.
#[test]
fn resp_command_compat() {
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
