#!/usr/bin/env python3
"""RFX (binary) twin of benchmarks/run.py — same workload, timing, and output
shape, only the wire framing differs. See docs/RFX.md for the format.

Example: python3 benchmarks/run_rfx.py --runs 5 --seconds 10 --clients 16 --pipeline 1,16
"""

import argparse
import json
import math
import selectors
import socket
import struct
import threading
import time
from collections import Counter


def encode(*parts):
    out = [b"!\x01", struct.pack(">H", len(parts))]
    for part in parts:
        data = part if isinstance(part, bytes) else str(part).encode()
        out.extend((struct.pack(">I", len(data)), data))
    return b"".join(out)


class Connection:
    def __init__(self, host, port, timeout=10):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.file = self.sock.makefile("rb")

    def command(self, *parts):
        self.sock.sendall(encode(*parts))
        return self.read()

    def read(self):
        marker = self.file.read(1)
        if not marker:
            raise ConnectionError("server closed the connection")
        if marker in (b"+", b"-", b"$", b"("):
            size = struct.unpack(">I", self.file.read(4))[0]
            return (marker, self.file.read(size))
        if marker == b":":
            return (marker, struct.unpack(">q", self.file.read(8))[0])
        if marker == b"_":
            return (marker, None)
        if marker == b"#":
            return (marker, self.file.read(1) != b"\x00")
        if marker == b",":
            return (marker, struct.unpack(">d", self.file.read(8))[0])
        if marker in (b"*", b"~", b">"):
            count = struct.unpack(">I", self.file.read(4))[0]
            return (marker, [self.read() for _ in range(count)])
        if marker == b"%":
            count = struct.unpack(">I", self.file.read(4))[0]
            return (marker, [(self.read(), self.read()) for _ in range(count)])
        raise ValueError(f"unsupported RFX marker {marker!r}")

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.file.close()
        finally:
            self.sock.close()


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def summarize(name, run, pipeline, clients, elapsed, samples, ops, errors=0, **extra):
    row = {
        "workload": name,
        "run": run,
        "pipeline": pipeline,
        "clients": clients,
        "operations": ops,
        "elapsed_seconds": round(elapsed, 6),
        "ops_per_second": round(ops / elapsed, 2) if elapsed else 0,
        "latency_us": {label: round(percentile(samples, fraction) * 1e6, 2)
                       for label, fraction in (("p50", .50), ("p99", .99), ("p999", .999))
                       if percentile(samples, fraction) is not None},
        "errors": errors,
    }
    row.update(extra)
    return row


def run_requests(host, port, workload, run, seconds, clients, pipeline, keys):
    stop = time.monotonic() + seconds
    lock = threading.Lock()
    all_samples, totals, errors = [], Counter(), Counter()

    def worker(worker_id):
        samples, ops, failed = [], 0, 0
        conn = None
        try:
            conn = Connection(host, port)
            counter = 0
            while time.monotonic() < stop:
                batch, sent_at = [], []
                for _ in range(pipeline):
                    key = "hot:shared" if workload == "hot-key" else f"bench:{(worker_id * 97 + counter) % keys}"
                    if workload == "hot-key":
                        cmd = ("GET", key) if counter % 2 == 0 else ("INCR", key)
                    else:
                        cmd = ("GET", key) if counter % 10 < 7 else ("SET", key, counter)
                    batch.append(encode(*cmd))
                    sent_at.append(time.perf_counter())
                    counter += 1
                conn.sock.sendall(b"".join(batch))
                for started in sent_at:
                    response = conn.read()
                    if response[0] == b"-":
                        failed += 1
                    samples.append(time.perf_counter() - started)
                    ops += 1
        except (OSError, ValueError, ConnectionError):
            failed += 1
        finally:
            if conn:
                conn.close()
            with lock:
                all_samples.extend(samples)
                totals["ops"] += ops
                errors["count"] += failed

    key = "hot:shared" if workload == "hot-key" else "bench:0"
    setup = Connection(host, port)
    setup.command("SET", key, "0")
    for i in range(min(keys, 10000)):
        setup.command("SET", f"bench:{i}", "0")
    setup.close()
    start = time.monotonic()
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - start
    return summarize(workload, run, pipeline, clients, elapsed, all_samples, totals["ops"], errors["count"])


def run_live(host, port, run, seconds, subscribers, drain_mode):
    setup = Connection(host, port)
    setup.command("SET", "live:hot", "0")
    setup.close()
    conns = []
    stop_drainers = threading.Event()
    drain_errors = Counter()

    def drain_all():
        selector = selectors.DefaultSelector()
        try:
            for conn in conns:
                conn.sock.setblocking(False)
                selector.register(conn.sock, selectors.EVENT_READ)
            while not stop_drainers.is_set():
                for key, _ in selector.select(timeout=0.05):
                    try:
                        if not key.fileobj.recv(65536):
                            selector.unregister(key.fileobj)
                    except (BlockingIOError, InterruptedError):
                        continue
                    except OSError:
                        try:
                            selector.unregister(key.fileobj)
                        except (KeyError, ValueError):
                            pass
                        if not stop_drainers.is_set():
                            drain_errors["count"] += 1
        finally:
            selector.close()

    for _ in range(subscribers):
        conn = Connection(host, port, timeout=30)
        if drain_mode == "buffered":
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
        conn.command("LIVE", "GET", "live:hot")
        conns.append(conn)
    drainer = None
    if drain_mode == "selector":
        drainer = threading.Thread(target=drain_all, daemon=True)
        drainer.start()

    writer = Connection(host, port, timeout=30)
    latencies, ops, errors = [], 0, 0
    started_run = time.monotonic()
    deadline = started_run + seconds
    elapsed = 0.0
    try:
        while time.monotonic() < deadline:
            started = time.perf_counter()
            response = writer.command("INCR", "live:hot")
            latencies.append(time.perf_counter() - started)
            ops += 1
            errors += response[0] == b"-"
        elapsed = time.monotonic() - started_run
    finally:
        stop_drainers.set()
        writer.close()
        for conn in conns:
            conn.close()
        if drainer:
            drainer.join(timeout=2)
    return summarize("live-fanout", run, 1, 1, elapsed,
                     latencies, ops, errors + drain_errors["count"], subscribers=subscribers,
                     drain_mode=drain_mode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7379)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--pipeline", default="1,16", help="comma-separated pipeline depths")
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--live-subscribers", type=int, default=1000)
    parser.add_argument("--live-drain", choices=("buffered", "selector"), default="buffered",
                        help="buffer watcher pushes in kernel sockets or drain them in a selector")
    parser.add_argument("--workloads", default="small-mixed,hot-key,live-fanout")
    parser.add_argument("--output", help="JSONL output path; stdout if omitted")
    args = parser.parse_args()
    pipelines = [int(x) for x in args.pipeline.split(",")]
    workloads = [x.strip() for x in args.workloads.split(",")]
    allowed = {"small-mixed", "hot-key", "live-fanout"}
    unknown = set(workloads) - allowed
    if (unknown or args.runs < 1 or args.seconds <= 0 or args.clients < 1 or args.keys < 1
            or args.live_subscribers < 0 or any(pipeline < 1 for pipeline in pipelines)):
        parser.error(f"invalid workload/options: {', '.join(sorted(unknown))}")

    output = open(args.output, "a", encoding="utf-8") if args.output else None
    try:
        for run in range(1, args.runs + 1):
            for workload in workloads:
                if workload == "live-fanout":
                    row = run_live(args.host, args.port, run, args.seconds, args.live_subscribers,
                                   args.live_drain)
                else:
                    for pipeline in pipelines:
                        row = run_requests(args.host, args.port, workload, run, args.seconds,
                                           args.clients, pipeline, args.keys)
                        line = json.dumps(row, sort_keys=True)
                        print(line, flush=True) if output is None else output.write(line + "\n")
                        if output:
                            output.flush()
                    continue
                line = json.dumps(row, sort_keys=True)
                print(line, flush=True) if output is None else output.write(line + "\n")
                if output:
                    output.flush()
    finally:
        if output:
            output.close()


if __name__ == "__main__":
    main()
