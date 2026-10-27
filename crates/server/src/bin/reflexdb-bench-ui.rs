use reflexdb_server::resp_client::{Reply, RespClient};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{info, warn};

const LOGO_PNG: &[u8] = include_bytes!("../../assets/logo.png");

const HTML: &str = r###"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/png" href="/logo.png">
<title>ReflexDB benchmark UI</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: system-ui, -apple-system, sans-serif; background: #090910; color: #e8e8ee; max-width: 1300px; margin: 0 auto; padding: 2rem; }
  .brand { display: flex; align-items: center; gap: .9rem; }
  .brand img { width: 52px; height: 52px; filter: drop-shadow(0 0 14px rgba(239, 68, 68, .45)); }
  h1 { font-size: 2.1rem; margin-bottom: .2rem; background: linear-gradient(90deg, #a78bfa, #22d3ee); -webkit-background-clip: text; background-clip: text; color: transparent; }
  h2 { color: #d4d4de; font-size: 1.25rem; margin: 2rem 0 .9rem; }
  .subtitle { color: #8a8a99; margin-bottom: 1.5rem; }
  form, .panel { background: #14141c; border: 1px solid #2a2a38; border-radius: 16px; padding: 1.4rem; display: grid; gap: .9rem; grid-template-columns: auto 1fr; align-items: center; }
  label { color: #9a9aa8; font-size: .88rem; }
  input, select { background: #0c0c12; color: #fff; border: 1px solid #2a2a38; border-radius: 9px; padding: .55rem .7rem; font-family: inherit; }
  input:focus, select:focus { outline: 2px solid #8b5cf6; border-color: #8b5cf6; }
  .actions { grid-column: 1 / -1; display: flex; gap: 1rem; margin-top: .5rem; flex-wrap: wrap; align-items: center; }
  button { background: linear-gradient(90deg, #8b5cf6, #22d3ee); color: #0a0a10; border: 0; padding: .7rem 1.5rem; border-radius: 9px; cursor: pointer; font-weight: 700; font-size: .92rem; }
  button:disabled { opacity: .4; cursor: not-allowed; }
  button.secondary { background: #1e1e2a; color: #e8e8ee; font-weight: 600; }
  .preset-row { grid-column: 1 / -1; display: flex; gap: .6rem; flex-wrap: wrap; margin-bottom: .2rem; }
  .preset-btn { background: #1a1a24; border: 1px solid #333345; color: #c9c9d4; padding: .5rem 1rem; border-radius: 22px; font-size: .82rem; font-weight: 600; cursor: pointer; transition: all .15s; }
  .preset-btn:hover { border-color: #8b5cf6; color: #fff; transform: translateY(-1px); }
  .preset-btn.active { background: linear-gradient(90deg, #8b5cf6, #22d3ee); border-color: transparent; color: #0a0a10; }

  .tab-row { display: flex; gap: .5rem; margin-bottom: 1rem; border-bottom: 1px solid #2a2a38; padding-bottom: 0; }
  .tab-btn { background: transparent; color: #8a8a99; border: 0; border-bottom: 2px solid transparent; padding: .7rem 1.1rem; font-weight: 700; font-size: .9rem; cursor: pointer; border-radius: 0; }
  .tab-btn:hover { color: #d4d4de; }
  .tab-btn.active { color: #fff; border-bottom-color: #8b5cf6; }
  .tab-btn[hidden] { display: none; }
  .tab-badge { display: inline-block; background: #22c55e; color: #06140b; font-size: .62rem; font-weight: 800; padding: .08rem .4rem; border-radius: 8px; margin-left: .4rem; vertical-align: middle; }

  #output { background: #0c0c12; border: 1px solid #2a2a38; border-radius: 14px; padding: 1rem; white-space: pre-wrap; min-height: 100px; max-height: 300px; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .8rem; color: #9a9aa8; }
  #statsRaw { background: #0c0c12; border: 1px solid #2a2a38; border-radius: 10px; padding: .8rem; font-family: ui-monospace, Menlo, monospace; font-size: .78rem; color: #777; }
  .note { background: #171722; border-left: 4px solid #8b5cf6; padding: 1rem 1.2rem; border-radius: 10px; margin-bottom: 1.2rem; font-size: .9rem; color: #c5c5d0; line-height: 1.5; }
  .note code { background: #0c0c12; padding: .1rem .4rem; border-radius: 5px; }
  .info-icon { display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; background: #2a2a38; color: #9a9aa8; font-size: .66rem; font-weight: 800; font-style: italic; cursor: pointer; margin-left: .35rem; vertical-align: middle; position: relative; user-select: none; }
  .info-icon:hover, .info-icon.open { background: #8b5cf6; color: #fff; }
  .info-tooltip { display: none; position: absolute; top: 140%; left: 0; z-index: 30; width: 270px; background: #1a1a24; border: 1px solid #333345; border-radius: 10px; padding: .8rem .9rem; font-size: .78rem; line-height: 1.5; color: #c5c5d0; font-weight: 400; font-style: normal; text-transform: none; letter-spacing: normal; box-shadow: 0 10px 28px rgba(0,0,0,.45); cursor: default; }
  .info-tooltip code { background: #0c0c12; padding: .1rem .35rem; border-radius: 4px; }
  .info-icon:hover .info-tooltip, .info-icon.open .info-tooltip { display: block; }
  .limit-inputs { display: grid; grid-template-columns: auto 1fr auto 1fr; gap: 1rem; background: #14141c; border: 1px solid #2a2a38; border-radius: 16px; padding: 1.1rem; margin-bottom: 1rem; }
  .bar-wrap { background: #24242f; border-radius: 9px; height: 26px; overflow: hidden; margin-top: .4rem; }
  .bar-fill { height: 100%; width: 0%; background: linear-gradient(90deg, #8b5cf6, #22d3ee); transition: width .4s ease; border-radius: 9px; }
  .bar-fill.warn { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
  .bar-fill.danger { background: linear-gradient(90deg, #ef4444, #f87171); }
  .bar-label { display: flex; justify-content: space-between; font-size: .88rem; color: #9a9aa8; margin-top: .3rem; }

  .hero-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 1rem; margin: 1.2rem 0 .5rem; }
  .hero-card { background: linear-gradient(160deg, #181824, #101018); border: 1px solid #2a2a38; border-radius: 14px; padding: 1.1rem; text-align: center; }
  .hero-card .k { color: #8a8a99; font-size: .76rem; text-transform: uppercase; letter-spacing: .04em; }
  .hero-card .v { font-size: 1.7rem; font-weight: 800; margin-top: .35rem; }
  .hero-card .v.purple { color: #a78bfa; }
  .hero-card .v.cyan { color: #22d3ee; }
  .hero-card .v.red { color: #f87171; }
  .hero-card .v.green { color: #4ade80; }

  .progress-line { display: flex; align-items: center; gap: .7rem; color: #c5c5d0; font-size: .92rem; margin: .8rem 0; flex-wrap: wrap; }
  .spinner { width: 14px; height: 14px; border-radius: 50%; border: 2px solid #2a2a38; border-top-color: #8b5cf6; animation: spin .7s linear infinite; flex-shrink: 0; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .live-ticker { font-family: ui-monospace, Menlo, monospace; font-size: .82rem; color: #4ade80; background: #0c0c12; padding: .2rem .6rem; border-radius: 6px; border: 1px solid #1f3a28; }

  .cmd-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 1.1rem; }
  .cmd-card { background: #14141c; border: 1px solid #2a2a38; border-radius: 16px; padding: 1.1rem; border-top: 3px solid var(--accent, #8b5cf6); transition: transform .15s, border-color .15s; }
  .cmd-card:hover { transform: translateY(-2px); border-color: #3a3a4a; }
  .cmd-card-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: .5rem; }
  .cmd-name { font-weight: 800; font-size: 1.05rem; letter-spacing: .02em; }
  .cmd-status { font-size: .72rem; font-weight: 700; padding: .2rem .6rem; border-radius: 20px; text-transform: uppercase; letter-spacing: .03em; }
  .cmd-status.running { background: #3f2e0f; color: #fbbf24; }
  .cmd-status.done { background: #14301f; color: #4ade80; }
  .cmd-live { font-family: ui-monospace, Menlo, monospace; font-size: .72rem; color: #67e8a8; margin-bottom: .6rem; min-height: 1em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .cmd-variant { margin-bottom: .6rem; padding-bottom: .5rem; border-bottom: 1px dashed #24242f; }
  .cmd-variant:last-child { border-bottom: none; }
  .cmd-variant-label { font-size: .78rem; color: #8a8a99; margin-bottom: .25rem; }
  .cmd-stat-row { display: flex; gap: 1.1rem; flex-wrap: wrap; }
  .cmd-stat { min-width: 58px; }
  .cmd-stat .sk { font-size: .68rem; color: #8a8a99; text-transform: uppercase; letter-spacing: .03em; font-weight: 600; }
  .cmd-stat .sv { font-size: 1.55rem; font-weight: 800; color: #fff; line-height: 1.25; }
  .cmd-stat.rps .sv { color: #a78bfa; }
  .mini-label { font-size: .74rem; color: #8a8a99; margin: .7rem 0 .2rem; display: flex; justify-content: space-between; }
  .mini-label b { color: #d4d4de; font-weight: 700; }
  canvas.mini-bar { width: 100%; height: 26px; display: block; border-radius: 6px; }
  canvas.mini-spark { width: 100%; height: 54px; display: block; border-radius: 6px; margin-top: .3rem; }

  #liveFeed { max-height: 360px; overflow-y: auto; margin-bottom: 1rem; }
  .live-feed-row { background: #14141c; border: 1px solid #2a2a38; border-left: 4px solid #22d3ee; border-radius: 10px; padding: .7rem 1rem; margin-bottom: .6rem; font-size: .88rem; }
  .live-feed-row .lf-time { color: #8a8a99; font-size: .74rem; }
  .live-feed-row .lf-key { color: #a78bfa; font-weight: 700; }
  .live-feed-row .lf-result { font-weight: 700; color: #4ade80; margin-top: .25rem; font-family: ui-monospace, Menlo, monospace; }
  .live-row { display: flex; gap: .5rem; align-items: center; margin-bottom: .5rem; }
  .live-row input { flex: 1; min-width: 0; }
  .live-row button { padding: .4rem .7rem; font-size: .8rem; }

  table { width: 100%; border-collapse: collapse; margin-top: 1rem; font-size: .85rem; }
  th, td { padding: .5rem .65rem; text-align: right; border-bottom: 1px solid #24242f; }
  th:first-child, td:first-child { text-align: left; }
  th { color: #8a8a99; font-weight: 600; font-size: .78rem; text-transform: uppercase; }
  tr:hover td { background: #191924; }
</style>
</head>
<body>
<div class="brand">
  <img src="/logo.png" alt="ReflexDB logo">
  <h1>ReflexDB benchmark UI</h1>
</div>
<p class="subtitle">Pick a workload, run it, and watch RAM &amp; disk move in real time &mdash; one card per command.</p>

<div class="limit-inputs">
  <label for="ramLimitGB">Max RAM (GB)</label>
  <input id="ramLimitGB" type="number" value="8" step="0.1" min="0.1">
  <label for="diskLimitGB">Max Disk (GB)</label>
  <input id="diskLimitGB" type="number" value="0.5" step="0.1" min="0.1">
</div>

<button id="refreshStats" class="secondary">Show host memory &amp; disk</button>
<div id="statsArea" style="margin: 1rem 0; display:none;">
  <div style="margin-bottom:1rem;">
    <div class="bar-label"><span>Host RAM</span><span id="ramText">-</span></div>
    <div class="bar-wrap"><div id="ramBar" class="bar-fill"></div></div>
  </div>
  <div>
    <div class="bar-label"><span>Host Disk</span><span id="diskText">-</span></div>
    <div class="bar-wrap"><div id="diskBar" class="bar-fill"></div></div>
  </div>
  <pre id="statsRaw" style="display:none;"></pre>
</div>

<div class="tab-row" id="tabRow">
  <button type="button" class="tab-btn active" data-tab="sequential">Sequential</button>
  <button type="button" class="tab-btn" data-tab="parallel">Parallel (all at once)</button>
  <button type="button" class="tab-btn" data-tab="history" hidden id="historyTabBtn">History (GETAT/HISTORY)<span class="tab-badge">on</span><span class="info-icon" tabindex="0">i<span class="info-tooltip">
    <strong>History mode</strong> runs three steps in order: (1) <code>SET</code> writes to populate a keyspace
    (history is recorded automatically on every write once <code>history-enabled yes</code> is set), then
    (2) <code>GETAT</code> point-in-time reads and (3) <code>HISTORY</code> version-log reads against those same keys.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="live">LIVE demo<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Subscribes to <code>LIVE GET &lt;key&gt;</code> on a running server and shows every pushed
    update as it arrives — no polling. Use the "Set value" button to trigger one.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="tiered">Tiered storage demo<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Writes real keys to a running server, then shows how many stayed in RAM (<code>DBSIZE</code>)
    vs. spilled to the on-disk tier, and times a <code>GET</code> that promotes an evicted key
    back. Needs a server started with <code>tiered-storage yes</code> and a small
    <code>tiered-memory-limit</code> to actually see eviction happen.
  </span></span></button>
</div>

<form id="bench" class="panel">
  <div class="preset-row" id="presetRow"></div>
  <label>Host</label><input name="host" value="127.0.0.1">
  <label>Port</label><input name="port" value="7379">
  <label data-mode="history">Data dir (for disk tracking)</label><input data-mode="history" name="dir" value="data">
  <div data-mode="sequential,parallel">
    <label>Commands</label>
  </div>
  <input data-mode="sequential,parallel" name="cmd" id="cmdField" value="ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_500,mset,xadd">
  <label>Requests per command</label><input name="n" id="nField" value="200000" type="number">
  <label>Clients</label><input name="c" id="cField" value="50" type="number">
  <label data-mode="sequential,parallel">Pipeline</label><input data-mode="sequential,parallel" name="p" id="pField" value="10" type="number">
  <label data-mode="sequential,parallel">Data size (bytes)</label><input data-mode="sequential,parallel" name="d" id="dField" value="20" type="number">
  <label>Keyspace (-r, 0 = same key)<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    <strong>Keyspace (-r)</strong> spreads writes/reads across many keys instead of hammering one. Without it,
    <code>redis-benchmark</code> reuses a single key over and over, so memory barely moves — set it close to
    "Requests" to actually see memory/disk climb per operation. Presets set it automatically.
  </span></span></label><input name="r" id="rField" value="100000" type="number">
  <div class="actions">
    <button type="submit" id="runBtn">Run benchmark</button>
    <button type="button" id="stopBtn" disabled class="secondary">Stop</button>
  </div>
</form>

<div id="liveDemoPanel" class="panel" style="display:none;">
  <label>Host</label><input id="liveHost" value="127.0.0.1">
  <label>Port</label><input id="livePort" value="7379" type="number">
  <label>Keys &amp; values<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Each row subscribes to its own <code>LIVE GET &lt;key&gt;</code> over the same connection —
    add more rows to watch several keys at once. "Set" fires a one-off <code>SET</code> for that
    row; "Auto-change" below randomizes every row's value repeatedly for you.
  </span></span></label>
  <div id="liveRows"></div>
  <div class="actions">
    <button type="button" id="liveAddRowBtn" class="secondary">+ Add key</button>
  </div>
  <label>Auto-change</label>
  <div class="actions">
    <span style="color:#9a9aa8; font-size:.88rem;">for</span>
    <input id="liveAutoSeconds" value="10" type="number" min="1" style="width:5rem;">
    <span style="color:#9a9aa8; font-size:.88rem;">seconds, every</span>
    <input id="liveAutoIntervalMs" value="1000" type="number" min="100" style="width:6rem;">
    <span style="color:#9a9aa8; font-size:.88rem;">ms</span>
    <button type="button" id="liveAutoBtn">Start auto-change</button>
    <button type="button" id="liveAutoStopBtn" disabled class="secondary">Stop auto-change</button>
  </div>
  <div class="actions">
    <button type="button" id="liveWatchBtn">Start watching</button>
    <button type="button" id="liveStopBtn" disabled class="secondary">Stop watching</button>
  </div>
</div>

<div id="tieredDemoPanel" class="panel" style="display:none;">
  <div class="note" style="grid-column: 1 / -1;">
    <strong>This must point at a dedicated server</strong>, not your regular one (e.g. not the
    default port <code>7379</code>) — <code>tiered-storage</code> and
    <code>tiered-memory-limit</code> can only be set when a server <em>starts</em> (in its
    <code>.conf</code> file), there's no way to turn tiering on or change the limit from this UI
    or from a running client. Start one like:
    <pre style="margin:.5rem 0 0; background:#0c0c12; padding:.6rem .8rem; border-radius:8px; font-size:.78rem;">reflexdb-server --port 7391 --dir /tmp/tiered-demo \
  --tiered-storage yes --tiered-memory-limit 10kb --tiered-dir tiered</pre>
  </div>
  <label>Host</label><input id="tieredHost" value="127.0.0.1">
  <label>Port</label><input id="tieredPort" value="7391" type="number">
  <label>Tiered dir (on disk)</label><input id="tieredDir" value="/tmp/tiered-demo/tiered">
  <label>Keys to write</label><input id="tieredCount" value="2000" type="number">
  <label>Value size (bytes)</label><input id="tieredSize" value="2000" type="number">
  <div class="actions">
    <button type="button" id="tieredRunBtn">Run tiered storage demo</button>
  </div>
</div>

<div class="progress-line" id="progressLine" style="display:none;">
  <div class="spinner"></div>
  <span id="progressText">Starting...</span>
  <span class="live-ticker" id="liveTicker" style="display:none;"></span>
</div>

<div id="liveStats" class="hero-row" style="display:none;">
  <div class="hero-card"><div class="k">Subscribe requests sent</div><div class="v purple" id="liveSubCount">0</div></div>
  <div class="hero-card"><div class="k">Updates pushed to us</div><div class="v cyan" id="livePushCount">0</div></div>
  <div class="hero-card"><div class="k">If this were polling</div><div class="v red" id="livePollEquivalent">0 requests</div></div>
</div>
<p id="liveExplain" style="display:none; color:#8a8a99; font-size:.85rem; margin:-.5rem 0 1rem;">
  "Subscribe requests" only grows when you click "Start watching" — it's one <code>LIVE GET</code>
  per key, sent once. "Updates pushed to us" grows every time a value changes below, with zero
  extra requests from this browser. A polling client doing the same job would need one
  <code>GET</code> per key for <em>every</em> check, found-a-change or not — that's the number on
  the right, and it only goes up.
</p>
<div id="liveFeed"></div>

<div id="tieredSummary" class="hero-row" style="display:none;">
  <div class="hero-card"><div class="k">Keys written</div><div class="v purple" id="tsWritten">-</div></div>
  <div class="hero-card"><div class="k">DBSIZE (in RAM)</div><div class="v cyan" id="tsDbsize">-</div></div>
  <div class="hero-card"><div class="k">Tiered dir on disk</div><div class="v green" id="tsDisk">-</div></div>
  <div class="hero-card"><div class="k">Promote GET time</div><div class="v red" id="tsPromote">-</div></div>
</div>

<h2>Live output</h2>
<pre id="output">Pick a preset (or edit fields) and click "Run benchmark". A card appears for every command as it starts, and fills in live.</pre>

<div id="benchResults">
  <div id="heroSection" style="display:none;">
    <h2>Run summary</h2>
    <div class="hero-row">
      <div class="hero-card"><div class="k">Best throughput</div><div class="v purple" id="bestRps">-</div></div>
      <div class="hero-card"><div class="k">Commands tested</div><div class="v cyan" id="totalCmds">-</div></div>
      <div class="hero-card"><div class="k">Lowest p99 latency</div><div class="v red" id="bestP99">-</div></div>
      <div class="hero-card"><div class="k">Peak server memory</div><div class="v green" id="peakMem">-</div></div>
    </div>
  </div>

  <h2>Per-command results</h2>
  <div class="cmd-grid" id="cmdGrid"></div>

  <div id="tableSection" style="display:none;">
    <h2>Full numbers</h2>
    <table id="numbersTable">
      <thead>
        <tr><th>Command</th><th>Variant</th><th>RPS</th><th>Avg (ms)</th><th>p90</th><th>p95</th><th>p99</th><th>Mem &Delta;</th><th>Disk &Delta;</th></tr>
      </thead>
      <tbody id="numbersBody"></tbody>
    </table>
  </div>
</div>

<script>
const PRESETS = {
  'Redis default': { n: 100000, c: 50, p: 1, d: 3, r: 100000, cmd: 'ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_500,mset,xadd' },
  'Pipelined (prod-like)': { n: 500000, c: 50, p: 16, d: 3, r: 500000, cmd: 'ping,set,get,incr,lpush,rpush,sadd,hset,zadd,mset,xadd' },
  'High concurrency': { n: 500000, c: 200, p: 32, d: 100, r: 500000, cmd: 'set,get,incr,lpush,sadd,hset,zadd' },
  'Large payload (object cache)': { n: 100000, c: 50, p: 1, d: 1024, r: 100000, cmd: 'set,get,mset' },
  'Memory stress (write heavy)': { n: 1000000, c: 100, p: 16, d: 100, r: 1000000, cmd: 'set,lpush,sadd,hset,zadd,xadd' },
  'Everything (all commands)': { n: 1000000, c: 50, p: 16, d: 20, r: 1000000, cmd: 'ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_500,mset,xadd' },
};

const CMD_COLORS = ['#8b5cf6', '#22d3ee', '#f59e0b', '#ef4444', '#22c55e', '#60a5fa', '#f472b6', '#facc15', '#a78bfa', '#34d399', '#fb923c', '#38bdf8', '#e879f9', '#4ade80', '#c084fc', '#fca5a5', '#86efac', '#93c5fd', '#fde047'];

const bench = document.getElementById('bench');
const output = document.getElementById('output');
const runBtn = document.getElementById('runBtn');
const stopBtn = document.getElementById('stopBtn');
const statsArea = document.getElementById('statsArea');
const statsRaw = document.getElementById('statsRaw');
const refreshStats = document.getElementById('refreshStats');
const ramLimitGB = document.getElementById('ramLimitGB');
const diskLimitGB = document.getElementById('diskLimitGB');
const ramBar = document.getElementById('ramBar');
const diskBar = document.getElementById('diskBar');
const ramText = document.getElementById('ramText');
const diskText = document.getElementById('diskText');
const presetRow = document.getElementById('presetRow');
const cmdGrid = document.getElementById('cmdGrid');
const heroSection = document.getElementById('heroSection');
const tableSection = document.getElementById('tableSection');
const numbersBody = document.getElementById('numbersBody');
const bestRps = document.getElementById('bestRps');
const totalCmds = document.getElementById('totalCmds');
const bestP99 = document.getElementById('bestP99');
const peakMem = document.getElementById('peakMem');
const progressLine = document.getElementById('progressLine');
const progressText = document.getElementById('progressText');
const liveTicker = document.getElementById('liveTicker');
const tabRow = document.getElementById('tabRow');
const historyTabBtn = document.getElementById('historyTabBtn');
const runEndpoints = { sequential: '/run-profiled', parallel: '/run-profiled', history: '/run-history-profiled' };
const benchResults = document.getElementById('benchResults');
const liveDemoPanel = document.getElementById('liveDemoPanel');
const tieredDemoPanel = document.getElementById('tieredDemoPanel');
const liveFeed = document.getElementById('liveFeed');
const liveStats = document.getElementById('liveStats');
const liveExplain = document.getElementById('liveExplain');
const liveSubCount = document.getElementById('liveSubCount');
const livePushCount = document.getElementById('livePushCount');
const livePollEquivalent = document.getElementById('livePollEquivalent');
const tieredSummary = document.getElementById('tieredSummary');

let controller = null;
let liveController = null;
let liveAutoController = null;
let tieredController = null;
let currentTab = 'sequential';
let liveStartedAt = 0;
let liveWatchedKeyCount = 0;

for (const name of Object.keys(PRESETS)) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'preset-btn';
  btn.textContent = name;
  btn.onclick = () => {
    const p = PRESETS[name];
    bench.n.value = p.n;
    bench.c.value = p.c;
    bench.p.value = p.p;
    bench.d.value = p.d;
    bench.r.value = p.r;
    bench.cmd.value = p.cmd;
    [...presetRow.children].forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
  };
  presetRow.appendChild(btn);
}

function setTab(tab) {
  currentTab = tab;
  [...tabRow.children].forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  [...bench.querySelectorAll('[data-mode]')].forEach(el => {
    el.style.display = el.dataset.mode.split(',').includes(tab) ? '' : 'none';
  });
  runBtn.textContent = tab === 'parallel' ? 'Run all commands in parallel' : tab === 'history' ? 'Run history benchmark' : 'Run benchmark';

  const isLive = tab === 'live';
  const isTiered = tab === 'tiered';
  bench.style.display = (isLive || isTiered) ? 'none' : '';
  benchResults.style.display = (isLive || isTiered) ? 'none' : '';
  liveDemoPanel.style.display = isLive ? 'grid' : 'none';
  tieredDemoPanel.style.display = isTiered ? 'grid' : 'none';
  liveFeed.style.display = isLive ? '' : 'none';
  liveStats.style.display = isLive ? 'grid' : 'none';
  liveExplain.style.display = isLive ? '' : 'none';
  if (!isLive && liveController) { liveController.abort(); liveController = null; }
  if (!isLive && liveAutoController) { liveAutoController.abort(); liveAutoController = null; }
  if (!isTiered && tieredController) { tieredController.abort(); tieredController = null; }
  if (!isTiered) tieredSummary.style.display = 'none';
}
tabRow.addEventListener('click', (e) => {
  if (e.target.closest('.info-icon')) return;
  const btn = e.target.closest('.tab-btn');
  if (btn && !btn.hidden) setTab(btn.dataset.tab);
});
setTab('sequential');

// Info icons (the small "i" badges next to labels/tabs): click to toggle
// the tooltip open (for touch/no-hover devices), click anywhere else to
// close whichever one is open. Hover alone already shows it via CSS.
document.addEventListener('click', (e) => {
  const icon = e.target.closest('.info-icon');
  document.querySelectorAll('.info-icon.open').forEach(el => {
    if (el !== icon) el.classList.remove('open');
  });
  if (icon) {
    e.stopPropagation();
    icon.classList.toggle('open');
  }
});

// Only show the History tab if the target server actually has
// `history-enabled yes` — GETAT/HISTORY just error out otherwise.
async function checkHistorySupport() {
  try {
    const qs = new URLSearchParams({ host: bench.host.value, port: bench.port.value }).toString();
    const r = await fetch('/history-status?' + qs);
    const data = await r.json();
    historyTabBtn.hidden = !data.enabled;
  } catch {
    historyTabBtn.hidden = true;
  }
}
checkHistorySupport();
bench.host.addEventListener('change', checkHistorySupport);
bench.port.addEventListener('change', checkHistorySupport);

function fmtGB(kb) { return (kb / 1024 / 1024).toFixed(2); }
function fmtKB(kb) {
  const sign = kb < 0 ? '-' : '';
  kb = Math.abs(kb);
  if (kb >= 1024 * 1024) return sign + (kb / 1024 / 1024).toFixed(2) + ' GB';
  if (kb >= 1024) return sign + (kb / 1024).toFixed(2) + ' MB';
  return sign + kb.toFixed(0) + ' KB';
}

function updateBar(kb, limitGB, fill, text) {
  const limitKB = limitGB * 1024 * 1024;
  const pct = limitKB > 0 ? Math.min(100, (kb / limitKB) * 100) : 0;
  fill.style.width = pct + '%';
  fill.className = 'bar-fill' + (pct > 90 ? ' danger' : pct > 70 ? ' warn' : '');
  text.textContent = `${fmtGB(kb)} / ${limitGB} GB used (${pct.toFixed(1)}%)`;
}

refreshStats.onclick = async () => {
  statsArea.style.display = 'block';
  statsRaw.textContent = 'Loading...';
  try {
    const r = await fetch('/stats');
    const text = await r.text();
    const map = {};
    text.split('\n').forEach(line => {
      const [k, v] = line.split(':').map(s => s.trim());
      if (k && v) map[k] = parseFloat(v) || 0;
    });
    statsRaw.textContent = text;
    updateBar(map.ram_used_kb, parseFloat(ramLimitGB.value) || 1, ramBar, ramText);
    updateBar(map.disk_used_kb, parseFloat(diskLimitGB.value) || 1, diskBar, diskText);
  } catch (e) {
    statsRaw.textContent = 'Failed to load stats: ' + e.message;
  }
};

// Reads the fixed set of percentile/latency pairs redis-benchmark prints
// under "Latency by percentile distribution:" (0%, 50%, 75%, 87.5%, ...)
// and linearly interpolates whatever percentile isn't one of those exact
// points (p90 never is — only p50/p95/p99 appear verbatim in the final
// "Summary" line).
function interpolatePercentile(section, target) {
  const start = section.indexOf('Latency by percentile distribution:');
  const end = section.indexOf('Cumulative distribution of latencies:');
  if (start === -1) return null;
  const sub = section.slice(start, end === -1 ? undefined : end);
  const re = /([\d.]+)%\s*<=\s*([\d.]+) milliseconds/g;
  const pts = [];
  let m;
  while ((m = re.exec(sub)) !== null) pts.push({ p: parseFloat(m[1]), v: parseFloat(m[2]) });
  if (pts.length === 0) return null;
  for (let i = 0; i < pts.length; i++) {
    if (pts[i].p === target) return pts[i].v;
    if (pts[i].p > target) {
      if (i === 0) return pts[i].v;
      const a = pts[i - 1], b = pts[i];
      const frac = (target - a.p) / (b.p - a.p);
      return a.v + frac * (b.v - a.v);
    }
  }
  return pts[pts.length - 1].v;
}

// Parses every "====== LABEL ======" section out of one command's own log
// text. Most commands produce exactly one section; PING produces two
// (PING_INLINE / PING_MBULK) and LRANGE produces a setup LPUSH plus the
// real LRANGE_N section — all surfaced as separate "variants" on the card
// instead of being silently dropped or merged.
function parseVariants(text) {
  const variants = [];
  const re = /======\s*([^=\n]+?)\s*======/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const label = m[1].trim();
    const start = m.index;
    const next = text.indexOf('======', re.lastIndex);
    const section = text.slice(start, next === -1 ? undefined : next);
    const t = section.match(/throughput summary: ([\d.]+) requests per second/);
    const l = section.match(/latency summary \(msec\):[\s\S]*?avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)/);
    if (t) {
      variants.push({
        label,
        rps: parseFloat(t[1]),
        avg: l ? parseFloat(l[1]) : 0,
        p90: interpolatePercentile(section, 90) ?? (l ? parseFloat(l[4]) : 0),
        p95: l ? parseFloat(l[4]) : 0,
        p99: l ? parseFloat(l[5]) : 0,
        max: l ? parseFloat(l[6]) : 0,
      });
    }
  }
  return variants;
}

function setupHiDpi(canvas, cssHeight) {
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth || canvas.parentElement.clientWidth;
  canvas.width = Math.max(100, cssWidth) * dpr;
  canvas.height = cssHeight * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w: Math.max(100, cssWidth), h: cssHeight };
}

// A single before -> after horizontal bar: the lighter full-width segment is
// "after", the solid darker segment overlaid on top is "before" — so the gap
// between them *is* the visual delta. Works for growth and shrinkage alike.
function drawDeltaBar(canvas, before, after, accentColor) {
  const { ctx, w, h } = setupHiDpi(canvas, 26);
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#24242f';
  ctx.fillRect(0, 0, w, h);

  const scale = Math.max(before, after, 1);
  const afterW = (Math.min(after, scale) / scale) * w;
  const beforeW = (Math.min(before, scale) / scale) * w;

  ctx.fillStyle = accentColor + '55';
  ctx.fillRect(0, 0, Math.max(afterW, 2), h);
  ctx.fillStyle = accentColor;
  ctx.fillRect(0, 0, Math.max(beforeW, after > before ? beforeW : 2), h);
}

function drawSpark(canvas, samples, accentColor) {
  const { ctx, w, h } = setupHiDpi(canvas, 54);
  ctx.clearRect(0, 0, w, h);
  if (samples.length < 2) {
    ctx.fillStyle = '#555';
    ctx.font = '11px system-ui, sans-serif';
    ctx.fillText('collecting...', 6, h / 2 + 4);
    return;
  }
  const minV = Math.min(...samples.map(s => s.v));
  const maxV = Math.max(...samples.map(s => s.v), minV + 1);
  const maxT = samples[samples.length - 1].t || 1;

  const pts = samples.map(s => [
    (s.t / maxT) * (w - 4) + 2,
    h - 4 - ((s.v - minV) / (maxV - minV || 1)) * (h - 8),
  ]);

  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, accentColor + '55');
  grad.addColorStop(1, accentColor + '05');
  ctx.beginPath();
  ctx.moveTo(pts[0][0], h);
  pts.forEach(p => ctx.lineTo(p[0], p[1]));
  ctx.lineTo(pts[pts.length - 1][0], h);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  pts.forEach((p, i) => (i === 0 ? ctx.moveTo(p[0], p[1]) : ctx.lineTo(p[0], p[1])));
  ctx.strokeStyle = accentColor;
  ctx.lineWidth = 1.8;
  ctx.stroke();
}

function makeCard(cmd, color) {
  const card = document.createElement('div');
  card.className = 'cmd-card';
  card.style.setProperty('--accent', color);
  card.innerHTML = `
    <div class="cmd-card-head">
      <span class="cmd-name">${cmd.toUpperCase()}</span>
      <span class="cmd-status running" data-role="status">running</span>
    </div>
    <div class="cmd-live" data-role="live"></div>
    <div data-role="variants"></div>
    <div class="mini-label"><span>RAM</span><b data-role="ramText">-</b></div>
    <canvas class="mini-bar" data-role="ramBar"></canvas>
    <div class="mini-label"><span>Disk</span><b data-role="diskText">-</b></div>
    <canvas class="mini-bar" data-role="diskBar"></canvas>
    <div class="mini-label"><span>Memory while running</span></div>
    <canvas class="mini-spark" data-role="spark"></canvas>
  `;
  cmdGrid.appendChild(card);
  return card;
}

function renderCard(card, state, color) {
  const variants = parseVariants(state.logLines.join('\n'));
  const variantsEl = card.querySelector('[data-role="variants"]');
  variantsEl.innerHTML = variants.length
    ? variants.map(v => {
        // Skip the label line when it's just the command name again — the
        // card header already says that. Only show it when it adds real
        // info (PING_INLINE vs PING_MBULK, "MSET (10 keys)", etc).
        const redundant = v.label.toUpperCase() === state.cmd.toUpperCase();
        return `
      <div class="cmd-variant">
        ${redundant ? '' : `<div class="cmd-variant-label">${v.label}</div>`}
        <div class="cmd-stat-row">
          <div class="cmd-stat rps"><div class="sk">RPS</div><div class="sv">${v.rps.toLocaleString('en-US', { maximumFractionDigits: 0 })}</div></div>
          <div class="cmd-stat"><div class="sk">Avg</div><div class="sv">${v.avg.toFixed(2)}</div></div>
          <div class="cmd-stat"><div class="sk">p90</div><div class="sv">${v.p90.toFixed(2)}</div></div>
          <div class="cmd-stat"><div class="sk">p95</div><div class="sv">${v.p95.toFixed(2)}</div></div>
          <div class="cmd-stat"><div class="sk">p99</div><div class="sv">${v.p99.toFixed(2)}</div></div>
        </div>
      </div>`;
      }).join('')
    : '<div class="cmd-variant-label">waiting for results...</div>';

  const ramBefore = state.memStart ?? 0;
  const ramAfter = state.memNow ?? ramBefore;
  const diskBefore = state.diskStart ?? 0;
  const diskAfter = state.diskNow ?? diskBefore;

  drawDeltaBar(card.querySelector('[data-role="ramBar"]'), ramBefore, ramAfter, color);
  drawDeltaBar(card.querySelector('[data-role="diskBar"]'), diskBefore, diskAfter, color);

  const ramDelta = ramAfter - ramBefore;
  const diskDelta = diskAfter - diskBefore;
  card.querySelector('[data-role="ramText"]').textContent = `${fmtKB(ramBefore)} -> ${fmtKB(ramAfter)} (${ramDelta >= 0 ? '+' : ''}${fmtKB(ramDelta)})`;
  card.querySelector('[data-role="diskText"]').textContent = `${fmtKB(diskBefore)} -> ${fmtKB(diskAfter)} (${diskDelta >= 0 ? '+' : ''}${fmtKB(diskDelta)})`;

  drawSpark(
    card.querySelector('[data-role="spark"]'),
    state.samples.map(s => ({ t: s.t_ms, v: s.mem_kb })),
    color
  );

  state.variants = variants;
}

function finalizeCard(card, state) {
  card.querySelector('[data-role="status"]').textContent = 'done';
  card.querySelector('[data-role="status"]').className = 'cmd-status done';
  card.querySelector('[data-role="live"]').textContent = '';
}

function renderSummaryAndTable(cmdOrder, cmdState) {
  const allVariants = [];
  for (const cmd of cmdOrder) {
    const st = cmdState[cmd];
    (st.variants || []).forEach(v => allVariants.push({ cmd, ...v, memDelta: (st.memNow ?? 0) - (st.memStart ?? 0), diskDelta: (st.diskNow ?? 0) - (st.diskStart ?? 0) }));
  }
  if (allVariants.length === 0) { heroSection.style.display = 'none'; tableSection.style.display = 'none'; return; }

  heroSection.style.display = 'block';
  tableSection.style.display = 'block';

  const best = allVariants.reduce((a, b) => (a.rps > b.rps ? a : b));
  const bestL = allVariants.reduce((a, b) => (a.p99 < b.p99 ? a : b));
  const peak = Math.max(...cmdOrder.flatMap(cmd => cmdState[cmd].samples.map(s => s.mem_kb)), 0);

  bestRps.textContent = best.rps.toLocaleString('en-US', { maximumFractionDigits: 0 }) + ' rps (' + best.cmd.toUpperCase() + ')';
  totalCmds.textContent = cmdOrder.length;
  bestP99.textContent = bestL.p99.toFixed(3) + ' ms (' + bestL.cmd.toUpperCase() + ')';
  peakMem.textContent = fmtKB(peak);

  numbersBody.innerHTML = allVariants
    .slice().sort((a, b) => b.rps - a.rps)
    .map(v => `<tr>
      <td>${v.cmd.toUpperCase()}</td>
      <td>${v.label}</td>
      <td>${v.rps.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
      <td>${v.avg.toFixed(3)}</td>
      <td>${v.p90.toFixed(3)}</td>
      <td>${v.p95.toFixed(3)}</td>
      <td>${v.p99.toFixed(3)}</td>
      <td>${fmtKB(v.memDelta)}</td>
      <td>${fmtKB(v.diskDelta)}</td>
    </tr>`).join('');
}

// ---- LIVE demo: subscribe to LIVE GET <key> for every row's key and show
// every pushed update as it streams in (ndjson over a chunked response,
// same shape as the benchmark endpoints above, just driven by a persistent
// RESP connection instead of spawning redis-benchmark). Rows are added/
// removed freely so this can watch one key or several at once.
const liveWatchBtn = document.getElementById('liveWatchBtn');
const liveStopBtn = document.getElementById('liveStopBtn');
const liveRows = document.getElementById('liveRows');
const liveAddRowBtn = document.getElementById('liveAddRowBtn');
const liveAutoBtn = document.getElementById('liveAutoBtn');
const liveAutoStopBtn = document.getElementById('liveAutoStopBtn');
let liveRowCount = 0;

function addLiveRow(key, value) {
  liveRowCount += 1;
  const row = document.createElement('div');
  row.className = 'live-row';
  row.innerHTML = `
    <input class="live-row-key" value="${key}" placeholder="key">
    <input class="live-row-value" value="${value}" placeholder="value">
    <button type="button" class="secondary live-row-set">Set</button>
    <button type="button" class="secondary live-row-remove">&times;</button>`;
  row.querySelector('.live-row-set').onclick = async () => {
    const qs = new URLSearchParams({
      host: document.getElementById('liveHost').value,
      port: document.getElementById('livePort').value,
      key: row.querySelector('.live-row-key').value,
      value: row.querySelector('.live-row-value').value,
    }).toString();
    try {
      const r = await fetch('/live-set?' + qs);
      const data = await r.json();
      if (!data.ok) output.textContent += `\n[error] SET failed: ${data.error}\n`;
    } catch (err) {
      output.textContent += '\nError: ' + err.message;
    }
  };
  row.querySelector('.live-row-remove').onclick = () => {
    if (liveRows.children.length > 1) row.remove();
  };
  liveRows.appendChild(row);
}
addLiveRow('live:demo:1', 'hello');
addLiveRow('live:demo:2', 'world');
liveAddRowBtn.onclick = () => addLiveRow(`live:demo:${liveRowCount + 1}`, 'value');

function liveKeys() {
  return [...liveRows.querySelectorAll('.live-row-key')].map(i => i.value.trim()).filter(Boolean);
}

function addLiveFeedRow(label, key, fingerprint, result, tMs) {
  const row = document.createElement('div');
  row.className = 'live-feed-row';
  const fp = fingerprint ? fingerprint.slice(0, 12) : '-';
  const resultText = result === null || result === undefined ? '(nil)' : JSON.stringify(result);
  row.innerHTML = `<div class="lf-time">${label} &middot; <span class="lf-key"></span> &middot; t+${tMs}ms &middot; fingerprint ${fp}</div><div class="lf-result"></div>`;
  row.querySelector('.lf-key').textContent = key || '?';
  row.querySelector('.lf-result').textContent = resultText;
  liveFeed.prepend(row);
  // Keep the DOM bounded during a long auto-change run (or anyone just
  // leaving a watch open) — the scrollable box only shows a handful at
  // once anyway, no need to keep thousands of old rows around.
  while (liveFeed.children.length > 200) liveFeed.lastElementChild.remove();
}

function updateLiveStats() {
  const elapsedSec = liveStartedAt ? (Date.now() - liveStartedAt) / 1000 : 0;
  // What a client polling once/sec per key (a typical, not even
  // aggressive, polling interval) would have had to send by now, found a
  // change or not — contrast with "subscribe requests", which never grows
  // past one-per-key no matter how long this runs or how many pushes
  // arrive.
  const pollEquivalent = Math.ceil(elapsedSec) * liveWatchedKeyCount;
  livePollEquivalent.textContent = pollEquivalent.toLocaleString() + ' requests';
}

liveWatchBtn.onclick = async () => {
  if (liveController) liveController.abort();
  const keys = liveKeys();
  if (keys.length === 0) { output.textContent += '\n[error] add at least one key first\n'; return; }
  liveController = new AbortController();
  liveFeed.innerHTML = '';
  output.textContent = '';
  liveWatchBtn.disabled = true;
  liveStopBtn.disabled = false;
  liveStats.style.display = 'grid';
  liveSubCount.textContent = '0';
  livePushCount.textContent = '0';
  liveStartedAt = Date.now();
  liveWatchedKeyCount = keys.length;
  updateLiveStats();
  const statsTimer = setInterval(updateLiveStats, 500);
  const qs = new URLSearchParams({
    host: document.getElementById('liveHost').value,
    port: document.getElementById('livePort').value,
    keys: keys.join(','),
  }).toString();
  try {
    const resp = await fetch('/live-watch?' + qs, { signal: liveController.signal });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let evt;
        try { evt = JSON.parse(line); } catch { continue; }
        if (evt.type === 'update') {
          const label = evt.event === 'subscribed' ? 'Subscribed' : 'Pushed update';
          if (evt.event === 'subscribed') {
            liveSubCount.textContent = (parseInt(liveSubCount.textContent, 10) + 1).toString();
          } else {
            livePushCount.textContent = (parseInt(livePushCount.textContent, 10) + 1).toString();
          }
          addLiveFeedRow(label, evt.key, evt.fingerprint, evt.result, evt.t_ms);
          output.textContent += `[t+${evt.t_ms}ms] ${label} (${evt.key}): fingerprint=${evt.fingerprint} result=${JSON.stringify(evt.result)}\n`;
          output.scrollTop = output.scrollHeight;
        } else if (evt.type === 'error') {
          output.textContent += `\n[error] ${evt.message}\n`;
        }
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') output.textContent += '\nError: ' + err.message;
  } finally {
    clearInterval(statsTimer);
    liveWatchBtn.disabled = false;
    liveStopBtn.disabled = true;
    liveController = null;
  }
};
liveStopBtn.onclick = () => { if (liveController) liveController.abort(); };

// "Auto-change": instead of clicking "Set" by hand, ask the server to
// randomize every watched key's value on an interval for N seconds — the
// already-running /live-watch stream above picks up the resulting pushes
// on its own, same as it would for a manual SET.
liveAutoBtn.onclick = async () => {
  const keys = liveKeys();
  if (keys.length === 0) { output.textContent += '\n[error] add at least one key first\n'; return; }
  if (liveAutoController) liveAutoController.abort();
  liveAutoController = new AbortController();
  liveAutoBtn.disabled = true;
  liveAutoStopBtn.disabled = false;
  const seconds = parseFloat(document.getElementById('liveAutoSeconds').value) || 10;
  const intervalMs = parseInt(document.getElementById('liveAutoIntervalMs').value, 10) || 1000;
  const qs = new URLSearchParams({
    host: document.getElementById('liveHost').value,
    port: document.getElementById('livePort').value,
    keys: keys.join(','),
    duration_ms: Math.round(seconds * 1000),
    interval_ms: intervalMs,
  }).toString();
  try {
    const resp = await fetch('/live-autoset?' + qs, { signal: liveAutoController.signal });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let evt;
        try { evt = JSON.parse(line); } catch { continue; }
        if (evt.type === 'tick') {
          output.textContent += `[auto] SET ${evt.key} = ${evt.value}\n`;
          output.scrollTop = output.scrollHeight;
        } else if (evt.type === 'error') {
          output.textContent += `\n[error] ${evt.message}\n`;
        }
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') output.textContent += '\nError: ' + err.message;
  } finally {
    liveAutoBtn.disabled = false;
    liveAutoStopBtn.disabled = true;
    liveAutoController = null;
  }
};
liveAutoStopBtn.onclick = () => { if (liveAutoController) liveAutoController.abort(); };

// ---- Tiered storage demo: write real keys to a running server, then show
// DBSIZE (what's left in RAM) vs. the tiered dir's on-disk size, and time a
// GET that promotes an evicted key back.
const tieredRunBtn = document.getElementById('tieredRunBtn');

tieredRunBtn.onclick = async () => {
  if (tieredController) tieredController.abort();
  tieredController = new AbortController();
  output.textContent = '';
  tieredSummary.style.display = 'none';
  tieredRunBtn.disabled = true;
  const qs = new URLSearchParams({
    host: document.getElementById('tieredHost').value,
    port: document.getElementById('tieredPort').value,
    dir: document.getElementById('tieredDir').value,
    count: document.getElementById('tieredCount').value,
    size: document.getElementById('tieredSize').value,
  }).toString();
  try {
    const resp = await fetch('/tiered-demo?' + qs, { signal: tieredController.signal });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let evt;
        try { evt = JSON.parse(line); } catch { continue; }
        if (evt.type === 'progress') {
          output.textContent = `Writing keys: ${evt.written} / ${evt.total}\n`;
        } else if (evt.type === 'settling') {
          output.textContent = 'Letting the background eviction tick catch up...\n';
        } else if (evt.type === 'summary') {
          document.getElementById('tsWritten').textContent = evt.written.toLocaleString();
          document.getElementById('tsDbsize').textContent = evt.dbsize.toLocaleString();
          document.getElementById('tsDisk').textContent = fmtKB(evt.tiered_disk_kb);
          tieredSummary.style.display = 'grid';
          output.textContent += `\nWrote ${evt.written} keys. DBSIZE (in RAM) = ${evt.dbsize}. ` +
            `Tiered dir on disk = ${fmtKB(evt.tiered_disk_kb)}.\n` +
            (evt.dbsize < evt.written
              ? `${evt.written - evt.dbsize} key(s) spilled to disk.\n`
              : evt.tiered_disk_kb === 0
                ? `Nothing evicted, and the tiered dir is empty — this server almost certainly wasn't ` +
                  `started with "tiered-storage yes" (or "dir" above points at the wrong path). ` +
                  `tiered-memory-limit can't be set from here — it only takes effect at server startup.\n`
                : `Nothing newly evicted since last run, but ${fmtKB(evt.tiered_disk_kb)} is already on disk from before.\n`);
        } else if (evt.type === 'promote_check') {
          document.getElementById('tsPromote').textContent = evt.ms + ' ms';
          output.textContent += `GET ${evt.key}: ${evt.found ? 'found' : 'not found'} in ${evt.ms}ms` +
            (evt.found ? ` (promoted from disk if it had been evicted) — preview: ${evt.preview}\n` : '\n');
        } else if (evt.type === 'exists_check') {
          output.textContent += `EXISTS ${evt.key}: ${evt.exists ? 'true' : 'false'} (answered truthfully without needing to promote it)\n`;
        } else if (evt.type === 'error') {
          output.textContent += `\n[error] ${evt.message}\n`;
        } else if (evt.type === 'done') {
          output.textContent += '\nDone.\n';
        }
        output.scrollTop = output.scrollHeight;
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') output.textContent += '\nError: ' + err.message;
  } finally {
    tieredRunBtn.disabled = false;
    tieredController = null;
  }
};

bench.onsubmit = async (e) => {
  e.preventDefault();
  if (controller) controller.abort();
  controller = new AbortController();

  const formData = new FormData(bench);
  if (currentTab === 'parallel') formData.set('mode', 'parallel');
  const qs = new URLSearchParams(formData).toString();
  const endpoint = runEndpoints[currentTab] || '/run-profiled';

  output.textContent = '';
  cmdGrid.innerHTML = '';
  heroSection.style.display = 'none';
  tableSection.style.display = 'none';
  progressLine.style.display = 'flex';
  liveTicker.style.display = 'none';
  runBtn.disabled = true;
  stopBtn.disabled = false;

  const cmdOrder = [];
  const cmdState = {};
  const cmdCards = {};
  let buf = '';
  const total = currentTab === 'history' ? 3 : (bench.cmd.value.match(/,/g) || []).length + 1;

  try {
    const resp = await fetch(endpoint + '?' + qs, { signal: controller.signal });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        let evt;
        try { evt = JSON.parse(line); } catch { continue; }

        if (evt.type === 'cmd_start') {
          cmdOrder.push(evt.cmd);
          const color = CMD_COLORS[(cmdOrder.length - 1) % CMD_COLORS.length];
          cmdState[evt.cmd] = { cmd: evt.cmd, memStart: evt.mem_kb, diskStart: evt.disk_kb, memNow: evt.mem_kb, diskNow: evt.disk_kb, samples: [evt], logLines: [], color };
          cmdCards[evt.cmd] = makeCard(evt.cmd, color);
          if (currentTab !== 'parallel') {
            progressText.textContent = `Running ${evt.cmd.toUpperCase()} (${cmdOrder.length} / ${total})`;
          } else {
            progressText.textContent = `Running ${cmdOrder.length} / ${total} commands in parallel...`;
          }
          renderCard(cmdCards[evt.cmd], cmdState[evt.cmd], color);
        } else if (evt.type === 'sample') {
          const st = cmdState[evt.cmd];
          if (st) {
            st.memNow = evt.mem_kb;
            st.diskNow = evt.disk_kb;
            st.samples.push(evt);
            renderCard(cmdCards[evt.cmd], st, st.color);
          }
        } else if (evt.type === 'progress') {
          // Ephemeral \r-updating line redis-benchmark prints while a
          // command is in flight (its own live rps/avg ticker) — shown,
          // not stored, so it doesn't pollute the parsed summary later.
          const live = `${evt.cmd.toUpperCase()}: ${evt.line}`;
          liveTicker.style.display = 'inline-block';
          liveTicker.textContent = live;
          const card = cmdCards[evt.cmd];
          if (card) card.querySelector('[data-role="live"]').textContent = evt.line;
        } else if (evt.type === 'log') {
          const st = cmdState[evt.cmd];
          if (st) {
            st.logLines.push(evt.line);
            renderCard(cmdCards[evt.cmd], st, st.color);
          }
          output.textContent += evt.line + '\n';
          output.scrollTop = output.scrollHeight;
        } else if (evt.type === 'cmd_done') {
          const st = cmdState[evt.cmd];
          if (st) {
            st.memNow = evt.mem_kb;
            st.diskNow = evt.disk_kb;
            st.samples.push(evt);
            renderCard(cmdCards[evt.cmd], st, st.color);
            finalizeCard(cmdCards[evt.cmd], st);
          }
        } else if (evt.type === 'error') {
          output.textContent += `\n[error] ${evt.message}\n`;
        } else if (evt.type === 'done') {
          progressText.textContent = 'Done';
          liveTicker.style.display = 'none';
        }
      }
    }
    renderSummaryAndTable(cmdOrder, cmdState);
  } catch (err) {
    if (err.name !== 'AbortError') output.textContent += '\nError: ' + err.message;
  } finally {
    runBtn.disabled = false;
    stopBtn.disabled = true;
    progressLine.style.display = 'none';
    controller = null;
  }
};

stopBtn.onclick = () => { if (controller) { controller.abort(); } };
ramLimitGB.onchange = diskLimitGB.onchange = () => { if (statsArea.style.display !== 'none') refreshStats.click(); };
</script>
</body>
</html>"###;

fn percent_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'+' {
            out.push(' ');
            i += 1;
        } else if b == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("00");
            if let Ok(v) = u8::from_str_radix(hex, 16) {
                out.push(v as char);
            }
            i += 3;
        } else {
            out.push(b as char);
            i += 1;
        }
    }
    out
}

fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(percent_decode(k), percent_decode(v));
        } else if !pair.is_empty() {
            map.insert(percent_decode(pair), String::new());
        }
    }
    map
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {}
            c => out.push(c),
        }
    }
    out
}

async fn get_disk_kb() -> std::io::Result<(u64, u64)> {
    let out = Command::new("df").arg("-k").arg(".").output().await?;
    let text = String::from_utf8_lossy(&out.stdout);
    let mut lines = text.lines();
    let _header = lines.next();
    if let Some(line) = lines.next() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4 {
            let total = parts[1].parse::<u64>().ok();
            let used = parts[2].parse::<u64>().ok();
            if let (Some(t), Some(u)) = (total, used) {
                return Ok((t, u));
            }
        }
    }
    Err(std::io::Error::other("could not parse df output"))
}

/// Size, in KB, of a specific directory (e.g. the server's `dir`), so disk
/// deltas reflect data actually written by the benchmark, not whole-disk
/// noise from unrelated processes.
async fn get_dir_disk_kb(dir: &str) -> u64 {
    match Command::new("du").arg("-sk").arg(dir).output().await {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

#[cfg(target_os = "linux")]
async fn get_memory_kb() -> std::io::Result<(u64, u64)> {
    let out = Command::new("free").arg("-k").output().await?;
    let text = String::from_utf8_lossy(&out.stdout);
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("Mem:") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() >= 3 {
                if let (Some(t), Some(u)) = (parts[0].parse::<u64>().ok(), parts[1].parse::<u64>().ok()) {
                    return Ok((t, u));
                }
            }
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::Other, "could not parse free output"))
}

#[cfg(target_os = "macos")]
async fn get_memory_kb() -> std::io::Result<(u64, u64)> {
    let sys = Command::new("sysctl").arg("-n").arg("hw.memsize").output().await?;
    let total_bytes = String::from_utf8_lossy(&sys.stdout)
        .trim()
        .parse::<u64>()
        .map_err(|_| std::io::Error::other("could not parse hw.memsize"))?;
    let total_kb = total_bytes / 1024;

    let vm = Command::new("vm_stat").output().await?;
    let text = String::from_utf8_lossy(&vm.stdout);

    let mut page_size: u64 = 16384;
    let mut free_pages: u64 = 0;
    let mut inactive_pages: u64 = 0;
    let mut speculative_pages: u64 = 0;

    for line in text.lines() {
        if line.contains("page size of") {
            if let Some(num) = line
                .split("page size of ")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .and_then(|n| n.parse::<u64>().ok())
            {
                page_size = num;
            }
        } else if let Some(rest) = line.strip_prefix("Pages free:") {
            free_pages = rest.trim().trim_end_matches('.').replace(",", "").parse::<u64>().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("Pages inactive:") {
            inactive_pages = rest.trim().trim_end_matches('.').replace(",", "").parse::<u64>().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("Pages speculative:") {
            speculative_pages =
                rest.trim().trim_end_matches('.').replace(",", "").parse::<u64>().unwrap_or(0);
        }
    }

    let available_kb = (free_pages + inactive_pages + speculative_pages) * page_size / 1024;
    let used_kb = total_kb.saturating_sub(available_kb);
    Ok((total_kb, used_kb))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
async fn get_memory_kb() -> std::io::Result<(u64, u64)> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, "memory stats not supported on this OS"))
}

/// Finds a RESP bulk-string reply (`$<len>\r\n<data>\r\n`) at the start of
/// `buf`, if the whole reply has arrived yet. Good enough for this tool's
/// own `INFO` round-trip to reflexdb — not a general RESP parser.
fn try_parse_bulk(buf: &[u8]) -> Option<Vec<u8>> {
    if buf.first() != Some(&b'$') {
        return None;
    }
    let crlf = buf.windows(2).position(|w| w == b"\r\n")?;
    let len: i64 = std::str::from_utf8(&buf[1..crlf]).ok()?.trim().parse().ok()?;
    if len < 0 {
        return Some(vec![]);
    }
    let data_start = crlf + 2;
    let data_end = data_start + len as usize;
    if buf.len() < data_end + 2 {
        return None;
    }
    Some(buf[data_start..data_end].to_vec())
}

/// Queries the reflexdb server's own `used_memory` (from `INFO`) over a
/// fresh connection — more meaningful than host-wide RAM when watching how
/// a single server process behaves under a workload.
async fn fetch_used_memory_kb(host: &str, port: u16) -> std::io::Result<u64> {
    let mut stream =
        tokio::time::timeout(Duration::from_millis(800), TcpStream::connect((host, port))).await??;
    stream.write_all(b"*1\r\n$4\r\nINFO\r\n").await?;

    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(Duration::from_millis(800), stream.read(&mut tmp)).await??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(payload) = try_parse_bulk(&buf) {
            let text = String::from_utf8_lossy(&payload);
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix("used_memory:") {
                    if let Ok(v) = rest.trim().parse::<u64>() {
                        return Ok(v / 1024);
                    }
                }
            }
            break;
        }
    }
    Err(std::io::Error::other("used_memory not found in INFO reply"))
}

/// Sends a harmless `GETAT` and checks whether the server replies with the
/// "history is disabled" error — the only way to tell short of a `CONFIG
/// GET`, which reflexdb doesn't implement.
async fn probe_history_enabled(host: &str, port: u16) -> bool {
    let probe = async {
        let mut stream =
            tokio::time::timeout(Duration::from_millis(800), TcpStream::connect((host, port))).await??;
        let key = "__reflexdb_bench_ui_probe__";
        let cmd = format!("*3\r\n$5\r\nGETAT\r\n${}\r\n{}\r\n$1\r\n0\r\n", key.len(), key);
        stream.write_all(cmd.as_bytes()).await?;

        let mut buf = Vec::new();
        let mut tmp = [0u8; 512];
        loop {
            let n = tokio::time::timeout(Duration::from_millis(800), stream.read(&mut tmp)).await??;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.contains(&b'\n') {
                break;
            }
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    };

    match probe.await {
        Ok(buf) => !String::from_utf8_lossy(&buf).contains("history is disabled"),
        Err(_) => false,
    }
}

async fn handle_root(socket: &mut TcpStream) -> std::io::Result<()> {
    let body = HTML.as_bytes();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(body).await?;
    Ok(())
}

async fn handle_logo(socket: &mut TcpStream) -> std::io::Result<()> {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nCache-Control: max-age=31536000, immutable\r\nContent-Length: {}\r\n\r\n",
        LOGO_PNG.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(LOGO_PNG).await?;
    Ok(())
}

async fn handle_stats(socket: &mut TcpStream) -> std::io::Result<()> {
    let (ram_total_kb, ram_used_kb) = get_memory_kb().await.unwrap_or((0, 0));
    let (disk_total_kb, disk_used_kb) = get_disk_kb().await.unwrap_or((0, 0));

    let body = format!(
        "ram_total_kb: {}\nram_used_kb: {}\ndisk_total_kb: {}\ndisk_used_kb: {}\n",
        ram_total_kb, ram_used_kb, disk_total_kb, disk_used_kb
    );

    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    Ok(())
}

async fn handle_history_status(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);

    let enabled = probe_history_enabled(&host, port).await;
    let body = format!("{{\"enabled\":{}}}", enabled);
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    Ok(())
}

/// Converts a parsed RESP `Reply` into a JSON *value* (not an event
/// wrapper) — recurses for `Array`, matching the general shape of any
/// reply, not just the ones this file happens to issue today.
fn reply_to_json_value(r: &Reply) -> String {
    match r {
        Reply::Simple(s) => format!("\"{}\"", json_escape(s)),
        Reply::Error(e) => format!("{{\"error\":\"{}\"}}", json_escape(e)),
        Reply::Integer(i) => i.to_string(),
        Reply::Bulk(None) | Reply::Array(None) => "null".to_string(),
        Reply::Bulk(Some(b)) => format!("\"{}\"", json_escape(&String::from_utf8_lossy(b))),
        Reply::Array(Some(items)) => {
            let parts: Vec<String> = items.iter().map(reply_to_json_value).collect();
            format!("[{}]", parts.join(","))
        }
    }
}

/// A `LIVE`/push reply is the flat array `["live", "fingerprint", <hex>,
/// "result", <value>]` — pull `fingerprint`/`result` out of those label/value
/// pairs instead of shipping the raw positional array to the UI.
fn parse_live_reply(r: &Reply) -> (String, String) {
    let mut fingerprint = String::new();
    let mut result = "null".to_string();
    if let Reply::Array(Some(items)) = r {
        let mut i = 1; // skip the leading "live" tag
        while i + 1 < items.len() {
            if let Reply::Bulk(Some(label)) = &items[i] {
                match label.as_slice() {
                    b"fingerprint" => {
                        if let Reply::Bulk(Some(hex)) = &items[i + 1] {
                            fingerprint = String::from_utf8_lossy(hex).into_owned();
                        }
                    }
                    b"result" => result = reply_to_json_value(&items[i + 1]),
                    _ => {}
                }
            }
            i += 2;
        }
    }
    (fingerprint, result)
}

/// Subscribes to `LIVE GET <key>` for every key in `keys` (comma-separated)
/// on the target server, all over one connection, and streams every pushed
/// update from any of them (ndjson, chunked — same wire shape as
/// `/run-profiled`) until the browser disconnects. A 2s idle timeout
/// between reads doubles as a heartbeat: without it, idle keys with no
/// writes would never notice the client went away, since nothing would
/// ever reach the `write_chunk` call that'd surface the broken pipe.
async fn handle_live_watch(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let keys: Vec<String> = params
        .get("keys")
        .map(|s| s.split(',').map(|k| k.trim().to_string()).filter(|k| !k.is_empty()).collect())
        .unwrap_or_default();
    let keys = if keys.is_empty() { vec!["live:demo".to_string()] } else { keys };

    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    let mut client = match RespClient::connect(&host, port).await {
        Ok(c) => c,
        Err(e) => {
            let _ = write_chunk(
                socket,
                &format!("{{\"type\":\"error\",\"message\":\"{}\"}}\n", json_escape(&e.to_string())),
            )
            .await;
            socket.write_all(b"0\r\n\r\n").await?;
            return Ok(());
        }
    };

    let t0 = Instant::now();
    // `fingerprint`s are unique per distinct `LIVE GET <key>` (CRC-64 of
    // the inner command), so this is enough to label every later push with
    // which row/key it belongs to, even though the push itself only
    // carries its fingerprint, not the key.
    let mut fingerprint_to_key: HashMap<String, String> = HashMap::new();
    for key in &keys {
        let subscribed = match client.call(&["LIVE", "GET", key]).await {
            Ok(r) => r,
            Err(e) => {
                let _ = write_chunk(
                    socket,
                    &format!(
                        "{{\"type\":\"error\",\"message\":\"subscribing to {}: {}\"}}\n",
                        json_escape(key),
                        json_escape(&e.to_string())
                    ),
                )
                .await;
                continue;
            }
        };
        let (fingerprint, result) = parse_live_reply(&subscribed);
        fingerprint_to_key.insert(fingerprint.clone(), key.clone());
        let line = format!(
            "{{\"type\":\"update\",\"event\":\"subscribed\",\"t_ms\":0,\"key\":\"{}\",\"fingerprint\":\"{}\",\"result\":{}}}\n",
            json_escape(key),
            json_escape(&fingerprint),
            result
        );
        if write_chunk(socket, &line).await.is_err() {
            return Ok(());
        }
    }

    loop {
        match tokio::time::timeout(Duration::from_secs(2), client.read_next()).await {
            Ok(Ok(reply)) => {
                let (fingerprint, result) = parse_live_reply(&reply);
                let key = fingerprint_to_key.get(&fingerprint).cloned().unwrap_or_default();
                let line = format!(
                    "{{\"type\":\"update\",\"event\":\"push\",\"t_ms\":{},\"key\":\"{}\",\"fingerprint\":\"{}\",\"result\":{}}}\n",
                    t0.elapsed().as_millis(),
                    json_escape(&key),
                    json_escape(&fingerprint),
                    result
                );
                if write_chunk(socket, &line).await.is_err() {
                    break;
                }
            }
            Ok(Err(_)) => break, // connection to the target server died
            Err(_) => {
                // Idle timeout tick: no push happened, just confirm the
                // browser's still there so a dead client doesn't leak these
                // LIVE subscriptions forever.
                let line = format!("{{\"type\":\"heartbeat\",\"t_ms\":{}}}\n", t0.elapsed().as_millis());
                if write_chunk(socket, &line).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
    Ok(())
}

/// One-shot `SET <key> <value>` against the target server — the "other
/// connection" that triggers a push to whatever's watching via
/// `/live-watch`, issued from the browser's "Set value" button instead of a
/// second terminal.
async fn handle_live_set(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let key = params.get("key").cloned().unwrap_or_default();
    let value = params.get("value").cloned().unwrap_or_default();

    let result = async {
        let mut client = RespClient::connect(&host, port).await?;
        client.call(&["SET", &key, &value]).await
    }
    .await;

    let body = match result {
        Ok(Reply::Error(e)) => format!("{{\"ok\":false,\"error\":\"{}\"}}", json_escape(&e)),
        Ok(other) => format!("{{\"ok\":true,\"reply\":{}}}", reply_to_json_value(&other)),
        Err(e) => format!("{{\"ok\":false,\"error\":\"{}\"}}", json_escape(&e.to_string())),
    };
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    Ok(())
}

/// Generates an `n`-byte random alphanumeric value — used by
/// `/live-autoset` so repeated ticks actually produce a visibly different
/// pushed result each time, not just the same string over and over.
fn random_value(n: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..n).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect()
}

/// Repeatedly `SET`s every key in `keys` to a fresh random value on
/// `interval_ms`, for `duration_ms` total, streaming a `tick` event (ndjson,
/// chunked) per `SET` — the "auto-change" button's backend, so the already
/// running `/live-watch` stream picks up the resulting pushes the same way
/// it would for a manual `SET` from another terminal.
async fn handle_live_autoset(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let keys: Vec<String> = params
        .get("keys")
        .map(|s| s.split(',').map(|k| k.trim().to_string()).filter(|k| !k.is_empty()).collect())
        .unwrap_or_default();
    let duration_ms =
        params.get("duration_ms").and_then(|s| s.parse::<u64>().ok()).unwrap_or(10_000).clamp(100, 600_000);
    let interval_ms =
        params.get("interval_ms").and_then(|s| s.parse::<u64>().ok()).unwrap_or(1_000).clamp(50, 60_000);

    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    if keys.is_empty() {
        let _ = write_chunk(socket, "{\"type\":\"error\",\"message\":\"no keys given\"}\n").await;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    let mut client = match RespClient::connect(&host, port).await {
        Ok(c) => c,
        Err(e) => {
            let _ = write_chunk(
                socket,
                &format!("{{\"type\":\"error\",\"message\":\"{}\"}}\n", json_escape(&e.to_string())),
            )
            .await;
            socket.write_all(b"0\r\n\r\n").await?;
            return Ok(());
        }
    };

    let deadline = Instant::now() + Duration::from_millis(duration_ms);
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
    tick.tick().await; // first tick fires immediately
    while Instant::now() < deadline {
        for key in &keys {
            let value = random_value(8);
            if client.call(&["SET", key, &value]).await.is_err() {
                let _ = write_chunk(
                    socket,
                    &format!("{{\"type\":\"error\",\"message\":\"SET {} failed\"}}\n", json_escape(key)),
                )
                .await;
                socket.write_all(b"0\r\n\r\n").await?;
                return Ok(());
            }
            let line = format!(
                "{{\"type\":\"tick\",\"key\":\"{}\",\"value\":\"{}\"}}\n",
                json_escape(key),
                json_escape(&value)
            );
            if write_chunk(socket, &line).await.is_err() {
                return Ok(());
            }
        }
        tick.tick().await;
    }
    let _ = write_chunk(socket, "{\"type\":\"done\"}\n").await;
    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// Writes `count` real keys of `size` bytes each to the target server, then
/// reports `DBSIZE` (what's left in RAM) against the tiered directory's
/// on-disk size, and times a `GET`/`EXISTS` against early keys to
/// demonstrate promotion-on-read and `EXISTS` staying truthful without one.
/// Only actually shows eviction if the target server was started with
/// `tiered-storage yes` and a `tiered-memory-limit` small enough for this
/// workload to exceed — this issues real commands, it doesn't simulate.
async fn handle_tiered_demo(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let dir = params
        .get("dir")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "tiered".to_string());
    let count = params.get("count").and_then(|s| s.parse::<usize>().ok()).unwrap_or(2000).clamp(1, 1_000_000);
    let size = params.get("size").and_then(|s| s.parse::<usize>().ok()).unwrap_or(2000).clamp(1, 10_000_000);
    let prefix = "tiered:demo:";

    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    let mut client = match RespClient::connect(&host, port).await {
        Ok(c) => c,
        Err(e) => {
            let _ = write_chunk(
                socket,
                &format!("{{\"type\":\"error\",\"message\":\"{}\"}}\n", json_escape(&e.to_string())),
            )
            .await;
            socket.write_all(b"0\r\n\r\n").await?;
            return Ok(());
        }
    };

    let value = "x".repeat(size);
    for i in 0..count {
        let key = format!("{prefix}{i}");
        if client.call(&["SET", &key, &value]).await.is_err() {
            let _ = write_chunk(socket, "{\"type\":\"error\",\"message\":\"SET failed mid-run\"}\n").await;
            socket.write_all(b"0\r\n\r\n").await?;
            return Ok(());
        }
        if i % 50 == 0 || i + 1 == count {
            let line = format!("{{\"type\":\"progress\",\"written\":{},\"total\":{}}}\n", i + 1, count);
            if write_chunk(socket, &line).await.is_err() {
                return Ok(());
            }
        }
    }

    // The background eviction tick (100ms interval, capped batch size —
    // see `ServerContext::run_eviction_tick`) runs concurrently with the
    // writes above, not synchronously on every `SET`; give it a moment to
    // actually catch up on a big burst before reading `DBSIZE`, or this
    // would often read a stale "nothing evicted yet" count.
    if write_chunk(socket, "{\"type\":\"settling\"}\n").await.is_err() {
        return Ok(());
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let dbsize = match client.call(&["DBSIZE"]).await {
        Ok(Reply::Integer(n)) => n,
        _ => -1,
    };
    let disk_kb = get_dir_disk_kb(&dir).await;
    let line = format!(
        "{{\"type\":\"summary\",\"written\":{count},\"dbsize\":{dbsize},\"tiered_disk_kb\":{disk_kb}}}\n"
    );
    if write_chunk(socket, &line).await.is_err() {
        return Ok(());
    }

    // The very first key written is the one most likely to have already
    // been evicted by the time `count` more keys have gone in — GET-ing it
    // exercises the promote-on-read path (or just confirms it's still
    // in RAM, if the server isn't tiered or the limit wasn't exceeded).
    let first_key = format!("{prefix}0");
    let t0 = Instant::now();
    let got = client.call(&["GET", &first_key]).await;
    let get_ms = t0.elapsed().as_millis();
    let (found, preview) = match &got {
        Ok(Reply::Bulk(Some(b))) => (true, String::from_utf8_lossy(&b[..b.len().min(40)]).into_owned()),
        _ => (false, String::new()),
    };
    let line = format!(
        "{{\"type\":\"promote_check\",\"key\":\"{}\",\"found\":{},\"ms\":{},\"preview\":\"{}\"}}\n",
        json_escape(&first_key),
        found,
        get_ms,
        json_escape(&preview)
    );
    if write_chunk(socket, &line).await.is_err() {
        return Ok(());
    }

    // `EXISTS` on a later key — answers truthfully for tiered keys without
    // needing to actually read/promote from disk.
    let mid_key = format!("{prefix}{}", count / 2);
    let exists = match client.call(&["EXISTS", &mid_key]).await {
        Ok(Reply::Integer(n)) => n,
        _ => -1,
    };
    let line = format!(
        "{{\"type\":\"exists_check\",\"key\":\"{}\",\"exists\":{}}}\n",
        json_escape(&mid_key),
        exists
    );
    let _ = write_chunk(socket, &line).await;

    let _ = write_chunk(socket, "{\"type\":\"done\"}\n").await;
    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

async fn write_chunk(socket: &mut TcpStream, data: &str) -> std::io::Result<()> {
    let size = format!("{:x}\r\n", data.len());
    socket.write_all(size.as_bytes()).await?;
    socket.write_all(data.as_bytes()).await?;
    socket.write_all(b"\r\n").await?;
    Ok(())
}

struct RunParams {
    host: String,
    port: u16,
    dir: String,
    commands: Vec<String>,
    n: usize,
    c: usize,
    p: usize,
    d: usize,
    r: usize,
    parallel: bool,
}

fn parse_run_params(query: Option<&str>) -> RunParams {
    let params = query.map(parse_query).unwrap_or_default();

    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let dir = params
        .get("dir")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "data".to_string());
    let cmd = params
        .get("cmd")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_500,mset,xadd".to_string());
    let n = params.get("n").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1_000_000);
    let c = params.get("c").and_then(|s| s.parse::<usize>().ok()).unwrap_or(50);
    let p = params.get("p").and_then(|s| s.parse::<usize>().ok()).unwrap_or(16);
    let d = params.get("d").and_then(|s| s.parse::<usize>().ok()).unwrap_or(20);
    let r = params.get("r").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1_000_000);
    let parallel = params.get("mode").map(|m| m == "parallel").unwrap_or(false);

    let commands: Vec<String> = cmd
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .take(30) // cap how many single-command runs one request can trigger
        .collect();

    RunParams {
        host,
        port,
        dir,
        commands,
        n: n.clamp(1, 100_000_000),
        c: c.clamp(1, 10_000),
        p: p.clamp(1, 10_000),
        d: d.clamp(1, 1_000_000),
        r: r.clamp(0, 100_000_000),
        parallel,
    }
}

/// Common `redis-benchmark` flags shared by every invocation this tool
/// makes; callers append either `-t <name>` (a built-in test) or a literal
/// command line (e.g. `GETAT key:__rand_int__ <ts>`) after this.
fn base_bench_args(host: &str, port: u16, n: usize, c: usize, p: usize, d: usize, r: usize) -> Vec<String> {
    let mut args = vec![
        "-h".to_string(),
        host.to_string(),
        "-p".to_string(),
        port.to_string(),
        "-n".to_string(),
        n.to_string(),
        "-c".to_string(),
        c.to_string(),
        "-P".to_string(),
        p.to_string(),
        "-d".to_string(),
        d.to_string(),
    ];
    // Without -r, redis-benchmark hammers a single key for SET/GET/INCR/etc,
    // so server memory barely moves — spreading across a keyspace is what
    // makes the per-operation memory/disk graphs meaningful.
    if r > 0 {
        args.push("-r".to_string());
        args.push(r.to_string());
    }
    args
}

/// Reads a child process's stdout byte-by-byte, splitting on `\r` as well as
/// `\n`. `redis-benchmark` prints a live-updating single-line progress
/// ticker using bare `\r` (no `\n`) several times a second while a command
/// runs, then the full multi-line report with real `\n`s at the end — using
/// `BufReader::lines()` (which only splits on `\n`) would buffer all of
/// that live progress invisibly until the final report arrived. The `bool`
/// in each emitted pair is `true` for an ephemeral `\r`-terminated progress
/// line, `false` for a permanent `\n`-terminated report line.
async fn stream_bench_output(
    mut stdout: tokio::process::ChildStdout,
    tx: mpsc::UnboundedSender<(bool, String)>,
) {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match stdout.read(&mut byte).await {
            Ok(0) => break,
            Ok(_) => match byte[0] {
                b'\r' => {
                    if !buf.is_empty() {
                        let s = String::from_utf8_lossy(&buf).trim().to_string();
                        buf.clear();
                        if !s.is_empty() && tx.send((true, s)).is_err() {
                            return;
                        }
                    }
                }
                b'\n' => {
                    let s = String::from_utf8_lossy(&buf).trim_end_matches('\r').to_string();
                    buf.clear();
                    if tx.send((false, s)).is_err() {
                        return;
                    }
                }
                b => buf.push(b),
            },
            Err(_) => break,
        }
    }
    if !buf.is_empty() {
        let s = String::from_utf8_lossy(&buf).trim().to_string();
        let _ = tx.send((false, s));
    }
}

/// Runs one `redis-benchmark` process to completion, streaming
/// `cmd_start`/`progress`/`log`/`sample`/`cmd_done` JSON events (as
/// pre-formatted strings) into `tx`. `label` is what the UI groups/displays
/// this run under — usually the test name (`"set"`), but for a literal
/// command line it's just whatever string the caller wants on the card.
/// Shared by sequential mode (awaited one at a time), parallel mode (many
/// spawned concurrently), and history mode (a fixed 3-step sequence).
async fn run_bench_process(
    label: String,
    bench_bin: String,
    bench_args: Vec<String>,
    host: String,
    port: u16,
    dir: String,
    tx: mpsc::UnboundedSender<String>,
) {
    let t0 = Instant::now();
    let mem0 = fetch_used_memory_kb(&host, port).await.unwrap_or(0);
    let disk0 = get_dir_disk_kb(&dir).await;

    if tx
        .send(format!(
            "{{\"type\":\"cmd_start\",\"cmd\":\"{}\",\"t_ms\":0,\"mem_kb\":{},\"disk_kb\":{}}}\n",
            json_escape(&label),
            mem0,
            disk0
        ))
        .is_err()
    {
        return;
    }

    let spawned = Command::new(&bench_bin).args(&bench_args).stdout(std::process::Stdio::piped()).spawn();
    let mut child = match spawned {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(format!(
                "{{\"type\":\"error\",\"message\":\"failed to spawn {}: {}\"}}\n",
                json_escape(&bench_bin),
                json_escape(&e.to_string())
            ));
            return;
        }
    };

    let stdout = child.stdout.take().expect("piped stdout");
    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<(bool, String)>();
    let reader_task = tokio::spawn(stream_bench_output(stdout, line_tx));

    let mut interval = tokio::time::interval(Duration::from_millis(300));
    interval.tick().await; // first tick fires immediately

    loop {
        tokio::select! {
            maybe = line_rx.recv() => {
                match maybe {
                    Some((is_progress, line)) => {
                        let typ = if is_progress { "progress" } else { "log" };
                        if tx.send(format!(
                            "{{\"type\":\"{}\",\"cmd\":\"{}\",\"line\":\"{}\"}}\n",
                            typ, json_escape(&label), json_escape(&line)
                        )).is_err() {
                            let _ = child.kill().await;
                            return;
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                let mem = fetch_used_memory_kb(&host, port).await.unwrap_or(mem0);
                let disk = get_dir_disk_kb(&dir).await;
                let t_ms = t0.elapsed().as_millis();
                if tx.send(format!(
                    "{{\"type\":\"sample\",\"cmd\":\"{}\",\"t_ms\":{},\"mem_kb\":{},\"disk_kb\":{}}}\n",
                    json_escape(&label), t_ms, mem, disk
                )).is_err() {
                    let _ = child.kill().await;
                    return;
                }
            }
        }
    }

    let _ = reader_task.await;
    let _ = child.wait().await;

    let mem1 = fetch_used_memory_kb(&host, port).await.unwrap_or(mem0);
    let disk1 = get_dir_disk_kb(&dir).await;
    let t_ms = t0.elapsed().as_millis();
    let _ = tx.send(format!(
        "{{\"type\":\"cmd_done\",\"cmd\":\"{}\",\"t_ms\":{},\"mem_kb\":{},\"disk_kb\":{}}}\n",
        json_escape(&label),
        t_ms,
        mem1,
        disk1
    ));
}

/// Drains `rx` and writes every event straight to the client as it arrives,
/// regardless of which producer task(s) are feeding it — this is what lets
/// sequential mode (one producer, awaited step by step) and parallel mode
/// (several producers running concurrently) share the same wire format.
async fn pump_events_to_socket(
    socket: &mut TcpStream,
    mut rx: mpsc::UnboundedReceiver<String>,
) -> std::io::Result<bool> {
    while let Some(line) = rx.recv().await {
        if write_chunk(socket, &line).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Runs `redis-benchmark` against the target server — one command at a time
/// by default, or all commands concurrently when `mode=parallel` — and
/// streams newline-delimited JSON events. Sampling the server's own
/// `used_memory` (via `INFO`) and the data directory's on-disk size (via
/// `du`) every 300ms per command is what lets the UI graph memory/disk
/// consumption *per operation*, not just an aggregate.
async fn handle_run_profiled(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = parse_run_params(query);
    let bench_bin = std::env::var("REDIS_BENCHMARK").unwrap_or_else(|_| "redis-benchmark".to_string());

    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let host = params.host.clone();
    let port = params.port;
    let dir = params.dir.clone();
    let commands = params.commands.clone();
    let (n, c, p, d, r) = (params.n, params.c, params.p, params.d, params.r);
    let parallel = params.parallel;

    let producer = tokio::spawn(async move {
        if parallel {
            let mut handles = Vec::new();
            for cmd in commands {
                let mut args = base_bench_args(&host, port, n, c, p, d, r);
                args.push("-t".to_string());
                args.push(cmd.clone());
                handles.push(tokio::spawn(run_bench_process(
                    cmd,
                    bench_bin.clone(),
                    args,
                    host.clone(),
                    port,
                    dir.clone(),
                    tx.clone(),
                )));
            }
            drop(tx);
            for h in handles {
                let _ = h.await;
            }
        } else {
            for cmd in commands {
                let mut args = base_bench_args(&host, port, n, c, p, d, r);
                args.push("-t".to_string());
                args.push(cmd.clone());
                run_bench_process(cmd, bench_bin.clone(), args, host.clone(), port, dir.clone(), tx.clone())
                    .await;
            }
            drop(tx);
        }
    });

    let ok = pump_events_to_socket(socket, rx).await?;
    if !ok {
        producer.abort();
        return Ok(());
    }
    let _ = producer.await;

    write_chunk(socket, "{\"type\":\"done\"}\n").await?;
    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// History mode: populate a keyspace with `SET`s (which, with
/// `history-enabled yes`, automatically records a version per write), then
/// benchmark `GETAT`/`HISTORY` reads against those same keys. `-t` doesn't
/// support custom command names, so these two steps use redis-benchmark's
/// literal-command-line mode instead (see its `--help` "Benchmark a
/// specific command line" example).
async fn handle_run_history_profiled(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    let dir = params
        .get("dir")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "data".to_string());
    let n = params.get("n").and_then(|s| s.parse::<usize>().ok()).unwrap_or(100_000).clamp(1, 100_000_000);
    let c = params.get("c").and_then(|s| s.parse::<usize>().ok()).unwrap_or(50).clamp(1, 10_000);
    let p = params.get("p").and_then(|s| s.parse::<usize>().ok()).unwrap_or(16).clamp(1, 10_000);
    // History mode always needs a real keyspace — GETAT/HISTORY on a single
    // repeatedly-overwritten key isn't an interesting benchmark.
    let r =
        params.get("r").and_then(|s| s.parse::<usize>().ok()).unwrap_or(100_000).clamp(1, 100_000_000).max(1);

    let bench_bin = std::env::var("REDIS_BENCHMARK").unwrap_or_else(|_| "redis-benchmark".to_string());
    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let host2 = host.clone();
    let dir2 = dir.clone();
    let bench_bin2 = bench_bin.clone();

    let producer = tokio::spawn(async move {
        let mut setup_args = base_bench_args(&host2, port, n, c, p, 20, r);
        setup_args.push("-t".to_string());
        setup_args.push("set".to_string());
        run_bench_process(
            "set (setup writes)".to_string(),
            bench_bin2.clone(),
            setup_args,
            host2.clone(),
            port,
            dir2.clone(),
            tx.clone(),
        )
        .await;

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let mut getat_args = base_bench_args(&host2, port, n, c, p, 20, r);
        getat_args.extend(["GETAT".to_string(), "key:__rand_int__".to_string(), now_ms.to_string()]);
        run_bench_process(
            "getat".to_string(),
            bench_bin2.clone(),
            getat_args,
            host2.clone(),
            port,
            dir2.clone(),
            tx.clone(),
        )
        .await;

        let mut history_args = base_bench_args(&host2, port, n, c, p, 20, r);
        history_args.extend(["HISTORY".to_string(), "key:__rand_int__".to_string()]);
        run_bench_process("history".to_string(), bench_bin2, history_args, host2, port, dir2, tx).await;
    });

    let ok = pump_events_to_socket(socket, rx).await?;
    if !ok {
        producer.abort();
        return Ok(());
    }
    let _ = producer.await;

    write_chunk(socket, "{\"type\":\"done\"}\n").await?;
    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

async fn handle_connection(socket: &mut TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 8192];
    let mut request = Vec::new();

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buf[..n]);
        if request.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if request.len() > 32_768 {
            return Ok(());
        }
    }

    let first_line = request.split(|&b| b == b'\r' || b == b'\n').next().unwrap_or(b"");
    let parts: Vec<&[u8]> = first_line.split(|&b| b == b' ').collect();
    if parts.len() < 2 {
        return Ok(());
    }

    let method = std::str::from_utf8(parts[0]).unwrap_or("");
    let path_query = std::str::from_utf8(parts[1]).unwrap_or("");
    let (path, query) = path_query.split_once('?').map(|(p, q)| (p, Some(q))).unwrap_or((path_query, None));

    match (method, path) {
        ("GET", "/") => handle_root(socket).await,
        ("GET", "/logo.png") => handle_logo(socket).await,
        ("GET", "/run-profiled") => handle_run_profiled(query, socket).await,
        ("GET", "/run-history-profiled") => handle_run_history_profiled(query, socket).await,
        ("GET", "/history-status") => handle_history_status(query, socket).await,
        ("GET", "/live-watch") => handle_live_watch(query, socket).await,
        ("GET", "/live-set") => handle_live_set(query, socket).await,
        ("GET", "/live-autoset") => handle_live_autoset(query, socket).await,
        ("GET", "/tiered-demo") => handle_tiered_demo(query, socket).await,
        ("GET", "/stats") => handle_stats(socket).await,
        _ => {
            socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").await?;
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let bind: SocketAddr = std::env::var("BENCH_UI_BIND")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());

    let listener = TcpListener::bind(bind).await?;
    info!("reflexdb-bench-ui listening on http://{bind}");
    info!("make sure a reflexdb server is running before clicking 'Run'");

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket).await {
                warn!("connection error: {e}");
            }
        });
    }
}
