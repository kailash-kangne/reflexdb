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

const HTML: &str = concat!(
    r###"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/png" href="/logo.png">
<title>ReflexDB Studio</title>
<style>
  * { box-sizing: border-box; }
  :root { --bg: #07070d; --inset: #0c0c14; --panel: #111119; --panel2: #1a1a24; --border: #22222f; --border2: #333345; --navpad: 258px; }
  body { font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: #e8e8ee; max-width: 1300px; margin: 0 auto; padding: 2rem 2rem 2rem var(--navpad); transition: padding .2s ease; }
  body.nav-hidden { --navpad: 2rem; }
  body.nav-hidden #tabRow { transform: translateX(-100%); }
  #navToggle { display: none; position: fixed; left: .7rem; top: .7rem; z-index: 60; background: var(--panel2); border: 1px solid var(--border2); color: #c5c5d0; border-radius: 8px; padding: .4rem .6rem; font-size: .95rem; cursor: pointer; }
  body.nav-hidden #navToggle { display: block; }
  .nav-foot { margin-top: auto; padding: .8rem .3rem .2rem; border-top: 1px solid var(--border); }
  .swatch-row { display: flex; align-items: center; gap: .45rem; margin-bottom: .6rem; }
  .swatch { width: 17px; height: 17px; border-radius: 50%; border: 2px solid var(--border2); cursor: pointer; padding: 0; background: var(--c); }
  .swatch.active { border-color: #a78bfa; }
  .nav-collapse { width: 100%; background: none; border: 1px solid var(--border); color: #8b8b9e; border-radius: 8px; padding: .35rem 0; font-size: .8rem; cursor: pointer; }
  .nav-collapse:hover { color: #fff; border-color: #a78bfa; }
  .brand { display: flex; align-items: center; gap: .9rem; }
  .brand img { width: 52px; height: 52px; filter: drop-shadow(0 0 14px rgba(239, 68, 68, .45)); }
  .brand-tag { align-self: flex-end; padding-bottom: .5rem; font-size: .66rem; color: #55556a; letter-spacing: .16em; text-transform: uppercase; font-weight: 800; }
  #tabRow { position: fixed; left: 0; top: 0; bottom: 0; width: 226px; background: linear-gradient(180deg, var(--panel), var(--inset)); border-right: 1px solid var(--border); padding: 1.2rem .65rem; display: flex; flex-direction: column; gap: .2rem; overflow-y: auto; z-index: 50; transition: transform .2s ease; }
  #tabRow .logo { display: flex; align-items: center; gap: .6rem; margin: 0 .3rem 1.2rem; }
  #tabRow .logo img { width: 38px; height: 38px; filter: drop-shadow(0 0 12px rgba(239,68,68,.5)); }
  #tabRow .logo .lname { font-size: 1.05rem; font-weight: 800; background: linear-gradient(90deg, #a78bfa, #22d3ee); -webkit-background-clip: text; background-clip: text; color: transparent; }
  #tabRow .logo .lsub { font-size: .6rem; color: #55556a; letter-spacing: .14em; text-transform: uppercase; font-weight: 800; }
  #tabRow .nav-group { font-size: .6rem; color: #44445a; letter-spacing: .14em; text-transform: uppercase; font-weight: 800; padding: .95rem .7rem .3rem; }
  h1 { font-size: 2.1rem; margin-bottom: .2rem; background: linear-gradient(90deg, #a78bfa, #22d3ee); -webkit-background-clip: text; background-clip: text; color: transparent; }
  h2 { color: #d4d4de; font-size: 1.25rem; margin: 2rem 0 .9rem; }
  .subtitle { color: #8a8a99; margin-bottom: 1.5rem; }
  form, .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 1.4rem; display: grid; gap: .9rem; grid-template-columns: auto 1fr; align-items: center; }
  label { color: #9a9aa8; font-size: .88rem; }
  input, select { background: var(--inset); color: #fff; border: 1px solid var(--border); border-radius: 9px; padding: .55rem .7rem; font-family: inherit; }
  input:focus, select:focus { outline: 2px solid #a78bfa; border-color: #a78bfa; }
  .frow { grid-column: 1 / -1; display: grid; grid-template-columns: 148px 1fr 118px 1fr; gap: .55rem .9rem; align-items: center; }
  .frow input { width: 100%; max-width: none; }
  .frow input.grow { grid-column: 2 / -1; }
  .actions { grid-column: 1 / -1; display: flex; gap: 1rem; margin-top: .5rem; flex-wrap: wrap; align-items: center; }
  button { background: linear-gradient(90deg, #a78bfa, #22d3ee); color: #0a0a10; border: 0; padding: .7rem 1.5rem; border-radius: 9px; cursor: pointer; font-weight: 700; font-size: .92rem; }
  button:disabled { opacity: .4; cursor: not-allowed; }
  button.secondary { background: var(--panel2); color: #e8e8ee; font-weight: 600; }
  .preset-row { grid-column: 1 / -1; display: flex; gap: .6rem; flex-wrap: wrap; margin-bottom: .2rem; }
  .preset-btn { background: var(--panel2); border: 1px solid var(--border2); color: #c9c9d4; padding: .5rem 1rem; border-radius: 22px; font-size: .82rem; font-weight: 600; cursor: pointer; transition: all .15s; }
  .preset-btn:hover { border-color: #a78bfa; color: #fff; transform: translateY(-1px); }
  .preset-btn.active { background: linear-gradient(90deg, #a78bfa, #22d3ee); border-color: transparent; color: #0a0a10; }

  .tab-row { display: flex; gap: .5rem; margin-bottom: 1rem; border-bottom: 1px solid var(--border); padding-bottom: 0; }
  .tab-btn { display: flex; align-items: center; gap: .55rem; width: 100%; background: none; color: #8b8b9e; border: 0; border-left: 3px solid transparent; padding: .55rem .7rem; font-weight: 600; font-size: .88rem; cursor: pointer; border-radius: 8px; text-align: left; }
  .tab-btn:hover { background: var(--panel2); color: #fff; }
  .tab-btn.active { background: linear-gradient(90deg, rgba(167,139,250,.16), rgba(34,211,238,.07)); border-left-color: #a78bfa; color: #fff; }
  .tab-btn[hidden] { display: none; }
  .tab-badge { display: inline-block; background: #22c55e; color: #06140b; font-size: .62rem; font-weight: 800; padding: .08rem .4rem; border-radius: 8px; margin-left: .4rem; vertical-align: middle; }

  #output { background: var(--inset); border: 1px solid var(--border); border-radius: 14px; padding: 1rem; white-space: pre-wrap; min-height: 100px; max-height: 300px; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .8rem; color: #9a9aa8; }
  .note { background: var(--panel2); border-left: 4px solid #a78bfa; padding: 1rem 1.2rem; border-radius: 10px; margin-bottom: 1.2rem; font-size: .9rem; color: #c5c5d0; line-height: 1.5; }
  .note code { background: var(--inset); padding: .1rem .4rem; border-radius: 5px; }
  .info-icon { display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; background: var(--border); color: #9a9aa8; font-size: .66rem; font-weight: 800; font-style: italic; cursor: pointer; margin-left: .35rem; vertical-align: middle; position: relative; user-select: none; }
  .info-icon:hover, .info-icon.open { background: #a78bfa; color: #fff; }
  .info-tooltip { display: none; position: absolute; top: 140%; left: 0; z-index: 30; width: 270px; background: var(--panel2); border: 1px solid var(--border2); border-radius: 10px; padding: .8rem .9rem; font-size: .78rem; line-height: 1.5; color: #c5c5d0; font-weight: 400; font-style: normal; text-transform: none; letter-spacing: normal; box-shadow: 0 10px 28px rgba(0,0,0,.45); cursor: default; }
  .info-tooltip code { background: var(--inset); padding: .1rem .35rem; border-radius: 4px; }
  .info-icon:hover .info-tooltip, .info-icon.open .info-tooltip { display: block; }
  .topstats { display: flex; gap: 1.4rem; align-items: center; background: var(--inset); border: 1px solid var(--border); border-radius: 10px; padding: .4rem .9rem; margin-bottom: 1rem; font-size: .78rem; color: #9a9aa8; }
  .topstats .ts { display: flex; align-items: center; gap: .45rem; white-space: nowrap; font-variant-numeric: tabular-nums; }
  .topstats .ts #ramText, .topstats .ts #diskText { display: inline-block; min-width: 17ch; }
  .topstats .ts b { color: #55556a; font-size: .68rem; letter-spacing: .08em; }
  .topstats .mini-wrap { display: inline-block; width: 72px; height: 9px; background: var(--border); border-radius: 5px; overflow: hidden; }
  .topstats .mini-wrap .bar-fill { display: block; height: 100%; width: 0; border-radius: 5px; }
  .topstats .ts input { width: 52px; padding: .2rem .35rem; font-size: .76rem; border-radius: 6px; }
  .topstats .ts-btn { margin-left: auto; background: none; border: 1px solid var(--border); color: #9a9aa8; border-radius: 6px; padding: .2rem .55rem; font-size: .72rem; cursor: pointer; }
  .topstats .ts-btn:hover { color: #fff; border-color: #a78bfa; }
  .topstats .ts-btn + .ts-btn { margin-left: 0; }
  .topstats #refreshStats { width: 5.6em; }
  .live-dot { width: 8px; height: 8px; border-radius: 50%; background: #4ade80; box-shadow: 0 0 8px #4ade80; animation: livePulse 1.2s ease-in-out infinite; }
  .live-dot.off { background: #55556a; box-shadow: none; animation: none; }
  @keyframes livePulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: .45; transform: scale(.75); } }
  .topstats.live .mini-wrap .bar-fill { transition: width .5s ease, background .3s ease; }
  .topstats.live .mini-wrap { box-shadow: 0 0 10px rgba(167, 139, 250, .12); }
  .topstats.big { display: grid; grid-template-columns: 1fr 1fr; gap: .7rem 1.4rem; padding: .8rem 1rem; }
  .topstats.big .ts { font-size: .85rem; }
  .topstats.big .ts b { font-size: .76rem; }
  .topstats.big .mini-wrap { flex: 1; width: auto; height: 15px; }
  .topstats.big .ts-warn { grid-column: 1 / -2; }
  .bar-wrap { background: var(--border); border-radius: 7px; height: 16px; overflow: hidden; margin-top: .3rem; }
  .bar-fill { height: 100%; width: 0%; background: linear-gradient(90deg, #a78bfa, #22d3ee); transition: width .4s ease; border-radius: 9px; }
  .bar-fill.warn { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
  .bar-fill.danger { background: linear-gradient(90deg, #ef4444, #f87171); }
  .bar-label { display: flex; justify-content: space-between; font-size: .88rem; color: #9a9aa8; margin-top: .3rem; }

  .hero-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 1rem; margin: 1.2rem 0 .5rem; }
  .hero-card { background: linear-gradient(160deg, var(--panel2), var(--panel)); border: 1px solid var(--border); border-radius: 14px; padding: 1.1rem; text-align: center; }
  .hero-card .k { color: #8a8a99; font-size: .76rem; text-transform: uppercase; letter-spacing: .04em; }
  .hero-card .v { font-size: 1.7rem; font-weight: 800; margin-top: .35rem; }
  .hero-card .v.purple { color: #a78bfa; }
  .hero-card .v.cyan { color: #22d3ee; }
  .hero-card .v.red { color: #f87171; }
  .hero-card .v.green { color: #4ade80; }

  .landing-hero { background: radial-gradient(1200px 300px at 30% -40%, rgba(167,139,250,.18), transparent), linear-gradient(160deg, var(--panel2), var(--panel)); border: 1px solid var(--border); border-radius: 18px; padding: 1.6rem 1.8rem 1.4rem; margin-bottom: 1.2rem; overflow: hidden; }
  .landing-hero .mission { font-size: 2.4rem; font-weight: 900; line-height: 1.15; letter-spacing: -.02em; margin: 0 0 .5rem; background: linear-gradient(90deg, #a78bfa, #22d3ee, #f472b6, #a78bfa); background-size: 300% 100%; -webkit-background-clip: text; background-clip: text; color: transparent; animation: shimmer 6s linear infinite; filter: drop-shadow(0 0 22px rgba(167,139,250,.35)); }
  .landing-hero .mission span { display: inline-block; opacity: 0; animation: wordIn .7s cubic-bezier(.22,1,.36,1) forwards; }
  .landing-hero .mission span:nth-child(2) { animation-delay: .07s; }
  .landing-hero .mission span:nth-child(3) { animation-delay: .14s; }
  .landing-hero .mission span:nth-child(4) { animation-delay: .21s; }
  .landing-hero .mission span:nth-child(5) { animation-delay: .28s; }
  .landing-hero .mission span:nth-child(6) { animation-delay: .35s; }
  .landing-hero .mission span:nth-child(7) { animation-delay: .42s; }
  @keyframes wordIn { from { opacity: 0; transform: translateY(.45em) scale(.94); filter: blur(6px); } to { opacity: 1; transform: none; filter: none; } }
  @keyframes shimmer { to { background-position: 300% 0; } }
  .landing-hero .tagline { color: #8a8a99; font-size: .92rem; margin: 0 0 1.3rem; }
  .hbar-row { display: grid; grid-template-columns: 104px 1fr 96px; align-items: center; gap: .8rem; margin-bottom: .55rem; }
  .hbar-row .engine { font-size: .8rem; font-weight: 700; color: #c9c9d4; text-align: right; }
  .hbar-track { background: var(--inset); border: 1px solid var(--border); border-radius: 8px; height: 18px; overflow: hidden; }
  .hbar-fill { height: 100%; width: 0; border-radius: 8px; transition: width 1.2s cubic-bezier(.22,1,.36,1); }
  .hbar-fill.redis { background: linear-gradient(90deg, #f87171, #fca5a5); }
  .hbar-fill.dragonfly { background: linear-gradient(90deg, #60a5fa, #93c5fd); }
  .hbar-fill.reflexdb { background: linear-gradient(90deg, #a78bfa, #22d3ee); box-shadow: 0 0 14px rgba(167,139,250,.55); }
  .hbar-row .ops { font-family: ui-monospace, 'SF Mono', Menlo, monospace; font-size: .8rem; color: #e8e8f0; text-align: right; }
  .metric-card { display: grid; grid-template-columns: 168px 1fr; gap: 1.4rem; background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 1.1rem; margin-bottom: 1rem; }
  .metric-stat { background: var(--panel2); border-radius: 12px; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; padding: 1rem .8rem; }
  .metric-stat .big { font-size: 2.1rem; font-weight: 900; line-height: 1.05; }
  .metric-stat .cap { color: #8a8a99; font-size: .78rem; margin-top: .4rem; line-height: 1.35; }
  .metric-bars .mname { color: #8a8a99; font-size: .82rem; font-weight: 600; margin: .15rem 0 .8rem; }
  .metric-legend { display: flex; gap: 1.3rem; align-items: center; flex-wrap: wrap; background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: .7rem 1.1rem; margin-bottom: 1.2rem; }
  .metric-legend .src { margin-left: auto; display: grid; gap: .15rem; }
  .metric-legend .src a { color: #8a8a99; font-size: .74rem; text-decoration: underline; }

  .progress-line { display: flex; align-items: center; gap: .7rem; color: #c5c5d0; font-size: .92rem; margin: .8rem 0; flex-wrap: wrap; }
  .spinner { width: 14px; height: 14px; border-radius: 50%; border: 2px solid var(--border); border-top-color: #a78bfa; animation: spin .7s linear infinite; flex-shrink: 0; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .live-ticker { font-family: ui-monospace, Menlo, monospace; font-size: .82rem; color: #4ade80; background: var(--inset); padding: .2rem .6rem; border-radius: 6px; border: 1px solid #1f3a28; }

  .cmd-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 1.1rem; }
  .cmd-card { background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 1.1rem; border-top: 3px solid var(--accent, #a78bfa); transition: transform .15s, border-color .15s; }
  .cmd-card:hover { transform: translateY(-2px); border-color: var(--border2); }
  .latency-panel { display: grid; gap: 1rem; background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 1.25rem; }
  .latency-fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: .8rem; }
  .latency-field { display: grid; gap: .35rem; }
  .latency-field label { color: #9a9aa8; font-size: .82rem; }
  .latency-field input { width: 100%; }
  .scenario-picks { display: flex; gap: .55rem; flex-wrap: wrap; }
  .scenario-pick { display: flex; align-items: center; gap: .45rem; background: var(--inset); border: 1px solid var(--border); padding: .55rem .75rem; border-radius: 9px; color: #c5c5d0; font-size: .84rem; }
  .scenario-pick input { accent-color: #a78bfa; }
  .latency-results { display: grid; gap: 1rem; }
  .latency-card { background: linear-gradient(155deg,var(--panel2),var(--panel)); border: 1px solid var(--border); border-radius: 14px; padding: 1rem 1.15rem; }
  .latency-card h3 { margin: 0 0 .8rem; color: #e8e8ee; font-size: 1rem; }
  .latency-kpis { display: grid; grid-template-columns: repeat(auto-fit,minmax(125px,1fr)); gap: .6rem; }
  .latency-kpi { background:var(--inset); border:1px solid var(--border); border-radius:10px; padding:.7rem; }
  .latency-kpi span { display:block; color:#8a8a99; font-size:.7rem; text-transform:uppercase; letter-spacing:.04em; }
  .latency-kpi strong { display:block; color:#e8e8ee; font-size:1.2rem; margin-top:.25rem; }
  .latency-table-wrap { overflow-x:auto; }
  #latencyTable { min-width:760px; }
  .latency-empty { color:#8a8a99; border:1px dashed var(--border2); padding:1rem; border-radius:10px; text-align:center; }
  .latency-chart-wrap { background:var(--panel); border:1px solid var(--border); border-radius:14px; padding:1rem; overflow-x:auto; }
  #shootoutChart { width:100%; min-width:620px; height:auto; display:block; }
  .chart-legend { display:flex; gap:1rem; flex-wrap:wrap; color:#a8a8b5; font-size:.78rem; margin:.2rem 0 .75rem; }
  .legend-dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:.35rem; }
  #shootoutTable { min-width:760px; }
  .shootout-engine-grid { display:flex; flex-direction:column; gap: 1rem; }
  .shootout-engine-card { background:var(--panel); border:1px solid var(--border); border-radius:14px; padding:1rem; border-top: 3px solid var(--accent, #a78bfa); }
  .shootout-engine-card h3 { margin: 0 0 .6rem; color:#e8e8ee; font-size:1rem; display:flex; align-items:center; gap:.5rem; }
  .shootout-engine-card .engine-status { font-size:.74rem; color:#9a9aa8; font-weight: 400; }
  .shootout-engine-card svg { width:100%; max-width:460px; height:auto; display:block; }
  .shootout-status-row { display:flex; gap:.6rem; flex-wrap:wrap; margin: .75rem 0; }
  .shootout-status-chip { display:flex; align-items:center; gap:.4rem; background:var(--panel); border:1px solid var(--border); border-radius:20px; padding:.3rem .75rem; font-size:.78rem; color:#d4d4de; }
  .shootout-status-chip .legend-dot { margin-right:0; }
  .shootout-status-chip .chip-status { color:#9a9aa8; }
  #shootoutTable tbody tr.workload-group-start td { border-top: 2px solid var(--border2); }
  .cmd-card-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: .5rem; }
  .cmd-name { font-weight: 800; font-size: 1.05rem; letter-spacing: .02em; }
  .cmd-status { font-size: .72rem; font-weight: 700; padding: .2rem .6rem; border-radius: 20px; text-transform: uppercase; letter-spacing: .03em; }
  .cmd-status.running { background: #3f2e0f; color: #fbbf24; }
  .cmd-status.done { background: #14301f; color: #4ade80; }
  .cmd-live { font-family: ui-monospace, Menlo, monospace; font-size: .72rem; color: #67e8a8; margin-bottom: .6rem; min-height: 1em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .cmd-variant { margin-bottom: .6rem; padding-bottom: .5rem; border-bottom: 1px dashed var(--border); }
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
  .live-feed-row { background: var(--panel); border: 1px solid var(--border); border-left: 4px solid #22d3ee; border-radius: 10px; padding: .7rem 1rem; margin-bottom: .6rem; font-size: .88rem; }
  .live-feed-row .lf-time { color: #8a8a99; font-size: .74rem; }
  .live-feed-row .lf-key { color: #a78bfa; font-weight: 700; }
  .live-feed-row .lf-result { font-weight: 700; color: #4ade80; margin-top: .25rem; font-family: ui-monospace, Menlo, monospace; }
  .live-row { display: flex; gap: .5rem; align-items: center; margin-bottom: .5rem; }
  .live-row input { flex: 1; min-width: 0; }
  .live-row button { padding: .4rem .7rem; font-size: .8rem; }

  table { width: 100%; border-collapse: collapse; margin-top: 1rem; font-size: .85rem; }
  th, td { padding: .5rem .65rem; text-align: right; border-bottom: 1px solid var(--border); }
  th:first-child, td:first-child { text-align: left; }
  th { color: #8a8a99; font-weight: 600; font-size: .78rem; text-transform: uppercase; }
  tr:hover td { background: var(--panel2); }
  td.win { font-weight: 800; color: #22d3ee; }
  td.win::before { content: '🏆 '; }
  th.e-redis { color: #f87171; }
  th.e-dragonfly { color: #60a5fa; }
  th.e-reflexdb { color: #22d3ee; }
  .hero-card .d { font-size: .78rem; color: #8a8a99; margin-top: .4rem; line-height: 1.5; }
  code { font-family: ui-monospace, 'SF Mono', Menlo, monospace; background: var(--inset); padding: .1rem .35rem; border-radius: 5px; font-size: .85em; }
</style>
</head>
<body>
<div class="topstats">
  <span class="ts"><b>RAM</b> <span class="mini-wrap"><span id="ramBar"></span></span> <span id="ramText">–</span></span>
  <span class="ts"><b>DISK</b> <span class="mini-wrap"><span id="diskBar"></span></span> <span id="diskText">–</span></span>
  <span class="ts ts-warn">warn above — RAM <input id="ramLimitGB" type="number" value="8" step="0.1" min="0.1"> GB · Disk <input id="diskLimitGB" type="number" value="20" step="0.1" min="0.1"> GB</span>
  <button class="ts-btn" id="statsExpand" type="button" title="Expand">⤢</button>
  <button class="ts-btn" id="refreshStats" type="button"><span class="live-dot off" id="liveDot"></span> <span id="liveLabel">live</span></button>
</div>
<p class="subtitle">Run workloads and demos, then flip to the <b>Studio</b> section to inspect the data they wrote — keys, values, history, LIVE, health, slowlog.</p>

<section id="homePanel">
<div class="landing-hero" id="landingHero">
  <div class="mission"><span>Accelerating</span> <span>the</span> <span>future</span> <span>with</span> <span>a</span> <span>modern</span> <span>cache.</span></div>
  <p class="tagline">Redis-compatible, built in Rust — with time-travel key history and LIVE push queries, not just GET/SET.</p>
  <div class="metric-card">
    <div class="metric-stat"><div class="big" style="color:#a78bfa">8/8</div><div class="cap">head-to-head medians won</div></div>
    <div class="metric-bars">
      <div class="mname">Throughput — pipeline 16, mixed workload, 4 vCPU containers</div>
      <div class="hbar-row"><div class="engine">🏆 ReflexDB</div><div class="hbar-track"><div class="hbar-fill reflexdb" data-w="100"></div></div><div class="ops"><span class="ops-count" data-n="154605">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥈 Redis 7</div><div class="hbar-track"><div class="hbar-fill redis" data-w="99"></div></div><div class="ops"><span class="ops-count" data-n="153239">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥉 Dragonfly</div><div class="hbar-track"><div class="hbar-fill dragonfly" data-w="94"></div></div><div class="ops"><span class="ops-count" data-n="144954">0</span> ops/s</div></div>
    </div>
  </div>
  <div class="metric-card">
    <div class="metric-stat"><div class="big" style="color:#22d3ee">287K</div><div class="cap">peak ops/s — deep pipeline</div></div>
    <div class="metric-bars">
      <div class="mname">Deep pipeline — 16 clients, pipeline 64, mixed workload</div>
      <div class="hbar-row"><div class="engine">🏆 ReflexDB</div><div class="hbar-track"><div class="hbar-fill reflexdb" data-w="100"></div></div><div class="ops"><span class="ops-count" data-n="286638">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥈 Redis 7</div><div class="hbar-track"><div class="hbar-fill redis" data-w="99"></div></div><div class="ops"><span class="ops-count" data-n="282380">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥉 Dragonfly</div><div class="hbar-track"><div class="hbar-fill dragonfly" data-w="94"></div></div><div class="ops"><span class="ops-count" data-n="270308">0</span> ops/s</div></div>
    </div>
  </div>
  <div class="metric-card">
    <div class="metric-stat"><div class="big" style="color:#f472b6">8KiB</div><div class="cap">values — payload-bound workload</div></div>
    <div class="metric-bars">
      <div class="mname">Large values — 8 KiB payloads, pipeline 16</div>
      <div class="hbar-row"><div class="engine">🏆 ReflexDB</div><div class="hbar-track"><div class="hbar-fill reflexdb" data-w="100"></div></div><div class="ops"><span class="ops-count" data-n="50688">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥈 Redis 7</div><div class="hbar-track"><div class="hbar-fill redis" data-w="95"></div></div><div class="ops"><span class="ops-count" data-n="48356">0</span> ops/s</div></div>
      <div class="hbar-row"><div class="engine">🥉 Dragonfly</div><div class="hbar-track"><div class="hbar-fill dragonfly" data-w="92"></div></div><div class="ops"><span class="ops-count" data-n="46633">0</span> ops/s</div></div>
    </div>
  </div>
  <div class="metric-legend">
    <span><span class="legend-dot" style="background:linear-gradient(90deg,#a78bfa,#22d3ee)"></span>ReflexDB</span>
    <span><span class="legend-dot" style="background:#f87171"></span>Redis 7</span>
    <span><span class="legend-dot" style="background:#60a5fa"></span>Dragonfly</span>
    <div class="src"><a href="https://github.com/kailash-kangne/reflexdb/blob/main/docs/BENCHMARKS.md" target="_blank" rel="noopener">Benchmark source &amp; methodology</a><a href="https://github.com/kailash-kangne/reflexdb/blob/main/docs/BENCHMARKS.md" target="_blank" rel="noopener">Full tables (all workloads)</a></div>
  </div>
</div>

<div class="hero-row">
  <div class="hero-card"><div class="k">Drop-in compatible</div><div class="d">Speaks RESP — point <code>redis-cli</code>, <code>redis-benchmark</code>, or any Redis client at it, unchanged.</div></div>
  <div class="hero-card"><div class="k">Time-travel history</div><div class="d"><code>GETAT</code>/<code>HISTORY</code> — read any key as it was at a point in time, per-key version log.</div></div>
  <div class="hero-card"><div class="k">LIVE push queries</div><div class="d"><code>LIVE GET</code> subscribes to a key and pushes every change — no polling.</div></div>
  <div class="hero-card"><div class="k">Tiered storage</div><div class="d">Hot keys in RAM, cold keys spill to disk — datasets bigger than memory.</div></div>
</div>

<div class="panel">
  <h3 style="margin:.2rem 0 0;">Benchmarks — measured, not claimed<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    ops/sec, medians of 3 runs. Capped rows: all three engines in identical containers
    (<code>--cpus</code>/<code>--memory</code>), same workload replayed one engine at a time.
    Machine specs and full methodology in <code>docs/BENCHMARKS.md</code>.
  </span></span></h3>
  <table>
    <thead>
      <tr><th rowspan="2">Workload</th><th rowspan="2">CPUs</th>
        <th colspan="3" style="text-align:center;">Pipeline 1</th>
        <th colspan="3" style="text-align:center;">Pipeline 16</th></tr>
      <tr><th class="e-redis">Redis</th><th class="e-dragonfly">Dragonfly</th><th class="e-reflexdb">ReflexDB</th><th class="e-redis">Redis</th><th class="e-dragonfly">Dragonfly</th><th class="e-reflexdb">ReflexDB</th></tr>
    </thead>
    <tbody>
      <tr><td>small-mixed<span class="info-icon" tabindex="0">i<span class="info-tooltip">Small 8–64B keys, realistic mixed read/write traffic — the everyday cache shape.</span></span></td>
        <td>1</td><td>12,734</td><td>11,264</td><td class="win">17,249</td><td>146,259</td><td>128,202</td><td class="win">149,301</td></tr>
      <tr><td>small-mixed</td>
        <td>4</td><td>14,699</td><td>14,388</td><td class="win">16,200</td><td>153,239</td><td>144,954</td><td class="win">154,605</td></tr>
      <tr><td>hot-key<span class="info-icon" tabindex="0">i<span class="info-tooltip">All traffic hammers a small hot key set — worst case for lock contention.</span></span></td>
        <td>1</td><td>15,494</td><td>12,010</td><td class="win">16,708</td><td>147,983</td><td>142,760</td><td class="win">152,644</td></tr>
      <tr><td>hot-key</td>
        <td>4</td><td>14,925</td><td>13,990</td><td class="win">16,866</td><td>161,254</td><td>153,218</td><td class="win">163,143</td></tr>
    </tbody>
  </table>

  <h3 style="margin:1.6rem 0 0;">Second pass — idle host<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Same engine-comparison rerun on an <em>idle</em> arm64 host (earlier tree, pre-hardening) —
    absolute numbers are higher without background load; placement is what matters.
  </span></span></h3>
  <table>
    <thead>
      <tr><th rowspan="2">Workload</th><th rowspan="2">CPUs</th>
        <th colspan="3" style="text-align:center;">Pipeline 1</th>
        <th colspan="3" style="text-align:center;">Pipeline 16</th></tr>
      <tr><th class="e-redis">Redis</th><th class="e-dragonfly">Dragonfly</th><th class="e-reflexdb">ReflexDB</th><th class="e-redis">Redis</th><th class="e-dragonfly">Dragonfly</th><th class="e-reflexdb">ReflexDB</th></tr>
    </thead>
    <tbody>
      <tr><td>small-mixed</td><td>1</td><td>18,814</td><td>17,007</td><td class="win">21,498</td><td>172,702</td><td>161,951</td><td class="win">187,945</td></tr>
      <tr><td>small-mixed</td><td>4</td><td>18,842</td><td>14,815</td><td class="win">20,574</td><td>174,222</td><td>134,244</td><td class="win">181,563</td></tr>
      <tr><td>hot-key</td><td>1</td><td>19,003</td><td>15,895</td><td class="win">22,366</td><td>180,397</td><td>158,948</td><td class="win">201,045</td></tr>
      <tr><td>hot-key</td><td>4</td><td>18,652</td><td>14,838</td><td class="win">21,773</td><td>175,988</td><td>172,598</td><td class="win">199,243</td></tr>
    </tbody>
  </table>

  <h3 style="margin:1.6rem 0 0;">Aggressive profile<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Stress dimensions the main table doesn't: client count, pipeline depth, keyspace size,
    value size. Same loaded-host setup (<code>--cpus 4</code>, <code>small-mixed</code>, median of 3 × 5s).
  </span></span></h3>
  <table>
    <thead><tr><th>Case</th><th class="e-redis">Redis</th><th class="e-dragonfly">Dragonfly</th><th class="e-reflexdb">ReflexDB</th></tr></thead>
    <tbody>
      <tr><td>64 clients, pipeline 1</td><td>15,244</td><td>16,485</td><td class="win">18,314</td></tr>
      <tr><td>64 clients, pipeline 1, hot-key</td><td>18,146</td><td>18,206</td><td class="win">19,205</td></tr>
      <tr><td>16 clients, pipeline 64<span class="info-icon" tabindex="0">i<span class="info-tooltip">Deep pipelines scale best for every engine — P64 roughly triples the P16 baseline.</span></span></td><td>282,380</td><td>270,308</td><td class="win">286,638</td></tr>
      <tr><td>100k-key keyspace, pipeline 16</td><td>140,996</td><td>147,472</td><td class="win">149,601</td></tr>
      <tr><td>8 KiB values, pipeline 16<span class="info-icon" tabindex="0">i<span class="info-tooltip">Payload/alloc-bound — ~51k ops/s is ~400 MiB/s of stored payload on ReflexDB.</span></span></td><td>48,356</td><td>46,633</td><td class="win">50,688</td></tr>
    </tbody>
  </table>
</div>
</section>

<div id="tabRow">
  <div class="logo"><img src="/logo.png" alt=""><div><div class="lname">ReflexDB</div><div class="lsub">bench &amp; studio</div></div></div>
  <button type="button" class="tab-btn active" data-tab="home">🏠 Home</button>
  <div class="nav-group">Benchmark</div>
  <button type="button" class="tab-btn" data-tab="sequential">⚡ Sequential</button>
  <button type="button" class="tab-btn" data-tab="parallel">🚀 Parallel</button>
  <button type="button" class="tab-btn" data-tab="latency">📈 Latency lab</button>
  <div class="nav-group">Test</div>
  <button type="button" class="tab-btn" data-tab="durability">💥 Durability / crash<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Spawns its own throwaway <code>reflexdb-server</code> (not your running one), writes under load,
    force-kills it (<code>SIGKILL</code>), restarts it against the same data dir, and compares
    acknowledged writes to what AOF replay actually recovered — per <code>appendfsync</code> policy.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="reslimit">🧱 Resource limits<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Runs <code>reflexdb-server</code> in one Docker container with a real, OS-enforced
    <code>--memory</code> cap and a size-capped <code>--tmpfs /data</code> mount applied at the
    same time — not reflexdb's own <code>maxmemory</code> (which degrades gracefully by rejecting
    writes, never crashes). Shows whether the kernel OOM-kills it and/or the disk fills up, and
    whether it keeps responding once it does.
    Needs Docker running and <code>docker build -f Dockerfile.bench -t reflexdb-bench:latest .</code>
    done once beforehand.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="shootout">⚔️ Engine shootout<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Runs Redis, Dragonfly, and ReflexDB each in their own Docker container with an identical
    <code>--memory</code>/<code>--cpus</code> cap, feeds each the exact same workload (one at a
    time, never concurrently — otherwise they'd be competing for the same physical CPU/memory
    bandwidth), and charts throughput/tail latency side by side. Needs Docker running; pulls
    <code>redis:7-alpine</code> and the Dragonfly image automatically the first time, and needs
    <code>docker build -f Dockerfile.bench -t reflexdb-bench:latest .</code> done once for ReflexDB.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="history" hidden id="historyTabBtn">⏪ History bench<span class="tab-badge">on</span><span class="info-icon" tabindex="0">i<span class="info-tooltip">
    <strong>History mode</strong> runs three steps in order: (1) <code>SET</code> writes to populate a keyspace
    (history is recorded automatically on every write once <code>history-enabled yes</code> is set), then
    (2) <code>GETAT</code> point-in-time reads and (3) <code>HISTORY</code> version-log reads against those same keys.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="live">🔴 LIVE demo<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Subscribes to <code>LIVE GET &lt;key&gt;</code> on a running server and shows every pushed
    update as it arrives — no polling. Use the "Set value" button to trigger one.
  </span></span></button>
  <button type="button" class="tab-btn" data-tab="tiered">💾 Tiered demo<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Writes real keys to a running server, then shows how many stayed in RAM (<code>DBSIZE</code>)
    vs. spilled to the on-disk tier, and times a <code>GET</code> that promotes an evicted key
    back. Needs a server started with <code>tiered-storage yes</code> and a small
    <code>tiered-memory-limit</code> to actually see eviction happen.
  </span></span></button>
  <div class="nav-group">Studio</div>
  <button type="button" class="tab-btn" data-tab="studio:keys">🔑 Keys</button>
  <button type="button" class="tab-btn" data-tab="studio:history">🕰️ History</button>
  <button type="button" class="tab-btn" data-tab="studio:live">📡 LIVE viewer</button>
  <button type="button" class="tab-btn" data-tab="studio:health">🩺 Health</button>
  <button type="button" class="tab-btn" data-tab="studio:slowlog">🐢 Slowlog</button>
  <button type="button" class="tab-btn" data-tab="studio:settings">⚙️ Settings</button>
  <button type="button" class="tab-btn" data-tab="studio:commands">⌨️ Commands</button>
  <div class="nav-foot">
    <div class="swatch-row" id="themeSwatches">
      <span class="muted" style="font-size:.66rem;color:#55556a;letter-spacing:.1em;">THEME</span>
      <button type="button" class="swatch" style="--c:#111119" data-theme="default" title="Default"></button>
      <button type="button" class="swatch" style="--c:#0f1729" data-theme="navy" title="Navy"></button>
      <button type="button" class="swatch" style="--c:#171029" data-theme="purple" title="Purple"></button>
      <button type="button" class="swatch" style="--c:#0f1d15" data-theme="forest" title="Forest"></button>
      <button type="button" class="swatch" style="--c:#1c150f" data-theme="ember" title="Ember"></button>
      <button type="button" class="swatch" style="--c:#151515" data-theme="graphite" title="Graphite"></button>
    </div>
    <button type="button" class="nav-collapse" id="navCollapse">« hide sidebar</button>
  </div>
</div>
<button type="button" id="navToggle" title="Show sidebar">☰</button>

<form id="bench" class="panel">
  <div class="preset-row" id="presetRow"></div>
  <div class="frow"><label>Host</label><input name="host" value="127.0.0.1">
    <label>Port</label><input name="port" value="7379"></div>
  <div class="frow"><label>Requests per command</label><input name="n" id="nField" value="200000" type="number">
    <label>Clients</label><input name="c" id="cField" value="50" type="number"></div>
  <div class="frow" data-mode="sequential,parallel"><label>Pipeline</label><input data-mode="sequential,parallel" name="p" id="pField" value="10" type="number">
    <label>Data size (bytes)</label><input data-mode="sequential,parallel" name="d" id="dField" value="20" type="number"></div>
  <div class="frow"><label>Keyspace (-r, 0 = same key)<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    <strong>Keyspace (-r)</strong> spreads writes/reads across many keys instead of hammering one. Without it,
    <code>redis-benchmark</code> reuses a single key over and over, so memory barely moves — set it close to
    "Requests" to actually see memory/disk climb per operation. Presets set it automatically.
  </span></span></label><input name="r" id="rField" value="100000" type="number">
    <label data-mode="history">Data dir</label><input data-mode="history" name="dir" value="data"></div>
  <div class="note" data-mode="history" style="grid-column:1/-1; margin:0;">
    <strong>3 steps, not a command list:</strong> (1) <code>SET</code> writes populate the keyspace —
    with <code>history-enabled yes</code> every write also records a version, so this shows history's
    write overhead; (2) <code>GETAT</code> point-in-time reads over those keys; (3) <code>HISTORY</code>
    version-log reads. Fields below still apply; the command list doesn't (these run as literal
    command lines).
  </div>
  <div class="frow" data-mode="sequential,parallel"><label>Commands</label>
    <input name="cmd" id="cmdField" class="grow" value="ping,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,xadd"></div>
  <div class="actions">
    <button type="submit" id="runBtn">Run benchmark</button>
    <button type="button" id="stopBtn" disabled class="secondary">Stop</button>
  </div>
</form>

<section id="latencyPanel" class="latency-panel" style="display:none;">
  <div class="note" style="margin:0;">
    <strong>Explore the tradeoffs.</strong> Each run connects to your server, measures client-observed latency,
    and reports p50/p99/p999 plus throughput. Use short runs to explore; use repeated longer runs on an idle,
    release-build server before sharing a result.
  </div>
  <div class="latency-fields">
    <div class="latency-field"><label for="latHost">Server host</label><input id="latHost" value="127.0.0.1"></div>
    <div class="latency-field"><label for="latPort">Server port</label><input id="latPort" type="number" value="7379" min="1" max="65535"></div>
    <div class="latency-field"><label for="latSeconds">Seconds per run</label><input id="latSeconds" type="number" value="5" min="1" max="60"></div>
    <div class="latency-field"><label for="latRuns">Repeated runs</label><input id="latRuns" type="number" value="3" min="1" max="10"></div>
    <div class="latency-field"><label for="latClients">Concurrent clients</label><input id="latClients" type="number" value="16" min="1" max="512"></div>
    <div class="latency-field"><label for="latPipeline">Pipeline depths</label><input id="latPipeline" value="1,16" inputmode="numeric"></div>
    <div class="latency-field"><label for="latKeys">Small-key dataset size</label><input id="latKeys" type="number" value="1000" min="1" max="100000"></div>
    <div class="latency-field"><label for="latWatchers">LIVE watchers</label><input id="latWatchers" type="number" value="1000" min="0" max="10000"></div>
    <div class="latency-field"><label for="latLiveDrain">LIVE measurement</label><select id="latLiveDrain"><option value="buffered">Writer latency (buffered clients)</option><option value="selector">End-to-end delivery (selector)</option></select></div>
    <div class="latency-field"><label for="latWire">Wire protocol</label><select id="latWire"><option value="resp">RESP</option><option value="rfx">RFX (binary)</option><option value="resp,rfx">Both — compare</option></select></div>
  </div>
  <div>
    <div class="latency-field" style="margin-bottom:.5rem;"><label>Workloads to compare</label></div>
    <div class="scenario-picks">
      <label class="scenario-pick"><input type="checkbox" name="latWorkload" value="small-mixed" checked> Small-key mixed</label>
      <label class="scenario-pick"><input type="checkbox" name="latWorkload" value="hot-key" checked> Hot-key contention</label>
      <label class="scenario-pick"><input type="checkbox" name="latWorkload" value="live-fanout" checked> LIVE fan-out</label>
    </div>
  </div>
  <div class="actions" style="margin:0;">
    <button type="button" id="latRunBtn">Run comparison</button>
    <button type="button" id="latStopBtn" class="secondary" disabled>Stop</button>
    <span id="latStatus" style="color:#9a9aa8;font-size:.86rem;">Ready. Defaults are editable.</span>
  </div>
  <div id="latencyResults" class="latency-results"><div class="latency-empty">Run a workload to see throughput and tail latency.</div></div>
  <div id="latencyTableWrap" class="latency-table-wrap" style="display:none;">
    <table id="latencyTable"><thead><tr><th>Run</th><th>Workload</th><th>Wire</th><th>Pipeline</th><th>Clients</th><th>Ops/s</th><th>p50</th><th>p99</th><th>p999</th><th>Errors</th></tr></thead><tbody id="latencyBody"></tbody></table>
  </div>
</section>

<section id="durabilityPanel" class="latency-panel" style="display:none;">
  <div class="note" style="margin:0;">
    <strong>This spawns its own server</strong> on the demo port below (default <code>17380</code>,
    not your running server) in a throwaway data dir, so it's safe to run against any machine with
    <code>reflexdb-server</code> built. Each policy gets a clean data dir and is tested one at a time.
  </div>
  <div class="note" style="margin:0; border-left-color:#f59e0b;">
    <strong>What this does and doesn't prove.</strong> The kill here is <code>SIGKILL</code> on the
    server process — it proves AOF replay correctly recovers whatever actually hit disk, but it does
    <em>not</em> simulate a real power loss/OS crash. <code>appendfsync no</code> — the default if
    <code>appendfsync</code> isn't set at all — relies on the OS page cache, which usually survives a
    killed process but would not survive a hard power cut — a clean (0 lost) result for <code>no</code>
    here is expected and isn't evidence it's as safe as <code>always</code>/<code>everysec</code>
    against a real outage. If you need a bounded worst case, set <code>appendfsync everysec</code> or
    <code>always</code> explicitly — don't rely on the default for that.<br><br>
    Also: unlike Redis, <code>appendfsync always</code> here does <strong>not</strong> block the
    client's reply until the write is fsync'd — every policy is fire-and-forget to a background
    writer thread (see <code>crates/persistence/src/aof.rs</code>); <code>always</code> only fsyncs
    after every batch. A small, non-zero <code>lost</code> count for <code>always</code> on a
    process-kill test is therefore expected, not a bug — don't present it to a client as
    "zero data loss," and consider <code>WAITAOF</code> for callers that need a real durability ack.
  </div>
  <div class="latency-fields">
    <div class="latency-field"><label for="durHost">Demo server host</label><input id="durHost" value="127.0.0.1"></div>
    <div class="latency-field"><label for="durPort">Demo server port</label><input id="durPort" type="number" value="17380" min="1" max="65535"></div>
    <div class="latency-field"><label for="durSeconds">Seconds of writes before the kill</label><input id="durSeconds" type="number" value="4" min="1" max="30"></div>
  </div>
  <div>
    <div class="latency-field" style="margin-bottom:.5rem;"><label>appendfsync policies to compare</label></div>
    <div class="scenario-picks">
      <label class="scenario-pick"><input type="checkbox" name="durPolicy" value="always" checked> always</label>
      <label class="scenario-pick"><input type="checkbox" name="durPolicy" value="everysec" checked> everysec</label>
      <label class="scenario-pick"><input type="checkbox" name="durPolicy" value="no" checked> no</label>
    </div>
  </div>
  <div class="actions" style="margin:0;">
    <button type="button" id="durRunBtn">Run crash-recovery test</button>
    <button type="button" id="durStopBtn" class="secondary" disabled>Stop</button>
    <span id="durStatus" style="color:#9a9aa8;font-size:.86rem;">Ready. Each policy runs sequentially — this takes a while.</span>
  </div>
  <div id="durabilityResults" class="latency-results"><div class="latency-empty">Run the test to see acknowledged vs. recovered writes per policy.</div></div>
</section>

<section id="reslimitPanel" class="latency-panel" style="display:none;">
  <div class="note" style="margin:0;">
    <strong>Runs in Docker</strong>, not as a plain process — that's the only reliable
    cross-platform way to get a real, OS-enforced RAM/disk ceiling (macOS has no cgroups).
    Build the image once: <code>docker build -f Dockerfile.bench -t reflexdb-bench:latest .</code>
  </div>
  <div class="latency-fields">
    <div class="latency-field"><label for="resImage">Docker image</label><input id="resImage" value="reflexdb-bench:latest"></div>
    <div class="latency-field"><label for="resPort">Demo server port</label><input id="resPort" type="number" value="17385" min="1" max="65535"></div>
    <div class="latency-field"><label for="resRamGB">RAM limit (GB)</label><input id="resRamGB" type="number" value="1" step="0.1" min="0.1"></div>
    <div class="latency-field"><label for="resDiskGB">Disk limit (GB)</label><input id="resDiskGB" type="number" value="0.25" step="0.05" min="0.05"></div>
    <div class="latency-field"><label for="resValueBytes">Value size (bytes)</label><input id="resValueBytes" type="number" value="4096" min="64" max="65536"></div>
    <div class="latency-field"><label for="resMaxSeconds">Safety cap (seconds)</label><input id="resMaxSeconds" type="number" value="90" min="5" max="300"></div>
  </div>
  <div class="note" style="margin:0;">
    Both limits are applied to one container at once, so whichever fills up first is what
    actually crashes it — the result reports which (RAM, disk, or both).
  </div>
  <div class="actions" style="margin:0;">
    <button type="button" id="resRunBtn">Run resource-limit test</button>
    <button type="button" id="resStopBtn" class="secondary" disabled>Stop</button>
    <span id="resStatus" style="color:#9a9aa8;font-size:.86rem;">Ready. Needs Docker running and the image built.</span>
  </div>
  <div id="reslimitResults" class="latency-results"><div class="latency-empty">Run the test to see crash behavior under a real hard limit.</div></div>
</section>

<section id="shootoutPanel" class="latency-panel" style="display:none;">
  <div class="note" style="margin:0;">
    <strong>Quick side-by-side, not a lab-grade isolated benchmark.</strong> All three engines run
    <em>at the same time</em>, each in its own container, each with the same <code>--memory</code>/
    <code>--cpus</code> cap, on its own port (<code>port</code>, <code>port+1</code>, <code>port+2</code>
    below). Running concurrently means they share the host's real CPU/memory bandwidth while all three
    are active — each container's limit still caps what <em>it</em> can use, but "nothing else was
    running" is no longer true. Persistence is off on all three. This keeps running even if you switch
    to another tab; use Stop to actually cancel it.
  </div>
  <div class="note" style="margin:0; border-left-color:#f59e0b;">
    <strong>⚠️ Requires Docker — run this locally.</strong> Every engine is launched as a real
    container by the bench-UI <em>host</em>, so Docker must be installed and running wherever this
    UI server runs, plus a one-time <code>docker build -f Dockerfile.bench -t reflexdb-bench:latest .</code>
    (the Redis and Dragonfly images pull automatically on first run). It's disabled in public
    sandbox mode — <code>/run-*</code> endpoints are blocked there — and on a remote host without
    Docker the run just fails.
  </div>
  <div class="latency-fields">
    <div class="latency-field"><label for="sMemMB">Memory limit (MB, each engine)</label><input id="sMemMB" type="number" value="512" min="64" max="65536"></div>
    <div class="latency-field"><label for="sCpus">CPU limit (cores, each engine)</label><input id="sCpus" type="number" value="1" step="0.1" min="0.1" max="64"></div>
    <div class="latency-field"><label for="sPort">Base port (each engine gets port, port+1, port+2\u2026)</label><input id="sPort" type="number" value="17400" min="1" max="65500"></div>
    <div class="latency-field"><label for="sSeconds">Seconds per run</label><input id="sSeconds" type="number" value="5" min="1" max="60"></div>
    <div class="latency-field"><label for="sRuns">Repeated runs</label><input id="sRuns" type="number" value="1" min="1" max="10"></div>
    <div class="latency-field"><label for="sClients">Concurrent clients</label><input id="sClients" type="number" value="16" min="1" max="512"></div>
    <div class="latency-field"><label for="sPipeline">Pipeline depths</label><input id="sPipeline" value="1,16" inputmode="numeric"></div>
    <div class="latency-field"><label for="sKeys">Small-key dataset size</label><input id="sKeys" type="number" value="1000" min="1" max="100000"></div>
  </div>
  <div>
    <div class="latency-field" style="margin-bottom:.5rem;"><label>Engines</label></div>
    <div class="scenario-picks">
      <label class="scenario-pick"><input type="checkbox" name="sEngine" value="redis" checked> Redis</label>
      <label class="scenario-pick"><input type="checkbox" name="sEngine" value="dragonfly" checked> Dragonfly</label>
      <label class="scenario-pick"><input type="checkbox" name="sEngine" value="reflexdb" checked> ReflexDB</label>
    </div>
  </div>
  <div>
    <div class="latency-field" style="margin-bottom:.5rem;"><label>Workloads</label></div>
    <div class="scenario-picks">
      <label class="scenario-pick"><input type="checkbox" name="sWorkload" value="small-mixed" checked> Small-key mixed</label>
      <label class="scenario-pick"><input type="checkbox" name="sWorkload" value="hot-key" checked> Hot-key contention</label>
    </div>
  </div>
  <div class="actions" style="margin:0;">
    <button type="button" id="sRunBtn">Run shootout</button>
    <button type="button" id="sStopBtn" class="secondary" disabled>Stop</button>
    <span id="sStatus" style="color:#9a9aa8;font-size:.86rem;">Ready. Needs Docker running.</span>
  </div>
  <div id="shootoutEngineStatusRow" class="shootout-status-row"></div>
  <div id="shootoutEngineGrid" class="shootout-engine-grid"></div>
  <div id="shootoutTableWrap" class="latency-table-wrap" style="display:none;">
    <table id="shootoutTable"><thead><tr><th>Engine</th><th>Run</th><th>Workload</th><th>Pipeline</th><th>Ops/s</th><th>p50</th><th>p99</th><th>p999</th><th>Errors</th></tr></thead><tbody id="shootoutBody"></tbody></table>
  </div>
</section>

<div id="liveDemoPanel" class="panel" style="display:none;">
  <div class="frow"><label>Host</label><input id="liveHost" value="127.0.0.1">
    <label>Port</label><input id="livePort" value="7379" type="number"></div>
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
    <input id="liveAutoSeconds" value="60" type="number" min="1" style="width:5rem;">
    <span style="color:#9a9aa8; font-size:.88rem;">seconds, every</span>
    <input id="liveAutoIntervalMs" value="10" type="number" min="10" style="width:6rem;">
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
    <strong>Spawns its own throwaway server</strong> on the demo port below (not your regular
    one) in a throwaway data dir — <code>tiered-storage</code>, <code>tiered-memory-limit</code>,
    and <code>tiered-disk-limit</code> can only be set at server <em>startup</em>, there's no live
    <code>CONFIG SET</code> for them, so this is the only way to make them UI-configurable at all.
    Needs <code>reflexdb-server</code> built (<code>cargo build --bin reflexdb-server</code>).
  </div>
  <label>Demo server port</label><input id="tieredPort" value="17391" type="number">
  <label>Tiered memory limit<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    RAM budget before keys start spilling to disk. Accepts the same size strings as the config
    file: <code>10kb</code>, <code>1mb</code>, etc.
  </span></span></label><input id="tieredMemoryLimit" value="10kb">
  <label>Tiered disk limit (blank = unbounded)<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    Disk-tier capacity. What a full tier does is the overflow policy's choice below —
    <code>reject-writes</code> refuses new keys, <code>evict-oldest</code> drops cold tiered
    entries outright (real loss). Leave blank for unbounded.
  </span></span></label><input id="tieredDiskLimit" value="50kb">
  <label>Overflow policy<span class="info-icon" tabindex="0">i<span class="info-tooltip">
    <code>reject-writes</code> (recommended) — once RAM and disk are both full, new-key writes
    fail with <code>ERR TIERED_STORAGE_FULL</code>; deletes and overwrites still work, nothing
    is lost. <code>evict-oldest</code> — drops oldest tiered entries (cache semantics, real
    data loss). <code>no-limit</code> — disk limit ignored.
  </span></span></label><select id="tieredOverflowPolicy">
    <option value="reject-writes" selected>reject-writes (no data loss)</option>
    <option value="evict-oldest">evict-oldest (cache semantics)</option>
    <option value="no-limit">no-limit (unbounded)</option>
  </select>
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
  <div class="hero-card"><div class="k">Tracked tiered usage</div><div class="v green" id="tsDisk">-</div></div>
  <div class="hero-card"><div class="k">Promote GET time</div><div class="v red" id="tsPromote">-</div></div>
</div>

<div id="outputSection">
<h2>Live output</h2>
<pre id="output">Pick a preset (or edit fields) and click "Run benchmark". A card appears for every command as it starts, and fills in live.</pre>
</div>

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
  'Default command mix': { n: 100000, c: 50, p: 1, d: 3, r: 100000, cmd: 'ping,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,xadd' },
  'Pipelined (prod-like)': { n: 500000, c: 50, p: 16, d: 3, r: 500000, cmd: 'ping,set,get,incr,lpush,rpush,sadd,hset,zadd,mset,xadd' },
  'High concurrency': { n: 500000, c: 200, p: 32, d: 100, r: 500000, cmd: 'set,get,incr,lpush,sadd,hset,zadd' },
  'Large payload (object cache)': { n: 100000, c: 50, p: 1, d: 1024, r: 100000, cmd: 'set,get,mset' },
  'Memory stress (write heavy)': { n: 1000000, c: 100, p: 16, d: 100, r: 1000000, cmd: 'set,lpush,sadd,hset,zadd,xadd' },
  'Everything (all commands)': { n: 1000000, c: 50, p: 16, d: 20, r: 1000000, cmd: 'ping,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,xadd' },
};

const CMD_COLORS = ['#a78bfa', '#22d3ee', '#f59e0b', '#ef4444', '#22c55e', '#60a5fa', '#f472b6', '#facc15', '#a78bfa', '#34d399', '#fb923c', '#38bdf8', '#e879f9', '#4ade80', '#c084fc', '#fca5a5', '#86efac', '#93c5fd', '#fde047'];

const bench = document.getElementById('bench');
const output = document.getElementById('output');
const runBtn = document.getElementById('runBtn');
const stopBtn = document.getElementById('stopBtn');
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
const latencyPanel = document.getElementById('latencyPanel');
const durabilityPanel = document.getElementById('durabilityPanel');
const reslimitPanel = document.getElementById('reslimitPanel');
const shootoutPanel = document.getElementById('shootoutPanel');
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
let latencyController = null;
let durabilityController = null;
let reslimitController = null;
let shootoutController = null;
let currentTab = 'sequential';
let liveStartedAt = 0;
let liveWatchedKeyCount = 0;

// Sidebar collapse + whole-UI theme, both persisted across reloads. Each
// theme re-shades every surface (bg/inset/panel/borders) in the same hue —
// accents (purple/cyan) stay fixed so the current look is 'default'.
const THEMES = {
  default:  { bg: '#07070d', inset: '#0c0c14', panel: '#111119', panel2: '#1a1a24', border: '#22222f', border2: '#333345' },
  navy:     { bg: '#060a13', inset: '#0a101f', panel: '#0f1729', panel2: '#16203a', border: '#1f2c4a', border2: '#2c3d63' },
  purple:   { bg: '#0b0614', inset: '#110b1f', panel: '#171029', panel2: '#20163a', border: '#2e2150', border2: '#453169' },
  forest:   { bg: '#060c09', inset: '#0a1410', panel: '#0f1d15', panel2: '#14281f', border: '#1e3a2e', border2: '#2d5344' },
  ember:    { bg: '#0d0806', inset: '#150f0a', panel: '#1c150f', panel2: '#271d14', border: '#3a2b1f', border2: '#543e2b' },
  graphite: { bg: '#090909', inset: '#101010', panel: '#151515', panel2: '#1d1d1d', border: '#272727', border2: '#3a3a3a' },
};
const navToggle = document.getElementById('navToggle');
const navCollapse = document.getElementById('navCollapse');
const setNavHidden = hidden => {
  document.body.classList.toggle('nav-hidden', hidden);
  localStorage.setItem('ui.navHidden', hidden ? '1' : '');
};
const setTheme = name => {
  const t = THEMES[name];
  if (!t) return;
  for (const [k, v] of Object.entries(t)) document.documentElement.style.setProperty('--' + k, v);
  localStorage.setItem('ui.theme', name);
  document.querySelectorAll('#themeSwatches .swatch').forEach(s => s.classList.toggle('active', s.dataset.theme === name));
};
navToggle.onclick = () => setNavHidden(false);
navCollapse.onclick = () => setNavHidden(true);
document.getElementById('themeSwatches').addEventListener('click', e => {
  const s = e.target.closest('.swatch');
  if (s) setTheme(s.dataset.theme);
});
if (localStorage.getItem('ui.navHidden')) setNavHidden(true);
setTheme(localStorage.getItem('ui.theme') || 'default');

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
  const base = tab.split(':')[0], sub = tab.split(':')[1];
  [...tabRow.children].forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  [...bench.querySelectorAll('[data-mode]')].forEach(el => {
    el.style.display = el.dataset.mode.split(',').includes(base) ? '' : 'none';
  });
  runBtn.textContent = base === 'parallel' ? 'Run all commands in parallel' : base === 'history' ? 'Run history benchmark' : 'Run benchmark';

  const isLive = base === 'live';
  const isTiered = base === 'tiered';
  const isLatency = base === 'latency';
  const isDurability = base === 'durability';
  const isReslimit = base === 'reslimit';
  const isShootout = base === 'shootout';
  const isStudio = base === 'studio';
  const isHome = base === 'home';
  bench.style.display = (isHome || isLive || isTiered || isLatency || isDurability || isReslimit || isShootout || isStudio) ? 'none' : '';
  benchResults.style.display = (isHome || isLive || isTiered || isLatency || isDurability || isReslimit || isShootout || isStudio) ? 'none' : '';
  const hp = document.getElementById('homePanel');
  if (hp) hp.style.display = isHome ? '' : 'none';
  document.getElementById('outputSection').style.display = (isHome || isStudio) ? 'none' : '';
  latencyPanel.style.display = isLatency ? 'grid' : 'none';
  durabilityPanel.style.display = isDurability ? 'grid' : 'none';
  reslimitPanel.style.display = isReslimit ? 'grid' : 'none';
  shootoutPanel.style.display = isShootout ? 'grid' : 'none';
  const sp = document.getElementById('studioPanel');
  if (sp) sp.style.display = isStudio ? '' : 'none';
  if (isStudio && window.stShowStudio) stShowStudio();
  if (isStudio && sub) document.querySelector('#stTabs .st-tab[data-stab="' + sub + '"]')?.click();
  // Deliberately NOT stopping the demo's LIVE watch/auto-change or the
  // studio LIVE subscription here: all are explicit user actions with
  // their own Stop buttons, and killing them on navigation made a watch
  // receive nothing — writes stopped the moment you switched tabs.
  liveDemoPanel.style.display = isLive ? 'grid' : 'none';
  tieredDemoPanel.style.display = isTiered ? 'grid' : 'none';
  liveFeed.style.display = isLive ? '' : 'none';
  liveStats.style.display = isLive ? 'grid' : 'none';
  liveExplain.style.display = isLive ? '' : 'none';
  if (!isTiered && tieredController) { tieredController.abort(); tieredController = null; }
  if (!isLatency && latencyController) { latencyController.abort(); latencyController = null; }
  if (!isDurability && durabilityController) { durabilityController.abort(); durabilityController = null; }
  if (!isReslimit && reslimitController) { reslimitController.abort(); reslimitController = null; }
  // Deliberately NOT aborting shootoutController here: all three engines
  // run in parallel on the backend regardless of which tab is open, and
  // navigating away used to kill the whole in-flight request, silently
  // dropping any engine that hadn't started yet. Use the Stop button to
  // actually cancel a run.
  if (!isTiered) tieredSummary.style.display = 'none';
}
tabRow.addEventListener('click', (e) => {
  if (e.target.closest('.info-icon')) return;
  const btn = e.target.closest('.tab-btn');
  if (btn && !btn.hidden) setTab(btn.dataset.tab);
});
setTab('home');

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

// Landing hero: grow the bars and count the numbers up once on load.
requestAnimationFrame(() => {
  document.querySelectorAll('#landingHero .hbar-fill').forEach(el => {
    el.style.width = el.dataset.w + '%';
  });
  document.querySelectorAll('#landingHero .ops-count').forEach(el => {
    const target = Number(el.dataset.n), t0 = performance.now();
    const tick = now => {
      const p = Math.min((now - t0) / 1200, 1);
      el.textContent = Math.round(target * (1 - Math.pow(1 - p, 3))).toLocaleString();
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
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

// Percentage is always against the host's *actual* total (from /stats),
// not the "warn above" field below — that field only decides when the bar
// turns red, it never used to be the denominator, which is why it could
// show a pinned, meaningless 100% whenever someone's real usage was
// larger than whatever GB they'd typed in.
function updateBar(usedKb, totalKb, warnAboveGB, fill, text) {
  const pct = totalKb > 0 ? Math.min(100, (usedKb / totalKb) * 100) : 0;
  const usedGB = usedKb / 1024 / 1024;
  fill.style.width = pct + '%';
  fill.className = 'bar-fill' + (usedGB > warnAboveGB ? ' danger' : pct > 70 ? ' warn' : '');
  const totalText = totalKb > 0 ? `${fmtGB(totalKb)} GB` : '?';
  text.textContent = `${fmtGB(usedKb)}/${totalText} · ${pct.toFixed(0)}%`;
}

let statsPollTimer = null;

let statsFailCount = 0;

async function pollStatsOnce() {
  try {
    const r = await fetch('/stats');
    const text = await r.text();
    const map = {};
    text.split('\n').forEach(line => {
      const [k, v] = line.split(':').map(s => s.trim());
      if (k && v) map[k] = parseFloat(v) || 0;
    });
    if (!map.ram_total_kb && !map.disk_total_kb) throw new Error('no stats');
    statsFailCount = 0;
    topstats.style.display = '';
    updateBar(map.ram_used_kb, map.ram_total_kb, parseFloat(ramLimitGB.value) || Infinity, ramBar, ramText);
    updateBar(map.disk_used_kb, map.disk_total_kb, parseFloat(diskLimitGB.value) || Infinity, diskBar, diskText);
  } catch (e) {
    if (++statsFailCount >= 2) topstats.style.display = 'none';
  }
}

const liveDot = document.getElementById('liveDot');
const liveLabel = document.getElementById('liveLabel');
const topstats = document.querySelector('.topstats');
refreshStats.onclick = () => {
  if (statsPollTimer) {
    clearInterval(statsPollTimer);
    statsPollTimer = null;
    liveLabel.textContent = 'paused';
    liveDot.classList.add('off');
    topstats.classList.remove('live');
    return;
  }
  liveLabel.textContent = 'live';
  liveDot.classList.remove('off');
  topstats.classList.add('live');
  pollStatsOnce();
  statsPollTimer = setInterval(pollStatsOnce, 1500);
};
refreshStats.click();

document.getElementById('statsExpand').onclick = (e) => {
  const big = document.querySelector('.topstats').classList.toggle('big');
  e.target.textContent = big ? '⤡' : '⤢';
  localStorage.setItem('ui.statsBig', big ? '1' : '');
};
if (localStorage.getItem('ui.statsBig')) document.getElementById('statsExpand').click();

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
  ctx.fillStyle = 'var(--border)';
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
  const seconds = parseFloat(document.getElementById('liveAutoSeconds').value) || 60;
  const intervalMs = parseInt(document.getElementById('liveAutoIntervalMs').value, 10) || 10;
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
    port: document.getElementById('tieredPort').value,
    tiered_memory_limit: document.getElementById('tieredMemoryLimit').value.trim(),
    tiered_disk_limit: document.getElementById('tieredDiskLimit').value.trim(),
    overflow_policy: document.getElementById('tieredOverflowPolicy').value,
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
        if (evt.type === 'progress' && evt.message) {
          output.textContent = evt.message + '\n';
        } else if (evt.type === 'progress') {
          output.textContent = `Writing keys: ${evt.written} / ${evt.total}\n`;
        } else if (evt.type === 'settling') {
          output.textContent = 'Letting the background eviction tick catch up...\n';
        } else if (evt.type === 'summary') {
          document.getElementById('tsWritten').textContent = evt.written.toLocaleString();
          document.getElementById('tsDbsize').textContent = evt.dbsize.toLocaleString();
          document.getElementById('tsDisk').textContent = fmtKB(evt.tiered_disk_tracked_bytes / 1024);
          tieredSummary.style.display = 'grid';
          // DBSIZE counts tiered (on-disk) keys too — it only drops when a
          // key is actually deleted outright (tiered-disk-limit or expiry),
          // never just from moving RAM -> disk. So "tiered_disk_tracked_bytes
          // > 0" is the RAM-limit-working signal, and "dbsize < written" is
          // the disk-limit-working (or TTL) signal — two different things.
          // "tiered dir on disk" (raw `du`) is shown too since it's what
          // you'd actually see poking around on the filesystem, but it's
          // dominated by fjall's pre-allocated journal segments and isn't
          // the number tiered-disk-limit is compared against.
          output.textContent += `\nWrote ${evt.written} keys. DBSIZE (RAM + tiered) = ${evt.dbsize}. ` +
            `Tracked tiered usage = ${fmtKB(evt.tiered_disk_tracked_bytes / 1024)} ` +
            `(raw tiered dir on disk = ${fmtKB(evt.tiered_disk_kb)}, inflated by fjall's own ` +
            `pre-allocated journal files).\n` +
            (evt.tiered_disk_tracked_bytes > 0
              ? `tiered-memory-limit is working: some keys moved to disk.\n`
              : `Nothing moved to disk yet — tiered-memory-limit may be larger than what was written.\n`) +
            (evt.rejected > 0
              ? `reject-writes is working: ${evt.rejected} write(s) refused with TIERED_STORAGE_FULL ` +
                `once the disk tier filled up — nothing was discarded.\n`
              : evt.dbsize < evt.written
              ? `evict-oldest is working: ${evt.written - evt.dbsize} key(s) were dropped outright ` +
                `once the disk tier filled up.\n`
              : '') +
            (evt.spill_failures > 0
              ? `${evt.spill_failures} spill attempt(s) hit the full tier — those keys stayed in RAM.\n`
              : '');
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

const latRunBtn = document.getElementById('latRunBtn');
const latStopBtn = document.getElementById('latStopBtn');
const latStatus = document.getElementById('latStatus');
const latencyResults = document.getElementById('latencyResults');
const latencyBody = document.getElementById('latencyBody');
let latencyRows = [];

function fmtUs(value) {
  if (!Number.isFinite(value)) return '—';
  return value >= 1000 ? (value / 1000).toFixed(value >= 10000 ? 1 : 2) + ' ms' : value.toFixed(1) + ' µs';
}
function fmtOps(value) { return Number(value || 0).toLocaleString('en-US', { maximumFractionDigits: 0 }); }

// One small, fixed-height chart per run's own p50/p99/p999 (log scale),
// rendered right under that run's numbers instead of one combined chart
// that keeps growing taller with every run added.
function miniLatencyChart(row) {
  const names = ['p50', 'p99', 'p999'];
  const colors = ['#a78bfa', '#22d3ee', '#fb7185'];
  const values = names.map(n => row.latency_us?.[n]).filter(v => Number.isFinite(v) && v > 0);
  if (!values.length) return '';
  const width = 420, left = 46, right = 8, barH = 14, gap = 6, top = 4;
  const min = Math.max(1, Math.pow(10, Math.floor(Math.log10(Math.min(...values)))));
  const max = Math.max(min * 10, Math.pow(10, Math.ceil(Math.log10(Math.max(...values)))));
  const plotWidth = width - left - right;
  const x = v => left + (Math.log10(Math.max(min, v)) - Math.log10(min)) / (Math.log10(max) - Math.log10(min)) * plotWidth;
  let svg = '';
  names.forEach((name, j) => {
    const v = row.latency_us?.[name];
    if (!Number.isFinite(v)) return;
    const y = top + j * (barH + gap);
    svg += `<text x="${left - 6}" y="${y + barH - 3}" fill="#8a8a99" font-size="10" text-anchor="end">${name}</text>`;
    svg += `<rect x="${left}" y="${y}" width="${Math.max(2, x(v) - left)}" height="${barH}" rx="4" fill="${colors[j]}" opacity=".88"><title>${name}: ${fmtUs(v)}</title></rect>`;
    svg += `<text x="${Math.min(width - right + 4, x(v) + 4)}" y="${y + barH - 3}" fill="#d4d4de" font-size="10">${fmtUs(v)}</text>`;
  });
  const height = top + names.length * (barH + gap);
  return `<svg viewBox="0 0 ${width} ${height}" style="width:100%;height:auto;display:block;margin-top:.7rem;" role="img" aria-label="p50/p99/p999 for this run">${svg}</svg>`;
}

function renderLatencyRows() {
  latencyResults.innerHTML = '';
  if (!latencyRows.length) {
    latencyResults.innerHTML = '<div class="latency-empty">Run a workload to see throughput and tail latency.</div>';
    document.getElementById('latencyTableWrap').style.display = 'none';
    return;
  }
  for (const row of latencyRows) {
    const card = document.createElement('article');
    card.className = 'latency-card';
    const title = document.createElement('h3');
    title.textContent = `${row.wire === 'rfx' ? 'RFX · ' : ''}${row.workload} · run ${row.run}${row.pipeline ? ` · pipeline ${row.pipeline}` : ''}`;
    card.appendChild(title);
    const kpis = document.createElement('div');
    kpis.className = 'latency-kpis';
    const values = [
      ['Throughput', `${fmtOps(row.ops_per_second)} ops/s`],
      ['p50', fmtUs(row.latency_us?.p50)],
      ['p99', fmtUs(row.latency_us?.p99)],
      ['p999', fmtUs(row.latency_us?.p999)],
      ['Requests', fmtOps(row.operations)],
      ['Errors', fmtOps(row.errors)],
    ];
    for (const [label, value] of values) {
      const item = document.createElement('div'); item.className = 'latency-kpi';
      const name = document.createElement('span'); name.textContent = label;
      const number = document.createElement('strong'); number.textContent = value;
      item.append(name, number); kpis.appendChild(item);
    }
    card.appendChild(kpis);
    const chart = document.createElement('div');
    chart.innerHTML = miniLatencyChart(row);
    card.appendChild(chart);
    latencyResults.appendChild(card);
  }
  latencyBody.innerHTML = latencyRows.map(row => `<tr><td>${row.run}</td><td>${row.workload}</td><td>${(row.wire ?? 'resp').toUpperCase()}</td><td>${row.pipeline ?? '—'}</td><td>${row.clients}</td><td>${fmtOps(row.ops_per_second)}</td><td>${fmtUs(row.latency_us?.p50)}</td><td>${fmtUs(row.latency_us?.p99)}</td><td>${fmtUs(row.latency_us?.p999)}</td><td>${row.errors}</td></tr>`).join('');
  document.getElementById('latencyTableWrap').style.display = 'block';
}

latRunBtn.onclick = async () => {
  if (latencyController) latencyController.abort();
  latencyController = new AbortController();
  const workloads = [...document.querySelectorAll('input[name="latWorkload"]:checked')].map(input => input.value);
  if (!workloads.length) { latStatus.textContent = 'Choose at least one workload.'; return; }
  const params = new URLSearchParams({
    host: document.getElementById('latHost').value.trim(),
    port: document.getElementById('latPort').value,
    seconds: document.getElementById('latSeconds').value,
    runs: document.getElementById('latRuns').value,
    clients: document.getElementById('latClients').value,
    pipeline: document.getElementById('latPipeline').value.trim(),
    keys: document.getElementById('latKeys').value,
    live_subscribers: document.getElementById('latWatchers').value,
    live_drain: document.getElementById('latLiveDrain').value,
    wire: document.getElementById('latWire').value,
    workloads: workloads.join(','),
  });
  latencyRows = [];
  renderLatencyRows();
  latRunBtn.disabled = true; latStopBtn.disabled = false;
  latStatus.textContent = 'Starting scenarios… results appear after each run.';
  let buffer = '';
  try {
    const response = await fetch('/run-latency?' + params, { signal: latencyController.signal });
    if (!response.ok || !response.body) throw new Error(`Benchmark UI returned HTTP ${response.status}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end;
      while ((end = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
        if (!line) continue;
        const row = JSON.parse(line);
        if (row.type === 'progress') { latStatus.textContent = row.message; continue; }
        if (row.type === 'error') throw new Error(row.message);
        latencyRows.push(row); renderLatencyRows();
        latStatus.textContent = `${latencyRows.length} run(s) complete · ${row.workload} · ${fmtOps(row.ops_per_second)} ops/s`;
      }
    }
    if (buffer.trim()) { const row = JSON.parse(buffer); if (row.type === 'error') throw new Error(row.message); latencyRows.push(row); renderLatencyRows(); }
    if (!latencyRows.length) latStatus.textContent = 'No results returned. Check the target and benchmark output.';
    else latStatus.textContent += ' · complete';
  } catch (error) {
    if (error.name === 'AbortError') latStatus.textContent = 'Run stopped.';
    else latStatus.textContent = 'Run failed: ' + error.message;
  } finally {
    latRunBtn.disabled = false; latStopBtn.disabled = true; latencyController = null;
  }
};
latStopBtn.onclick = () => { if (latencyController) latencyController.abort(); };

const durRunBtn = document.getElementById('durRunBtn');
const durStopBtn = document.getElementById('durStopBtn');
const durStatus = document.getElementById('durStatus');
const durabilityResults = document.getElementById('durabilityResults');
let durabilityRows = [];

function renderDurabilityRows() {
  durabilityResults.innerHTML = '';
  if (!durabilityRows.length) {
    durabilityResults.innerHTML = '<div class="latency-empty">Run the test to see acknowledged vs. recovered writes per policy.</div>';
    return;
  }
  for (const row of durabilityRows) {
    const card = document.createElement('article');
    card.className = 'latency-card';
    const title = document.createElement('h3');
    title.textContent = `appendfsync=${row.policy}`;
    card.appendChild(title);
    const kpis = document.createElement('div');
    kpis.className = 'latency-kpis';
    const lossPct = row.acknowledged > 0 ? (100 * row.lost / row.acknowledged).toFixed(2) : '0.00';
    const values = [
      ['Acknowledged writes', fmtOps(row.acknowledged)],
      ['Recovered writes', fmtOps(row.recovered)],
      ['Lost', `${fmtOps(row.lost)} (${lossPct}%)`],
      ['Writes/s before kill', fmtOps(row.writes_per_second)],
      ['Restart + replay time', `${fmtOps(row.restart_ms)} ms`],
    ];
    for (const [label, value] of values) {
      const item = document.createElement('div'); item.className = 'latency-kpi';
      const name = document.createElement('span'); name.textContent = label;
      const number = document.createElement('strong');
      number.textContent = value;
      if (label === 'Lost') number.style.color = row.lost > 0 ? '#fb7185' : '#4ade80';
      item.append(name, number); kpis.appendChild(item);
    }
    card.appendChild(kpis);
    durabilityResults.appendChild(card);
  }
}

durRunBtn.onclick = async () => {
  if (durabilityController) durabilityController.abort();
  durabilityController = new AbortController();
  const policies = [...document.querySelectorAll('input[name="durPolicy"]:checked')].map(input => input.value);
  if (!policies.length) { durStatus.textContent = 'Choose at least one appendfsync policy.'; return; }
  const params = new URLSearchParams({
    host: document.getElementById('durHost').value.trim(),
    port: document.getElementById('durPort').value,
    seconds: document.getElementById('durSeconds').value,
    policies: policies.join(','),
  });
  durabilityRows = [];
  renderDurabilityRows();
  durRunBtn.disabled = true; durStopBtn.disabled = false;
  durStatus.textContent = 'Starting…';
  let buffer = '';
  try {
    const response = await fetch('/run-durability?' + params, { signal: durabilityController.signal });
    if (!response.ok || !response.body) throw new Error(`Benchmark UI returned HTTP ${response.status}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end;
      while ((end = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
        if (!line) continue;
        const row = JSON.parse(line);
        if (row.type === 'progress') { durStatus.textContent = row.message; continue; }
        if (row.type === 'error') throw new Error(row.message);
        durabilityRows.push(row); renderDurabilityRows();
        durStatus.textContent = `${durabilityRows.length}/${policies.length} polic${durabilityRows.length === 1 ? 'y' : 'ies'} done`;
      }
    }
    if (!durabilityRows.length) durStatus.textContent = 'No results returned.';
    else durStatus.textContent += ' · complete';
  } catch (error) {
    if (error.name === 'AbortError') durStatus.textContent = 'Run stopped.';
    else durStatus.textContent = 'Run failed: ' + error.message;
  } finally {
    durRunBtn.disabled = false; durStopBtn.disabled = true; durabilityController = null;
  }
};
durStopBtn.onclick = () => { if (durabilityController) durabilityController.abort(); };

const resRunBtn = document.getElementById('resRunBtn');
const resStopBtn = document.getElementById('resStopBtn');
const resStatus = document.getElementById('resStatus');
const reslimitResults = document.getElementById('reslimitResults');
let reslimitRows = [];

function renderReslimitRows() {
  reslimitResults.innerHTML = '';
  if (!reslimitRows.length) {
    reslimitResults.innerHTML = '<div class="latency-empty">Run the test to see crash behavior under a real hard limit.</div>';
    return;
  }
  for (const row of reslimitRows) {
    const card = document.createElement('article');
    card.className = 'latency-card';
    const title = document.createElement('h3');
    title.textContent = `RAM ${row.mem_limit_gb} GB / Disk ${row.disk_limit_gb} GB`;
    card.appendChild(title);
    const kpis = document.createElement('div');
    kpis.className = 'latency-kpis';
    const outcome = row.oom_killed ? 'OOM-killed by the kernel' : row.crashed ? `Exited (code ${row.exit_code})` : row.still_running ? 'Still running (hit safety cap)' : 'Stopped';
    const causeLabel = { 'ram+disk': 'RAM and disk both full', ram: 'RAM limit', disk: 'Disk limit', unknown: 'Unclear', none: 'n/a (hit safety cap)' }[row.cause] || row.cause;
    const values = [
      ['Outcome', outcome],
      ['Likely cause', causeLabel],
      ['Still responsive', row.still_running ? (row.still_responsive ? 'Yes — degraded gracefully' : 'No') : 'n/a (not running)'],
      ['Writes acknowledged', fmtOps(row.acknowledged)],
      ['Peak resident memory', `${fmtOps(row.peak_mem_mb)} MB`],
      ['Peak /data usage', `${fmtOps(Math.round(row.peak_disk_kb / 1024))} MB`],
    ];
    for (const [label, value] of values) {
      const item = document.createElement('div'); item.className = 'latency-kpi';
      const name = document.createElement('span'); name.textContent = label;
      const number = document.createElement('strong'); number.textContent = value;
      if (label === 'Outcome') number.style.color = row.oom_killed ? '#fb7185' : '#4ade80';
      item.append(name, number); kpis.appendChild(item);
    }
    card.appendChild(kpis);
    reslimitResults.appendChild(card);
  }
}

resRunBtn.onclick = async () => {
  if (reslimitController) reslimitController.abort();
  reslimitController = new AbortController();
  const params = new URLSearchParams({
    image: document.getElementById('resImage').value.trim(),
    port: document.getElementById('resPort').value,
    ram_gb: document.getElementById('resRamGB').value,
    disk_gb: document.getElementById('resDiskGB').value,
    value_bytes: document.getElementById('resValueBytes').value,
    max_seconds: document.getElementById('resMaxSeconds').value,
  });
  reslimitRows = [];
  renderReslimitRows();
  resRunBtn.disabled = true; resStopBtn.disabled = false;
  resStatus.textContent = 'Starting…';
  let buffer = '';
  try {
    const response = await fetch('/run-resource-limit?' + params, { signal: reslimitController.signal });
    if (!response.ok || !response.body) throw new Error(`Benchmark UI returned HTTP ${response.status}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end;
      while ((end = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
        if (!line) continue;
        const row = JSON.parse(line);
        if (row.type === 'progress') { resStatus.textContent = row.message; continue; }
        if (row.type === 'error') throw new Error(row.message);
        reslimitRows.push(row); renderReslimitRows();
      }
    }
    if (!reslimitRows.length) resStatus.textContent = 'No results returned.';
    else resStatus.textContent = 'Test complete';
  } catch (error) {
    if (error.name === 'AbortError') resStatus.textContent = 'Run stopped.';
    else resStatus.textContent = 'Run failed: ' + error.message;
  } finally {
    resRunBtn.disabled = false; resStopBtn.disabled = true; reslimitController = null;
  }
};
resStopBtn.onclick = () => { if (reslimitController) reslimitController.abort(); };

const sRunBtn = document.getElementById('sRunBtn');
const sStopBtn = document.getElementById('sStopBtn');
const sStatus = document.getElementById('sStatus');
const shootoutEngineGrid = document.getElementById('shootoutEngineGrid');
const shootoutEngineStatusRow = document.getElementById('shootoutEngineStatusRow');
const shootoutBody = document.getElementById('shootoutBody');
const SHOOTOUT_COLORS = { redis: '#f87171', dragonfly: '#22d3ee', reflexdb: '#a78bfa' };
const SHOOTOUT_LABELS = { redis: 'Redis', dragonfly: 'Dragonfly', reflexdb: 'ReflexDB' };
const SHOOTOUT_ENGINE_ORDER = ['redis', 'dragonfly', 'reflexdb'];
let shootoutRows = [];
let shootoutEngineStatus = {};

// One card per workload+pipeline combo, each holding a bar per engine —
// so comparing engines for the same traffic is a glance at one chart
// instead of eyeballing matching bars across three separate cards.
function shootoutWorkloadKey(row) { return `${row.workload}::${row.pipeline ?? ''}`; }

function shootoutWorkloadPanel(key, workload, pipeline) {
  const existing = document.getElementById(`shootoutCard-${key}`);
  if (existing) return existing;
  if (shootoutEngineGrid.querySelector('.latency-empty')) shootoutEngineGrid.innerHTML = '';
  const card = document.createElement('article');
  card.className = 'shootout-engine-card';
  card.id = `shootoutCard-${key}`;
  const title = pipeline != null && pipeline !== '' ? `${workload} \u00b7 pipeline ${pipeline}` : workload;
  card.innerHTML =
    `<h3>${title}</h3>` +
    `<svg id="shootoutCardChart-${key}" role="img" aria-label="${title} engine comparison chart"></svg>`;
  shootoutEngineGrid.appendChild(card);
  return card;
}

// Scaled to the max within this same workload+pipeline combo (not globally)
// since every bar in the chart is already a direct, same-traffic comparison.
function drawWorkloadChart(key) {
  const chart = document.getElementById(`shootoutCardChart-${key}`);
  if (!chart) return;
  const rows = shootoutRows
    .filter(r => shootoutWorkloadKey(r) === key)
    .sort((a, b) => SHOOTOUT_ENGINE_ORDER.indexOf(a.engine) - SHOOTOUT_ENGINE_ORDER.indexOf(b.engine));
  if (!rows.length) { chart.innerHTML = ''; return; }
  const width = 420, left = 90, right = 56, barHeight = 16, rowGap = 10, top = 4;
  const maxOps = Math.max(1, ...rows.map(r => r.ops_per_second || 0));
  const plotWidth = width - left - right;
  const barWidth = v => Math.max(2, (v / maxOps) * plotWidth);
  let svg = '';
  let y = top;
  for (const row of rows) {
    const label = row.engineLabel || SHOOTOUT_LABELS[row.engine] || row.engine;
    svg += `<text x="${left - 6}" y="${y + barHeight - 4}" fill="#8a8a99" font-size="10" text-anchor="end">${label}</text>`;
    svg += `<rect x="${left}" y="${y}" width="${barWidth(row.ops_per_second)}" height="${barHeight}" rx="3" fill="${SHOOTOUT_COLORS[row.engine] || '#888'}" opacity=".9"><title>${label}: ${fmtOps(row.ops_per_second)} ops/s, p99 ${fmtUs(row.latency_us?.p99)}</title></rect>`;
    svg += `<text x="${Math.min(width - right + 5, left + barWidth(row.ops_per_second) + 5)}" y="${y + barHeight - 4}" fill="#d4d4de" font-size="10">${fmtOps(row.ops_per_second)}/s</text>`;
    y += barHeight + rowGap;
  }
  chart.setAttribute('viewBox', `0 0 ${width} ${y + 4}`);
  chart.innerHTML = svg;
}

function renderShootoutStatusRow() {
  const engineIds = Object.keys(shootoutEngineStatus);
  shootoutEngineStatusRow.innerHTML = engineIds
    .map(id => {
      const label = SHOOTOUT_LABELS[id] || id;
      const dotColor = SHOOTOUT_COLORS[id] || '#a78bfa';
      return `<span class="shootout-status-chip"><span class="legend-dot" style="background:${dotColor}"></span>` +
        `<b>${label}</b><span class="chip-status">${shootoutEngineStatus[id] || ''}</span></span>`;
    })
    .join('');
}

function renderShootoutRows() {
  const knownEngines = [...new Set([...shootoutRows.map(r => r.engine), ...Object.keys(shootoutEngineStatus)])];
  renderShootoutStatusRow();
  if (!knownEngines.length) {
    shootoutEngineGrid.innerHTML = '<div class="latency-empty">Run the shootout to see throughput and tail latency per engine.</div>';
    document.getElementById('shootoutTableWrap').style.display = 'none';
    return;
  }
  const workloadKeys = [...new Set(shootoutRows.map(shootoutWorkloadKey))];
  for (const key of workloadKeys) {
    const sample = shootoutRows.find(r => shootoutWorkloadKey(r) === key);
    shootoutWorkloadPanel(key, sample.workload, sample.pipeline);
    drawWorkloadChart(key);
  }
  if (shootoutRows.length) {
    const sorted = [...shootoutRows].sort((a, b) => {
      const ak = shootoutWorkloadKey(a), bk = shootoutWorkloadKey(b);
      if (ak !== bk) return ak < bk ? -1 : 1;
      return SHOOTOUT_ENGINE_ORDER.indexOf(a.engine) - SHOOTOUT_ENGINE_ORDER.indexOf(b.engine);
    });
    let prevKey = null;
    shootoutBody.innerHTML = sorted
      .map(row => {
        const key = shootoutWorkloadKey(row);
        const groupStart = key !== prevKey;
        prevKey = key;
        return `<tr class="${groupStart ? 'workload-group-start' : ''}"><td>${row.engineLabel}</td><td>${row.run}</td><td>${row.workload}</td><td>${row.pipeline ?? '\u2014'}</td><td>${fmtOps(row.ops_per_second)}</td><td>${fmtUs(row.latency_us?.p50)}</td><td>${fmtUs(row.latency_us?.p99)}</td><td>${fmtUs(row.latency_us?.p999)}</td><td>${row.errors}</td></tr>`;
      })
      .join('');
    document.getElementById('shootoutTableWrap').style.display = 'block';
  }
}

sRunBtn.onclick = async () => {
  if (shootoutController) shootoutController.abort();
  shootoutController = new AbortController();
  const engines = [...document.querySelectorAll('input[name="sEngine"]:checked')].map(i => i.value);
  const workloads = [...document.querySelectorAll('input[name="sWorkload"]:checked')].map(i => i.value);
  if (!engines.length) { sStatus.textContent = 'Choose at least one engine.'; return; }
  if (!workloads.length) { sStatus.textContent = 'Choose at least one workload.'; return; }
  const params = new URLSearchParams({
    mem_mb: document.getElementById('sMemMB').value,
    cpus: document.getElementById('sCpus').value,
    port: document.getElementById('sPort').value,
    seconds: document.getElementById('sSeconds').value,
    runs: document.getElementById('sRuns').value,
    clients: document.getElementById('sClients').value,
    pipeline: document.getElementById('sPipeline').value.trim(),
    keys: document.getElementById('sKeys').value,
    engines: engines.join(','),
    workloads: workloads.join(','),
  });
  shootoutRows = [];
  // Clear out any charts/table rows left over from the previous run —
  // otherwise stale workload cards from before would just sit there
  // alongside the new ones since they're keyed by workload+pipeline.
  shootoutEngineGrid.innerHTML = '';
  shootoutBody.innerHTML = '';
  document.getElementById('shootoutTableWrap').style.display = 'none';
  // Seed a panel per selected engine immediately — all three start at once
  // on the backend, so the UI shouldn't wait for the first result line to
  // show that they're running.
  shootoutEngineStatus = Object.fromEntries(engines.map(id => [id, 'starting\u2026']));
  renderShootoutRows();
  sRunBtn.disabled = true; sStopBtn.disabled = false;
  sStatus.textContent = 'Starting all engines in parallel\u2026';
  let buffer = '';
  try {
    const response = await fetch('/run-shootout?' + params, { signal: shootoutController.signal });
    if (!response.ok || !response.body) throw new Error(`Benchmark UI returned HTTP ${response.status}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end;
      while ((end = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
        if (!line) continue;
        let row;
        try { row = JSON.parse(line); } catch { continue; }
        const engineLabel = SHOOTOUT_LABELS[row.engine] || row.engine || '?';
        // One engine's error (missing image, failed pull, container that
        // never came up) doesn't stop the others — they're independent
        // tasks on the backend — so this just updates that engine's own
        // status line and keeps reading everyone else's results.
        if (row.type === 'progress' || row.type === 'error') {
          shootoutEngineStatus[row.engine] = row.message;
          renderShootoutRows();
          continue;
        }
        row.engineLabel = engineLabel;
        shootoutRows.push(row);
        shootoutEngineStatus[row.engine] = `${row.workload} p${row.pipeline} \u00b7 ${fmtOps(row.ops_per_second)} ops/s`;
        renderShootoutRows();
      }
      sStatus.textContent = `Running\u2026 ${shootoutRows.length} result(s) so far across ${engines.length} engine(s).`;
    }
    if (!shootoutRows.length) sStatus.textContent = 'No results returned \u2014 check Docker and that the images are available.';
    else sStatus.textContent = `Done \u00b7 ${shootoutRows.length} result(s) across ${engines.length} engine(s).`;
  } catch (error) {
    if (error.name === 'AbortError') sStatus.textContent = 'Run stopped.';
    else sStatus.textContent = 'Run failed: ' + error.message;
  } finally {
    sRunBtn.disabled = false; sStopBtn.disabled = true; shootoutController = null;
  }
};
sStopBtn.onclick = () => { if (shootoutController) shootoutController.abort(); };

ramLimitGB.onchange = diskLimitGB.onchange = () => { if (statsPollTimer) pollStatsOnce(); };
</script>"###,
    include_str!("studio_ui.html"),
    r###"
</body>
</html>"###
);

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

fn params_to_query(params: &HashMap<String, String>) -> String {
    params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
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
    Err(std::io::Error::other("could not parse free output"))
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
    Err(std::io::Error::other("memory stats not supported on this OS"))
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

/// Reads one `INFO` field (e.g. `used_memory`, `tiered_disk_used_bytes`) as
/// a raw integer, over a fresh connection.
async fn fetch_info_u64(host: &str, port: u16, field: &str) -> std::io::Result<u64> {
    let mut stream =
        tokio::time::timeout(Duration::from_millis(800), TcpStream::connect((host, port))).await??;
    stream.write_all(b"*1\r\n$4\r\nINFO\r\n").await?;

    let prefix = format!("{field}:");
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
                if let Some(rest) = line.strip_prefix(prefix.as_str()) {
                    if let Ok(v) = rest.trim().parse::<u64>() {
                        return Ok(v);
                    }
                }
            }
            break;
        }
    }
    Err(std::io::Error::other(format!("{field} not found in INFO reply")))
}

/// Queries the reflexdb server's own `used_memory` (from `INFO`) over a
/// fresh connection — more meaningful than host-wide RAM when watching how
/// a single server process behaves under a workload.
async fn fetch_used_memory_kb(host: &str, port: u16) -> std::io::Result<u64> {
    fetch_info_u64(host, port, "used_memory").await.map(|v| v / 1024)
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

/// `/live-autoset` values — a readable, ordered string per tick
/// (`update #N · HH:MM:SS UTC`) instead of random noise, so the demo's
/// history/export reads sensibly.
fn demo_value(seq: u64) -> String {
    let secs = now_s().rem_euclid(86_400);
    format!("update #{seq} · {:02}:{:02}:{:02} UTC", secs / 3600, secs % 3600 / 60, secs % 60)
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
    let mut seq = 0u64;
    while Instant::now() < deadline {
        for key in &keys {
            seq += 1;
            let value = demo_value(seq);
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
    let text = |name: &str, fallback: &str| params.get(name).cloned().unwrap_or_else(|| fallback.to_string());
    let host = "127.0.0.1".to_string();
    let port = text("port", "17391").parse::<u16>().unwrap_or(17391);
    let mem_limit = text("tiered_memory_limit", "10kb");
    let disk_limit = text("tiered_disk_limit", "");
    let overflow_policy = match text("overflow_policy", "reject-writes").as_str() {
        "evict-oldest" => "evict-oldest",
        "no-limit" => "no-limit",
        _ => "reject-writes",
    };
    let count = text("count", "2000").parse::<usize>().unwrap_or(2000).clamp(1, 1_000_000);
    let size = text("size", "2000").parse::<usize>().unwrap_or(2000).clamp(1, 10_000_000);
    let prefix = "tiered:demo:";

    let headers = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    socket.write_all(headers.as_bytes()).await?;

    // Spawns its own throwaway server (like the Durability/Resource-limit
    // tabs) instead of requiring one already running — `tiered-storage`,
    // `tiered-memory-limit`, and `tiered-disk-limit` can only be set at
    // server *startup*, there's no live `CONFIG SET` for them, so this is
    // the only way to make them UI-configurable at all.
    let server_bin = reflexdb_server_bin();
    let data_dir = std::env::temp_dir().join(format!("reflexdb-tiered-demo-{}-{port}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"error\",\"message\":\"could not create temp data dir: {}\"}}\n",
                json_escape(&e.to_string())
            ),
        )
        .await?;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    write_chunk(
        socket,
        &format!(
            "{{\"type\":\"progress\",\"message\":\"starting a fresh server (tiered-memory-limit={}, tiered-disk-limit={})\u{2026}\"}}\n",
            json_escape(&mem_limit),
            if disk_limit.is_empty() { "unbounded".to_string() } else { json_escape(&disk_limit) }
        ),
    )
    .await?;

    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--dir".to_string(),
        data_dir.to_string_lossy().into_owned(),
        "--tiered-storage".to_string(),
        "yes".to_string(),
        "--tiered-memory-limit".to_string(),
        mem_limit,
        "--tiered-dir".to_string(),
        "tiered".to_string(),
    ];
    if !disk_limit.is_empty() {
        args.push("--tiered-disk-limit".to_string());
        args.push(disk_limit);
        args.push("--tiered-overflow-policy".to_string());
        args.push(overflow_policy.to_string());
    }
    let spawned = Command::new(&server_bin)
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    let mut child = match spawned {
        Ok(c) => c,
        Err(e) => {
            write_chunk(
                socket,
                &format!(
                    "{{\"type\":\"error\",\"message\":\"could not spawn {}: {}. Build it first with `cargo build --bin reflexdb-server`.\"}}\n",
                    json_escape(&server_bin),
                    json_escape(&e.to_string())
                ),
            )
            .await?;
            socket.write_all(b"0\r\n\r\n").await?;
            let _ = std::fs::remove_dir_all(&data_dir);
            return Ok(());
        }
    };
    if !wait_for_server_ready(&host, port, Duration::from_secs(5)).await {
        let _ = child.kill().await;
        write_chunk(socket, "{\"type\":\"error\",\"message\":\"server did not become ready in time\"}\n")
            .await?;
        socket.write_all(b"0\r\n\r\n").await?;
        let _ = std::fs::remove_dir_all(&data_dir);
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
            let _ = child.kill().await;
            let _ = std::fs::remove_dir_all(&data_dir);
            return Ok(());
        }
    };

    let value = "x".repeat(size);
    let mut rejected = 0usize;
    for i in 0..count {
        let key = format!("{prefix}{i}");
        match client.call(&["SET", &key, &value]).await {
            // Under `reject-writes` a full tier answers `-ERR
            // TIERED_STORAGE_FULL` — that's the point of the demo, not a
            // transport failure, so count it and keep going.
            Ok(Reply::Error(_)) => rejected += 1,
            Ok(_) => {}
            Err(_) => {
                let _ =
                    write_chunk(socket, "{\"type\":\"error\",\"message\":\"SET failed mid-run\"}\n").await;
                socket.write_all(b"0\r\n\r\n").await?;
                let _ = child.kill().await;
                let _ = std::fs::remove_dir_all(&data_dir);
                return Ok(());
            }
        }
        // Pace the loop a little — `reject-writes` only engages once the
        // ~100ms maintenance tick has refreshed `approx_memory`, and a
        // flat-out burst would finish before the first tick.
        if i % 10 == 9 {
            tokio::time::sleep(Duration::from_millis(20)).await;
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
    // `du`-style filesystem size (what a user would see poking around on
    // disk) and the self-tracked payload-byte total `tiered-disk-limit`
    // actually enforces are two different numbers — fjall pre-allocates
    // journal segments well ahead of real usage, so the former is often
    // much larger. Report both rather than picking one and confusing
    // whichever doesn't match the configured limit.
    let disk_kb = get_dir_disk_kb(&data_dir.join("tiered").to_string_lossy()).await;
    let tracked_bytes = fetch_info_u64(&host, port, "tiered_disk_used_bytes").await.unwrap_or(0);
    let spill_failures = fetch_info_u64(&host, port, "tiered_spill_failures").await.unwrap_or(0);
    let line = format!(
        "{{\"type\":\"summary\",\"written\":{count},\"dbsize\":{dbsize},\"tiered_disk_kb\":{disk_kb},\"tiered_disk_tracked_bytes\":{tracked_bytes},\"rejected\":{rejected},\"spill_failures\":{spill_failures}}}\n"
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
    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = std::fs::remove_dir_all(&data_dir);
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
        .unwrap_or_else(|| "ping,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,lrange_300,lrange_500,lrange_600,mset,xadd".to_string());
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

    let spawned = Command::new(&bench_bin)
        .args(&bench_args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();
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
    let stderr = child.stderr.take().expect("piped stderr");
    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<(bool, String)>();
    let stderr_tx = line_tx.clone();
    let reader_task = tokio::spawn(stream_bench_output(stdout, line_tx));
    tokio::spawn(async move {
        // stderr is plain lines (connect failures, usage errors) — no
        // \r-progress to split, so forward each line as a log line.
        let mut r = tokio::io::BufReader::new(stderr);
        let mut buf = String::new();
        while tokio::io::AsyncBufReadExt::read_line(&mut r, &mut buf).await.unwrap_or(0) > 0 {
            let line = buf.trim_end().to_string();
            buf.clear();
            if line.is_empty() || stderr_tx.send((false, line)).is_err() {
                return;
            }
        }
    });

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

/// Runs the checked-in, dependency-free workload harness and forwards its
/// per-run JSONL rows to the browser as they complete. Keeping the harness
/// as the source of truth means terminal and UI runs use identical traffic.
async fn handle_run_latency(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let text = |name: &str, fallback: &str| params.get(name).cloned().unwrap_or_else(|| fallback.to_string());
    let host = text("host", "127.0.0.1").trim().to_string();
    let port = text("port", "7379").parse::<u16>().unwrap_or(7379);
    let seconds = text("seconds", "5").parse::<f64>().unwrap_or(5.0).clamp(1.0, 60.0);
    let runs = text("runs", "3").parse::<usize>().unwrap_or(3).clamp(1, 10);
    let clients = text("clients", "16").parse::<usize>().unwrap_or(16).clamp(1, 512);
    let keys = text("keys", "1000").parse::<usize>().unwrap_or(1000).clamp(1, 100_000);
    let live_subscribers = text("live_subscribers", "1000").parse::<usize>().unwrap_or(1000).clamp(0, 10_000);
    let live_drain = text("live_drain", "buffered");
    let wires: Vec<String> = text("wire", "resp")
        .split(',')
        .map(str::trim)
        .filter(|w| matches!(*w, "resp" | "rfx"))
        .map(str::to_string)
        .collect();
    let pipelines = text("pipeline", "1,16");
    let valid_pipelines =
        pipelines.split(',').all(|part| part.trim().parse::<usize>().is_ok_and(|n| (1..=1024).contains(&n)))
            && pipelines.split(',').count() <= 4;
    let workload_text = text("workloads", "small-mixed,hot-key,live-fanout");
    let workloads: Vec<String> = workload_text
        .split(',')
        .map(str::trim)
        .filter(|w| matches!(*w, "small-mixed" | "hot-key" | "live-fanout"))
        .map(str::to_string)
        .collect();
    if host.is_empty()
        || !valid_pipelines
        || workloads.is_empty()
        || !matches!(live_drain.as_str(), "buffered" | "selector")
        || wires.is_empty()
    {
        socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }

    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;
    write_chunk(
        socket,
        "{\"type\":\"progress\",\"message\":\"Starting selected workloads against the target server…\"}\n",
    )
    .await?;

    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python3".to_string());
    for wire in &wires {
        if wires.len() > 1 {
            let row =
                format!("{{\"type\":\"progress\",\"message\":\"Running {} wire…\"}}\n", wire.to_uppercase());
            write_chunk(socket, &row).await?;
        }
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks")
            .join(if wire == "rfx" { "run_rfx.py" } else { "run.py" });
        let mut child = match Command::new(&python)
            .arg(script)
            .args(["--host", &host, "--port", &port.to_string(), "--runs", &runs.to_string()])
            .args(["--seconds", &seconds.to_string(), "--clients", &clients.to_string()])
            .args(["--pipeline", &pipelines, "--keys", &keys.to_string()])
            .args(["--live-subscribers", &live_subscribers.to_string(), "--workloads", &workloads.join(",")])
            .args(["--live-drain", &live_drain])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                let row = format!(
                    "{{\"type\":\"error\",\"message\":\"Could not start benchmark harness: {}\"}}\n",
                    json_escape(&error.to_string())
                );
                write_chunk(socket, &row).await?;
                socket.write_all(b"0\r\n\r\n").await?;
                return Ok(());
            }
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill().await;
            write_chunk(
                socket,
                "{\"type\":\"error\",\"message\":\"Benchmark harness has no output stream\"}\n",
            )
            .await?;
            socket.write_all(b"0\r\n\r\n").await?;
            return Ok(());
        };
        let mut reader = tokio::io::BufReader::new(stdout);
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line).await?;
            if read == 0 {
                break;
            }
            let row = tag_json_line(&String::from_utf8_lossy(&line), "wire", wire);
            if write_chunk(socket, &row).await.is_err() {
                let _ = child.kill().await;
                return Ok(());
            }
        }
        let status = child.wait().await?;
        if !status.success() {
            write_chunk(socket, "{\"type\":\"error\",\"message\":\"Benchmark process exited unsuccessfully; check that Python 3 and the target server are available\"}\n").await?;
        }
    }
    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// Resolves the `reflexdb-server` binary to spawn for the durability demo:
/// an explicit override, then a sibling of this binary (how `cargo build`
/// lays out a workspace's binaries), then falling back to `PATH`.
fn reflexdb_server_bin() -> String {
    if let Ok(path) = std::env::var("REFLEXDB_SERVER_BIN") {
        return path;
    }
    let name = if cfg!(windows) { "reflexdb-server.exe" } else { "reflexdb-server" };
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(name);
            if sibling.is_file() {
                return sibling.to_string_lossy().into_owned();
            }
        }
    }
    name.to_string()
}

/// Polls `host:port` until a connection + `PING` succeeds or `timeout` elapses.
async fn wait_for_server_ready(host: &str, port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(mut client) = RespClient::connect(host, port).await {
            if client.call(&["PING"]).await.is_ok() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Writes `count` keys (`durability:0`..`durability:N`) over one connection,
/// bumping `acked` after each confirmed `+OK` — a lower bound on what the
/// server had actually accepted before it gets killed out from under this
/// loop (the next `call` simply errors and the loop exits).
async fn run_durability_writer(host: String, port: u16, acked: std::sync::Arc<std::sync::atomic::AtomicU64>) {
    let Ok(mut client) = RespClient::connect(&host, port).await else { return };
    let mut i: u64 = 0;
    loop {
        let key = format!("durability:{i}");
        let value = i.to_string();
        match client.call(&["SET", &key, &value]).await {
            Ok(_) => {
                acked.store(i + 1, std::sync::atomic::Ordering::Relaxed);
                i += 1;
            }
            Err(_) => break,
        }
    }
}

/// Crash-recovery demo: for each requested `appendfsync` policy, spawns a
/// fresh `reflexdb-server` in a throwaway data dir, hammers it with `SET`s,
/// force-kills it (`SIGKILL`/`TerminateProcess` via `Child::kill`) mid-write,
/// restarts it pointed at the same data dir, and compares acknowledged
/// writes against what actually came back after AOF replay. Runs are
/// sequential (same demo port) so results aren't skewed by one policy's
/// leftover data dir or port contention with the next.
async fn handle_run_durability(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let text = |name: &str, fallback: &str| params.get(name).cloned().unwrap_or_else(|| fallback.to_string());
    let host = text("host", "127.0.0.1").trim().to_string();
    let port = text("port", "17380").parse::<u16>().unwrap_or(17380);
    let write_secs = text("seconds", "4").parse::<u64>().unwrap_or(4).clamp(1, 30);
    let policy_text = text("policies", "always,everysec,no");
    let policies: Vec<String> = policy_text
        .split(',')
        .map(str::trim)
        .filter(|p| matches!(*p, "always" | "everysec" | "no"))
        .map(str::to_string)
        .collect();
    if host.is_empty() || policies.is_empty() || policies.len() > 3 {
        socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }

    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;

    let server_bin = reflexdb_server_bin();
    for policy in &policies {
        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"progress\",\"message\":\"appendfsync={}: starting a fresh server…\"}}\n",
                json_escape(policy)
            ),
        )
        .await?;

        let dir = std::env::temp_dir().join(format!(
            "reflexdb-durability-{}-{}-{policy}",
            std::process::id(),
            port
        ));
        let _ = std::fs::remove_dir_all(&dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            write_chunk(
                socket,
                &format!(
                    "{{\"type\":\"error\",\"message\":\"could not create temp data dir: {}\"}}\n",
                    json_escape(&e.to_string())
                ),
            )
            .await?;
            break;
        }

        let spawn = |dir: &std::path::Path| {
            Command::new(&server_bin)
                .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
                .args(["--appendonly", "yes", "--appendfsync", policy])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
        };

        let mut child = match spawn(&dir) {
            Ok(c) => c,
            Err(e) => {
                write_chunk(
                    socket,
                    &format!(
                        "{{\"type\":\"error\",\"message\":\"could not spawn {}: {}. Set REFLEXDB_SERVER_BIN or build it first.\"}}\n",
                        json_escape(&server_bin),
                        json_escape(&e.to_string())
                    ),
                )
                .await?;
                break;
            }
        };
        if !wait_for_server_ready(&host, port, Duration::from_secs(5)).await {
            let _ = child.kill().await;
            write_chunk(socket, "{\"type\":\"error\",\"message\":\"server did not become ready in time\"}\n")
                .await?;
            break;
        }

        write_chunk(socket, "{\"type\":\"progress\",\"message\":\"writing under load…\"}\n").await?;
        let acked = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let writer = tokio::spawn(run_durability_writer(host.clone(), port, std::sync::Arc::clone(&acked)));
        tokio::time::sleep(Duration::from_secs(write_secs)).await;

        // Kill first, then stop the writer task — order matters: we want the
        // acknowledged count to reflect exactly what the server saw before
        // the kill, not a race against the writer noticing first.
        let kill_result = child.kill().await;
        let acknowledged = acked.load(std::sync::atomic::Ordering::Relaxed);
        writer.abort();
        let _ = child.wait().await;
        if let Err(e) = kill_result {
            write_chunk(
                socket,
                &format!(
                    "{{\"type\":\"error\",\"message\":\"could not kill server: {}\"}}\n",
                    json_escape(&e.to_string())
                ),
            )
            .await?;
            break;
        }

        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"progress\",\"message\":\"killed after {} acknowledged writes — restarting from the same data dir…\"}}\n",
                acknowledged
            ),
        )
        .await?;

        let restart_started = Instant::now();
        let mut restarted = match spawn(&dir) {
            Ok(c) => c,
            Err(e) => {
                write_chunk(
                    socket,
                    &format!(
                        "{{\"type\":\"error\",\"message\":\"could not restart server: {}\"}}\n",
                        json_escape(&e.to_string())
                    ),
                )
                .await?;
                break;
            }
        };
        let ready = wait_for_server_ready(&host, port, Duration::from_secs(10)).await;
        let restart_ms = restart_started.elapsed().as_millis();
        if !ready {
            let _ = restarted.kill().await;
            write_chunk(
                socket,
                "{\"type\":\"error\",\"message\":\"server did not come back up after restart\"}\n",
            )
            .await?;
            break;
        }

        let recovered = match RespClient::connect(&host, port).await {
            Ok(mut client) => match client.call(&["DBSIZE"]).await {
                Ok(Reply::Integer(n)) => n.max(0) as u64,
                _ => 0,
            },
            Err(_) => 0,
        };
        let lost = acknowledged.saturating_sub(recovered);
        let writes_per_second = if write_secs > 0 { acknowledged as f64 / write_secs as f64 } else { 0.0 };

        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"durability\",\"policy\":\"{policy}\",\"acknowledged\":{acknowledged},\"recovered\":{recovered},\"lost\":{lost},\"writes_per_second\":{writes_per_second:.0},\"restart_ms\":{restart_ms}}}\n"
            ),
        )
        .await?;

        let _ = restarted.kill().await;
        let _ = restarted.wait().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// `docker inspect -f '<format>' <name>`, trimmed. `None` on any failure
/// (daemon not running, container gone, bad format, ...) — every caller
/// treats that as "couldn't find out", not an error worth surfacing on its
/// own; the surrounding test reports it as such.
async fn docker_inspect_field(name: &str, format: &str) -> Option<String> {
    let out = Command::new("docker").args(["inspect", "-f", format, name]).output().await.ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

async fn docker_container_running(name: &str) -> bool {
    docker_inspect_field(name, "{{.State.Running}}").await.as_deref() == Some("true")
}

/// Disk used inside the container's `/data` mount, in KB — `0` if the
/// container has no shell/`du` (e.g. a `scratch`-based image) or is gone.
async fn docker_dir_usage_kb(name: &str) -> u64 {
    let out = Command::new("docker")
        .args(["exec", name, "sh", "-c", "du -sk /data 2>/dev/null | cut -f1"])
        .output()
        .await;
    match out {
        Ok(out) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().unwrap_or(0)
        }
        _ => 0,
    }
}

/// Parses `docker stats`' `MemUsage` column (e.g. `512.3MiB / 1GiB`) into
/// the "used" side, in MB. Good enough for a live progress readout, not
/// meant to be exact.
fn parse_docker_mem_usage_mb(text: &str) -> Option<f64> {
    let used = text.split('/').next()?.trim();
    let (number, unit) = used.split_at(used.find(|c: char| c.is_alphabetic())?);
    let value: f64 = number.trim().parse().ok()?;
    Some(match unit.trim() {
        "GiB" | "GB" => value * 1024.0,
        "MiB" | "MB" => value,
        "KiB" | "KB" => value / 1024.0,
        "B" => value / 1024.0 / 1024.0,
        _ => return None,
    })
}

async fn docker_mem_usage_mb(name: &str) -> Option<f64> {
    let out = Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{.MemUsage}}", name])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_docker_mem_usage_mb(String::from_utf8_lossy(&out.stdout).trim())
}

/// Same writer shape as `run_durability_writer`, but with a caller-chosen
/// value size so RAM/disk fill at a predictable, configurable rate.
async fn run_resource_writer(
    host: String,
    port: u16,
    value: String,
    acked: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let Ok(mut client) = RespClient::connect(&host, port).await else { return };
    let mut i: u64 = 0;
    loop {
        let key = format!("fill:{i}");
        match client.call(&["SET", &key, &value]).await {
            Ok(_) => {
                acked.store(i + 1, std::sync::atomic::Ordering::Relaxed);
                i += 1;
            }
            Err(_) => break,
        }
    }
}

/// Hard resource-limit demo: runs `reflexdb-server` inside a single Docker
/// container with both a real `--memory` cap and a size-capped `--tmpfs`
/// mount applied at the same time — an actual OS-enforced ceiling, unlike
/// `maxmemory`/`noeviction`, which is reflexdb's own graceful (non-crashing)
/// degradation. Writes fixed-size values until the container dies or a
/// safety timeout elapses, then reports whether the kernel OOM-killed it
/// and/or the disk filled up, and whether it kept responding instead of
/// crashing.
async fn handle_run_resource_limit(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let text = |name: &str, fallback: &str| params.get(name).cloned().unwrap_or_else(|| fallback.to_string());
    let image = text("image", "reflexdb-bench:latest");
    let port = text("port", "17385").parse::<u16>().unwrap_or(17385);
    let mem_limit_gb = text("ram_gb", "1").parse::<f64>().unwrap_or(1.0).clamp(0.1, 64.0);
    let disk_limit_gb = text("disk_gb", "0.25").parse::<f64>().unwrap_or(0.25).clamp(0.05, 64.0);
    let value_bytes = text("value_bytes", "4096").parse::<usize>().unwrap_or(4096).clamp(64, 65536);
    let max_seconds = text("max_seconds", "90").parse::<u64>().unwrap_or(90).clamp(5, 300);
    if image.is_empty() {
        socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }

    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;

    let docker_ok =
        Command::new("docker").args(["version", "--format", "{{.Server.Version}}"]).output().await;
    if !matches!(docker_ok, Ok(out) if out.status.success()) {
        write_chunk(socket, "{\"type\":\"error\",\"message\":\"Docker isn't reachable \u{2014} start Docker Desktop (or the daemon) and try again.\"}\n").await?;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }
    let image_ok = Command::new("docker").args(["image", "inspect", &image]).output().await;
    if !matches!(image_ok, Ok(out) if out.status.success()) {
        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"error\",\"message\":\"image '{}' not found \u{2014} build it first: docker build -f Dockerfile.bench -t {} .\"}}\n",
                json_escape(&image),
                json_escape(&image)
            ),
        )
        .await?;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    let value = "x".repeat(value_bytes);
    let host = "127.0.0.1".to_string();

    // Docker's `--memory`/`tmpfs size=` reject fractional unit suffixes
    // (e.g. "0.1g" errors out) — pass whole megabytes instead.
    let mem_limit_mb = ((mem_limit_gb * 1024.0).round() as u64).max(6);
    let disk_limit_mb = ((disk_limit_gb * 1024.0).round() as u64).max(1);
    let name = format!("reflexdb-limit-{}-{}", std::process::id(), port);
    let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;

    write_chunk(
        socket,
        &format!(
            "{{\"type\":\"progress\",\"message\":\"starting container (memory={mem_limit_mb}m, /data tmpfs={disk_limit_mb}m)\u{2026}\"}}\n"
        ),
    )
    .await?;

    let spawned = Command::new("docker")
        .args(["run", "-d", "--name", &name])
        .args(["--memory", &format!("{mem_limit_mb}m"), "--memory-swap", &format!("{mem_limit_mb}m")])
        .args(["--tmpfs", &format!("/data:rw,size={disk_limit_mb}m")])
        .args(["-p", &format!("{port}:{port}")])
        .arg(&image)
        .args(["--port", &port.to_string(), "--dir", "/data", "--appendonly", "yes"])
        .args(["--appendfsync", "everysec"])
        .output()
        .await;
    if !matches!(&spawned, Ok(out) if out.status.success()) {
        let msg = match spawned {
            Ok(out) => String::from_utf8_lossy(&out.stderr).trim().to_string(),
            Err(e) => e.to_string(),
        };
        write_chunk(
            socket,
            &format!("{{\"type\":\"error\",\"message\":\"docker run failed: {}\"}}\n", json_escape(&msg)),
        )
        .await?;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    if !wait_for_server_ready(&host, port, Duration::from_secs(10)).await {
        let still_running = docker_container_running(&name).await;
        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"error\",\"message\":\"container did not become ready in time (still running: {still_running})\"}}\n"
            ),
        )
        .await?;
        let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    write_chunk(socket, &format!("{{\"type\":\"progress\",\"message\":\"writing {value_bytes}-byte values under load\u{2026}\"}}\n")).await?;
    let acked = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer =
        tokio::spawn(run_resource_writer(host.clone(), port, value.clone(), std::sync::Arc::clone(&acked)));

    let deadline = Instant::now() + Duration::from_secs(max_seconds);
    let mut peak_mem_mb = 0.0f64;
    let mut peak_disk_kb = 0u64;
    let mut crashed_early = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(750)).await;
        if let Some(mb) = docker_mem_usage_mb(&name).await {
            peak_mem_mb = peak_mem_mb.max(mb);
        }
        peak_disk_kb = peak_disk_kb.max(docker_dir_usage_kb(&name).await);
        if !docker_container_running(&name).await {
            crashed_early = true;
            break;
        }
        write_chunk(
            socket,
            &format!(
                "{{\"type\":\"progress\",\"message\":\"{} writes acknowledged, ~{:.0} MB resident, ~{} KB on /data\"}}\n",
                acked.load(std::sync::atomic::Ordering::Relaxed),
                peak_mem_mb,
                peak_disk_kb
            ),
        )
        .await?;
    }

    let acknowledged = acked.load(std::sync::atomic::Ordering::Relaxed);
    writer.abort();

    let still_running = docker_container_running(&name).await;
    let still_responsive = if still_running {
        wait_for_server_ready(&host, port, Duration::from_millis(500)).await
    } else {
        false
    };
    let oom_killed = docker_inspect_field(&name, "{{.State.OOMKilled}}").await.as_deref() == Some("true");
    let exit_code = docker_inspect_field(&name, "{{.State.ExitCode}}")
        .await
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(-1);

    let _ = Command::new("docker").args(["stop", "-t", "2", &name]).output().await;
    let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;

    let crashed = crashed_early || !still_running;
    // Both limits are tight on the same container, so a crash could in
    // principle be caused by either — report whichever evidence is
    // actually there instead of guessing from which checkbox was ticked.
    let disk_full = peak_disk_kb as f64 >= disk_limit_mb as f64 * 1024.0 * 0.9;
    let cause = if !crashed {
        "none"
    } else if oom_killed && disk_full {
        "ram+disk"
    } else if oom_killed {
        "ram"
    } else if disk_full {
        "disk"
    } else {
        "unknown"
    };

    write_chunk(
        socket,
        &format!(
            "{{\"type\":\"resource_limit\",\"cause\":\"{cause}\",\"mem_limit_gb\":{mem_limit_gb},\"disk_limit_gb\":{disk_limit_gb},\"acknowledged\":{acknowledged},\"crashed\":{crashed},\"oom_killed\":{oom_killed},\"exit_code\":{exit_code},\"still_running\":{still_running},\"still_responsive\":{still_responsive},\"peak_mem_mb\":{peak_mem_mb:.0},\"peak_disk_kb\":{peak_disk_kb}}}\n"
        ),
    )
    .await?;

    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

/// One competing engine in the shootout. Every engine listens on its
/// container's `6379` — only the host-side port differs — so the harness
/// doesn't need any engine-specific wiring beyond this struct.
struct Engine {
    id: &'static str,
    label: &'static str,
    image: &'static str,
    /// Extra `docker run` flags, inserted *before* the image name (e.g.
    /// Dragonfly's `--ulimit memlock=-1`) — these configure the container
    /// itself, not the process running inside it, so they can't go after
    /// the image name the way a CMD override does.
    docker_flags: &'static [&'static str],
    /// Overrides the image's default CMD, inserted *after* the image name
    /// (e.g. disabling Redis's persistence) — passed straight to the
    /// containerized process's own argv, not to `docker run`.
    container_args: &'static [&'static str],
}

const SHOOTOUT_ENGINES: &[Engine] = &[
    Engine {
        id: "redis",
        label: "Redis",
        image: "redis:7-alpine",
        docker_flags: &[],
        container_args: &["redis-server", "--save", "", "--appendonly", "no"],
    },
    Engine {
        id: "dragonfly",
        label: "Dragonfly",
        image: "docker.dragonflydb.io/dragonflydb/dragonfly:latest",
        docker_flags: &["--ulimit", "memlock=-1"],
        container_args: &[],
    },
    Engine {
        id: "reflexdb",
        label: "ReflexDB",
        image: "reflexdb-bench:latest",
        docker_flags: &[],
        container_args: &[],
    },
];

/// Inserts `"<key>":"<value>",` right after a JSON object line's opening
/// `{` — cheaper than a real JSON parse/re-serialize round trip for
/// tagging lines that are already known to be `{...}` objects (both
/// `benchmarks/run.py`'s `json.dumps` rows and this file's own hand-built
/// `{"type":...}` progress/error rows always are).
fn tag_json_line(line: &str, key: &str, value: &str) -> String {
    match line.find('{') {
        Some(pos) => {
            let mut out = String::with_capacity(line.len() + key.len() + value.len() + 7);
            out.push_str(&line[..=pos]);
            out.push_str(&format!("\"{key}\":\"{value}\","));
            out.push_str(&line[pos + 1..]);
            out
        }
        None => line.to_string(),
    }
}

fn tag_engine_line(line: &str, engine_id: &str) -> String {
    tag_json_line(line, "engine", engine_id)
}

/// Runs the same fixed-workload harness (`benchmarks/run.py`, standard
/// `GET`/`SET`/`INCR` only — no `LIVE`, so every engine actually supports
/// the traffic) against Redis, Dragonfly, and ReflexDB **at the same
/// time**, each in its own container under an identical `--memory`/
/// `--cpus` cap on its own host port. Running them concurrently does mean
/// they share the host's physical CPU/memory bandwidth while all three are
/// active — each container's cgroup limit still caps what *it* can use,
/// but it's no longer "nothing else was running," so treat this as a
/// quick side-by-side, not a lab-grade isolated benchmark. The previous,
/// one-at-a-time design was more isolated but had a real bug: navigating
/// away from this tab mid-run aborted the whole in-flight request, so
/// whichever engines hadn't started yet just never ran. Parallel execution
/// also fixes that — the whole comparison finishes in roughly one engine's
/// run time instead of three, and the frontend no longer cancels this
/// fetch on a tab switch (see `setTab`), so leaving the tab mid-run is
/// safe either way now.
async fn handle_run_shootout(query: Option<&str>, socket: &mut TcpStream) -> std::io::Result<()> {
    let params = query.map(parse_query).unwrap_or_default();
    let text = |name: &str, fallback: &str| params.get(name).cloned().unwrap_or_else(|| fallback.to_string());
    let mem_mb = text("mem_mb", "512").parse::<u64>().unwrap_or(512).clamp(64, 65536);
    let cpus = text("cpus", "1").parse::<f64>().unwrap_or(1.0).clamp(0.1, 64.0);
    let base_port = text("port", "17400").parse::<u16>().unwrap_or(17400).min(65000);
    let seconds = text("seconds", "5").parse::<f64>().unwrap_or(5.0).clamp(1.0, 60.0);
    let runs = text("runs", "1").parse::<usize>().unwrap_or(1).clamp(1, 10);
    let clients = text("clients", "16").parse::<usize>().unwrap_or(16).clamp(1, 512);
    let keys = text("keys", "1000").parse::<usize>().unwrap_or(1000).clamp(1, 100_000);
    let pipelines = text("pipeline", "1,16");
    let valid_pipelines =
        pipelines.split(',').all(|part| part.trim().parse::<usize>().is_ok_and(|n| (1..=1024).contains(&n)))
            && pipelines.split(',').count() <= 4;
    let workload_text = text("workloads", "small-mixed,hot-key");
    let workloads: Vec<String> = workload_text
        .split(',')
        .map(str::trim)
        .filter(|w| matches!(*w, "small-mixed" | "hot-key"))
        .map(str::to_string)
        .collect();
    let engine_text = text("engines", "redis,dragonfly,reflexdb");
    let engines: Vec<&'static Engine> = engine_text
        .split(',')
        .map(str::trim)
        .filter_map(|id| SHOOTOUT_ENGINES.iter().find(|e| e.id == id))
        .collect();
    if !valid_pipelines || workloads.is_empty() || engines.is_empty() {
        socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await?;
        return Ok(());
    }

    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;

    let docker_ok =
        Command::new("docker").args(["version", "--format", "{{.Server.Version}}"]).output().await;
    if !matches!(docker_ok, Ok(out) if out.status.success()) {
        write_chunk(socket, "{\"type\":\"error\",\"message\":\"Docker isn't reachable \u{2014} start Docker Desktop (or the daemon) and try again.\"}\n").await?;
        socket.write_all(b"0\r\n\r\n").await?;
        return Ok(());
    }

    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/run.py");
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python3".to_string());
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // One task per engine, each on its own host port so they can bind
    // concurrently — fans results in through `tx` as they're produced
    // rather than writing to the socket directly, since multiple tasks
    // can't share one `TcpStream` without interleaving corrupted chunks.
    let mut handles = Vec::with_capacity(engines.len());
    for (i, engine) in engines.into_iter().enumerate() {
        let tx = tx.clone();
        let script = script.clone();
        let python = python.clone();
        let pipelines = pipelines.clone();
        let workloads = workloads.join(",");
        let port = base_port.saturating_add(i as u16);
        handles.push(tokio::spawn(async move {
            run_shootout_engine(ShootoutEngineRun {
                tx,
                engine,
                port,
                mem_mb,
                cpus,
                seconds,
                runs,
                clients,
                keys,
                pipelines,
                workloads,
                script,
                python,
            })
            .await;
        }));
    }
    drop(tx);

    while let Some(line) = rx.recv().await {
        if write_chunk(socket, &line).await.is_err() {
            // Client actually gone (not just a tab switch — the frontend
            // no longer aborts this fetch for that): stop waiting on the
            // remaining engines. Each one's own `finally`-equivalent still
            // runs (it's not aborted, just no longer being awaited here),
            // so in-flight containers still get cleaned up; we just don't
            // delay the HTTP response for it.
            for handle in handles {
                handle.abort();
            }
            return Ok(());
        }
    }
    for handle in handles {
        let _ = handle.await;
    }

    socket.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

struct ShootoutEngineRun {
    tx: mpsc::UnboundedSender<String>,
    engine: &'static Engine,
    port: u16,
    mem_mb: u64,
    cpus: f64,
    seconds: f64,
    runs: usize,
    clients: usize,
    keys: usize,
    pipelines: String,
    workloads: String,
    script: std::path::PathBuf,
    python: String,
}

/// One engine's full lifecycle — pull/check the image, run its container,
/// wait for it to accept connections, run the benchmark harness against
/// it, and clean the container up — sending every line it would otherwise
/// have written to the HTTP response through `tx` instead, tagged with
/// this engine's id so the frontend can route it to the right chart.
/// Independent of every other engine's run: failure here just stops this
/// one engine's lines, not the whole comparison.
async fn run_shootout_engine(run: ShootoutEngineRun) {
    let ShootoutEngineRun {
        tx,
        engine,
        port,
        mem_mb,
        cpus,
        seconds,
        runs,
        clients,
        keys,
        pipelines,
        workloads,
        script,
        python,
    } = run;
    let send = |line: String| {
        let _ = tx.send(line);
    };
    let host = "127.0.0.1".to_string();

    let image_ok = Command::new("docker").args(["image", "inspect", engine.image]).output().await;
    if !matches!(image_ok, Ok(out) if out.status.success()) {
        // ReflexDB's image has to be built from this checkout — there's
        // nowhere to pull it from. Redis/Dragonfly are published images,
        // so pull them on the spot instead of making the user leave the
        // UI to run a command by hand.
        if engine.id == "reflexdb" {
            send(tag_engine_line(
                "{\"type\":\"error\",\"message\":\"image 'reflexdb-bench:latest' not found \u{2014} run: docker build -f Dockerfile.bench -t reflexdb-bench:latest .\"}\n",
                engine.id,
            ));
            return;
        }
        send(tag_engine_line(
            &format!(
                "{{\"type\":\"progress\",\"message\":\"Pulling {} (first run only, may take a minute)\u{2026}\"}}\n",
                engine.image
            ),
            engine.id,
        ));
        let pulled = Command::new("docker").args(["pull", engine.image]).output().await;
        if !matches!(&pulled, Ok(out) if out.status.success()) {
            let msg = match pulled {
                Ok(out) => String::from_utf8_lossy(&out.stderr).trim().to_string(),
                Err(e) => e.to_string(),
            };
            send(tag_engine_line(
                &format!(
                    "{{\"type\":\"error\",\"message\":\"docker pull {} failed: {}\"}}\n",
                    json_escape(engine.image),
                    json_escape(&msg)
                ),
                engine.id,
            ));
            return;
        }
    }

    let name = format!("reflexdb-shootout-{}-{}-{port}", std::process::id(), engine.id);
    let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;

    send(tag_engine_line(
        &format!(
            "{{\"type\":\"progress\",\"message\":\"Starting {} on port {port} (memory={mem_mb}m, cpus={cpus})\u{2026}\"}}\n",
            engine.label
        ),
        engine.id,
    ));

    let mut cmd = Command::new("docker");
    cmd.args(["run", "-d", "--name", &name])
        .args(["--memory", &format!("{mem_mb}m"), "--cpus", &cpus.to_string()])
        .args(["-p", &format!("{port}:6379")])
        .args(engine.docker_flags)
        .arg(engine.image)
        .args(engine.container_args);
    // Dragonfly sizes its io-thread pool to the cgroup cpu count by default,
    // then refuses to start unless there's ~256MiB of the memory cap per
    // thread (independent of `--memory` itself) — e.g. `--cpus 4` with the
    // default 512MiB demands 1GiB and exits immediately, which otherwise
    // just looks like a silent 15s readiness timeout. Cap its thread count
    // to what the memory budget can actually support (with some margin —
    // the check is against cgroup-available memory, which is a bit less
    // than the raw `--memory` value).
    if engine.id == "dragonfly" {
        let threads = ((mem_mb / 300).max(1)).min(cpus.floor().max(1.0) as u64);
        cmd.arg(format!("--proactor_threads={threads}"));
    }
    let spawned = cmd.output().await;
    if !matches!(&spawned, Ok(out) if out.status.success()) {
        let msg = match spawned {
            Ok(out) => String::from_utf8_lossy(&out.stderr).trim().to_string(),
            Err(e) => e.to_string(),
        };
        send(tag_engine_line(
            &format!("{{\"type\":\"error\",\"message\":\"docker run failed: {}\"}}\n", json_escape(&msg)),
            engine.id,
        ));
        let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;
        return;
    }

    if !wait_for_server_ready(&host, port, Duration::from_secs(15)).await {
        // Surface the container's own log tail instead of a bare timeout —
        // e.g. this is also where Dragonfly's "N threads, so XGiB required"
        // startup check would show up if the margin above wasn't enough.
        let logs = Command::new("docker").args(["logs", "--tail", "5", &name]).output().await;
        let detail = match logs {
            Ok(out) if !out.stdout.is_empty() || !out.stderr.is_empty() => {
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                format!(" \u{2014} {}", combined.trim().replace('\n', " / "))
            }
            _ => String::new(),
        };
        send(tag_engine_line(
            &format!(
                "{{\"type\":\"error\",\"message\":\"container did not become ready in time{}\"}}\n",
                json_escape(&detail)
            ),
            engine.id,
        ));
        let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;
        return;
    }

    let mut child = match Command::new(&python)
        .arg(&script)
        .args(["--host", &host, "--port", &port.to_string(), "--runs", &runs.to_string()])
        .args(["--seconds", &seconds.to_string(), "--clients", &clients.to_string()])
        .args(["--pipeline", &pipelines, "--keys", &keys.to_string()])
        .args(["--workloads", &workloads])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            send(tag_engine_line(
                &format!(
                    "{{\"type\":\"error\",\"message\":\"could not start benchmark harness: {}\"}}\n",
                    json_escape(&error.to_string())
                ),
                engine.id,
            ));
            let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;
            return;
        }
    };
    if let Some(stdout) = child.stdout.take() {
        let mut reader = tokio::io::BufReader::new(stdout);
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = match tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line).await {
                Ok(n) => n,
                Err(_) => break,
            };
            if read == 0 {
                break;
            }
            send(tag_engine_line(&String::from_utf8_lossy(&line), engine.id));
        }
    }
    let _ = child.wait().await;
    let _ = Command::new("docker").args(["stop", "-t", "2", &name]).output().await;
    let _ = Command::new("docker").args(["rm", "-f", &name]).output().await;
}

// ---------------- Studio API (key browser / history / health / slowlog / LIVE) ----------------

const STUDIO_SCAN_PAGE: usize = 200;
const STUDIO_INSPECT_LIMIT: usize = 200;

fn studio_target(params: &HashMap<String, String>) -> (String, u16) {
    let host = params
        .get("host")
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = params.get("port").and_then(|s| s.parse::<u16>().ok()).unwrap_or(7379);
    (host, port)
}

async fn http_json(socket: &mut TcpStream, status: &str, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await
}

fn studio_err_json(msg: impl AsRef<str>) -> String {
    format!("{{\"error\":\"{}\"}}", json_escape(msg.as_ref()))
}

fn studio_reply_str(r: &Reply) -> String {
    match r {
        Reply::Simple(s) | Reply::Error(s) => s.clone(),
        Reply::Integer(i) => i.to_string(),
        Reply::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Reply::Bulk(None) => String::new(),
        Reply::Array(_) => format!("{r:?}"),
    }
}

async fn studio_connect(params: &HashMap<String, String>) -> Result<RespClient, std::io::Error> {
    let (h, p) = studio_target(params);
    RespClient::connect(&h, p).await
}

fn studio_int_reply(r: &std::io::Result<Reply>) -> i64 {
    match r {
        Ok(Reply::Integer(n)) => *n,
        _ => 0,
    }
}

fn studio_items_json(r: &std::io::Result<Reply>) -> String {
    match r {
        Ok(Reply::Array(Some(items))) => {
            let inner: Vec<String> = items.iter().map(reply_to_json_value).collect();
            format!("[{}]", inner.join(","))
        }
        Ok(Reply::Error(e)) => format!("{{\"error\":\"{}\"}}", json_escape(e)),
        _ => "[]".into(),
    }
}

fn studio_pairs_json(r: &std::io::Result<Reply>) -> String {
    match r {
        Ok(Reply::Array(Some(flat))) => {
            let mut pairs = Vec::with_capacity(flat.len() / 2);
            for pair in flat.chunks(2) {
                let f = pair.first().map(reply_to_json_value).unwrap_or_else(|| "null".into());
                let v = pair.get(1).map(reply_to_json_value).unwrap_or_else(|| "null".into());
                pairs.push(format!("[{f},{v}]"));
            }
            format!("[{}]", pairs.join(","))
        }
        _ => "[]".into(),
    }
}

fn studio_stream_json(r: &std::io::Result<Reply>) -> String {
    match r {
        Ok(Reply::Array(Some(entries))) => {
            let mut out = Vec::with_capacity(entries.len());
            for e in entries {
                if let Reply::Array(Some(pair)) = e {
                    let id = pair.first().map(reply_to_json_value).unwrap_or_else(|| "null".into());
                    let fields = match pair.get(1) {
                        Some(Reply::Array(Some(flat))) => {
                            let mut obj = Vec::with_capacity(flat.len() / 2);
                            for p in flat.chunks(2) {
                                let f = p.first().map(studio_reply_str).unwrap_or_default();
                                let v = p.get(1).map(reply_to_json_value).unwrap_or_else(|| "null".into());
                                obj.push(format!("\"{}\":{v}", json_escape(&f)));
                            }
                            format!("{{{}}}", obj.join(","))
                        }
                        _ => "{}".into(),
                    };
                    out.push(format!("[{id},{fields}]"));
                }
            }
            format!("[{}]", out.join(","))
        }
        _ => "[]".into(),
    }
}

async fn api_keys(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let pattern = params.get("pattern").cloned().unwrap_or_else(|| "*".into());
    let cursor = params.get("cursor").cloned().unwrap_or_else(|| "0".into());
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let reply = c.call(&["SCAN", &cursor, "MATCH", &pattern, "COUNT", &STUDIO_SCAN_PAGE.to_string()]).await;
    let (next, keys) = match reply {
        Ok(Reply::Array(Some(items))) if items.len() == 2 => (
            studio_reply_str(&items[0]),
            match &items[1] {
                Reply::Array(Some(k)) => k.iter().map(studio_reply_str).collect::<Vec<String>>(),
                _ => vec![],
            },
        ),
        Ok(Reply::Error(e)) => return http_json(socket, "200 OK", &studio_err_json(e)).await,
        _ => return http_json(socket, "502 Bad Gateway", &studio_err_json("bad SCAN reply")).await,
    };
    // TYPE+PTTL+MEMORY per key, pipelined — one write burst instead of
    // round-trips per key (a 500-key page on a busy server would otherwise
    // stall for seconds). A second burst fetches the first 64 bytes of
    // string keys to refine the type (int/float/bool/json).
    let mut cmds: Vec<Vec<&str>> = Vec::with_capacity(keys.len() * 3);
    for key in &keys {
        cmds.push(vec!["TYPE", key]);
        cmds.push(vec!["PTTL", key]);
        cmds.push(vec!["MEMORY", "USAGE", key]);
    }
    let replies = match c.call_many(&cmds).await {
        Ok(r) => r,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let string_idx: Vec<usize> = keys
        .iter()
        .enumerate()
        .filter(|(i, _)| matches!(replies.get(i * 3), Some(Reply::Simple(t)) if t == "string"))
        .map(|(i, _)| i)
        .collect();
    let get_cmds: Vec<Vec<&str>> =
        string_idx.iter().map(|&i| vec!["GETRANGE", keys[i].as_str(), "0", "63"]).collect();
    let string_replies = match c.call_many(&get_cmds).await {
        Ok(r) => r,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let classify = |s: &str| -> &str {
        let t = s.trim();
        if t == "true" || t == "false" {
            "bool"
        } else if t.parse::<i64>().is_ok() {
            "int"
        } else if t.parse::<f64>().is_ok() {
            "float"
        } else if t.starts_with('{') || t.starts_with('[') {
            "json"
        } else {
            "string"
        }
    };
    let mut rows = Vec::with_capacity(keys.len());
    let mut si = 0;
    for (i, key) in keys.iter().enumerate() {
        let mut ty = match replies.get(i * 3) {
            Some(Reply::Simple(s)) => s.clone(),
            _ => "?".into(),
        };
        let ttl = match replies.get(i * 3 + 1) {
            Some(Reply::Integer(ms)) => *ms,
            _ => -2,
        };
        let mem = match replies.get(i * 3 + 2) {
            Some(Reply::Integer(b)) => b.to_string(),
            _ => "null".into(),
        };
        if ty == "string" {
            if let Some(Reply::Bulk(Some(b))) = string_replies.get(si) {
                ty = classify(&String::from_utf8_lossy(b)).to_string();
            }
            si += 1;
        }
        rows.push(format!(
            "{{\"key\":\"{}\",\"type\":\"{}\",\"ttl_ms\":{ttl},\"mem_bytes\":{mem}}}",
            json_escape(key),
            json_escape(&ty)
        ));
    }
    http_json(
        socket,
        "200 OK",
        &format!("{{\"cursor\":\"{}\",\"keys\":[{}]}}", json_escape(&next), rows.join(",")),
    )
    .await
}

async fn api_key(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let Some(name) = params.get("name") else {
        return http_json(socket, "400 Bad Request", &studio_err_json("missing name")).await;
    };
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let ty = c.call(&["TYPE", name]).await.map(|r| studio_reply_str(&r)).unwrap_or_else(|_| "none".into());
    if ty == "none" {
        return http_json(socket, "200 OK", &studio_err_json("key not found")).await;
    }
    let ttl = match c.call(&["PTTL", name]).await {
        Ok(Reply::Integer(ms)) => ms,
        _ => -2,
    };
    let mem = match c.call(&["MEMORY", "USAGE", name]).await {
        Ok(Reply::Integer(b)) => b.to_string(),
        _ => "null".into(),
    };
    let lim = STUDIO_INSPECT_LIMIT.to_string();
    let (value_json, truncated, total) = match ty.as_str() {
        "string" => match c.call(&["GET", name]).await {
            Ok(Reply::Bulk(Some(b))) => {
                let s = String::from_utf8_lossy(&b);
                (format!("\"{}\"", json_escape(&s)), false, 0)
            }
            Ok(Reply::Error(e)) => return http_json(socket, "200 OK", &studio_err_json(e)).await,
            _ => ("null".into(), false, 0),
        },
        "hash" => (studio_pairs_json(&c.call(&["HGETALL", name]).await), false, 0),
        "list" => {
            let len = studio_int_reply(&c.call(&["LLEN", name]).await);
            let items = c.call(&["LRANGE", name, "0", &(STUDIO_INSPECT_LIMIT - 1).to_string()]).await;
            (studio_items_json(&items), len > STUDIO_INSPECT_LIMIT as i64, len.max(0) as usize)
        }
        "set" => {
            let len = studio_int_reply(&c.call(&["SCARD", name]).await);
            let items = c.call(&["SRANDMEMBER", name, &lim]).await;
            (studio_items_json(&items), len > STUDIO_INSPECT_LIMIT as i64, len.max(0) as usize)
        }
        "zset" => {
            let len = studio_int_reply(&c.call(&["ZCARD", name]).await);
            let items =
                c.call(&["ZRANGE", name, "0", &(STUDIO_INSPECT_LIMIT - 1).to_string(), "WITHSCORES"]).await;
            (studio_pairs_json(&items), len > STUDIO_INSPECT_LIMIT as i64, len.max(0) as usize)
        }
        "stream" => {
            let len = studio_int_reply(&c.call(&["XLEN", name]).await);
            let items = c.call(&["XRANGE", name, "-", "+", "COUNT", "50"]).await;
            (studio_stream_json(&items), len > 50, len.max(0) as usize)
        }
        other => (format!("\"unsupported type {other}\""), false, 0),
    };
    let body = format!(
        "{{\"type\":\"{}\",\"ttl_ms\":{ttl},\"memory_bytes\":{mem},\"value\":{value_json},\"truncated\":{truncated},\"total\":{total}}}",
        json_escape(&ty)
    );
    http_json(socket, "200 OK", &body).await
}

async fn api_history(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let Some(name) = params.get("name") else {
        return http_json(socket, "400 Bad Request", &studio_err_json("missing name")).await;
    };
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&["HISTORY", name, "LIMIT", "500"]).await {
        Ok(Reply::Error(e)) if e.contains("history is disabled") => {
            http_json(socket, "200 OK", "{\"enabled\":false,\"entries\":[]}").await
        }
        Ok(Reply::Error(e)) => http_json(socket, "200 OK", &studio_err_json(e)).await,
        Ok(Reply::Array(Some(items))) => {
            let mut entries = Vec::with_capacity(items.len());
            for item in items {
                if let Reply::Array(Some(pair)) = &item {
                    let ts = match pair.first() {
                        Some(Reply::Integer(t)) => *t,
                        _ => 0,
                    };
                    let v = pair.get(1).map(reply_to_json_value).unwrap_or_else(|| "null".into());
                    entries.push(format!("{{\"ts\":{ts},\"value\":{v}}}"));
                }
            }
            http_json(socket, "200 OK", &format!("{{\"enabled\":true,\"entries\":[{}]}}", entries.join(",")))
                .await
        }
        _ => http_json(socket, "502 Bad Gateway", &studio_err_json("bad HISTORY reply")).await,
    }
}

async fn api_info(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let raw = match c.call(&["INFO"]).await {
        Ok(r) => studio_reply_str(&r),
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let mut fields = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            fields.push(format!("\"{}\":\"{}\"", json_escape(k), json_escape(v)));
        }
    }
    let slowlog_len = match c.call(&["SLOWLOG", "LEN"]).await {
        Ok(Reply::Integer(n)) => n.to_string(),
        _ => "null".into(),
    };
    let history_enabled = match c.call(&["HISTORY", "__studio_probe__", "LIMIT", "1"]).await {
        Ok(Reply::Error(e)) if e.contains("disabled") => false,
        Ok(_) => true,
        Err(_) => false,
    };
    let body = format!(
        "{{\"info\":{{{}}},\"slowlog_len\":{slowlog_len},\"history_enabled\":{history_enabled},\"raw\":\"{}\"}}",
        fields.join(","),
        json_escape(&raw)
    );
    http_json(socket, "200 OK", &body).await
}

async fn api_slowlog(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&["SLOWLOG", "GET", "50"]).await {
        Ok(Reply::Array(Some(items))) => {
            let mut entries = Vec::with_capacity(items.len());
            for item in items {
                if let Reply::Array(Some(f)) = &item {
                    let id = match f.first() {
                        Some(Reply::Integer(n)) => *n,
                        _ => 0,
                    };
                    let ts = match f.get(1) {
                        Some(Reply::Integer(n)) => *n,
                        _ => 0,
                    };
                    let dur = match f.get(2) {
                        Some(Reply::Integer(n)) => *n,
                        _ => 0,
                    };
                    let cmd = match f.get(3) {
                        Some(Reply::Array(Some(args))) => {
                            args.iter().map(studio_reply_str).collect::<Vec<_>>().join(" ")
                        }
                        _ => String::new(),
                    };
                    entries.push(format!(
                        "{{\"id\":{id},\"ts\":{ts},\"duration_us\":{dur},\"command\":\"{}\"}}",
                        json_escape(&cmd)
                    ));
                }
            }
            http_json(socket, "200 OK", &format!("{{\"entries\":[{}]}}", entries.join(","))).await
        }
        Ok(Reply::Error(e)) => http_json(socket, "200 OK", &studio_err_json(e)).await,
        _ => http_json(socket, "502 Bad Gateway", &studio_err_json("bad SLOWLOG reply")).await,
    }
}

/// `/api/config` — `CONFIG GET *` flattened to `{"values":{"name":"val"}}`.
async fn api_config(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&["CONFIG", "GET", "*"]).await {
        Ok(Reply::Array(Some(flat))) => {
            let mut pairs = Vec::with_capacity(flat.len() / 2);
            for pair in flat.chunks(2) {
                let name = pair.first().map(studio_reply_str).unwrap_or_default();
                let value = pair.get(1).map(studio_reply_str).unwrap_or_default();
                pairs.push(format!("\"{}\":\"{}\"", json_escape(&name), json_escape(&value)));
            }
            http_json(socket, "200 OK", &format!("{{\"values\":{{{}}}}}", pairs.join(","))).await
        }
        Ok(Reply::Error(e)) => http_json(socket, "200 OK", &studio_err_json(e)).await,
        _ => http_json(socket, "502 Bad Gateway", &studio_err_json("bad CONFIG GET reply")).await,
    }
}

/// `/api/config-set?name=<param>&value=<val>` — `CONFIG SET name value`.
async fn api_config_set(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let (Some(name), Some(value)) = (params.get("name"), params.get("value")) else {
        return http_json(socket, "400 Bad Request", &studio_err_json("missing name/value")).await;
    };
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&["CONFIG", "SET", name, value]).await {
        Ok(Reply::Simple(_)) => http_json(socket, "200 OK", "{\"ok\":true}").await,
        Ok(Reply::Error(e)) => http_json(socket, "200 OK", &studio_err_json(e)).await,
        _ => http_json(socket, "502 Bad Gateway", &studio_err_json("bad CONFIG SET reply")).await,
    }
}

/// `/api/commands` — the server's live command list via `COMMAND`.
async fn api_commands(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&["COMMAND"]).await {
        Ok(Reply::Array(Some(items))) => {
            let names: Vec<String> = items
                .iter()
                .filter_map(|item| match item {
                    Reply::Array(Some(entry)) => entry
                        .first()
                        .map(|n| format!("\"{}\"", json_escape(&studio_reply_str(n).to_uppercase()))),
                    _ => None,
                })
                .collect();
            http_json(socket, "200 OK", &format!("{{\"commands\":[{}]}}", names.join(","))).await
        }
        Ok(Reply::Error(e)) => http_json(socket, "200 OK", &studio_err_json(e)).await,
        _ => http_json(socket, "502 Bad Gateway", &studio_err_json("bad COMMAND reply")).await,
    }
}

/// `/api/exec?cmd=<words>` — run one command, reply as JSON. Args are
/// whitespace-split (no quoting) — a playground, not a shell.
async fn api_exec(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let Some(cmd) = params.get("cmd") else {
        return http_json(socket, "400 Bad Request", &studio_err_json("missing cmd")).await;
    };
    let words: Vec<&str> = cmd.split_whitespace().collect();
    if words.is_empty() {
        return http_json(socket, "400 Bad Request", &studio_err_json("empty cmd")).await;
    }
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    match c.call(&words).await {
        Ok(r) => http_json(socket, "200 OK", &format!("{{\"reply\":{}}}", reply_to_json_value(&r))).await,
        Err(e) => http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    }
}

/// `/api/live?query=<words>&host=..&port=..` — `LIVE <words...>` on a
/// dedicated connection, each pushed frame streamed as one JSON line.
async fn api_live(socket: &mut TcpStream, params: &HashMap<String, String>) -> std::io::Result<()> {
    let Some(query) = params.get("query") else {
        return http_json(socket, "400 Bad Request", &studio_err_json("missing query")).await;
    };
    let words: Vec<&str> = query.split_whitespace().collect();
    if words.is_empty() {
        return http_json(socket, "400 Bad Request", &studio_err_json("empty query")).await;
    }
    let mut c = match studio_connect(params).await {
        Ok(c) => c,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
    };
    let mut args = vec!["LIVE"];
    args.extend(words.iter());
    let first = match c.call(&args).await {
        Ok(Reply::Error(e)) => return http_json(socket, "200 OK", &studio_err_json(e)).await,
        Err(e) => return http_json(socket, "502 Bad Gateway", &studio_err_json(e.to_string())).await,
        Ok(r) => r,
    };
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await?;
    // The subscribe reply IS the first `live` frame (current value) —
    // forward it so the viewer isn't blank until the next write.
    let line = format!("{{\"frame\":{}}}\n", reply_to_json_value(&first));
    let chunk = format!("{:x}\r\n{}\r\n", line.len(), line);
    if socket.write_all(chunk.as_bytes()).await.is_err() {
        return Ok(());
    }
    while let Ok(frame) = c.read_next().await {
        let line = format!("{{\"frame\":{}}}\n", reply_to_json_value(&frame));
        let chunk = format!("{:x}\r\n{}\r\n", line.len(), line);
        if socket.write_all(chunk.as_bytes()).await.is_err() {
            break;
        }
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Public sandbox pool (`BENCH_UI_PUBLIC=1`): N small reflexdb-server
// instances spawned at startup; each visitor's session claims one, the
// instance is wiped (FLUSHALL) and returned when the session ends or goes
// idle. Sessions are recorded in SQLite (who had what, when). The pool
// pins every proxied call to the session's own instance — host/port query
// params are ignored and `/run-*` spawn endpoints are closed.
// ---------------------------------------------------------------------------

struct PoolInstance {
    port: u16,
    _dir: std::path::PathBuf,
    _child: tokio::process::Child,
    free: bool,
}

struct Pool {
    instances: Vec<PoolInstance>,
    sessions: HashMap<String, usize>, // token -> instance index
    started: HashMap<String, i64>,
    last_seen: HashMap<String, i64>,
    session_ip: HashMap<String, String>,
    db: rusqlite::Connection,
    idle_s: i64,
}

static POOL: std::sync::OnceLock<std::sync::Arc<tokio::sync::Mutex<Pool>>> = std::sync::OnceLock::new();

fn now_s() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn new_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..24).map(|_| std::char::from_digit(rng.gen_range(0..16), 16).unwrap()).collect()
}

async fn wait_ready(port: u16, tries: usize) -> bool {
    for _ in 0..tries {
        if let Ok(mut c) = RespClient::connect("127.0.0.1", port).await {
            if c.call(&["PING"]).await.is_ok() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    false
}

async fn pool_spawn() -> std::io::Result<()> {
    let size: usize = std::env::var("BENCH_UI_POOL_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let base: u16 =
        std::env::var("BENCH_UI_POOL_BASE_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(21100);
    let idle_s: i64 =
        std::env::var("BENCH_UI_SESSION_IDLE_S").ok().and_then(|s| s.parse().ok()).unwrap_or(1200);
    let maxmem = std::env::var("BENCH_UI_POOL_MAXMEM").unwrap_or_else(|_| "32mb".into());
    let db_path = std::env::var("BENCH_UI_SESSION_DB")
        .unwrap_or_else(|_| std::env::temp_dir().join("reflexdb-bench-sessions.db").display().to_string());
    let server = reflexdb_server_bin();

    let db = rusqlite::Connection::open(&db_path).map_err(|e| std::io::Error::other(e.to_string()))?;
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS sessions(
            token TEXT PRIMARY KEY, ip TEXT, port INTEGER,
            started INTEGER, last_seen INTEGER, ended INTEGER);",
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    let mut instances = Vec::new();
    for i in 0..size {
        let port = base + i as u16;
        let dir = std::env::temp_dir().join(format!("rdb-pool-{}-{i}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let child = Command::new(&server)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--dir",
                &dir.display().to_string(),
                "--appendonly",
                "no",
                "--maxmemory",
                &maxmem,
            ])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        instances.push(PoolInstance { port, _dir: dir, _child: child, free: true });
    }
    for inst in &instances {
        if !wait_ready(inst.port, 30).await {
            warn!("pool instance on port {} not ready", inst.port);
        }
    }
    info!(
        "sandbox pool: {} instances on ports {}-{} (idle {}s)",
        instances.len(),
        base,
        base + size as u16 - 1,
        idle_s
    );
    POOL.set(std::sync::Arc::new(tokio::sync::Mutex::new(Pool {
        instances,
        sessions: HashMap::new(),
        started: HashMap::new(),
        last_seen: HashMap::new(),
        session_ip: HashMap::new(),
        db,
        idle_s,
    })))
    .map_err(|_| std::io::Error::other("pool already set"))?;
    Ok(())
}

/// Allocate a free instance to a new token; `None` when the pool is full.
fn pool_allocate(pool: &mut Pool, ip: &str) -> Option<String> {
    let idx = pool.instances.iter().position(|i| i.free)?;
    let token = new_token();
    pool.instances[idx].free = false;
    pool.sessions.insert(token.clone(), idx);
    let now = now_s();
    pool.started.insert(token.clone(), now);
    pool.last_seen.insert(token.clone(), now);
    pool.session_ip.insert(token.clone(), ip.to_string());
    let port = pool.instances[idx].port as i64;
    let _ = pool.db.execute(
        "INSERT INTO sessions(token,ip,port,started,last_seen) VALUES(?1,?2,?3,?4,?4)",
        rusqlite::params![token, ip, port, now],
    );
    Some(token)
}

/// Wipe the instance and mark the session ended — the slot goes back to
/// the pool for the next visitor.
async fn pool_release(pool: &mut Pool, token: &str) {
    let Some(idx) = pool.sessions.remove(token) else { return };
    let port = pool.instances[idx].port;
    if let Ok(mut c) = RespClient::connect("127.0.0.1", port).await {
        let _ = c.call(&["FLUSHALL"]).await;
    }
    pool.instances[idx].free = true;
    let now = now_s();
    let _ = pool.db.execute("UPDATE sessions SET ended=?1 WHERE token=?2", rusqlite::params![now, token]);
    pool.started.remove(token);
    pool.last_seen.remove(token);
    pool.session_ip.remove(token);
}

async fn pool_reaper() {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let Some(pool) = POOL.get() else { return };
        let mut pool = pool.lock().await;
        let cutoff = now_s() - pool.idle_s;
        let expired: Vec<String> =
            pool.last_seen.iter().filter(|(_, &t)| t < cutoff).map(|(t, _)| t.clone()).collect();
        for token in expired {
            info!("session expired (idle) — instance recycled");
            pool_release(&mut pool, &token).await;
        }
    }
}

fn session_cookie(request: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(request);
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("Cookie:").or_else(|| line.strip_prefix("cookie:")) {
            for part in v.split(';') {
                let part = part.trim();
                if let Some(t) = part.strip_prefix("rdb_sess=") {
                    return Some(t.to_string());
                }
            }
        }
    }
    None
}

async fn handle_session(socket: &mut TcpStream, request: &[u8], method: &str) -> std::io::Result<()> {
    let Some(pool_arc) = POOL.get() else {
        return http_json(socket, "404 Not Found", &studio_err_json("sandbox pool disabled")).await;
    };
    let mut pool = pool_arc.lock().await;
    let peer_ip = socket.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
    if method == "POST" {
        if let Some(token) = session_cookie(request) {
            if pool.sessions.contains_key(&token) {
                let port = pool.sessions[&token] as i64;
                pool.last_seen.insert(token.clone(), now_s());
                let body = format!(
                    "{{\"session\":\"{}\",\"state\":\"active\",\"instance\":{port}}}",
                    json_escape(&token)
                );
                return http_json(socket, "200 OK", &body).await;
            }
        }
        match pool_allocate(&mut pool, &peer_ip) {
            Some(token) => {
                let port = pool.sessions[&token] as i64;
                let body = format!(
                    "{{\"session\":\"{}\",\"state\":\"active\",\"instance\":{port},\"idle_s\":{}}}",
                    json_escape(&token),
                    pool.idle_s
                );
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nSet-Cookie: rdb_sess={token}; Path=/; SameSite=Lax\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len());
                socket.write_all(head.as_bytes()).await?;
                return socket.write_all(body.as_bytes()).await;
            }
            None => {
                return http_json(
                    socket,
                    "503 Service Unavailable",
                    &studio_err_json("all sandboxes busy — try again later"),
                )
                .await
            }
        }
    }
    if method == "DELETE" || method == "POST" {
        if let Some(token) = session_cookie(request) {
            pool_release(&mut pool, &token).await;
            return http_json(socket, "200 OK", "{\"state\":\"released\"}").await;
        }
    }
    // GET — session status
    match session_cookie(request).and_then(|t| pool.sessions.get(&t).map(|i| (t, i))) {
        Some((token, idx)) => {
            let port = pool.instances[*idx].port;
            let idle = pool.idle_s - (now_s() - pool.last_seen.get(&token).copied().unwrap_or(0));
            http_json(
                socket,
                "200 OK",
                &format!("{{\"state\":\"active\",\"instance\":{port},\"expires_in_s\":{idle}}}"),
            )
            .await
        }
        None => {
            let free = pool.instances.iter().filter(|i| i.free).count();
            http_json(socket, "200 OK", &format!("{{\"state\":\"none\",\"free_sandboxes\":{free}}}")).await
        }
    }
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

    // Public sandbox mode: session endpoints, target pinning and lockout
    // of the local spawn/benchmark routes.
    if POOL.get().is_some() {
        if path == "/api/session" {
            return handle_session(socket, &request, method).await;
        }
        if path.starts_with("/run-") {
            return http_json(
                socket,
                "403 Forbidden",
                &studio_err_json("benchmark spawning is disabled in public sandbox mode"),
            )
            .await;
        }
        let needs_target = path.starts_with("/api/")
            || path.starts_with("/live-")
            || path == "/tiered-demo"
            || path == "/history-status";
        if needs_target {
            let mut params = query.map(parse_query).unwrap_or_default();
            let Some(pool_arc) = POOL.get() else { unreachable!() };
            let mut pool = pool_arc.lock().await;
            let token = session_cookie(&request).unwrap_or_default();
            match pool.sessions.get(&token).copied() {
                Some(idx) => {
                    pool.last_seen.insert(token.clone(), now_s());
                    let _ = pool.db.execute(
                        "UPDATE sessions SET last_seen=?1 WHERE token=?2",
                        rusqlite::params![now_s(), token],
                    );
                    let port = pool.instances[idx].port;
                    drop(pool);
                    params.insert("host".into(), "127.0.0.1".into());
                    params.insert("port".into(), port.to_string());
                }
                None => {
                    return http_json(
                        socket,
                        "401 Unauthorized",
                        &studio_err_json("no sandbox session — POST /api/session first"),
                    )
                    .await;
                }
            }
            return match (method, path) {
                ("GET", "/api/keys") => api_keys(socket, &params).await,
                ("GET", "/api/key") => api_key(socket, &params).await,
                ("GET", "/api/history") => api_history(socket, &params).await,
                ("GET", "/api/info") => api_info(socket, &params).await,
                ("GET", "/api/slowlog") => api_slowlog(socket, &params).await,
                ("GET", "/api/config") => api_config(socket, &params).await,
                ("GET", "/api/config-set") => api_config_set(socket, &params).await,
                ("GET", "/api/live") => api_live(socket, &params).await,
                ("GET", "/api/commands") => api_commands(socket, &params).await,
                ("GET", "/api/exec") => api_exec(socket, &params).await,
                ("GET", "/live-watch") => handle_live_watch(Some(&params_to_query(&params)), socket).await,
                ("GET", "/live-set") => handle_live_set(Some(&params_to_query(&params)), socket).await,
                ("GET", "/live-autoset") => {
                    handle_live_autoset(Some(&params_to_query(&params)), socket).await
                }
                ("GET", "/tiered-demo") => handle_tiered_demo(Some(&params_to_query(&params)), socket).await,
                ("GET", "/history-status") => {
                    handle_history_status(Some(&params_to_query(&params)), socket).await
                }
                _ => {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").await?;
                    Ok(())
                }
            };
        }
    }

    match (method, path) {
        ("GET", "/") => handle_root(socket).await,
        ("GET", "/logo.png") => handle_logo(socket).await,
        ("GET", "/run-profiled") => handle_run_profiled(query, socket).await,
        ("GET", "/run-latency") => handle_run_latency(query, socket).await,
        ("GET", "/run-durability") => handle_run_durability(query, socket).await,
        ("GET", "/run-resource-limit") => handle_run_resource_limit(query, socket).await,
        ("GET", "/run-shootout") => handle_run_shootout(query, socket).await,
        ("GET", "/run-history-profiled") => handle_run_history_profiled(query, socket).await,
        ("GET", "/history-status") => handle_history_status(query, socket).await,
        ("GET", "/live-watch") => handle_live_watch(query, socket).await,
        ("GET", "/live-set") => handle_live_set(query, socket).await,
        ("GET", "/live-autoset") => handle_live_autoset(query, socket).await,
        ("GET", "/tiered-demo") => handle_tiered_demo(query, socket).await,
        ("GET", "/stats") => handle_stats(socket).await,
        ("GET", "/api/keys") => api_keys(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/key") => api_key(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/history") => api_history(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/info") => api_info(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/slowlog") => api_slowlog(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/config") => api_config(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/config-set") => {
            api_config_set(socket, &query.map(parse_query).unwrap_or_default()).await
        }
        ("GET", "/api/live") => api_live(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/commands") => api_commands(socket, &query.map(parse_query).unwrap_or_default()).await,
        ("GET", "/api/exec") => api_exec(socket, &query.map(parse_query).unwrap_or_default()).await,
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

    if std::env::var("BENCH_UI_PUBLIC").is_ok_and(|v| v == "1") {
        pool_spawn().await?;
        tokio::spawn(pool_reaper());
        info!("public sandbox mode: /api/* pinned to per-session instances; /run-* disabled");
    }

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
