"""
USERNAME HUNTER — Web App + Marketplace v3.0
Работает и как сайт, и как Telegram WebApp
"""

import os, hmac, hashlib, json, time, random, sqlite3, secrets, re
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import unquote, parse_qs
from flask import (
    Flask, render_template_string, request, redirect,
    url_for, session, jsonify, flash, abort, g,
    get_flashed_messages, make_response
)
import requests as http_req

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(32))
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)

BOT_TOKEN = "8703697949:AAFxG1HqkeVWULUi9xuyPGmpuIkGFhcG35A"
BOT_USER = "SworuserN_bot"
DB_PATH = "hunter.db"
ADMIN_IDS = [5969266721, 7894051808]
COMMISSION = 0.10

GAMES = {
    "slots":    {"name": "🎰 Слоты",    "desc": "Крути барабаны!",       "free": 5, "prem": 15, "color": "#e17055",
                 "min_bet": 1, "max_bet": 50},
    "coinflip": {"name": "🪙 Монетка",  "desc": "Орёл или решка?",      "free": 7, "prem": 20, "color": "#fdcb6e",
                 "min_bet": 1, "max_bet": 100},
    "dice":     {"name": "🎲 Кости",    "desc": "Кинь кости на удачу!", "free": 5, "prem": 15, "color": "#6c5ce7",
                 "min_bet": 1, "max_bet": 50},
    "crash":    {"name": "📈 Краш",     "desc": "Забери до краша!",      "free": 4, "prem": 12, "color": "#00cec9",
                 "min_bet": 2, "max_bet": 200},
    "mines":    {"name": "💣 Мины",     "desc": "Не наступи на мину!",  "free": 4, "prem": 12, "color": "#e84393",
                 "min_bet": 1, "max_bet": 100},
}

ATTEMPT_PRICES = {5: 8, 10: 14, 25: 30, 50: 55, 100: 100}

ATTEMPT_PRICES = {3: 15, 5: 22, 10: 40, 25: 85, 50: 150, 100: 270}

CATEGORIES = {
    "short": "📏 Короткие", "word": "📖 Слова", "premium": "💎 Премиум",
    "og": "🔥 OG", "brand": "🏢 Бренды", "crypto": "₿ Крипто",
    "gaming": "🎮 Гейминг", "other": "📦 Другое",
}


# ══════════════════ CSS ══════════════════

CSS = """
:root {
    --bg: #0a0a1a; --bg2: #12122a; --bg3: #1a1a3a;
    --card: #14142e; --card-hover: #1c1c40; --border: #2a2a4a;
    --accent: #6c5ce7; --accent2: #a29bfe; --cyan: #00cec9;
    --green: #00b894; --yellow: #fdcb6e; --red: #e17055;
    --pink: #e84393; --text: #dfe6e9; --text2: #a0a0c0;
    --radius: 16px; --radius-sm: 10px;
}
* { margin:0; padding:0; box-sizing:border-box; }
html { scroll-behavior:smooth; }
body {
    font-family:'Inter',system-ui,sans-serif; background:var(--bg);
    color:var(--text); line-height:1.6; min-height:100vh;
    -webkit-tap-highlight-color:transparent;
    overscroll-behavior:none;
}
a { color:var(--accent2); text-decoration:none; }
a:hover { color:var(--cyan); }

/* Telegram WebApp mode */
body.webapp-mode { padding-bottom:env(safe-area-inset-bottom, 0); }
body.webapp-mode .navbar { display:none; }
body.webapp-mode .footer { display:none; }
body.webapp-mode .section { padding:1rem; }
body.webapp-mode .hero { padding:2rem 1rem; }

.navbar {
    background:rgba(10,10,26,.95); backdrop-filter:blur(20px);
    border-bottom:1px solid var(--border); position:sticky; top:0; z-index:100;
}
.nav-inner {
    max-width:1200px; margin:0 auto; display:flex;
    align-items:center; justify-content:space-between; padding:.8rem 1.5rem;
}
.logo { font-size:1.3rem; font-weight:800; color:var(--text); display:flex; align-items:center; gap:.5rem; }
.logo b { color:var(--accent); }
.nav-links { display:flex; align-items:center; gap:1.5rem; }
.nav-links a { color:var(--text2); font-weight:500; font-size:.95rem; }
.nav-links a:hover { color:var(--accent2); }
.nav-bal { background:var(--bg3); padding:.4rem .8rem; border-radius:20px; font-weight:600; font-size:.9rem; color:var(--yellow); }
.burger { display:none; background:none; border:none; color:var(--text); font-size:1.5rem; cursor:pointer; }
@media(max-width:768px) {
    .burger{display:block}
    .nav-links{display:none;flex-direction:column;position:absolute;top:100%;left:0;right:0;background:var(--bg2);padding:1rem;gap:.8rem;border-bottom:1px solid var(--border)}
    .nav-links.open{display:flex}
}

.flash-c { max-width:600px; margin:1rem auto; padding:0 1rem; }
.flash { padding:.8rem 1.2rem; border-radius:var(--radius-sm); margin-bottom:.5rem; font-weight:500; transition:opacity .5s; }
.flash-success { background:rgba(0,184,148,.15); border:1px solid var(--green); color:var(--green); }
.flash-error { background:rgba(225,112,85,.15); border:1px solid var(--red); color:var(--red); }
.flash-info { background:rgba(108,92,231,.15); border:1px solid var(--accent); color:var(--accent2); }

.btn { display:inline-flex; align-items:center; justify-content:center; gap:.5rem; padding:.7rem 1.5rem; border-radius:var(--radius-sm); font-weight:600; font-size:.95rem; border:none; cursor:pointer; transition:all .2s; color:#fff; text-decoration:none; }
.btn:hover { transform:translateY(-2px); filter:brightness(1.1); }
.btn:active { transform:translateY(0); }
.btn-p { background:linear-gradient(135deg,var(--accent),#8b5cf6); }
.btn-s { background:linear-gradient(135deg,var(--cyan),#0984e3); }
.btn-d { background:var(--red); }
.btn-g { background:transparent; border:1px solid var(--border); color:var(--text2); }
.btn-lg { padding:1rem 2rem; font-size:1.1rem; border-radius:var(--radius); }
.btn-sm { padding:.4rem .8rem; font-size:.85rem; }
.btn-full { width:100%; }

.card { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1.5rem; margin-bottom:1rem; }
.section { max-width:1200px; margin:0 auto; padding:2rem 1.5rem; }
.s-title { font-size:1.8rem; font-weight:800; margin-bottom:1.5rem; background:linear-gradient(135deg,var(--accent2),var(--cyan)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.p-title { font-size:2rem; font-weight:800; margin-bottom:1.5rem; }
.back { color:var(--text2); display:inline-block; margin-bottom:1rem; }
.empty { text-align:center; padding:3rem; color:var(--text2); }

.hero { position:relative; text-align:center; padding:4rem 1.5rem; overflow:hidden; }
.hero-bg { position:absolute; inset:0; background:radial-gradient(ellipse at 50% 0%,rgba(108,92,231,.2),transparent 70%),radial-gradient(ellipse at 80% 100%,rgba(0,206,201,.1),transparent 50%); }
.hero-c { position:relative; z-index:1; max-width:800px; margin:0 auto; }
.hero h1 { font-size:2.5rem; font-weight:800; margin-bottom:1rem; color:var(--text); }
.hero h1 b { color:var(--accent); }
.hero-sub { font-size:1.1rem; color:var(--text2); margin-bottom:2rem; }
.hero-stats { display:flex; justify-content:center; gap:1.5rem; margin-bottom:2rem; flex-wrap:wrap; }
.st-card { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1rem 1.5rem; text-align:center; min-width:100px; }
.st-num { display:block; font-size:1.8rem; font-weight:800; color:var(--accent2); }
.st-lbl { color:var(--text2); font-size:.85rem; }
.hero-act { display:flex; gap:1rem; justify-content:center; flex-wrap:wrap; }
@media(max-width:600px) { .hero h1{font-size:1.8rem} .hero-stats{gap:.8rem} .st-card{padding:.8rem 1rem;min-width:80px} .st-num{font-size:1.4rem} }

.l-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(220px,1fr)); gap:1rem; }
.l-card { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1.2rem; transition:all .25s; text-decoration:none; color:var(--text); display:flex; flex-direction:column; }
.l-card:hover { transform:translateY(-3px); border-color:var(--accent); box-shadow:0 8px 30px rgba(108,92,231,.15); }
.l-card:active { transform:scale(.98); }
.l-top { display:flex; justify-content:space-between; margin-bottom:.6rem; font-size:.85rem; }
.l-cat { background:var(--bg3); padding:.15rem .5rem; border-radius:6px; font-size:.8rem; }
.l-views { color:var(--text2); font-size:.8rem; }
.l-uname { font-size:1.3rem; font-weight:700; margin-bottom:.4rem; color:var(--accent2); }
.l-price { font-size:1.1rem; font-weight:700; color:var(--yellow); }
.l-date { font-size:.75rem; color:var(--text2); margin-top:.3rem; }
.l-len { font-size:.8rem; color:var(--text2); margin-bottom:.4rem; }

.toolbar { margin-bottom:2rem; }
.s-bar { display:flex; gap:.5rem; margin-bottom:1rem; }
.s-input { flex:1; padding:.7rem 1rem; border-radius:var(--radius-sm); background:var(--bg3); border:1px solid var(--border); color:var(--text); font-size:1rem; outline:none; }
.s-input:focus { border-color:var(--accent); }
.filters { display:flex; gap:.5rem; flex-wrap:wrap; align-items:center; }
.sel, .p-input { padding:.5rem .8rem; border-radius:var(--radius-sm); background:var(--bg3); border:1px solid var(--border); color:var(--text); font-size:.85rem; outline:none; }
.p-input { width:90px; }

.s-drop { position:fixed; top:120px; left:50%; transform:translateX(-50%); background:var(--card); border:1px solid var(--border); border-radius:var(--radius-sm); max-width:400px; width:90%; z-index:50; box-shadow:0 10px 40px rgba(0,0,0,.5); }
.sr-i { display:block; padding:.6rem 1rem; color:var(--text); border-bottom:1px solid var(--border); }
.sr-i:hover { background:var(--bg3); }

.ld { display:grid; grid-template-columns:1fr 320px; gap:2rem; }
@media(max-width:900px) { .ld{grid-template-columns:1fr} }
.ld-uname { font-size:2rem; font-weight:800; color:var(--accent2); margin-bottom:1.5rem; word-break:break-all; }
.ld-ig { display:grid; grid-template-columns:repeat(3,1fr); gap:.8rem; margin-bottom:1.5rem; }
@media(max-width:480px) { .ld-ig{grid-template-columns:1fr} }
.ld-ii { background:var(--bg3); padding:.8rem; border-radius:var(--radius-sm); text-align:center; }
.ld-lbl { display:block; font-size:.75rem; color:var(--text2); margin-bottom:.2rem; }
.ld-val { font-weight:600; font-size:.9rem; }
.ld-pb { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1.5rem; }
.ld-pr { font-size:2.2rem; font-weight:800; color:var(--yellow); margin-bottom:1.5rem; }

.mc { display:flex; flex-direction:column; gap:.6rem; margin-bottom:1.5rem; }
.mc-i { display:flex; cursor:pointer; border:2px solid var(--border); border-radius:var(--radius-sm); transition:.2s; }
.mc-i:has(input:checked) { border-color:var(--accent); background:rgba(108,92,231,.08); }
.mc-i input { display:none; }
.mc-b { display:flex; align-items:center; gap:.8rem; padding:.8rem; width:100%; }
.mc-ic { font-size:1.3rem; }
.mc-t { font-weight:600; font-size:.9rem; }
.mc-d { color:var(--text2); font-size:.8rem; margin-left:auto; }

.sc, .rc { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1.2rem; margin-bottom:1rem; }
.s-row { display:flex; align-items:center; gap:1rem; margin-bottom:.8rem; }
.s-av { width:45px; height:45px; border-radius:50%; background:var(--accent); display:flex; align-items:center; justify-content:center; font-size:1.2rem; font-weight:700; flex-shrink:0; }
.s-nm { font-weight:600; color:var(--accent2); display:block; font-size:.95rem; }
.s-sts { display:flex; flex-direction:column; gap:.2rem; font-size:.85rem; color:var(--text2); }

.badge { display:inline-block; padding:.15rem .5rem; border-radius:6px; font-size:.7rem; font-weight:600; }
.b-v { background:rgba(0,184,148,.2); color:var(--green); }
.b-pr { background:rgba(108,92,231,.2); color:var(--accent2); }
.b-act { background:rgba(0,184,148,.2); color:var(--green); }
.b-sold { background:rgba(253,203,110,.2); color:var(--yellow); }
.b-esc { background:rgba(0,206,201,.2); color:var(--cyan); }
.b-pending { background:rgba(253,203,110,.2); color:var(--yellow); }
.b-completed { background:rgba(0,184,148,.2); color:var(--green); }

.r-i { padding:.6rem 0; border-bottom:1px solid var(--border); }
.r-i:last-child { border:none; }
.r-h { display:flex; justify-content:space-between; align-items:center; margin-bottom:.2rem; }
.r-i p { color:var(--text2); font-size:.85rem; }

.p-grid { display:grid; grid-template-columns:1fr 340px; gap:2rem; }
@media(max-width:900px) { .p-grid{grid-template-columns:1fr} }
.p-hdr { display:flex; align-items:center; gap:1.2rem; margin-bottom:1.5rem; flex-wrap:wrap; }
.p-av { width:60px; height:60px; border-radius:50%; background:linear-gradient(135deg,var(--accent),var(--cyan)); display:flex; align-items:center; justify-content:center; font-size:1.8rem; font-weight:800; flex-shrink:0; }
.ps-g { display:grid; grid-template-columns:repeat(2,1fr); gap:.8rem; margin-bottom:1rem; }
@media(max-width:480px) { .ps-g{grid-template-columns:1fr} }
.ps-i { background:var(--bg3); padding:.8rem; border-radius:var(--radius-sm); text-align:center; }
.ps-v { display:block; font-size:1.3rem; font-weight:700; color:var(--accent2); }
.ps-l { font-size:.75rem; color:var(--text2); }
.sub-i { padding:.6rem; background:rgba(108,92,231,.1); border-radius:var(--radius-sm); margin-bottom:1rem; text-align:center; font-size:.9rem; }
.mn-l { display:flex; justify-content:space-between; align-items:center; padding:.5rem 0; border-bottom:1px solid var(--border); font-size:.85rem; gap:.5rem; }

.esc-b { background:rgba(253,203,110,.1); border:1px solid var(--yellow); border-radius:var(--radius); padding:1.2rem; }
.esc-a { background:rgba(253,203,110,.1); border:1px solid var(--yellow); border-radius:var(--radius-sm); padding:.8rem; margin-bottom:.8rem; }
.esc-a h4 { margin-bottom:.4rem; color:var(--yellow); font-size:.95rem; }
.sold-b { background:var(--green); color:#fff; padding:1rem; border-radius:var(--radius); text-align:center; font-size:1.3rem; font-weight:700; }

.vc { max-width:500px; margin:0 auto; }
.v-ic { font-size:3.5rem; display:block; margin-bottom:1rem; text-align:center; }
.v-cb { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius-sm); padding:.8rem; display:flex; align-items:center; justify-content:center; gap:.8rem; margin:1.2rem 0; }
.v-cb code { font-weight:700; color:var(--accent2); font-size:.95rem; word-break:break-all; }
.v-steps { text-align:left; margin:1.5rem 0; }
.v-s { display:flex; align-items:flex-start; gap:1rem; margin-bottom:1.2rem; }
.v-sn { width:32px; height:32px; border-radius:50%; background:var(--accent); display:flex; align-items:center; justify-content:center; font-weight:700; flex-shrink:0; font-size:.9rem; }
.v-s h4 { margin-bottom:.1rem; font-size:.95rem; }
.v-s p { color:var(--text2); font-size:.85rem; }
.v-acts { display:flex; gap:.8rem; justify-content:center; margin-top:1.2rem; flex-wrap:wrap; }

.g-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:1rem; }
.g-card { background:var(--card); border:2px solid var(--gc,var(--border)); border-radius:var(--radius); padding:1.5rem; text-align:center; transition:.3s; text-decoration:none; color:var(--text); }
.g-card:hover { transform:translateY(-4px); box-shadow:0 8px 30px rgba(0,0,0,.3); }
.g-card:active { transform:scale(.97); }
.g-em { font-size:2.5rem; margin-bottom:.8rem; }
.g-att { font-weight:600; color:var(--cyan); font-size:.9rem; }
.g-play { max-width:500px; margin:0 auto; background:var(--card); border:2px solid var(--gc,var(--border)); border-radius:var(--radius); padding:1.5rem; text-align:center; }
.g-bar { display:flex; justify-content:space-between; margin-bottom:1.5rem; font-weight:600; font-size:.85rem; padding-bottom:.8rem; border-bottom:1px solid var(--border); flex-wrap:wrap; gap:.5rem; }
.g-res { margin-top:1.2rem; padding:.8rem; border-radius:var(--radius-sm); font-weight:700; font-size:1.1rem; display:none; }
.g-res.win { background:rgba(0,184,148,.15); color:var(--green); display:block; }
.g-res.lose { background:rgba(225,112,85,.15); color:var(--red); display:block; }
.g-res a { color:var(--accent2); text-decoration:underline; }
.g-pay { margin-top:1.2rem; color:var(--text2); font-size:.8rem; }
.g-foot { margin-top:1.5rem; padding-top:.8rem; border-top:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.5rem; }

.reels { display:flex; justify-content:center; gap:.8rem; }
.reel { width:75px; height:75px; font-size:2.5rem; background:var(--bg3); border:2px solid var(--border); border-radius:var(--radius-sm); display:flex; align-items:center; justify-content:center; }
.coin { font-size:4rem; margin:1rem 0; }
.coin.spin { animation:coinSpin .8s ease-in-out; }
@keyframes coinSpin { 0%{transform:rotateY(0)} 100%{transform:rotateY(1080deg)} }
.cf-btns { display:flex; gap:.8rem; justify-content:center; margin-top:1rem; }
.dice-a { display:flex; gap:1rem; justify-content:center; }
.die { width:70px; height:70px; font-size:2.5rem; background:var(--bg3); border:2px solid var(--border); border-radius:var(--radius-sm); display:flex; align-items:center; justify-content:center; }
.cr-d { margin-bottom:1.2rem; }
.cr-m { font-size:2.5rem; font-weight:800; color:var(--cyan); margin-bottom:.8rem; }
.cr-bar { height:6px; background:var(--accent); border-radius:3px; width:0; transition:width .08s; }
.cr-c { display:flex; align-items:center; gap:.8rem; justify-content:center; flex-wrap:wrap; }
.in-sm { width:70px; padding:.3rem .5rem; background:var(--bg3); border:1px solid var(--border); border-radius:6px; color:var(--text); text-align:center; font-size:.9rem; }
.m-grid { display:grid; grid-template-columns:repeat(5,1fr); gap:.4rem; max-width:300px; margin:0 auto; }
.m-cell { aspect-ratio:1; font-size:1.2rem; background:var(--bg3); border:2px solid var(--border); border-radius:var(--radius-sm); cursor:pointer; transition:.2s; color:var(--text); }
.m-cell:hover:not(:disabled) { border-color:var(--accent); }
.m-cell:active:not(:disabled) { transform:scale(.95); }
.m-cell.hit { background:rgba(225,112,85,.2); border-color:var(--red); }
.m-cell.safe { background:rgba(0,184,148,.2); border-color:var(--green); }
.m-info { display:flex; justify-content:center; gap:1rem; align-items:center; margin:.8rem 0; font-size:.9rem; flex-wrap:wrap; }

.sell-g { display:grid; grid-template-columns:1fr 350px; gap:2rem; }
@media(max-width:900px) { .sell-g{grid-template-columns:1fr} }
.sell-f { display:flex; flex-direction:column; gap:1rem; }
.fg label { display:block; font-weight:600; margin-bottom:.3rem; font-size:.9rem; }
.fg input, .fg textarea, .fg select { width:100%; padding:.7rem .8rem; background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius-sm); color:var(--text); font-size:.95rem; outline:none; font-family:inherit; }
.fg input:focus, .fg textarea:focus { border-color:var(--accent); }
.iwp { display:flex; align-items:center; background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius-sm); overflow:hidden; }
.iwp span { padding:0 .6rem; color:var(--text2); font-weight:600; }
.iwp input { border:none!important; border-radius:0!important; }
.ml-row { display:flex; justify-content:space-between; align-items:center; padding:.6rem 0; border-bottom:1px solid var(--border); font-size:.85rem; gap:.5rem; flex-wrap:wrap; }

.buy-c { max-width:600px; margin:0 auto; }
.buy-s { margin-bottom:1.5rem; }
.buy-s h3 { margin-bottom:.8rem; color:var(--accent2); font-size:1rem; }
.gs-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(100px,1fr)); gap:.6rem; }
.gs-c { cursor:pointer; border:2px solid var(--border); border-radius:var(--radius-sm); transition:.2s; }
.gs-c:has(input:checked) { border-color:var(--accent); background:rgba(108,92,231,.08); }
.gs-c input { display:none; }
.gs-b { display:flex; flex-direction:column; align-items:center; padding:.8rem .5rem; gap:.2rem; }
.gs-e { font-size:1.5rem; }
.am-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(110px,1fr)); gap:.6rem; }
.am-c { cursor:pointer; border:2px solid var(--border); border-radius:var(--radius-sm); transition:.2s; }
.am-c:has(input:checked) { border-color:var(--accent); background:rgba(108,92,231,.08); }
.am-c input { display:none; }
.am-b { display:flex; flex-direction:column; align-items:center; padding:.8rem; }
.am-cnt { font-size:1.5rem; font-weight:800; color:var(--accent2); }
.am-lbl { font-size:.75rem; color:var(--text2); }
.am-pr { font-weight:700; color:var(--yellow); margin-top:.2rem; font-size:.9rem; }
.am-per { font-size:.7rem; color:var(--text2); }

.sp-h { display:flex; align-items:center; gap:1.2rem; margin-bottom:1.5rem; }
.sp-av { width:70px; height:70px; border-radius:50%; background:linear-gradient(135deg,var(--accent),var(--pink)); display:flex; align-items:center; justify-content:center; font-size:2rem; font-weight:800; flex-shrink:0; }
.sp-sts { display:flex; gap:1.5rem; margin-bottom:1.5rem; flex-wrap:wrap; }
.sp-st { text-align:center; }
.sp-v { display:block; font-size:1.1rem; font-weight:700; }

.feat-g { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:1rem; }
.feat-c { background:var(--card); border:1px solid var(--border); border-radius:var(--radius); padding:1.5rem; text-align:center; transition:.3s; }
.feat-c:hover { transform:translateY(-3px); border-color:var(--accent); }
.feat-c span { font-size:2rem; display:block; margin-bottom:.8rem; }
.feat-c h3 { margin-bottom:.3rem; font-size:1rem; }
.feat-c p { color:var(--text2); font-size:.85rem; }
.gp-g { display:flex; gap:.8rem; justify-content:center; flex-wrap:wrap; }
.gp-c { width:85px; height:85px; background:var(--card); border:2px solid var(--gc,var(--border)); border-radius:var(--radius); display:flex; flex-direction:column; align-items:center; justify-content:center; font-size:1.5rem; font-weight:700; transition:.3s; }
.gp-c:hover { transform:scale(1.08); }

.bm-ban { text-align:center; margin-top:1.5rem; padding:1rem; background:var(--card); border:1px solid var(--border); border-radius:var(--radius); display:flex; align-items:center; justify-content:center; gap:.8rem; flex-wrap:wrap; }

.tp-bal { text-align:center; padding:.8rem; }
.tp-bv { font-size:2rem; font-weight:800; color:var(--yellow); }
.tp-bl { display:block; color:var(--text2); font-size:.85rem; margin-bottom:.2rem; }
.tp-uses { display:flex; flex-direction:column; gap:.4rem; }
.tp-use { padding:.5rem .8rem; background:var(--bg3); border-radius:var(--radius-sm); font-size:.85rem; }

.rf { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); padding:1.2rem; margin-top:1.2rem; }
.star-r { display:flex; gap:.2rem; margin-bottom:.8rem; }
.star-b { background:none; border:none; font-size:1.3rem; cursor:pointer; transition:.2s; opacity:.4; padding:2px; }
.star-b.active { opacity:1; }

.footer { background:var(--bg2); border-top:1px solid var(--border); padding:1.2rem; }
.footer-i { max-width:1200px; margin:0 auto; display:flex; justify-content:space-between; align-items:center; color:var(--text2); font-size:.85rem; }
@media(max-width:600px) { .footer-i{flex-direction:column;gap:.3rem;text-align:center} }

@keyframes fadeIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
.l-card { animation:fadeIn .3s ease-out; }

/* WebApp bottom safe area */
.webapp-mode main { padding-bottom:80px; }

/* Tab bar for webapp */
.tab-bar {
    position:fixed; bottom:0; left:0; right:0;
    background:rgba(10,10,26,.95); backdrop-filter:blur(20px);
    border-top:1px solid var(--border);
    display:flex; justify-content:space-around; padding:.6rem 0;
    padding-bottom:max(.6rem, env(safe-area-inset-bottom));
    z-index:100;
}
.tab-bar a {
    display:flex; flex-direction:column; align-items:center; gap:.2rem;
    color:var(--text2); text-decoration:none; font-size:.7rem; font-weight:500;
    transition:.2s; padding:.2rem .5rem;
}
.tab-bar a:active { transform:scale(.9); }
.tab-bar a.active { color:var(--accent2); }
.tab-icon { font-size:1.3rem; }

.bet-controls { margin:1.2rem 0; padding:1rem; background:var(--bg3); border-radius:var(--radius-sm); }
.bet-row { display:flex; align-items:center; gap:.4rem; margin-top:.5rem; justify-content:center; flex-wrap:wrap; }
.bet-btn { background:var(--card); border:1px solid var(--border); color:var(--text); padding:.4rem .7rem; border-radius:8px; cursor:pointer; font-weight:600; font-size:.85rem; transition:.2s; }
.bet-btn:hover { border-color:var(--accent); background:var(--card-hover); }
.bet-btn:active { transform:scale(.95); }
.bet-input { width:70px; text-align:center; padding:.4rem; background:var(--bg); border:2px solid var(--accent); border-radius:8px; color:var(--yellow); font-weight:700; font-size:1.1rem; outline:none; }
"""


# ══════════════════ DB ══════════════════

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db: db.close()

def init_web_db():
    db = sqlite3.connect(DB_PATH); c = db.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS market_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, seller_uid INTEGER,
        username TEXT, price REAL, description TEXT DEFAULT '',
        category TEXT DEFAULT 'other', status TEXT DEFAULT 'active',
        created TEXT, sold_to INTEGER DEFAULT 0, sold_at TEXT DEFAULT '',
        views INTEGER DEFAULT 0, buyer_confirmed INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS seller_verifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER UNIQUE,
        status TEXT DEFAULT 'none', verify_code TEXT DEFAULT '',
        code_expires TEXT DEFAULT '', verified_at TEXT DEFAULT '',
        tg_username TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS game_attempts (
        uid INTEGER, game TEXT, attempts_left INTEGER DEFAULT 0,
        last_reset TEXT DEFAULT '', total_played INTEGER DEFAULT 0,
        total_won REAL DEFAULT 0, PRIMARY KEY (uid, game));
    CREATE TABLE IF NOT EXISTS market_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, listing_id INTEGER,
        buyer_uid INTEGER, seller_uid INTEGER, username TEXT,
        price REAL, method TEXT DEFAULT 'balance',
        status TEXT DEFAULT 'pending', created TEXT);
    CREATE TABLE IF NOT EXISTS seller_ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, seller_uid INTEGER,
        buyer_uid INTEGER, rating INTEGER DEFAULT 5,
        comment TEXT DEFAULT '', created TEXT);
    CREATE TABLE IF NOT EXISTS verification_codes (
        code TEXT PRIMARY KEY, uid INTEGER,
        created TEXT, used INTEGER DEFAULT 0);
    """)
    db.commit(); db.close()


# ══════════════════ AUTH ══════════════════

def verify_telegram_auth(data):
    ch = data.pop("hash", "")
    items = sorted(data.items())
    dc = "\n".join(f"{k}={v}" for k, v in items)
    secret = hashlib.sha256(BOT_TOKEN.encode()).digest()
    return hmac.new(secret, dc.encode(), hashlib.sha256).hexdigest() == ch

def validate_webapp_data(init_data_raw):
    """Валидация данных Telegram WebApp"""
    try:
        parsed = dict(parse_qs(init_data_raw))
        # Преобразуем списки в строки
        data = {k: v[0] if isinstance(v, list) else v for k, v in parsed.items()}
        
        check_hash = data.pop("hash", "")
        items = sorted(data.items())
        data_check = "\n".join(f"{k}={v}" for k, v in items)
        
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        h = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        
        if h != check_hash:
            return None
        
        user_data = json.loads(data.get("user", "{}"))
        return user_data
    except:
        return None

def current_user():
    uid = session.get("uid")
    if not uid: return None
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE uid=?", (uid,)).fetchone()
    if not row: return None
    u = dict(row)
    for k, v in [("balance",0.0),("banned",0),("sub_end",""),("searches",0),("ref_count",0),("free",0)]:
        u.setdefault(k, v)
    return u

def login_required(f):
    @wraps(f)
    def w(*a, **kw):
        if not session.get("uid"):
            if request.path.startswith("/webapp"):
                return redirect("/webapp")
            flash("Войдите через Telegram", "error")
            return redirect("/")
        return f(*a, **kw)
    return w

def has_premium(uid):
    if uid in ADMIN_IDS: return True
    db = get_db()
    r = db.execute("SELECT sub_end FROM users WHERE uid=?", (uid,)).fetchone()
    if not r or not r["sub_end"]: return False
    try: return datetime.strptime(r["sub_end"], "%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def is_verified_seller(uid):
    db = get_db()
    r = db.execute("SELECT status FROM seller_verifications WHERE uid=?", (uid,)).fetchone()
    return r and r["status"] == "verified"

def seller_info(uid):
    db = get_db()
    r = db.execute("SELECT * FROM seller_verifications WHERE uid=?", (uid,)).fetchone()
    listings = db.execute("SELECT COUNT(*) as c FROM market_listings WHERE seller_uid=? AND status='active'", (uid,)).fetchone()["c"]
    sold = db.execute("SELECT COUNT(*) as c FROM market_listings WHERE seller_uid=? AND status='sold'", (uid,)).fetchone()["c"]
    rt = db.execute("SELECT AVG(rating) as a, COUNT(*) as c FROM seller_ratings WHERE seller_uid=?", (uid,)).fetchone()
    return {"verified": r and r["status"]=="verified", "listings": listings, "sold": sold,
            "avg_rating": round(rt["a"] or 0, 1), "rating_count": rt["c"]}

def create_invoice_link(title, desc, payload, amount):
    try:
        r = http_req.post(f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink",
            json={"title":title,"description":desc,"payload":payload,"provider_token":"",
                  "currency":"XTR","prices":json.dumps([{"label":title,"amount":int(amount)}])}, timeout=10)
        d = r.json()
        return d.get("result") if d.get("ok") else None
    except: return None

def send_tg_message(uid, text):
    try: http_req.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id":uid,"text":text,"parse_mode":"HTML"}, timeout=5)
    except: pass


# ══════════════════ GAMES ══════════════════

def get_game_attempts(uid, game):
    db = get_db()
    r = db.execute("SELECT * FROM game_attempts WHERE uid=? AND game=?", (uid, game)).fetchone()
    today = datetime.now().strftime("%Y-%m-%d")
    daily = GAMES[game]["prem"] if has_premium(uid) else GAMES[game]["free"]
    if not r:
        db.execute("INSERT INTO game_attempts (uid,game,attempts_left,last_reset) VALUES (?,?,?,?)",
                   (uid, game, daily, today)); db.commit(); return daily
    if r["last_reset"] != today:
        n = daily + max(r["attempts_left"], 0)
        db.execute("UPDATE game_attempts SET attempts_left=?,last_reset=? WHERE uid=? AND game=?",
                   (n, today, uid, game)); db.commit(); return n
    return r["attempts_left"]

def use_attempt(uid, game):
    db = get_db()
    db.execute("UPDATE game_attempts SET attempts_left=MAX(attempts_left-1,0),total_played=total_played+1 WHERE uid=? AND game=?",
               (uid, game)); db.commit()

def add_attempts(uid, game, count):
    get_game_attempts(uid, game)
    db = get_db()
    db.execute("UPDATE game_attempts SET attempts_left=attempts_left+? WHERE uid=? AND game=?",
               (count, uid, game)); db.commit()

def add_winnings(uid, game, amount, bet):
    """amount = чистый выигрыш (уже минус ставка)"""
    db = get_db()
    db.execute("UPDATE game_attempts SET total_won=total_won+? WHERE uid=? AND game=?",
               (amount, uid, game))
    db.execute("UPDATE users SET balance=balance+? WHERE uid=?", (amount, uid))
    db.commit()

def deduct_bet(uid, amount):
    """Списать ставку с баланса"""
    db = get_db()
    bal = db.execute("SELECT balance FROM users WHERE uid=?", (uid,)).fetchone()
    if not bal or bal["balance"] < amount:
        return False
    db.execute("UPDATE users SET balance=balance-? WHERE uid=?", (amount, uid))
    db.commit()
    return True

def get_balance_db(uid):
    db = get_db()
    row = db.execute("SELECT balance FROM users WHERE uid=?", (uid,)).fetchone()
    return round(row["balance"], 1) if row else 0

def play_slots(bet):
    symbols = ["🍒","🍋","🍊","🍇","💎","⭐","7️⃣"]
    weights = [25, 25, 20, 15, 5, 5, 5]
    reels = random.choices(symbols, weights=weights, k=3)
    if reels[0] == reels[1] == reels[2]:
        mult = {"💎": 15.0, "7️⃣": 10.0, "⭐": 8.0, "🍇": 5.0,
                "🍊": 3.0, "🍋": 2.5, "🍒": 2.0}.get(reels[0], 2.0)
    elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
        mult = 1.5
    else:
        mult = 0
    win = round(bet * mult, 1) if mult > 0 else 0
    return {"reels": reels, "win": win, "mult": mult, "bet": bet}

def play_coinflip(bet, choice):
    result = random.choice(["heads", "tails"])
    win = round(bet * 1.9, 1) if result == choice else 0
    return {"result": result, "win": win, "mult": 1.9 if win > 0 else 0,
            "bet": bet, "emoji": "🦅" if result == "heads" else "🌸"}

def play_dice(bet):
    d1, d2 = random.randint(1, 6), random.randint(1, 6)
    s = d1 + d2
    if s == 12: mult = 6.0
    elif s == 11: mult = 4.0
    elif s >= 10: mult = 3.0
    elif s >= 9: mult = 2.0
    elif s >= 8: mult = 1.5
    elif s == 7: mult = 1.2
    else: mult = 0
    win = round(bet * mult, 1) if mult > 0 else 0
    return {"d1": d1, "d2": d2, "sum": s, "win": win, "mult": mult, "bet": bet}

def play_crash(bet, cashout_at):
    r = random.random()
    if r < 0.04:
        crash_at = 1.0
    elif r < 0.15:
        crash_at = round(1.0 + random.random() * 0.5, 2)
    else:
        crash_at = round(1.0 / (1 - random.random() * 0.94), 2)
    crash_at = min(crash_at, 100.0)

    if cashout_at <= crash_at:
        win = round(bet * cashout_at, 1)
        cashed = True
    else:
        win = 0
        cashed = False
    return {"crash_at": crash_at, "win": win, "mult": cashout_at if cashed else 0,
            "bet": bet, "cashed_out": cashed}

def play_mines(bet, revealed):
    total_cells = 25
    mine_count = 5
    safe_cells = total_cells - mine_count

    if revealed >= safe_cells:
        mult = 10.0
        return {"hit_mine": False, "win": round(bet * mult, 1),
                "multiplier": mult, "bet": bet, "max_reached": True}

    prob_mine = mine_count / (total_cells - revealed)
    hit = random.random() < prob_mine

    if hit:
        return {"hit_mine": True, "win": 0, "multiplier": 0, "bet": bet, "max_reached": False}

    mult = round(1.0 + revealed * 0.35, 2)
    return {"hit_mine": False, "win": round(bet * mult, 1),
            "multiplier": mult, "bet": bet, "max_reached": False}


# ══════════════════ MAKE PAGE ══════════════════

def make_page(content_html, scripts_html="", is_webapp=False, active_tab=""):
    user = current_user()
    bal = user.get("balance", 0) if user else 0
    logged_in = bool(session.get("uid"))
    
    if is_webapp and logged_in:
        nav = ""
        tab_bar = f'''
        <div class="tab-bar">
            <a href="/webapp" class="{'active' if active_tab=='home' else ''}">
                <span class="tab-icon">🏪</span>Маркет</a>
            <a href="/webapp/games" class="{'active' if active_tab=='games' else ''}">
                <span class="tab-icon">🎮</span>Игры</a>
            <a href="/webapp/sell" class="{'active' if active_tab=='sell' else ''}">
                <span class="tab-icon">💰</span>Продать</a>
            <a href="/webapp/profile" class="{'active' if active_tab=='profile' else ''}">
                <span class="tab-icon">👤</span>Профиль</a>
        </div>'''
        body_class = "webapp-mode"
    elif is_webapp:
        nav = ""
        tab_bar = ""
        body_class = "webapp-mode"
    else:
        if logged_in:
            nav_right = f'''
            <a href="/games">🎮 Игры</a>
            <a href="/sell">💰 Продать</a>
            <a href="/profile">👤 Профиль</a>
            <span class="nav-bal" id="navBal">{bal:.1f}⭐</span>
            <a href="/logout" class="btn btn-sm btn-g">Выйти</a>'''
        else:
            nav_right = f'''
            <script async src="https://telegram.org/js/telegram-widget.js?22"
                    data-telegram-login="{BOT_USER}" data-size="medium" data-radius="12"
                    data-auth-url="/auth/telegram" data-request-access="write"></script>'''
        nav = f'''<nav class="navbar"><div class="nav-inner">
            <a href="/" class="logo">🔍 <b>USERNAME</b>HUNTER</a>
            <div class="nav-links" id="navLinks"><a href="/market">🏪 Маркет</a>{nav_right}</div>
            <button class="burger" onclick="document.getElementById('navLinks').classList.toggle('open')">☰</button>
            </div></nav>'''
        tab_bar = ""
        body_class = ""
    
    flashes = ""
    for cat, msg in get_flashed_messages(with_categories=True):
        flashes += f'<div class="flash flash-{cat}">{msg}</div>'
    flash_block = f'<div class="flash-c">{flashes}</div>' if flashes else ""
    
    webapp_script = ""
    if is_webapp:
        webapp_script = '<script src="https://telegram.org/js/telegram-web-app.js"></script>'
    
    return f'''<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
<title>Username Hunter</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>{CSS}</style>
{webapp_script}
</head><body class="{body_class}">
{nav}
{flash_block}
<main style="min-height:calc(100vh - 140px)">{content_html}</main>
<footer class="footer"><div class="footer-i">
<p>🔍 Username Hunter © 2025</p>
<p>Бот: <a href="https://t.me/{BOT_USER}">@{BOT_USER}</a></p>
</div></footer>
{tab_bar}
<script>
document.querySelectorAll('.flash').forEach(el=>{{setTimeout(()=>el.style.opacity='0',4000);setTimeout(()=>el.remove(),4500)}});
if(window.Telegram&&window.Telegram.WebApp){{
    Telegram.WebApp.ready();
    Telegram.WebApp.expand();
    Telegram.WebApp.headerColor='#0a0a1a';
    Telegram.WebApp.backgroundColor='#0a0a1a';
}}
</script>
{scripts_html}
</body></html>'''


def render(page_tpl, scripts_tpl="", is_webapp=False, active_tab="", **ctx):
    user = current_user()
    ctx.setdefault("u", user)
    ctx["session"] = session
    try: content = render_template_string(page_tpl, **ctx)
    except Exception as e: content = f"<div class='empty'><p>Error: {e}</p></div>"
    try: scripts = render_template_string(scripts_tpl, **ctx) if scripts_tpl else ""
    except: scripts = ""
    return make_page(content, scripts, is_webapp, active_tab)


# ══════════════════ PAGE TEMPLATES ══════════════════

INDEX_TPL = """
<section class="hero"><div class="hero-bg"></div><div class="hero-c">
<h1>🔍 USERNAME <b>HUNTER</b></h1>
<p class="hero-sub">Покупай, продавай и находи Telegram юзернеймы</p>
<div class="hero-stats">
<div class="st-card"><span class="st-num">{{ st.listings }}</span><span class="st-lbl">Лотов</span></div>
<div class="st-card"><span class="st-num">{{ st.sold }}</span><span class="st-lbl">Продано</span></div>
<div class="st-card"><span class="st-num">{{ st.users }}</span><span class="st-lbl">Юзеров</span></div>
</div>
<div class="hero-act">
<a href="{{ prefix }}/market" class="btn btn-p btn-lg">🏪 Маркет</a>
{% if session.uid %}<a href="{{ prefix }}/games" class="btn btn-s btn-lg">🎮 Играть</a>{% endif %}
</div></div></section>
<section class="section"><h2 class="s-title">🔥 Новые лоты</h2>
<div class="l-grid">{% for i in recent %}
<a href="{{ prefix }}/listing/{{ i.id }}" class="l-card">
<div class="l-top"><span class="l-cat">{{ cats.get(i.category,'📦') }}</span><span class="l-views">👁 {{ i.views }}</span></div>
<div class="l-uname">@{{ i.username }}</div>
<div class="l-price">{{ "%.0f"|format(i.price) }} ⭐</div></a>
{% endfor %}{% if not recent %}<div class="empty"><p>🏪 Пока нет лотов</p></div>{% endif %}
</div></section>
<section class="section"><h2 class="s-title">🎮 Мини-игры</h2>
<div class="gp-g">
<div class="gp-c" style="--gc:#e17055">🎰</div>
<div class="gp-c" style="--gc:#fdcb6e">🪙</div>
<div class="gp-c" style="--gc:#6c5ce7">🎲</div>
<div class="gp-c" style="--gc:#00cec9">📈</div>
<div class="gp-c" style="--gc:#e84393">💣</div>
</div>
{% if session.uid %}<div style="text-align:center;margin-top:1rem"><a href="{{ prefix }}/games" class="btn btn-s">Играть →</a></div>{% endif %}
</section>
"""

MARKET_TPL = """
<section class="section"><h1 class="p-title">🏪 Маркет</h1>
<div class="toolbar"><form method="GET" action="{{ prefix }}/market">
<div class="s-bar"><input type="text" name="q" placeholder="Поиск @username..." value="{{ search }}" class="s-input" id="si">
<button type="submit" class="btn btn-p">🔍</button></div>
<div class="filters">
<select name="cat" class="sel" onchange="this.form.submit()"><option value="">Все</option>
{% for cid,cn in cats.items() %}<option value="{{ cid }}" {{ 'selected' if cat==cid }}>{{ cn }}</option>{% endfor %}</select>
<select name="sort" class="sel" onchange="this.form.submit()">
<option value="newest" {{ 'selected' if sort=='newest' }}>🆕 Новые</option>
<option value="price_asc" {{ 'selected' if sort=='price_asc' }}>💰 Дешёвые</option>
<option value="price_desc" {{ 'selected' if sort=='price_desc' }}>💎 Дорогие</option>
<option value="popular" {{ 'selected' if sort=='popular' }}>🔥 Хиты</option></select></div></form></div>
<div class="l-grid">{% for i in listings %}
<a href="{{ prefix }}/listing/{{ i.id }}" class="l-card">
<div class="l-top"><span class="l-cat">{{ cats.get(i.category,'📦') }}</span><span class="l-views">👁 {{ i.views }}</span></div>
<div class="l-uname">@{{ i.username }}</div>
<div class="l-len">{{ i.username|length }} символов</div>
<div class="l-price">{{ "%.0f"|format(i.price) }} ⭐</div></a>
{% endfor %}</div>
{% if not listings %}<div class="empty"><p>😔 Ничего не найдено</p></div>{% endif %}
</section>
"""

LISTING_TPL = """
<section class="section"><a href="{{ prefix }}/market" class="back">← Маркет</a>
<div class="ld"><div>
<div class="l-top"><span class="l-cat" style="font-size:1rem">{{ cats.get(item.category,'📦') }}</span>
<span style="color:var(--text2)">👁 {{ item.views }}</span></div>
<h1 class="ld-uname">@{{ item.username }}</h1>
<div class="ld-ig">
<div class="ld-ii"><span class="ld-lbl">Длина</span><span class="ld-val">{{ item.username|length }}</span></div>
<div class="ld-ii"><span class="ld-lbl">Категория</span><span class="ld-val">{{ item.category }}</span></div>
<div class="ld-ii"><span class="ld-lbl">Дата</span><span class="ld-val">{{ item.created[:10] if item.created else '—' }}</span></div></div>
{% if item.description %}<div style="margin-bottom:1.5rem"><h3>Описание</h3><p style="color:var(--text2)">{{ item.description }}</p></div>{% endif %}
{% if item.status == 'active' %}
<div class="ld-pb"><div class="ld-pr">{{ "%.0f"|format(item.price) }} ⭐</div>
{% if session.uid and session.uid != item.seller_uid %}
<form method="POST" action="{{ prefix }}/listing/{{ item.id }}/buy">
<div class="mc">
<label class="mc-i"><input type="radio" name="method" value="balance" checked><div class="mc-b"><span class="mc-ic">💰</span><span class="mc-t">Баланс</span><span class="mc-d">{{ "%.1f"|format(u.balance|default(0)) }}⭐</span></div></label>
<label class="mc-i"><input type="radio" name="method" value="tg_stars"><div class="mc-b"><span class="mc-ic">⭐</span><span class="mc-t">TG Stars</span></div></label>
<label class="mc-i"><input type="radio" name="method" value="funpay"><div class="mc-b"><span class="mc-ic">💳</span><span class="mc-t">FunPay</span></div></label></div>
<button type="submit" class="btn btn-p btn-lg btn-full">Купить {{ "%.0f"|format(item.price) }}⭐</button></form>
{% elif not session.uid %}<p style="color:var(--text2)">Войдите чтобы купить</p>{% endif %}</div>
{% elif item.status == 'escrow' and session.uid == item.sold_to %}
<div class="esc-b"><h3>⏳ Ожидание</h3>
<form method="POST" action="{{ prefix }}/listing/{{ item.id }}/confirm">
<button type="submit" class="btn btn-p btn-lg" onclick="return confirm('Получили?')">✅ Подтвердить</button></form></div>
{% elif item.status == 'sold' %}<div class="sold-b">✅ ПРОДАНО</div>{% endif %}
</div>
<div>
<div class="sc"><h3>Продавец</h3><div class="s-row">
<div class="s-av">{{ seller.uname[0]|upper if seller and seller.uname else '?' }}</div>
<div><a href="{{ prefix }}/seller/{{ item.seller_uid }}" class="s-nm">@{{ seller.uname if seller and seller.uname else '?' }}</a>
{% if si.verified %}<span class="badge b-v">✅</span>{% endif %}</div></div>
<div class="s-sts"><div>⭐ {{ si.avg_rating }}/5 ({{ si.rating_count }})</div>
<div>📦 {{ si.listings }} | ✅ {{ si.sold }}</div></div></div>
{% if ratings %}<div class="rc"><h3>💬 Отзывы</h3>{% for r in ratings[:5] %}<div class="r-i">
<div class="r-h"><div>{{ '⭐'*r.rating }}</div></div><p>{{ r.comment or '—' }}</p></div>{% endfor %}</div>{% endif %}
</div></div></section>
"""

GAMES_TPL = """
<section class="section"><h1 class="p-title">🎮 Игры</h1>
{% if not ag %}
<p style="color:var(--text2);margin-bottom:1.5rem">Делай ставки звёздами ⭐ и выигрывай!</p>
<div class="g-grid">{% for gid,g in games.items() %}
<a href="{{ prefix }}/games/{{ gid }}" class="g-card" style="--gc:{{ g.color }}">
<div class="g-em">{{ g.name.split()[0] }}</div><h3>{{ g.name }}</h3>
<p style="color:var(--text2);font-size:.85rem;margin-bottom:.8rem">{{ g.desc }}</p>
<div style="font-size:.8rem;color:var(--text2)">Ставка: {{ g.min_bet }}-{{ g.max_bet }}⭐</div>
<div class="g-att">🎮 {{ att.get(gid,0) }} попыток</div></a>{% endfor %}</div>
<div class="bm-ban"><p>Нет попыток?</p><a href="{{ prefix }}/buy-attempts" class="btn btn-s">Купить →</a></div>
{% else %}
{% set g = games[ag] %}
<a href="{{ prefix }}/games" class="back">← Игры</a>
<div class="g-play" style="--gc:{{ g.color }}">
<div class="g-bar">
<span>{{ g.name }}</span>
<span id="aD">🎮 {{ att.get(ag,0) }}</span>
<span id="bD">💰 {{ "%.1f"|format(u.balance|default(0)) }}⭐</span>
</div>

<div class="bet-controls">
<label style="color:var(--text2);font-size:.9rem">Ставка:</label>
<div class="bet-row">
<button class="bet-btn" onclick="setBet({{ g.min_bet }})">MIN</button>
<button class="bet-btn" onclick="changeBet(-1)">-</button>
<input type="number" id="betInput" value="{{ g.min_bet }}" min="{{ g.min_bet }}" max="{{ g.max_bet }}" step="1" class="bet-input">
<button class="bet-btn" onclick="changeBet(1)">+</button>
<button class="bet-btn" onclick="setBet(Math.min({{ g.max_bet }},parseFloat(document.getElementById('bD').textContent)))">MAX</button>
<button class="bet-btn" onclick="setBet(Math.floor(parseFloat(document.getElementById('bD').textContent)/2))">½</button>
</div>
<div style="font-size:.75rem;color:var(--text2);margin-top:.3rem">Мин: {{ g.min_bet }}⭐ | Макс: {{ g.max_bet }}⭐</div>
</div>

{% if ag == 'slots' %}
<div class="reels"><div class="reel" id="r1">🍒</div><div class="reel" id="r2">🍋</div><div class="reel" id="r3">🍊</div></div>
<button class="btn btn-p btn-lg" style="margin-top:1rem" id="pB" onclick="pSlots()">🎰 Крутить!</button>
<div class="g-res" id="res"></div>
<div class="g-pay">
<p>💎💎💎 = x15 | 7️⃣7️⃣7️⃣ = x10 | ⭐⭐⭐ = x8</p>
<p>🍇🍇🍇 = x5 | 🍊🍊🍊 = x3 | 🍋🍋🍋 = x2.5 | 🍒🍒🍒 = x2</p>
<p>2 одинаковых = x1.5</p>
</div>

{% elif ag == 'coinflip' %}
<div class="coin" id="coin">🪙</div>
<div class="cf-btns">
<button class="btn btn-lg" style="background:#e17055" onclick="pCF('heads')">🦅 Орёл</button>
<button class="btn btn-lg" style="background:#6c5ce7" onclick="pCF('tails')">🌸 Решка</button>
</div>
<div class="g-res" id="res"></div>
<div class="g-pay"><p>Угадал = x1.9 от ставки</p></div>

{% elif ag == 'dice' %}
<div class="dice-a"><div class="die" id="d1">⚀</div><div class="die" id="d2">⚀</div></div>
<button class="btn btn-p btn-lg" style="margin-top:1rem" onclick="pDice()">🎲 Бросить!</button>
<div class="g-res" id="res"></div>
<div class="g-pay">
<p>12 = x6 | 11 = x4 | 10 = x3 | 9 = x2 | 8 = x1.5 | 7 = x1.2</p>
</div>

{% elif ag == 'crash' %}
<div class="cr-d"><div class="cr-m" id="cM">1.00x</div><div class="cr-bar" id="cB"></div></div>
<div class="cr-c">
<label style="color:var(--text2)">Кэшаут: <input type="number" id="cV" value="2.0" min="1.1" max="100" step="0.1" class="in-sm">x</label>
<button class="btn btn-p btn-lg" id="cBtn" onclick="pCrash()">📈 Играть!</button>
</div>
<div class="g-res" id="res"></div>
<div class="g-pay"><p>Выигрыш = ставка × кэшаут</p></div>

{% elif ag == 'mines' %}
<div class="m-grid" id="mG">{% for i in range(25) %}
<button class="m-cell" onclick="pM({{ i }})" data-i="{{ i }}" disabled>❓</button>{% endfor %}</div>
<div class="m-info">
<span>Открыто: <b id="mR">0</b>/20</span>
<span>×<b id="mMu">1.0</b></span>
<span>💰 <b id="mWin">0</b>⭐</span>
</div>
<button class="btn btn-p" id="mS" onclick="mSt()">💣 Начать раунд</button>
<button class="btn btn-s" id="mC" onclick="mCO()" style="display:none">💰 Забрать выигрыш</button>
<div class="g-res" id="res"></div>
{% endif %}

<div class="g-foot">
{% if gs %}<div style="color:var(--text2);font-size:.85rem">🎮 {{ gs.played }} игр | 💰 {{ "%.1f"|format(gs.won) }}⭐</div>{% endif %}
<a href="{{ prefix }}/buy-attempts?game={{ ag }}" class="btn btn-sm btn-s">🛒 Попытки</a>
</div>
</div>{% endif %}</section>
"""

GAMES_JS = """<script>
const G='{{ ag or "" }}';const P='{{ prefix }}';const dF=['⚀','⚁','⚂','⚃','⚄','⚅'];
const MIN_BET={{ games[ag].min_bet if ag else 1 }};
const MAX_BET={{ games[ag].max_bet if ag else 100 }};

function getBet(){
    let v=parseFloat(document.getElementById('betInput').value)||MIN_BET;
    return Math.max(MIN_BET,Math.min(MAX_BET,Math.round(v*10)/10));
}
function setBet(v){
    v=Math.max(MIN_BET,Math.min(MAX_BET,Math.round(v)));
    document.getElementById('betInput').value=v;
}
function changeBet(d){
    let c=parseFloat(document.getElementById('betInput').value)||MIN_BET;
    setBet(c+d);
}

function uU(d){
    document.getElementById('aD').textContent='🎮 '+d.attempts_left;
    document.getElementById('bD').textContent='💰 '+d.balance+'⭐';
    const n=document.getElementById('navBal');if(n)n.textContent=d.balance+'⭐';
}
function sR(t,w){
    const e=document.getElementById('res');e.innerHTML=t;
    e.className='g-res '+(w?'win':'lose');e.style.display='block';
}
function nA(msg){
    sR(msg||'❌ Нет попыток! <a href="'+P+'/buy-attempts?game='+G+'">Купить</a>',false);
}

async function aP(b={}){
    b.bet=getBet();
    const r=await fetch('/api/game/'+G+'/play',{
        method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)
    });
    if(r.status===403){
        const d=await r.json();
        nA(d.message||'Ошибка');return null;
    }
    return await r.json();
}

function profitText(d){
    if(d.profit>0) return '🎉 Выигрыш: +'+d.profit+'⭐ (x'+d.mult+')';
    return '😔 -'+Math.abs(d.profit)+'⭐';
}

async function pSlots(){
    const b=document.getElementById('pB');b.disabled=true;
    const s=['🍒','🍋','🍊','🍇','💎','⭐','7️⃣'];
    for(let i=0;i<12;i++){
        document.getElementById('r1').textContent=s[~~(Math.random()*s.length)];
        document.getElementById('r2').textContent=s[~~(Math.random()*s.length)];
        document.getElementById('r3').textContent=s[~~(Math.random()*s.length)];
        await new Promise(r=>setTimeout(r,70));
    }
    const d=await aP();b.disabled=false;if(!d)return;
    document.getElementById('r1').textContent=d.reels[0];
    document.getElementById('r2').textContent=d.reels[1];
    document.getElementById('r3').textContent=d.reels[2];
    sR(profitText(d), d.profit>0);uU(d);
}

async function pCF(c){
    const co=document.getElementById('coin');co.classList.add('spin');
    const d=await aP({choice:c});
    setTimeout(()=>{
        co.classList.remove('spin');if(!d)return;co.textContent=d.emoji;
        sR(d.emoji+' '+profitText(d), d.profit>0);uU(d);
    },800);
}

async function pDice(){
    const d1=document.getElementById('d1'),d2=document.getElementById('d2');
    for(let i=0;i<10;i++){
        d1.textContent=dF[~~(Math.random()*6)];d2.textContent=dF[~~(Math.random()*6)];
        await new Promise(r=>setTimeout(r,90));
    }
    const d=await aP();if(!d)return;
    d1.textContent=dF[d.d1-1];d2.textContent=dF[d.d2-1];
    sR('🎲 Сумма: '+d.sum+' — '+profitText(d), d.profit>0);uU(d);
}

async function pCrash(){
    const btn=document.getElementById('cBtn');
    const co=parseFloat(document.getElementById('cV').value)||2;
    btn.disabled=true;
    const d=await aP({cashout:co});
    if(!d){btn.disabled=false;return}
    const m=document.getElementById('cM');const br=document.getElementById('cB');
    let c=1;const tg=d.cashed_out?co:d.crash_at;const st=30;const inc=(tg-1)/st;
    for(let i=0;i<st;i++){
        c+=inc;m.textContent=c.toFixed(2)+'x';
        br.style.width=Math.min(c/10*100,100)+'%';
        br.style.background=d.cashed_out?'#00b894':'#e17055';
        await new Promise(r=>setTimeout(r,50));
    }
    m.textContent=tg.toFixed(2)+'x';
    sR(d.cashed_out?'📈 Кэшаут '+co+'x! '+profitText(d):'💥 КРАШ '+d.crash_at+'x! '+profitText(d), d.profit>0);
    uU(d);btn.disabled=false;
}

let mRev=0,mAct=false,mBet=0,mTotalWin=0;
function mSt(){
    const bet=getBet();
    const bal=parseFloat(document.getElementById('bD').textContent);
    if(bal<bet){sR('❌ Недостаточно звёзд!',false);return}
    mRev=0;mAct=true;mBet=bet;mTotalWin=0;
    document.getElementById('mR').textContent='0';
    document.getElementById('mMu').textContent='1.0';
    document.getElementById('mWin').textContent='0';
    document.querySelectorAll('.m-cell').forEach(c=>{c.textContent='❓';c.disabled=false;c.className='m-cell'});
    document.getElementById('mS').style.display='none';
    document.getElementById('mC').style.display='none';
    document.getElementById('res').style.display='none';
    // Первый клик по клетке спишет ставку
}
async function pM(i){
    if(!mAct)return;
    const c=document.querySelector('[data-i="'+i+'"]');if(c.disabled)return;c.disabled=true;
    const d=await aP({revealed:mRev});
    if(!d){mAct=false;return}
    if(d.hit_mine){
        c.textContent='💣';c.classList.add('hit');
        mAct=false;mTotalWin=0;
        document.getElementById('mC').style.display='none';
        document.getElementById('mS').style.display='block';
        sR('💥 БАБАХ! -'+mBet+'⭐',false);
        // Открыть остальные мины
        document.querySelectorAll('.m-cell').forEach(cell=>{
            if(!cell.disabled&&cell.textContent=='❓'){
                cell.disabled=true;cell.style.opacity='.4';
            }
        });
    }else{
        c.textContent='💎';c.classList.add('safe');mRev++;
        mTotalWin=d.win;
        document.getElementById('mR').textContent=mRev;
        document.getElementById('mMu').textContent=d.multiplier+'x';
        document.getElementById('mWin').textContent=d.win;
        document.getElementById('mC').style.display='inline-block';
        if(d.max_reached){
            mAct=false;
            document.getElementById('mC').style.display='none';
            document.getElementById('mS').style.display='block';
            sR('🏆 ВСЕ ОТКРЫТЫ! +'+Math.round(d.win-mBet)+'⭐ (x'+d.multiplier+')',true);
        }
    }
    uU(d);
}
function mCO(){
    if(!mAct)return;
    mAct=false;
    document.getElementById('mC').style.display='none';
    document.getElementById('mS').style.display='block';
    document.querySelectorAll('.m-cell').forEach(c=>{c.disabled=true});
    const profit=Math.round(mTotalWin-mBet);
    if(profit>0) sR('💰 Забрали! +'+profit+'⭐',true);
    else sR('💰 Забрали '+mTotalWin+'⭐',mTotalWin>0);
}
</script>"""

PROFILE_TPL = """
<section class="section"><h1 class="p-title">👤 Профиль</h1>
<div class="p-grid"><div>
<div class="card"><div class="p-hdr"><div class="p-av">{{ session.get('first_name','?')[0]|upper }}</div>
<div><h2>{{ session.get('first_name','') }}</h2><p style="color:var(--text2)">@{{ u.uname or '—' }}</p></div>
{% if prem %}<span class="badge b-pr">💎 PREMIUM</span>{% endif %}</div>
<div class="ps-g">
<div class="ps-i"><span class="ps-v">{{ "%.1f"|format(u.balance|default(0)) }}</span><span class="ps-l">⭐ Баланс</span></div>
<div class="ps-i"><span class="ps-v">{{ u.searches|default(0) }}</span><span class="ps-l">🔍 Поисков</span></div>
<div class="ps-i"><span class="ps-v">{{ u.ref_count|default(0) }}</span><span class="ps-l">👥 Рефералов</span></div>
<div class="ps-i"><span class="ps-v">{{ u.free|default(0) }}</span><span class="ps-l">🎯 Осталось</span></div></div>
{% if prem and u.sub_end %}<div class="sub-i">💎 до {{ u.sub_end }}</div>{% endif %}
<div style="display:flex;gap:.5rem;flex-wrap:wrap">
{% if si.verified %}<span class="badge b-v">✅ Продавец</span>{% else %}<a href="{{ prefix }}/verify" class="btn btn-sm btn-s">Стать продавцом</a>{% endif %}
<a href="{{ prefix }}/games" class="btn btn-sm btn-p">🎮 Играть</a></div></div>
{% if escrow %}<div class="card"><h3>⏳ Подтвердить</h3>
{% for e in escrow %}<div class="esc-a"><h4>@{{ e.username }}</h4>
<form method="POST" action="{{ prefix }}/listing/{{ e.id }}/confirm">
<button type="submit" class="btn btn-sm btn-p" onclick="return confirm('Получили?')">✅</button></form></div>{% endfor %}</div>{% endif %}
</div><div>
<div class="card"><h3>📦 Лоты</h3>
{% if listings %}{% for l in listings[:8] %}<div class="mn-l"><span>@{{ l.username }}</span><span>{{ "%.0f"|format(l.price) }}⭐</span>
<span class="badge b-{{ l.status }}">{{ l.status }}</span></div>{% endfor %}{% else %}<p style="color:var(--text2)">—</p>{% endif %}</div>
<div class="card"><h3>🛒 Покупки</h3>
{% if purchases %}{% for p in purchases[:8] %}<div class="mn-l"><span>@{{ p.username }}</span><span class="badge b-{{ p.status }}">{{ p.status }}</span></div>{% endfor %}
{% else %}<p style="color:var(--text2)">—</p>{% endif %}</div>
</div></div></section>
"""

SELL_TPL = """
<section class="section"><h1 class="p-title">💰 Продать</h1>
{% if not verified %}
<div class="card" style="text-align:center;padding:2rem"><h2>🛡️ Верификация</h2>
<p style="color:var(--text2);margin:1rem 0">Подтвердите Telegram чтобы продавать.</p>
<a href="{{ prefix }}/verify" class="btn btn-p btn-lg">Верификация →</a></div>
{% else %}
<div class="sell-g"><div class="card"><h2 style="margin-bottom:1rem">Новый лот</h2>
<form method="POST" action="{{ prefix }}/sell" class="sell-f">
<div class="fg"><label>Юзернейм</label><div class="iwp"><span>@</span>
<input type="text" name="username" placeholder="username" required minlength="3" maxlength="32"></div></div>
<div class="fg"><label>Цена ⭐</label><input type="number" name="price" placeholder="100" required min="1"></div>
<div class="fg"><label>Категория</label><select name="category" class="sel">
{% for cid,cn in cats.items() %}<option value="{{ cid }}">{{ cn }}</option>{% endfor %}</select></div>
<div class="fg"><label>Описание</label><textarea name="description" rows="2" placeholder="..."></textarea></div>
<button type="submit" class="btn btn-p btn-lg btn-full">📦 Выставить</button></form></div>
<div class="card"><h3>📦 Мои лоты</h3>
{% if my_listings %}{% for l in my_listings %}<div class="ml-row"><div><b>@{{ l.username }}</b>
<span class="badge b-{{ l.status }}">{{ l.status }}</span></div><div>{{ "%.0f"|format(l.price) }}⭐
{% if l.status == 'active' %}<form method="POST" action="{{ prefix }}/listing/{{ l.id }}/delete" style="display:inline">
<button type="submit" class="btn btn-sm btn-d" onclick="return confirm('Удалить?')">🗑</button></form>{% endif %}</div></div>
{% endfor %}{% else %}<p style="color:var(--text2)">—</p>{% endif %}</div></div>{% endif %}</section>
"""

VERIFY_TPL = """
<section class="section"><h1 class="p-title">🛡️ Верификация</h1>
<div class="vc">
{% if ver and ver.status == 'verified' %}
<div class="card" style="text-align:center"><span class="v-ic">✅</span><h2>Верифицированы!</h2>
<a href="{{ prefix }}/sell" class="btn btn-p btn-lg" style="margin-top:1rem">Продать →</a></div>
{% elif ver and ver.status == 'pending' %}
<div class="card" style="text-align:center"><span class="v-ic">⏳</span><h2>Отправьте боту:</h2>
<div class="v-cb"><code id="vc">/webverify {{ ver.verify_code }}</code>
<button onclick="navigator.clipboard.writeText(document.getElementById('vc').textContent);this.textContent='✅'" class="btn btn-sm btn-g">📋</button></div>
<div class="v-acts"><a href="https://t.me/{{ bot }}" class="btn btn-p" target="_blank">📱 Бот</a>
<button onclick="chkV()" class="btn btn-s" id="cb">🔄 Проверить</button></div></div>
{% else %}
<div class="card" style="text-align:center"><span class="v-ic">🛡️</span><h2>Стать продавцом</h2>
<div class="v-steps">
<div class="v-s"><span class="v-sn">1</span><div><h4>Получите код</h4><p>Нажмите кнопку ниже</p></div></div>
<div class="v-s"><span class="v-sn">2</span><div><h4>Отправьте боту</h4><p>@{{ bot }} команду</p></div></div>
<div class="v-s"><span class="v-sn">3</span><div><h4>Готово!</h4><p>Можно продавать</p></div></div></div>
<form method="POST" action="{{ prefix }}/verify/start"><button type="submit" class="btn btn-p btn-lg btn-full">🛡️ Начать</button></form></div>
{% endif %}</div></section>
"""

VERIFY_JS = """<script>
async function chkV(){const b=document.getElementById('cb');b.textContent='⏳';b.disabled=true;
try{const r=await fetch('/verify/check');const d=await r.json();
if(d.verified){b.textContent='✅';setTimeout(()=>location.reload(),1000)}
else{b.textContent='❌';setTimeout(()=>{b.textContent='🔄 Проверить';b.disabled=false},2000)}}
catch{b.textContent='⚠️';setTimeout(()=>{b.textContent='🔄';b.disabled=false},2000)}}
{% if ver and ver.status == 'pending' %}setInterval(async()=>{try{const r=await fetch('/verify/check');const d=await r.json();if(d.verified)location.reload()}catch{}},5000);{% endif %}
</script>"""

BUY_ATT_TPL = """
<section class="section"><h1 class="p-title">🛒 Попытки</h1>
<div class="buy-c"><form method="POST" action="{{ prefix }}/buy-attempts">
<div class="buy-s"><h3>1. Игра</h3><div class="gs-grid">
{% for gid,g in games.items() %}<label class="gs-c" style="--gc:{{ g.color }}">
<input type="radio" name="game" value="{{ gid }}" {{ 'checked' if sg==gid or loop.first }}>
<div class="gs-b"><span class="gs-e">{{ g.name.split()[0] }}</span><span style="font-size:.8rem">{{ g.name.split()[1] if g.name.split()|length > 1 else '' }}</span></div></label>{% endfor %}</div></div>
<div class="buy-s"><h3>2. Количество</h3><div class="am-grid">
{% for cnt,pr in prices.items() %}<label class="am-c">
<input type="radio" name="count" value="{{ cnt }}" {{ 'checked' if loop.first }}>
<div class="am-b"><span class="am-cnt">{{ cnt }}</span><span class="am-lbl">шт</span>
<span class="am-pr">{{ pr }}⭐</span></div></label>{% endfor %}</div></div>
<div class="buy-s"><h3>3. Оплата</h3><div class="mc">
<label class="mc-i"><input type="radio" name="method" value="balance" checked>
<div class="mc-b"><span class="mc-ic">💰</span><span class="mc-t">Баланс</span><span class="mc-d">{{ "%.1f"|format(u.balance|default(0)) }}⭐</span></div></label>
<label class="mc-i"><input type="radio" name="method" value="tg_stars">
<div class="mc-b"><span class="mc-ic">⭐</span><span class="mc-t">TG Stars</span></div></label>
<label class="mc-i"><input type="radio" name="method" value="funpay">
<div class="mc-b"><span class="mc-ic">💳</span><span class="mc-t">FunPay</span></div></label></div></div>
<button type="submit" class="btn btn-p btn-lg btn-full">🛒 Купить</button></form></div></section>
"""

SELLER_TPL = """
<section class="section"><a href="{{ prefix }}/market" class="back">← Маркет</a>
<div style="max-width:700px;margin:0 auto">
<div class="sp-h"><div class="sp-av">{{ seller.uname[0]|upper if seller and seller.uname else '?' }}</div>
<div><h1>@{{ seller.uname if seller else '?' }}</h1>
{% if si.verified %}<span class="badge b-v">✅ Верифицирован</span>{% endif %}</div></div>
<div class="sp-sts">
<div class="sp-st"><span class="sp-v">⭐{{ si.avg_rating }}</span><span>({{ si.rating_count }})</span></div>
<div class="sp-st"><span class="sp-v">📦{{ si.listings }}</span><span>Лотов</span></div>
<div class="sp-st"><span class="sp-v">✅{{ si.sold }}</span><span>Продаж</span></div></div>
{% if listings %}<div class="l-grid">{% for i in listings %}
<a href="{{ prefix }}/listing/{{ i.id }}" class="l-card"><div class="l-uname">@{{ i.username }}</div>
<div class="l-price">{{ "%.0f"|format(i.price) }}⭐</div></a>{% endfor %}</div>{% endif %}
</div></section>
"""


# ══════════════════ ROUTES ══════════════════

def get_prefix():
    return "/webapp" if request.path.startswith("/webapp") else ""

def is_webapp_request():
    return request.path.startswith("/webapp")


# ─── WebApp auth ───
@app.route("/webapp")
@app.route("/webapp/")
def webapp_index():
    # Автологин через initData
    init_data = request.args.get("initData", "")
    if init_data and not session.get("uid"):
        user_data = validate_webapp_data(init_data)
        if user_data:
            uid = user_data["id"]
            session["uid"] = uid
            session["first_name"] = user_data.get("first_name", "")
            session["username"] = user_data.get("username", "")
            session.permanent = True
            db = get_db()
            if not db.execute("SELECT uid FROM users WHERE uid=?", (uid,)).fetchone():
                db.execute("INSERT INTO users (uid,uname,joined,free) VALUES (?,?,?,3)",
                           (uid, user_data.get("username",""), datetime.now().strftime("%Y-%m-%d %H:%M")))
                db.commit()
    
    db = get_db()
    recent = db.execute("SELECT * FROM market_listings WHERE status='active' ORDER BY id DESC LIMIT 8").fetchall()
    st = {
        "listings": db.execute("SELECT COUNT(*) as c FROM market_listings WHERE status='active'").fetchone()["c"],
        "sold": db.execute("SELECT COUNT(*) as c FROM market_listings WHERE status='sold'").fetchone()["c"],
        "users": db.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"],
    }
    return render(INDEX_TPL, is_webapp=True, active_tab="home",
                  recent=recent, st=st, cats=CATEGORIES, prefix="/webapp")

@app.route("/webapp/auth", methods=["POST"])
def webapp_auth():
    """Auth via initData from JS"""
    data = request.get_json() or {}
    init_data = data.get("initData", "")
    user_data = validate_webapp_data(init_data)
    if user_data:
        uid = user_data["id"]
        session["uid"] = uid
        session["first_name"] = user_data.get("first_name", "")
        session["username"] = user_data.get("username", "")
        session.permanent = True
        db = get_db()
        if not db.execute("SELECT uid FROM users WHERE uid=?", (uid,)).fetchone():
            db.execute("INSERT INTO users (uid,uname,joined,free) VALUES (?,?,?,3)",
                       (uid, user_data.get("username",""), datetime.now().strftime("%Y-%m-%d %H:%M")))
            db.commit()
        return jsonify({"ok": True})
    return jsonify({"ok": False}), 401


# ─── WebApp routes ───
@app.route("/webapp/market")
def webapp_market():
    return market_route(is_wa=True)

@app.route("/webapp/listing/<int:lid>")
def webapp_listing(lid):
    return listing_route(lid, is_wa=True)

@app.route("/webapp/listing/<int:lid>/buy", methods=["POST"])
@login_required
def webapp_buy(lid):
    return buy_route(lid, prefix="/webapp")

@app.route("/webapp/listing/<int:lid>/confirm", methods=["POST"])
@login_required
def webapp_confirm(lid):
    return confirm_route(lid, prefix="/webapp")

@app.route("/webapp/games")
@login_required
def webapp_games():
    return games_route(is_wa=True)

@app.route("/webapp/games/<gid>")
@login_required
def webapp_game(gid):
    return game_route(gid, is_wa=True)

@app.route("/webapp/sell", methods=["GET", "POST"])
@login_required
def webapp_sell():
    return sell_route(is_wa=True)

@app.route("/webapp/listing/<int:lid>/delete", methods=["POST"])
@login_required
def webapp_delete(lid):
    return delete_route(lid, prefix="/webapp")

@app.route("/webapp/profile")
@login_required
def webapp_profile():
    return profile_route(is_wa=True)

@app.route("/webapp/verify")
@login_required
def webapp_verify():
    return verify_route(is_wa=True)

@app.route("/webapp/verify/start", methods=["POST"])
@login_required
def webapp_verify_start():
    return verify_start_route(prefix="/webapp")

@app.route("/webapp/seller/<int:sid>")
def webapp_seller(sid):
    return seller_route(sid, is_wa=True)

@app.route("/webapp/buy-attempts", methods=["GET", "POST"])
@login_required
def webapp_buy_att():
    return buy_att_route(is_wa=True)


# ─── Regular web routes ───
@app.route("/")
def index():
    db = get_db()
    recent = db.execute("SELECT * FROM market_listings WHERE status='active' ORDER BY id DESC LIMIT 8").fetchall()
    st = {
        "listings": db.execute("SELECT COUNT(*) as c FROM market_listings WHERE status='active'").fetchone()["c"],
        "sold": db.execute("SELECT COUNT(*) as c FROM market_listings WHERE status='sold'").fetchone()["c"],
        "users": db.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"],
    }
    return render(INDEX_TPL, recent=recent, st=st, cats=CATEGORIES, prefix="")

@app.route("/auth/telegram")
def auth_telegram():
    data = {k:v for k,v in request.args.items()}
    if not data.get("hash"): flash("Ошибка","error"); return redirect("/")
    dc = dict(data)
    if verify_telegram_auth(dc):
        uid = int(data["id"]); session["uid"] = uid
        session["first_name"] = data.get("first_name","")
        session["username"] = data.get("username",""); session.permanent = True
        db = get_db()
        if not db.execute("SELECT uid FROM users WHERE uid=?", (uid,)).fetchone():
            db.execute("INSERT INTO users (uid,uname,joined,free) VALUES (?,?,?,3)",
                       (uid, data.get("username",""), datetime.now().strftime("%Y-%m-%d %H:%M"))); db.commit()
        flash("Добро пожаловать!","success"); return redirect("/")
    flash("Ошибка","error"); return redirect("/")

@app.route("/logout")
def logout():
    session.clear(); return redirect("/")

@app.route("/market")
def market_page():
    return market_route(is_wa=False)

@app.route("/listing/<int:lid>")
def listing_page(lid):
    return listing_route(lid, is_wa=False)

@app.route("/listing/<int:lid>/buy", methods=["POST"])
@login_required
def buy_page(lid):
    return buy_route(lid, prefix="")

@app.route("/listing/<int:lid>/confirm", methods=["POST"])
@login_required
def confirm_page(lid):
    return confirm_route(lid, prefix="")

@app.route("/games")
@login_required
def games_page():
    return games_route(is_wa=False)

@app.route("/games/<gid>")
@login_required
def game_pg(gid):
    return game_route(gid, is_wa=False)

@app.route("/sell", methods=["GET", "POST"])
@login_required
def sell_pg():
    return sell_route(is_wa=False)

@app.route("/listing/<int:lid>/delete", methods=["POST"])
@login_required
def delete_pg(lid):
    return delete_route(lid, prefix="")

@app.route("/profile")
@login_required
def profile_pg():
    return profile_route(is_wa=False)

@app.route("/verify")
@login_required
def verify_pg():
    return verify_route(is_wa=False)

@app.route("/verify/start", methods=["POST"])
@login_required
def verify_start_pg():
    return verify_start_route(prefix="")

@app.route("/verify/check")
@login_required
def verify_check():
    db = get_db()
    v = db.execute("SELECT status FROM seller_verifications WHERE uid=?", (session["uid"],)).fetchone()
    return jsonify({"verified": v and v["status"]=="verified"})

@app.route("/seller/<int:sid>")
def seller_pg(sid):
    return seller_route(sid, is_wa=False)

@app.route("/buy-attempts", methods=["GET", "POST"])
@login_required
def buy_att_pg():
    return buy_att_route(is_wa=False)


# ─── Shared logic ───

def market_route(is_wa):
    db = get_db()
    prefix = "/webapp" if is_wa else ""
    cat = request.args.get("cat",""); search = request.args.get("q","").strip()
    sort = request.args.get("sort","newest")
    pmin = request.args.get("pmin",""); pmax = request.args.get("pmax","")
    q = "SELECT * FROM market_listings WHERE status='active'"; p = []
    if cat and cat in CATEGORIES: q += " AND category=?"; p.append(cat)
    if search: q += " AND username LIKE ?"; p.append(f"%{search}%")
    if pmin:
        try: q += " AND price>=?"; p.append(float(pmin))
        except: pass
    if pmax:
        try: q += " AND price<=?"; p.append(float(pmax))
        except: pass
    order = {"price_asc":" ORDER BY price ASC","price_desc":" ORDER BY price DESC","popular":" ORDER BY views DESC"}.get(sort," ORDER BY id DESC")
    q += order + " LIMIT 50"
    listings = db.execute(q, p).fetchall()
    return render(MARKET_TPL, is_webapp=is_wa, active_tab="home",
                  listings=listings, cats=CATEGORIES, cat=cat, search=search, sort=sort, pmin=pmin, pmax=pmax, prefix=prefix)

def listing_route(lid, is_wa):
    prefix = "/webapp" if is_wa else ""
    db = get_db()
    item = db.execute("SELECT * FROM market_listings WHERE id=?", (lid,)).fetchone()
    if not item: abort(404)
    db.execute("UPDATE market_listings SET views=views+1 WHERE id=?", (lid,)); db.commit()
    seller = db.execute("SELECT * FROM users WHERE uid=?", (item["seller_uid"],)).fetchone()
    si = seller_info(item["seller_uid"])
    ratings = db.execute("SELECT * FROM seller_ratings WHERE seller_uid=? ORDER BY id DESC LIMIT 5", (item["seller_uid"],)).fetchall()
    return render(LISTING_TPL, is_webapp=is_wa, item=item, seller=seller, si=si, ratings=ratings, cats=CATEGORIES, prefix=prefix)

def buy_route(lid, prefix):
    uid = session["uid"]; db = get_db()
    item = db.execute("SELECT * FROM market_listings WHERE id=? AND status='active'", (lid,)).fetchone()
    if not item: flash("Нет","error"); return redirect(f"{prefix}/market")
    if item["seller_uid"] == uid: flash("Свой","error"); return redirect(f"{prefix}/listing/{lid}")
    method = request.form.get("method","balance"); price = item["price"]
    if method == "balance":
        u = current_user()
        if u.get("balance",0) < price: flash(f"Мало! {u.get('balance',0):.1f}<{price:.1f}⭐","error"); return redirect(f"{prefix}/listing/{lid}")
        now = datetime.now().strftime("%Y-%m-%d %H:%M"); sa = round(price*0.9,1)
        db.execute("UPDATE users SET balance=balance-? WHERE uid=?", (price,uid))
        db.execute("UPDATE users SET balance=balance+? WHERE uid=?", (sa, item["seller_uid"]))
        db.execute("UPDATE market_listings SET status='escrow',sold_to=?,sold_at=? WHERE id=?", (uid,now,lid))
        db.execute("INSERT INTO market_transactions (listing_id,buyer_uid,seller_uid,username,price,method,status,created) VALUES (?,?,?,?,?,?,?,?)",
                   (lid,uid,item["seller_uid"],item["username"],price,"balance","escrow",now)); db.commit()
        send_tg_message(item["seller_uid"], f"💰 @{item['username']} куплен! {sa}⭐")
        flash("Оплачено!","success")
    elif method == "tg_stars":
        link = create_invoice_link(f"@{item['username']}", f"@{item['username']}", f"market_{lid}_{uid}", int(price))
        if link: return redirect(link)
        flash("Ошибка","error")
    return redirect(f"{prefix}/listing/{lid}")

def confirm_route(lid, prefix):
    uid = session["uid"]; db = get_db()
    item = db.execute("SELECT * FROM market_listings WHERE id=? AND sold_to=? AND status='escrow'", (lid,uid)).fetchone()
    if not item: flash("Ошибка","error"); return redirect(f"{prefix}/market")
    db.execute("UPDATE market_listings SET status='sold',buyer_confirmed=1 WHERE id=?", (lid,))
    db.execute("UPDATE market_transactions SET status='completed' WHERE listing_id=? AND buyer_uid=?", (lid,uid)); db.commit()
    send_tg_message(item["seller_uid"], f"✅ @{item['username']} подтверждён!")
    flash("Подтверждено!","success"); return redirect(f"{prefix}/profile")

def games_route(is_wa):
    uid = session["uid"]; prefix = "/webapp" if is_wa else ""
    att = {gid: get_game_attempts(uid, gid) for gid in GAMES}
    return render(GAMES_TPL, GAMES_JS, is_webapp=is_wa, active_tab="games",
                  games=GAMES, att=att, prem=has_premium(uid), ag=None, gs=None, prefix=prefix)

def game_route(gid, is_wa):
    if gid not in GAMES: abort(404)
    uid = session["uid"]; prefix = "/webapp" if is_wa else ""
    db = get_db()
    att = {g: get_game_attempts(uid, g) for g in GAMES}
    r = db.execute("SELECT total_played,total_won FROM game_attempts WHERE uid=? AND game=?", (uid,gid)).fetchone()
    gs = {"played": r["total_played"] if r else 0, "won": r["total_won"] if r else 0}
    return render(GAMES_TPL, GAMES_JS, is_webapp=is_wa, active_tab="games",
                  games=GAMES, att=att, prem=has_premium(uid), ag=gid, gs=gs, prefix=prefix)

def sell_route(is_wa):
    uid = session["uid"]; prefix = "/webapp" if is_wa else ""
    verified = is_verified_seller(uid)
    if request.method == "POST":
        if not verified: flash("Верификация","error"); return redirect(f"{prefix}/verify")
        username = request.form.get("username","").strip().replace("@","").lower()
        price = request.form.get("price","0"); category = request.form.get("category","other")
        description = request.form.get("description","").strip()[:1000]
        if not username or len(username)<3: flash("Мин 3","error"); return redirect(f"{prefix}/sell")
        try: price = float(price); assert price >= 1
        except: flash("Мин 1⭐","error"); return redirect(f"{prefix}/sell")
        if category not in CATEGORIES: category = "other"
        db = get_db()
        if db.execute("SELECT id FROM market_listings WHERE username=? AND status='active'", (username,)).fetchone():
            flash("Уже есть","error"); return redirect(f"{prefix}/sell")
        db.execute("INSERT INTO market_listings (seller_uid,username,price,description,category,status,created) VALUES (?,?,?,?,?,?,?)",
                   (uid,username,price,description,category,"active",datetime.now().strftime("%Y-%m-%d %H:%M"))); db.commit()
        flash(f"✅ @{username}!","success"); return redirect(f"{prefix}/market")
    my = get_db().execute("SELECT * FROM market_listings WHERE seller_uid=? ORDER BY id DESC", (uid,)).fetchall()
    return render(SELL_TPL, is_webapp=is_wa, active_tab="sell", verified=verified, cats=CATEGORIES, my_listings=my, prefix=prefix)

def delete_route(lid, prefix):
    db = get_db()
    db.execute("DELETE FROM market_listings WHERE id=? AND seller_uid=? AND status='active'", (lid, session["uid"])); db.commit()
    flash("Удалено","success"); return redirect(f"{prefix}/sell")

def profile_route(is_wa):
    uid = session["uid"]; prefix = "/webapp" if is_wa else ""
    u = current_user(); db = get_db(); si = seller_info(uid)
    purchases = db.execute("SELECT t.*,l.username FROM market_transactions t JOIN market_listings l ON t.listing_id=l.id WHERE t.buyer_uid=? ORDER BY t.id DESC LIMIT 10", (uid,)).fetchall()
    listings = db.execute("SELECT * FROM market_listings WHERE seller_uid=? ORDER BY id DESC LIMIT 10", (uid,)).fetchall()
    escrow = db.execute("SELECT * FROM market_listings WHERE sold_to=? AND status='escrow'", (uid,)).fetchall()
    return render(PROFILE_TPL, is_webapp=is_wa, active_tab="profile",
                  prem=has_premium(uid), si=si, purchases=purchases, listings=listings, escrow=escrow, prefix=prefix)

def verify_route(is_wa):
    prefix = "/webapp" if is_wa else ""
    db = get_db()
    ver = db.execute("SELECT * FROM seller_verifications WHERE uid=?", (session["uid"],)).fetchone()
    return render(VERIFY_TPL, VERIFY_JS, is_webapp=is_wa, ver=ver, bot=BOT_USER, prefix=prefix)

def verify_start_route(prefix):
    uid = session["uid"]; db = get_db()
    code = f"VERIFY-{secrets.token_hex(4).upper()}"
    expires = (datetime.now() + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M")
    db.execute("INSERT OR REPLACE INTO seller_verifications (uid,status,verify_code,code_expires) VALUES (?,?,?,?)",
               (uid,"pending",code,expires))
    db.execute("INSERT OR REPLACE INTO verification_codes (code,uid,created,used) VALUES (?,?,?,0)",
               (code,uid,datetime.now().strftime("%Y-%m-%d %H:%M"))); db.commit()
    flash(f"/webverify {code}","info"); return redirect(f"{prefix}/verify")

def seller_route(sid, is_wa):
    prefix = "/webapp" if is_wa else ""
    db = get_db()
    seller = db.execute("SELECT * FROM users WHERE uid=?", (sid,)).fetchone()
    if not seller: abort(404)
    si = seller_info(sid)
    listings = db.execute("SELECT * FROM market_listings WHERE seller_uid=? AND status='active' ORDER BY id DESC", (sid,)).fetchall()
    return render(SELLER_TPL, is_webapp=is_wa, seller=seller, si=si, listings=listings, cats=CATEGORIES, prefix=prefix)

def buy_att_route(is_wa):
    uid = session["uid"]; prefix = "/webapp" if is_wa else ""
    if request.method == "POST":
        game = request.form.get("game",""); count = int(request.form.get("count","0") or 0)
        method = request.form.get("method","balance")
        if game not in GAMES or count not in ATTEMPT_PRICES: flash("Ошибка","error"); return redirect(f"{prefix}/buy-attempts")
        price = ATTEMPT_PRICES[count]
        if method == "balance":
            u = current_user()
            if u.get("balance",0) < price: flash(f"Мало!","error"); return redirect(f"{prefix}/buy-attempts?game={game}")
            db = get_db()
            db.execute("UPDATE users SET balance=balance-? WHERE uid=?", (price,uid)); db.commit()
            add_attempts(uid, game, count)
            flash(f"✅ +{count}!","success"); return redirect(f"{prefix}/games/{game}")
        elif method == "tg_stars":
            link = create_invoice_link(f"{count} попыток", f"{GAMES[game]['name']}", f"attempts_{game}_{count}_{uid}", price)
            if link: return redirect(link)
            flash("Ошибка","error")
        return redirect(f"{prefix}/buy-attempts?game={game}")
    return render(BUY_ATT_TPL, is_webapp=is_wa, games=GAMES, prices=ATTEMPT_PRICES,
                  sg=request.args.get("game",""), prefix=prefix)


# ─── API ───
@app.route("/api/game/<gid>/play", methods=["POST"])
@login_required
def api_play(gid):
    if gid not in GAMES:
        return jsonify({"error": "unknown"}), 400

    uid = session["uid"]
    data = request.get_json() or {}
    game_cfg = GAMES[gid]

    # Проверка попыток
    att = get_game_attempts(uid, gid)
    if att <= 0:
        return jsonify({"error": "no_attempts",
                        "message": "Попытки закончились! Купите ещё."}), 403

    # Проверка ставки
    bet = data.get("bet", game_cfg["min_bet"])
    try:
        bet = float(bet)
    except:
        bet = game_cfg["min_bet"]

    bet = max(game_cfg["min_bet"], min(game_cfg["max_bet"], bet))
    bet = round(bet, 1)

    # Проверка баланса
    bal = get_balance_db(uid)
    if bal < bet:
        return jsonify({"error": "no_balance",
                        "message": f"Недостаточно звёзд! Баланс: {bal}⭐, ставка: {bet}⭐"}), 403

    # Списываем ставку ДО игры
    if not deduct_bet(uid, bet):
        return jsonify({"error": "no_balance",
                        "message": "Не удалось списать ставку"}), 403

    # Списываем попытку
    use_attempt(uid, gid)

    # Играем
    if gid == "slots":
        result = play_slots(bet)
    elif gid == "coinflip":
        choice = data.get("choice", "heads")
        result = play_coinflip(bet, choice)
    elif gid == "dice":
        result = play_dice(bet)
    elif gid == "crash":
        cashout = data.get("cashout", 2.0)
        try:
            cashout = float(cashout)
            cashout = max(1.1, min(100.0, cashout))
        except:
            cashout = 2.0
        result = play_crash(bet, cashout)
    elif gid == "mines":
        revealed = data.get("revealed", 0)
        try:
            revealed = int(revealed)
        except:
            revealed = 0
        result = play_mines(bet, revealed)
    else:
        result = {"win": 0, "bet": bet}

    # Начисляем выигрыш (весь выигрыш, ставка уже списана)
    win = result.get("win", 0)
    if win > 0:
        add_winnings(uid, gid, win, bet)

    # Результат
    result["attempts_left"] = get_game_attempts(uid, gid)
    result["balance"] = get_balance_db(uid)
    result["profit"] = round(win - bet, 1) if win > 0 else round(-bet, 1)
    return jsonify(result)

@app.route("/api/search")
def api_search():
    q = request.args.get("q","").strip().lower()
    if len(q) < 2: return jsonify({"results":[]})
    db = get_db()
    rows = db.execute("SELECT id,username,price FROM market_listings WHERE status='active' AND username LIKE ? LIMIT 10", (f"%{q}%",)).fetchall()
    return jsonify({"results": [dict(r) for r in rows]})


@app.route("/devlogin")
def dev_login():
    uid = request.args.get("uid", "")
    if not uid:
        db = get_db()
        users = db.execute("SELECT uid, uname FROM users LIMIT 20").fetchall()
        html = '<div class="section"><h1 class="p-title">Dev Login</h1><div class="card">'
        for u in users:
            name = f"@{u['uname']}" if u['uname'] else f"ID:{u['uid']}"
            html += f'<div class="mn-l"><span>{name} ({u["uid"]})</span><a href="/devlogin?uid={u["uid"]}" class="btn btn-sm btn-p">Войти</a></div>'
        html += '</div></div>'
        return make_page(html)
    try:
        uid = int(uid)
    except:
        return redirect("/devlogin")
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE uid=?", (uid,)).fetchone()
    if not row:
        return make_page('<div class="section"><p>Not found</p><a href="/devlogin">Back</a></div>')
    session["uid"] = uid
    session["first_name"] = row["uname"] or str(uid)
    session["username"] = row["uname"] or ""
    session.permanent = True
    flash("OK!", "success")
    return redirect("/")


with app.app_context():
    init_web_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)