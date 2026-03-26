"""
USERNAME HUNTER v26.0 — FULL REWRITE
"""

import asyncio
import random
import string
import logging
import sqlite3
import secrets
import time
import re
import json
import os
import sys
import subprocess
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    LabeledPrice, PreCheckoutQuery
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

try:
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.tl.functions.account import CheckUsernameRequest as AccountCheckUsername
    from telethon.errors import FloodWaitError, UsernameNotOccupiedError, UsernameInvalidError
    HAS_TELETHON = True
except ImportError:
    HAS_TELETHON = False

# ═══════════════════════ НАСТРОЙКИ ═══════════════════════

MAIN_TOKEN = "8325751391:AAHZo7xC6TO4ldKEEqM--Ik9EXaszULnwWc"
ADMIN_IDS = [5969266721, 7894051808]
ADMIN_CONTACT = "pdwqb"

ACCOUNTS = [
    {"api_id": 35094180, "api_hash": "8732d865063dadaf1cba0ace1ef87de9", "phone": "+959790770236"},
    {"api_id": 34992704, "api_hash": "d54449feb7289284c9e4598911d08068", "phone": "+959973228130"},
    {"api_id": 36284654, "api_hash": "1073109c2e1085dd601ad289a9a65562", "phone": "+67077454464"},
    {"api_id": 34792667, "api_hash": "fc2eb570576ddc72819a5ba22f8c0f5d", "phone": "+959980062721"},
    {"api_id": 36347986, "api_hash": "2ef08b03748cdf3b688efc18a1e540b7", "phone": "+13347793071"},
    {"api_id": 36037729, "api_hash": "c48c8326dfb577fd4b8d503cb7dce2a4", "phone": "+19316345068"},
    {"api_id": 36360664, "api_hash": "facb9902e2eafe009a2fb43c901c2328", "phone": "+959694410210"},
]

FREE_SEARCHES = 3
FREE_COUNT = 1
PREMIUM_COUNT = 3
VIP_COUNT = 5
PREMIUM_SEARCHES_LIMIT = 7
VIP_SEARCHES_LIMIT = 15
REF_BONUS = 2
REFERRAL_COMMISSION = 0.04
SEARCH_COOLDOWN = 10
MIN_WITHDRAW = 50
SEARCH_PRICE_STARS = 5
PAY_CONTACT = "Soveqk"
REQUIRED_CHANNELS = ["SwordUsers"]
MONITOR_CHECK_INTERVAL = 1800
MONITOR_MAX_FREE = 0
MONITOR_MAX_PREMIUM = 5
TEMP_BAN_MINUTES = 30
STAR_TO_RUB = 1.25

TIKTOK_COMMENT_TEXT = "@SworuserN_bot бесплатные звёзды, найти юз, оценить юз"
TIKTOK_REWARD_GIFT = "🧸 Мишка (15⭐)"
TIKTOK_SCREENSHOTS_NEEDED = 35
TIKTOK_DAILY_LIMIT = 2
REMINDER_DAYS = [3, 1]
REMINDER_CHECK_INTERVAL = 3600

PRICES = {
    "1d":      {"days": 1,     "stars": 36,   "rub": 45,    "label": "1 день"},
    "3d":      {"days": 3,     "stars": 90,   "rub": 120,   "label": "3 дня"},
    "7d":      {"days": 7,     "stars": 180,  "rub": 250,   "label": "7 дней"},
    "1m":      {"days": 30,    "stars": 585,  "rub": 800,   "label": "1 месяц"},
    "3m":      {"days": 90,    "stars": 1800, "rub": 2200,  "label": "3 месяца"},
    "1y":      {"days": 365,   "stars": 5850, "rub": 8000,  "label": "1 год"},
    "forever": {"days": 99999, "stars": 8999, "rub": 11999, "label": "Навсегда"},
}

VIP_PRICES = {}
for _k, _p in PRICES.items():
    VIP_PRICES[_k] = {"days": _p["days"], "stars": max(1, _p["stars"] // 2), "label": f"VIP {_p['label']}"}

BUNDLE_PRICES = {}
for _k, _p in PRICES.items():
    _vip_stars = VIP_PRICES[_k]["stars"]
    BUNDLE_PRICES[_k] = {"days": _p["days"], "stars": int((_p["stars"] + _vip_stars) * 0.95), "label": f"Premium+VIP {_p['label']}"}

DONATE_OPTIONS = [20, 50, 100, 200, 300, 500, 1000]

# ═══ МАРКЕТПЛЕЙС ═══
MARKET_COMMISSION = 0.10
MARKET_LISTING_FEE = 5
MARKET_NFT_LISTING_FEE = 25
MARKET_PROMOTE_PRICE = 15
MARKET_MIN_PRICE = 10
MARKET_MAX_PRICE = 100000
MARKET_MAX_LOTS = 3
MARKET_VIP_MAX_LOTS = 10
MARKET_EXTRA_SLOT_PRICE = 10
MARKET_ESCROW_HOURS = 24
MARKET_FAST_MOD_PRICE = 20
LOOTBOX_PRICE = 20
LOOTBOX_COOLDOWN = 3600
WITHDRAW_FEE_PERCENT = 5

# ═══════════════════════ INIT ═══════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
bot = Bot(token=MAIN_TOKEN)
dp = Dispatcher()
user_states = {}
http_session = None
bot_info = None
DB = "hunter.db"
os.makedirs("sessions", exist_ok=True)
searching_users = set()
user_search_cooldown = {}
_fragment_cache = {}
_fragment_cache_ttl = 600
BOT_CONFIG_FILE = "bot_config.json"
SESSIONS_FILE = "all_sessions.json"

async def edit_msg(msg, text, kb=None):
    try: await msg.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except: pass

async def answer_cb(cb, text=None, show_alert=False):
    try: await cb.answer(text, show_alert=show_alert)
    except: pass

# ═══════════════════════ КОНФИГ ═══════════════════════

DEFAULT_CONFIG = {
    "free_searches": 3, "free_count": 1, "premium_count": 3, "vip_count": 5,
    "premium_searches_limit": 7, "vip_searches_limit": 15,
    "ref_bonus": 2, "search_cooldown": 10,
    "search_price_stars": 5, "min_withdraw": 50, "pay_contact": "Soveqk",
    "required_channels": ["SwordUsers"],
    "text_welcome": "", "text_found": "", "text_empty": "",
    "text_profile_header": "", "text_shop_header": "",
    "btn_tiktok": True, "btn_monitor": True,
    "btn_shop": True, "btn_support": True, "btn_referral": True,
    "mode_default": True, "mode_beautiful": True, "mode_meaningful": True,
    "mode_default_premium": False, "mode_beautiful_premium": True,
    "mode_meaningful_premium": True,
    "prices": {}, "daily_report": True, "daily_report_hour": 23,
    "notify_purchases": True, "notify_milestones": True,
}

def load_bot_config():
    config = dict(DEFAULT_CONFIG)
    if os.path.exists(BOT_CONFIG_FILE):
        try:
            with open(BOT_CONFIG_FILE, "r") as f: config.update(json.load(f))
        except: pass
    return config

def save_bot_config(config):
    with open(BOT_CONFIG_FILE, "w") as f: json.dump(config, f, indent=2, ensure_ascii=False)

def apply_config(config):
    global FREE_SEARCHES, FREE_COUNT, PREMIUM_COUNT, VIP_COUNT
    global PREMIUM_SEARCHES_LIMIT, VIP_SEARCHES_LIMIT
    global REF_BONUS, SEARCH_COOLDOWN, SEARCH_PRICE_STARS, MIN_WITHDRAW
    global PAY_CONTACT, REQUIRED_CHANNELS
    FREE_SEARCHES = config.get("free_searches", 3)
    FREE_COUNT = config.get("free_count", 1)
    PREMIUM_COUNT = config.get("premium_count", 3)
    VIP_COUNT = config.get("vip_count", 5)
    PREMIUM_SEARCHES_LIMIT = config.get("premium_searches_limit", 7)
    VIP_SEARCHES_LIMIT = config.get("vip_searches_limit", 15)
    REF_BONUS = config.get("ref_bonus", 2)
    SEARCH_COOLDOWN = config.get("search_cooldown", 10)
    SEARCH_PRICE_STARS = config.get("search_price_stars", 5)
    MIN_WITHDRAW = config.get("min_withdraw", 50)
    PAY_CONTACT = config.get("pay_contact", "Soveqk")
    REQUIRED_CHANNELS = config.get("required_channels", ["SwordUsers"])
    for key in SEARCH_MODES:
        SEARCH_MODES[key]["premium"] = config.get(f"mode_{key}_premium", SEARCH_MODES[key].get("_default_premium", False))
        SEARCH_MODES[key]["disabled"] = not config.get(f"mode_{key}", True)

def is_button_enabled(name): return load_bot_config().get(f"btn_{name}", True)

# ═══════════════════════ RATE LIMITER ═══════════════════════

class RateLimiter:
    def __init__(self):
        self.temp_bans = {}
        self.warnings = {}
    def is_temp_banned(self, uid):
        if uid not in self.temp_bans: return False
        if time.time() > self.temp_bans[uid]: del self.temp_bans[uid]; return False
        return True
    def check_search(self, uid):
        if uid in ADMIN_IDS: return True, ""
        if self.is_temp_banned(uid): return False, "ban"
        return True, ""
    def check_action(self, uid):
        if uid in ADMIN_IDS: return True, ""
        if self.is_temp_banned(uid): return False, "ban"
        return True, ""
    def get_ban_remaining(self, uid):
        if uid not in self.temp_bans: return 0
        return max(0, int(self.temp_bans[uid] - time.time()) // 60)

rate_limiter = RateLimiter()

# ═══════════════════════ LOG ═══════════════════════

def log_action(uid, action, details=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try: c.execute("INSERT INTO action_log (uid,action,details,created) VALUES (?,?,?,?)",
                   (uid, action, details, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    except: pass
    conn.commit(); conn.close()

def get_action_log(limit=50):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try: c.execute("SELECT uid,action,details,created FROM action_log ORDER BY id DESC LIMIT ?", (limit,)); rows = c.fetchall()
    except: rows = []
    conn.close()
    return [{"uid": r[0], "action": r[1], "details": r[2], "created": r[3]} for r in rows]

# ═══════════════════════ СЕССИИ СОХРАНЕНИЕ ═══════════════════════

def load_saved_sessions():
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, "r") as f: return json.load(f)
        except: pass
    return []

def save_session_info(api_id, api_hash, phone, status="active"):
    sessions = load_saved_sessions()
    for s in sessions:
        if s.get("phone") == phone:
            s["status"] = status; s["api_id"] = api_id; s["api_hash"] = api_hash
            s["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            with open(SESSIONS_FILE, "w") as f: json.dump(sessions, f, indent=2)
            return
    sessions.append({"api_id": api_id, "api_hash": api_hash, "phone": phone, "status": status,
                     "added": datetime.now().strftime("%Y-%m-%d %H:%M")})
    with open(SESSIONS_FILE, "w") as f: json.dump(sessions, f, indent=2)

# ═══════════════════════ ПУЛ АККАУНТОВ ═══════════════════════

class AccountPool:
    def __init__(self):
        self.clients = []; self.accounts_data = []; self.lock = asyncio.Lock()
        self.status = {}; self.cooldown_until = {}; self.last_used = {}
        self.error_streak = {}; self.total_errors = {}; self.req_count = {}
        self.window_start = {}; self.flood_times = {}; self.adaptive_delay = {}
        self.BASE_DELAY = 1.5; self.MAX_DELAY = 12.0; self.BUDGET_PER_MIN = 18
        self.MAX_ERROR_STREAK = 5; self.FLOOD_REST_TIME = 180
        self.WARMUP_EXTRA_DELAY = 5.0
        self.total_checks = 0; self.caught_by_botapi = 0; self.caught_by_recheck = 0
        self.reconnect_count = 0; self.active_users = {}; self.max_users_per_account = 3
        self._health_task = None; self._monitor_task = None

    async def init(self, accounts):
        if not HAS_TELETHON or not accounts: logger.info("Bot API mode"); return
        self.accounts_data = list(accounts)
        try:
            if os.path.exists("added_sessions.json"):
                with open("added_sessions.json") as f: self.accounts_data += json.load(f)
        except: pass
        for acc in self.accounts_data:
            phone = acc["phone"].replace("+","").replace(" ","")
            try:
                c = TelegramClient(f"sessions/s_{phone}", acc["api_id"], acc["api_hash"],
                                   connection_retries=5, retry_delay=3, timeout=15, request_retries=2)
                await c.connect()
                if not await c.is_user_authorized(): await c.start(phone=acc["phone"])
                self.clients.append(c); idx = len(self.clients)-1; self._init(idx)
                save_session_info(acc["api_id"], acc["api_hash"], acc["phone"], "active")
                logger.info(f"✅ #{idx+1}: {acc['phone']}")
            except Exception as e:
                save_session_info(acc.get("api_id",0), acc.get("api_hash",""), acc.get("phone",""), "error")
                logger.error(f"❌ {acc['phone']}: {e}")
        logger.info(f"Пул: {len(self.clients)}")
        self._health_task = asyncio.create_task(self._health_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    def _init(self, i):
        self.status[i]='healthy'; self.cooldown_until[i]=0; self.last_used[i]=0
        self.error_streak[i]=0; self.total_errors[i]=0; self.req_count[i]=0
        self.window_start[i]=time.time(); self.flood_times[i]=[]; self.adaptive_delay[i]=self.BASE_DELAY

    def has_sessions(self):
        return any(self.status.get(i) in ('healthy','warming') for i in range(len(self.clients)))

    async def _health_loop(self):
        while True:
            try:
                await asyncio.sleep(30); now=time.time()
                for i in range(len(self.clients)):
                    st=self.status.get(i,'dead')
                    if st=='dead': await self._try_reconnect(i)
                    elif st=='cooldown' and now>=self.cooldown_until.get(i,0):
                        self.status[i]='healthy'; self.error_streak[i]=0; self.adaptive_delay[i]=self.BASE_DELAY
                    elif st=='warming' and now-self.last_used.get(i,0)>15 and self.error_streak.get(i,0)==0:
                        self.status[i]='healthy'; self.adaptive_delay[i]=self.BASE_DELAY
                    if now-self.window_start.get(i,0)>60: self.req_count[i]=0; self.window_start[i]=now
                    if i in self.flood_times: self.flood_times[i]=[t for t in self.flood_times[i] if now-t<3600]
            except Exception as e: logger.error(f"Health: {e}")

    async def _monitor_loop(self):
        while True:
            await asyncio.sleep(300)
            try: s=self.stats(); logger.info(f"📊 {s['active']}/{s['total']} checks={s['checks']}")
            except: pass

    async def _try_reconnect(self, i):
        try:
            c=self.clients[i]
            if c.is_connected(): await c.disconnect()
            await asyncio.sleep(2); await c.connect()
            if await c.is_user_authorized():
                self.status[i]='healthy'; self.error_streak[i]=0
                self.adaptive_delay[i]=self.BASE_DELAY; self.reconnect_count+=1
                logger.info(f"🔄 #{i+1} reconnected")
        except Exception as e: logger.error(f"Reconnect #{i+1}: {e}")

    def _best(self, uid=None):
        now=time.time(); cands=[]
        for i in range(len(self.clients)):
            st=self.status.get(i,'dead')
            if st not in ('healthy','warming'): continue
            if now-self.window_start.get(i,0)>60: self.req_count[i]=0; self.window_start[i]=now
            if self.req_count.get(i,0)>=self.BUDGET_PER_MIN: continue
            d=self.adaptive_delay.get(i,self.BASE_DELAY)
            if st=='warming': d+=self.WARMUP_EXTRA_DELAY
            if now-self.last_used.get(i,0)<d: continue
            sc=self.error_streak.get(i,0)*20+self.req_count.get(i,0)*2
            if st=='warming': sc+=50
            if uid and uid in self.active_users.get(i,set()): sc-=25
            cands.append((i,sc))
        if not cands: return None
        cands.sort(key=lambda x:x[1])
        return random.choice(cands[:min(3,len(cands))])[0]

    async def _acquire(self, uid=None, timeout=45):
        dl=time.time()+timeout; a=0
        while time.time()<dl:
            async with self.lock:
                i=self._best(uid)
                if i is not None:
                    self.last_used[i]=time.time(); self.req_count[i]=self.req_count.get(i,0)+1
                    self.total_checks+=1; return i, self.clients[i]
            await asyncio.sleep(min(0.3*(1.2**a),3)+random.uniform(0,0.5)); a+=1
        return None, None

    def _ok(self, i):
        self.error_streak[i]=0
        if self.status.get(i)=='warming' and self.req_count.get(i,0)>=3: self.status[i]='healthy'
        self.adaptive_delay[i]=max(self.BASE_DELAY, self.adaptive_delay.get(i,self.BASE_DELAY)*0.95)

    def _err(self, i, flood=False, secs=0):
        self.error_streak[i]=self.error_streak.get(i,0)+1
        self.total_errors[i]=self.total_errors.get(i,0)+1
        if flood:
            self.cooldown_until[i]=time.time()+secs+random.randint(10,25)
            self.status[i]='cooldown'; self.flood_times.setdefault(i,[]).append(time.time())
            if len([t for t in self.flood_times[i] if time.time()-t<3600])>=4:
                self.cooldown_until[i]=time.time()+self.FLOOD_REST_TIME
        elif self.error_streak[i]>=self.MAX_ERROR_STREAK: self.status[i]='dead'
        else:
            self.adaptive_delay[i]=min(self.adaptive_delay.get(i,self.BASE_DELAY)*1.5, self.MAX_DELAY)
            self.cooldown_until[i]=time.time()+3; self.status[i]='cooldown'

    async def _resolve(self, u, uid=None):
        i,c=await self._acquire(uid,30)
        if c is None: return "no_session",-1
        try: await c(ResolveUsernameRequest(u)); self._ok(i); return "taken",i
        except UsernameNotOccupiedError: self._ok(i); return "free",i
        except UsernameInvalidError: self._ok(i); return "invalid",i
        except FloodWaitError as e: self._err(i,True,e.seconds); return "flood",i
        except: self._err(i); return "error",i

    async def _check_avail(self, u, uid=None):
        i,c=await self._acquire(uid,20)
        if c is None: return "no_session",-1
        try: ok=await c(AccountCheckUsername(u)); self._ok(i); return ("free" if ok else "taken"),i
        except FloodWaitError as e: self._err(i,True,e.seconds); return "flood",i
        except: self._err(i); return "error",i

    async def _botapi(self, u):
        try: await bot.get_chat(f"@{u}"); return "taken"
        except TelegramBadRequest as e: return "taken" if "not found" not in str(e).lower() else "not_found"
        except: return "not_found"

    async def check(self, u, uid=None):
        if not self.has_sessions():
            r=await self._botapi(u); return "taken" if r=="taken" else "maybe_free"
        b=await self._botapi(u)
        if b=="taken": self.caught_by_botapi+=1; return "taken"
        r,_=await self._resolve(u,uid)
        if r in ("taken","invalid"): return "taken"
        if r in ("flood","no_session","error"): return "skip"
        return "maybe_free"

    async def strong_check(self, u, uid=None):
        if not self.has_sessions():
            r=await self._botapi(u); return "taken" if r=="taken" else "free"
        b=await self._botapi(u)
        if b=="taken": self.caught_by_botapi+=1; return "taken"
        await asyncio.sleep(random.uniform(0.5,1.5))
        r,_=await self._check_avail(u,uid)
        if r=="taken": return "taken"
        if r in ("flood","no_session","error"): return "skip"
        return "free"

    def add_user(self, uid):
        for i in range(len(self.clients)):
            if self.status.get(i)=='dead': continue
            if uid in self.active_users.get(i,set()): return i
        for i in range(len(self.clients)):
            if self.status.get(i)=='dead': continue
            us=self.active_users.setdefault(i,set())
            if len(us)<self.max_users_per_account: us.add(uid); return i
        return None

    def remove_user(self, uid):
        for i in self.active_users: self.active_users[i].discard(uid)

    def stats(self):
        a=sum(1 for i in range(len(self.clients)) if self.status.get(i) in ('healthy','warming'))
        w=sum(1 for i in range(len(self.clients)) if self.status.get(i)=='warming')
        cd=sum(1 for i in range(len(self.clients)) if self.status.get(i)=='cooldown')
        d=sum(1 for i in range(len(self.clients)) if self.status.get(i)=='dead')
        e=sum(self.total_errors.get(i,0) for i in range(len(self.clients)))
        return {"total":len(self.clients),"active":a,"warming":w,"cooldown":cd,"dead":d,
                "checks":self.total_checks,"errors":e,"botapi_saves":self.caught_by_botapi,
                "recheck_saves":self.caught_by_recheck,"reconnects":self.reconnect_count}

    def detailed_status(self):
        lines=[]
        for i in range(len(self.clients)):
            st=self.status.get(i,'dead')
            em={'healthy':'🟢','warming':'🟡','cooldown':'🟠','dead':'🔴'}.get(st,'⚪')
            lines.append(f"{em}#{i+1} {st} d={self.adaptive_delay.get(i,0):.1f} e={self.error_streak.get(i,0)} r={self.req_count.get(i,0)}")
        return "\n".join(lines)

    async def disconnect(self):
        if self._health_task: self._health_task.cancel()
        if self._monitor_task: self._monitor_task.cancel()
        for c in self.clients:
            try: await c.disconnect()
            except: pass

pool = AccountPool()
# ═══════════════════════ ГЕНЕРАТОРЫ ═══════════════════════

_V = "aeiou"
_C = "bcdfghjklmnprstvwxyz"
_DIGRAPHS = ["bl","br","ch","cl","cr","dr","fl","fr","gl","gr","kn","pl","pr",
             "sc","sh","sk","sl","sm","sn","sp","st","sw","th","tr","tw","wr","zh"]
_ENDINGS = ["ax","ex","ix","ox","en","on","an","er","or","ar","in","yn","us",
            "os","is","al","el","il","ol","ul","ay","ey","oy","ry","ly","ny","zy"]

def _pronounceable(n):
    w=[]; sc=random.choice([True,False])
    for i in range(n): w.append(random.choice(_C) if (i%2==0)==sc else random.choice(_V))
    return "".join(w)

def gen_default():
    s=random.randint(1,5)
    if s==1: return _pronounceable(5)
    elif s==2:
        d=random.choice(_DIGRAPHS); return d+random.choice(_V)+random.choice(_C)+random.choice(_V)
    elif s==3:
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        end=random.choice(_V)+random.choice(_C); return (start+end)[:5]
    elif s==4:
        return random.choice(_C)+random.choice(_V)+random.choice(_C)+random.choice(_V)+random.choice(_C)
    else:
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        end=random.choice(_ENDINGS); r=start+end; return r[:5] if len(r)>5 else r

def gen_beautiful():
    s=random.randint(1,6)
    if s==1:
        d=random.choice(_DIGRAPHS); return d+random.choice("aeiou")+random.choice(_C)+random.choice("aeiou")
    elif s==2:
        pats=["cvcvc","cvccv","ccvcv","vcvcv"]; pat=random.choice(pats); w=[]; uc=set()
        for ch in pat:
            if ch=="c":
                p=[x for x in _C if x not in uc]; l=random.choice(p if p else list(_C)); w.append(l); uc.add(l)
            else: w.append(random.choice(_V))
        return "".join(w)
    elif s==3:
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        return start+random.choice(_ENDINGS[:14])
    elif s==4:
        s1=random.choice(["ka","ki","ko","ku","ra","ri","ro","ru","na","ni","no","nu",
                          "ma","mi","mo","mu","ta","ti","to","la","li","lo","sa","si"])
        s2=random.choice(["zen","rix","lex","vin","ren","lin","rex","nox","vex","lux",
                          "ryn","lyn","nyx","fox","pax","dex","max","jax"])
        return (s1+s2)[:5]
    elif s==5:
        starts=["ky","zy","xy","vy","ry","ly","ny","my"]
        mids=["ro","ra","le","li","no","na","ve","vi","re","ri"]
        ends=["n","x","s","r","l","k","t","d"]
        return (random.choice(starts)+random.choice(mids)+random.choice(ends))[:5]
    else:
        return random.choice(_C)+random.choice(_V)+random.choice(_C)+random.choice(_V)+random.choice(_C)

def gen_meaningful():
    pre=["my","go","hi","ok","no","up","on","in","mr","dj","pro","top","hot","big",
         "old","new","red","max","neo","zen","ice","sun","sky","air","sea","own",
         "try","run","fly","win","get","set","fix","mix","pop","raw","now","day","one"]
    suf=["bot","dev","pro","man","boy","cat","dog","fox","owl","god","war","run",
         "fly","win","fan","art","lab","hub","app","web","net","box","job","pay",
         "buy","car","map","log","key","pin","tag","tip","spy","doc","gem","ink"]
    mid=["cool","fast","best","good","real","true","dark","wild","bold","epic",
         "mega","gold","blue","easy","mini","deep","kind","wise","calm","warm"]
    s=random.choice(["ps","pm","us","um","sm","pn","two"])
    if s=="ps": r=random.choice(pre)+random.choice(suf)
    elif s=="pm": r=random.choice(pre)+random.choice(mid)
    elif s=="us": r=random.choice(pre)+"_"+random.choice(suf)
    elif s=="um": r=random.choice(mid)+"_"+random.choice(suf)
    elif s=="sm": r=random.choice(suf)+random.choice(mid)
    elif s=="pn": r=random.choice(pre)+str(random.randint(1,99))
    else: r=random.choice(pre)+random.choice(pre)+random.choice(suf)
    if len(r)<5: r+=random.choice(suf)
    if len(r)>15: r=r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$',r): return _pronounceable(5)
    return r

def gen_thematic_variations(word):
    word = word.lower().replace("@","").replace(" ","")
    if len(word) < 2: return []
    results = set()
    leet = {"a":"4","e":"3","i":"1","o":"0","s":"5","t":"7","b":"8","g":"9"}
    for orig, rep in leet.items():
        if orig in word: results.add(word.replace(orig, rep, 1)); results.add(word.replace(orig, rep))
    for i in range(len(word)): results.add(word[:i] + word[i]*2 + word[i+1:])
    for i in range(len(word)):
        r = word[:i] + word[i+1:]
        if len(r) >= 4: results.add(r)
    for i in range(len(word)-1): results.add(word[:i] + word[i+1] + word[i] + word[i+2:])
    for i in range(2, len(word)-1): results.add(word[:i] + "_" + word[i:])
    rev = word[::-1]
    if rev != word and len(rev) >= 5: results.add(rev)
    similar = {"c":"k","k":"c","f":"ph","s":"z","z":"s","v":"w","w":"v","i":"y","y":"i"}
    for orig, rep in similar.items():
        if orig in word: results.add(word.replace(orig, rep, 1))
    for n in range(10): results.add(word + str(n))
    for n in [11,13,23,42,69,77,99,100,228,666,777]: results.add(word + str(n))
    for c in "xzkvwymr": results.add(c + word)
    no_v = word[0] + "".join(c for c in word[1:] if c not in "aeiou")
    if len(no_v) >= 4 and no_v != word: results.add(no_v)
    if len(word) <= 5: results.add(word + word)
    valid = []
    seen = set()
    for r in results:
        r = r.replace("__","_").strip("_")
        if r not in seen and r != word and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r) and 5 <= len(r) <= 15:
            valid.append(r); seen.add(r)
    random.shuffle(valid)
    return valid

def gen_from_template(template):
    result = ""
    for ch in template:
        if ch == "*": result += ''.join(random.choice(_C+_V) for _ in range(random.randint(2,4)))
        elif ch == "?": result += random.choice(_C+_V)
        else: result += ch
    if len(result)<5: result+=''.join(random.choice(_C+_V) for _ in range(5-len(result)))
    if len(result)>15: result=result[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', result): return None
    return result

def gen_similar(base):
    base=base.lower().replace("@",""); muts=[]
    for i in range(len(base)):
        m=base[:i]+base[i+1:]
        if len(m)>=5: muts.append(m)
    for i in range(len(base)): muts.append(base[:i]+random.choice(_C+_V)+base[i+1:])
    for i in range(len(base)+1):
        m=base[:i]+random.choice(_C+_V)+base[i:]
        if len(m)<=15: muts.append(m)
    for i in range(1,len(base)):
        m=base[:i]+"_"+base[i:]
        if len(m)<=15: muts.append(m)
    for sf in ["_x","_1","_go","_pro","x","1","o","i","_tg","_bot"]:
        m=base+sf
        if len(m)<=15: muts.append(m)
    valid=[]; seen=set()
    for m in muts:
        if m not in seen and m!=base and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$',m) and len(m)>=5:
            valid.append(m); seen.add(m)
    random.shuffle(valid)
    return valid

SEARCH_MODES = {
    "default":    {"name":"Дефолт",     "emoji":"🎲","desc":"Красивые (5 букв)",  "_default_premium":False,"premium":False,"func":gen_default,   "disabled":False},
    "beautiful":  {"name":"Красивые",   "emoji":"💎","desc":"Стильные паттерны",   "_default_premium":True, "premium":True, "func":gen_beautiful, "disabled":False},
    "meaningful": {"name":"Со смыслом", "emoji":"📖","desc":"Комбинации слов",     "_default_premium":True, "premium":True, "func":gen_meaningful,"disabled":False},
}

INVALID_WORDS = ["admin","support","help","test","telegram","bot","official",
                 "service","security","account","login","password","verify",
                 "moderator","system","null","undefined","root","user"]

def is_valid_username(u):
    ul=u.lower().replace("_","")
    for w in INVALID_WORDS:
        if w in ul: return False
    if "__" in u or u.startswith("_") or u.endswith("_"): return False
    if is_blacklisted(u): return False
    return True

# ═══════════════════════ ЧЕКЕРЫ ═══════════════════════

async def check_username(u): return await pool.strong_check(u)

async def check_fragment(u):
    now=time.time(); cached=_fragment_cache.get(u)
    if cached and now-cached[1]<_fragment_cache_ttl: return cached[0]
    try:
        async with http_session.get(f"https://fragment.com/username/{u.lower()}",
            timeout=aiohttp.ClientTimeout(total=8),
            headers={"User-Agent":"Mozilla/5.0"}) as resp:
            if resp.status!=200: return "unavailable"
            text=await resp.text()
            if "Sold" in text or "sold" in text: r="sold"
            elif any(x in text for x in ["Available","Make an offer","Bid","auction"]): r="fragment"
            else: r="unavailable"
            _fragment_cache[u]=(r,now); return r
    except: return "unavailable"

async def check_fragment_nft(username):
    try:
        async with http_session.get(f"https://fragment.com/username/{username.lower()}",
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as resp:
            if resp.status != 200: return {"is_nft": False, "status": "unknown"}
            text = await resp.text()
            if any(x in text for x in ["Sold","sold","Assigned","assigned","Owner","owner","Status:&nbsp;Taken"]):
                return {"is_nft": True, "status": "sold"}
            elif any(x in text for x in ["Available","Make an offer","Bid","auction","Minimum bid","Buy now"]):
                return {"is_nft": True, "status": "on_sale"}
            else: return {"is_nft": False, "status": "not_found"}
    except: return {"is_nft": False, "status": "error"}

async def verify_nft_owner(uid, username):
    try:
        chat = await bot.get_chat(uid)
        if chat.username and chat.username.lower() == username.lower(): return True
    except: pass
    if pool.has_sessions():
        try:
            i, client = await pool._acquire(uid, 10)
            if client:
                try:
                    user = await client.get_entity(uid); pool._ok(i)
                    if user.username and user.username.lower() == username.lower(): return True
                except: pool._err(i)
        except: pass
    return False

async def check_subscribed(uid):
    if uid in ADMIN_IDS or not REQUIRED_CHANNELS: return []
    bad=[]
    for ch in REQUIRED_CHANNELS:
        try:
            m=await bot.get_chat_member(f"@{ch}",uid)
            if m.status in ("left","kicked"): bad.append(ch)
        except: pass
    return bad

def validate_username(u):
    if not u or len(u)<5 or len(u)>32: return False
    return bool(re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$',u))

def evaluate_username(u):
    score=0; factors=[]; ul=u.lower().replace("_",""); ln=len(ul)
    if ln<=3: score+=95; factors.append("🔥 Ультракороткий")
    elif ln<=4: score+=80; factors.append("💎 Очень короткий")
    elif ln<=5: score+=60; factors.append("✨ Короткий")
    elif ln<=6: score+=30; factors.append("📏 Средний")
    else: score+=10; factors.append("📦 Длинный")
    if len(set(ul))==1 and ln>=3: score+=90; factors.append("🔥 Моно")
    if ul==ul[::-1] and ln>=3: score+=40; factors.append("🪞 Палиндром")
    if "_" in u: score+=5; factors.append("🔗 Разделитель")
    if ul.isalpha(): score+=15; factors.append("🔤 Чистые буквы")
    if not any(c.isdigit() for c in ul): score+=10; factors.append("🅰️ Без цифр")
    vc=sum(1 for c in ul if c in _V)
    if 0.3<=vc/max(len(ul),1)<=0.6: score+=15; factors.append("🗣 Произносимый")
    score=min(score,200)
    if score>=150: pr,ra="$3k+","🔥🔥🔥 ЛЕГЕНДА"
    elif score>=100: pr,ra="$500-$3k","💎💎 РЕДКИЙ"
    elif score>=70: pr,ra="$100-$500","💎 ХОРОШИЙ"
    elif score>=40: pr,ra="$20-$100","✨ СТАНДАРТ"
    else: pr,ra="$1-$20","📦 ОБЫЧНЫЙ"
    filled=min(score//20,10)
    return {"score":score,"bar":"▓"*filled+"░"*(10-filled),"factors":factors,"price":pr,"rarity":ra}

# ═══════════════════════ ПОИСК ═══════════════════════

async def do_search(count, gen_func, msg, mode_name, uid):
    found=[]; attempts=0; start=time.time(); last_update=0
    checked=set(); errors=0; fallback=not pool.has_sessions(); warned_slow=False
    try:
        while len(found)<count and attempts<1500:
            u=None
            for _ in range(30):
                c=gen_func()
                if len(c)>=5 and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$',c) and c.lower() not in checked and is_valid_username(c):
                    u=c.lower(); break
            if not u: attempts+=1; continue
            checked.add(u); attempts+=1
            if fallback:
                b=await pool._botapi(u)
                if b!="taken":
                    fr=await check_fragment(u); found.append({"username":u,"fragment":fr})
                    save_history(uid,u,mode_name,len(u))
                await asyncio.sleep(2)
            else:
                r=await pool.check(u,uid)
                if r=="taken": errors=0; await asyncio.sleep(0.2); continue
                if r=="skip":
                    errors+=1
                    if errors>=3 and not warned_slow: warned_slow=True; fallback=True; errors=0
                    await asyncio.sleep(2); continue
                errors=0
                if r=="maybe_free":
                    final=await pool.strong_check(u,uid)
                    if final=="taken": await asyncio.sleep(0.5); continue
                    if final=="skip": await asyncio.sleep(2); continue
                    if final=="free":
                        fr=await check_fragment(u)
                        if fr!="fragment":
                            found.append({"username":u,"fragment":fr}); save_history(uid,u,mode_name,len(u))
                    await asyncio.sleep(0.8)
            now=time.time()
            if now-last_update>2.0:
                last_update=now; el=int(now-start)
                ps=pool.stats()
                sl=f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
                await edit_msg(msg,f"🔎 <b>{mode_name}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n🔄 {sl}\n⏱ {el}с")
        return found, {"attempts":attempts,"elapsed":int(time.time()-start)}
    finally: pool.remove_user(uid)

async def do_template_search(template, count, msg, uid):
    found=[]; attempts=0; start=time.time(); last_update=0; checked=set()
    while len(found)<count and attempts<500:
        u=gen_from_template(template)
        if u is None or u.lower() in checked or not is_valid_username(u): attempts+=1; continue
        checked.add(u.lower()); attempts+=1
        r=await pool.check(u.lower(),uid)
        if r=="maybe_free":
            d=await pool.strong_check(u.lower(),uid)
            if d=="free":
                fr=await check_fragment(u.lower())
                if fr!="fragment": found.append({"username":u.lower(),"fragment":fr}); save_history(uid,u.lower(),"Шаблон",len(u))
        await asyncio.sleep(1)
        now=time.time()
        if now-last_update>3: last_update=now; await edit_msg(msg,f"🎯 <b>{template}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
    return found, {"attempts":attempts,"elapsed":int(time.time()-start)}

async def do_similar_search(base, count, msg, uid):
    muts=gen_similar(base); found=[]; attempts=0; start=time.time(); last_update=0
    for u in muts:
        if len(found)>=count or attempts>=200: break
        attempts+=1
        r=await pool.check(u,uid)
        if r=="maybe_free":
            d=await pool.strong_check(u,uid)
            if d=="free":
                fr=await check_fragment(u)
                if fr!="fragment": found.append({"username":u,"fragment":fr}); save_history(uid,u,"Похожие",len(u))
        await asyncio.sleep(1)
        now=time.time()
        if now-last_update>3: last_update=now; await edit_msg(msg,f"🔄 <b>@{base}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
    return found, {"attempts":attempts,"elapsed":int(time.time()-start)}

async def do_thematic_search(word, count, msg, uid):
    variations = gen_thematic_variations(word)
    found = []; attempts = 0; start = time.time(); last_update = 0
    try:
        for u in variations:
            if len(found) >= count or attempts >= 300: break
            if not is_valid_username(u): attempts += 1; continue
            attempts += 1
            r = await pool.check(u, uid)
            if r == "maybe_free":
                d = await pool.strong_check(u, uid)
                if d == "free":
                    fr = await check_fragment(u)
                    if fr != "fragment": found.append({"username": u, "fragment": fr}); save_history(uid, u, f"По слову: {word}", len(u))
            await asyncio.sleep(1)
            now = time.time()
            if now - last_update > 3: last_update = now; await edit_msg(msg, f"🎯 <b>По слову: {word}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
        return found, {"attempts": attempts, "elapsed": int(time.time()-start)}
    finally: pool.remove_user(uid)

def format_results(found, stats, mode_name, config=None):
    config = config or load_bot_config()
    if found:
        custom_found = config.get("text_found","")
        text = custom_found + "\n\n" if custom_found else ""
        text += f"✅ <b>{mode_name} — найдено {len(found)}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, item in enumerate(found, 1):
            ev = evaluate_username(item["username"])
            fri = " 💎" if item.get("fragment") == "fragment" else (" 🏷" if item.get("fragment") == "sold" else "")
            text += (f"{i}. <code>@{item['username']}</code> — {ev['rarity']}{fri}\n"
                     f"   📱 <a href='https://t.me/{item['username']}'>Telegram</a>"
                     f" · 💎 <a href='https://fragment.com/username/{item['username']}'>Fragment</a>\n\n")
        text += f"📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"
    else:
        custom_empty = config.get("text_empty","")
        text = custom_empty if custom_empty else f"😔 <b>Не найдено</b>"
        text += f"\n\n📊 <code>{stats['attempts']}</code> ⏱ <code>{stats['elapsed']}с</code>"
    return text

# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '', joined TEXT DEFAULT '',
        free INTEGER DEFAULT 3, searches INTEGER DEFAULT 0, sub_end TEXT DEFAULT '',
        referred_by INTEGER DEFAULT 0, ref_count INTEGER DEFAULT 0, sub_bonus INTEGER DEFAULT 0,
        auto_renew INTEGER DEFAULT 0, auto_renew_plan TEXT DEFAULT '',
        last_reminder TEXT DEFAULT '', banned INTEGER DEFAULT 0,
        balance REAL DEFAULT 0.0, pending_ref INTEGER DEFAULT 0,
        captcha_passed INTEGER DEFAULT 0, last_roulette TEXT DEFAULT '',
        extra_searches INTEGER DEFAULT 0, monitor_slots INTEGER DEFAULT 0,
        template_uses INTEGER DEFAULT 0, daily_searches_used INTEGER DEFAULT 0,
        daily_searches_date TEXT DEFAULT '', vip_end TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY, days INTEGER, ktype TEXT, created TEXT, used INTEGER DEFAULT 0, used_by INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, status TEXT DEFAULT 'pending',
        created TEXT, reviewed_by INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, username TEXT,
        found_at TEXT, mode TEXT, length INTEGER DEFAULT 5)""")
    c.execute("""CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, ptype TEXT,
        active INTEGER DEFAULT 1, data TEXT DEFAULT '{}',
        created TEXT, ended TEXT DEFAULT '', button_text TEXT DEFAULT '')""")
    c.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, amount REAL,
        status TEXT DEFAULT 'pending', created TEXT, processed_by INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_uid INTEGER,
        referred_uid INTEGER, referred_uname TEXT DEFAULT '', created TEXT DEFAULT '')""")
    c.execute("""CREATE TABLE IF NOT EXISTS action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER,
        action TEXT, details TEXT DEFAULT '', created TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        username TEXT PRIMARY KEY, added_by INTEGER, created TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS monitors (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, username TEXT,
        status TEXT DEFAULT 'active', created TEXT, expires TEXT,
        last_check TEXT DEFAULT '', last_status TEXT DEFAULT 'taken')""")
    # Фикс market
    try: c.execute("SELECT mtype FROM market LIMIT 1")
    except: c.execute("DROP TABLE IF EXISTS market")
    c.execute("""CREATE TABLE IF NOT EXISTS market (
        id INTEGER PRIMARY KEY AUTOINCREMENT, seller_uid INTEGER,
        mtype TEXT DEFAULT 'username', title TEXT DEFAULT '', description TEXT DEFAULT '',
        price INTEGER DEFAULT 0, status TEXT DEFAULT 'pending',
        buyer_uid INTEGER DEFAULT 0, created TEXT DEFAULT '', sold_at TEXT DEFAULT '',
        moderated_by INTEGER DEFAULT 0, escrow_deadline TEXT DEFAULT '',
        seller_confirmed INTEGER DEFAULT 0, buyer_confirmed INTEGER DEFAULT 0,
        dispute INTEGER DEFAULT 0, dispute_reason TEXT DEFAULT '',
        charge_id TEXT DEFAULT '', promoted INTEGER DEFAULT 0, promoted_until TEXT DEFAULT '',
        is_nft INTEGER DEFAULT 0, fragment_url TEXT DEFAULT '',
        fast_mod INTEGER DEFAULT 0, listing_paid INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS promocodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE,
        discount_percent INTEGER DEFAULT 0, discount_stars INTEGER DEFAULT 0,
        max_uses INTEGER DEFAULT 1, used_count INTEGER DEFAULT 0,
        min_purchase INTEGER DEFAULT 0, applies_to TEXT DEFAULT 'all',
        created_by INTEGER DEFAULT 0, created TEXT DEFAULT '', expires TEXT DEFAULT '',
        active INTEGER DEFAULT 1)""")
    c.execute("""CREATE TABLE IF NOT EXISTS promocode_uses (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, uid INTEGER,
        used_at TEXT DEFAULT '', discount_amount INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS exchanges (
        id INTEGER PRIMARY KEY AUTOINCREMENT, initiator_uid INTEGER,
        partner_uid INTEGER DEFAULT 0, initiator_offer TEXT DEFAULT '',
        partner_offer TEXT DEFAULT '', status TEXT DEFAULT 'open',
        created TEXT DEFAULT '', completed_at TEXT DEFAULT '',
        initiator_confirmed INTEGER DEFAULT 0, partner_confirmed INTEGER DEFAULT 0,
        escrow_deadline TEXT DEFAULT '', dispute INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT, from_uid INTEGER, to_uid INTEGER,
        rating INTEGER DEFAULT 5, text TEXT DEFAULT '', deal_id INTEGER DEFAULT 0,
        deal_type TEXT DEFAULT 'market', created TEXT DEFAULT '')""")
    c.execute("""CREATE TABLE IF NOT EXISTS lootbox_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, prize TEXT DEFAULT '',
        prize_type TEXT DEFAULT '', created TEXT DEFAULT '')""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_slots (
        uid INTEGER PRIMARY KEY, extra_slots INTEGER DEFAULT 0)""")
    for col, default in [
        ("banned","0"),("balance","0.0"),("pending_ref","0"),("captcha_passed","0"),
        ("last_roulette","''"),("auto_renew","0"),("auto_renew_plan","''"),
        ("last_reminder","''"),("extra_searches","0"),("monitor_slots","0"),
        ("template_uses","0"),("daily_searches_used","0"),("daily_searches_date","''"),("vip_end","''")
    ]:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except: pass
    try: c.execute("ALTER TABLE promotions ADD COLUMN button_text TEXT DEFAULT ''")
    except: pass
    conn.commit(); conn.close()

# ═══════════════════════ ФУНКЦИИ БД ═══════════════════════

def ensure_user(uid, uname=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM users WHERE uid=?", (uid,))
    if not c.fetchone():
        c.execute("INSERT INTO users (uid,uname,joined,free) VALUES (?,?,?,?)",
                  (uid, uname or "", datetime.now().strftime("%Y-%m-%d %H:%M"), FREE_SEARCHES))
    elif uname: c.execute("UPDATE users SET uname=? WHERE uid=?", (uname, uid))
    conn.commit(); conn.close()

def get_user(uid):
    ensure_user(uid)
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT * FROM users WHERE uid=?", (uid,)); row = c.fetchone(); conn.close()
    if not row: return {"uid":uid,"free":FREE_SEARCHES,"searches":0,"sub_end":"","balance":0.0,"vip_end":""}
    d = dict(row)
    for k, v in [("auto_renew",0),("auto_renew_plan",""),("banned",0),("balance",0.0),
                 ("extra_searches",0),("monitor_slots",0),("template_uses",0),
                 ("daily_searches_used",0),("daily_searches_date",""),("vip_end","")]:
        d.setdefault(k, v)
    return d

def is_banned(uid): return rate_limiter.is_temp_banned(uid) or get_user(uid).get("banned",0)==1
def ban_user(uid): ensure_user(uid); conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET banned=1 WHERE uid=?",(uid,)); conn.commit(); conn.close()
def unban_user(uid): conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET banned=0 WHERE uid=?",(uid,)); conn.commit(); conn.close()

def has_subscription(uid):
    if uid in ADMIN_IDS: return True
    sub_end = get_user(uid).get("sub_end","")
    if not sub_end: return False
    try: return datetime.strptime(sub_end,"%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def has_vip(uid):
    if uid in ADMIN_IDS: return True
    if not has_subscription(uid): return False
    vip_end = get_user(uid).get("vip_end","")
    if not vip_end: return False
    try: return datetime.strptime(vip_end,"%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def give_vip(uid, days):
    ensure_user(uid); conn=sqlite3.connect(DB); c=conn.cursor()
    now=datetime.now(); u=get_user(uid); vip_end=u.get("vip_end",""); base=now
    if vip_end:
        try:
            cur=datetime.strptime(vip_end,"%Y-%m-%d %H:%M")
            if cur>now: base=cur
        except: pass
    new_end=base+timedelta(days=days)
    c.execute("UPDATE users SET vip_end=? WHERE uid=?",(new_end.strftime("%Y-%m-%d %H:%M"),uid))
    conn.commit(); conn.close(); return new_end.strftime("%d.%m.%Y %H:%M")

def remove_vip(uid):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET vip_end='' WHERE uid=?",(uid,)); conn.commit(); conn.close()

def give_subscription(uid, days):
    ensure_user(uid); conn=sqlite3.connect(DB); c=conn.cursor()
    now=datetime.now(); u=get_user(uid); sub_end=u.get("sub_end",""); base=now
    if sub_end:
        try:
            cur=datetime.strptime(sub_end,"%Y-%m-%d %H:%M")
            if cur>now: base=cur
        except: pass
    new_end=base+timedelta(days=days)
    c.execute("UPDATE users SET sub_end=? WHERE uid=?",(new_end.strftime("%Y-%m-%d %H:%M"),uid))
    conn.commit(); conn.close(); return new_end.strftime("%d.%m.%Y %H:%M")

def remove_subscription(uid):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET sub_end='' WHERE uid=?",(uid,)); conn.commit(); conn.close()

def _reset_daily_if_needed(uid):
    u=get_user(uid); today=datetime.now().strftime("%Y-%m-%d")
    if u.get("daily_searches_date","")!=today:
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("UPDATE users SET daily_searches_used=0,daily_searches_date=? WHERE uid=?",(today,uid))
        conn.commit(); conn.close()

def can_search(uid):
    if uid in ADMIN_IDS: return True
    _reset_daily_if_needed(uid); u=get_user(uid)
    if has_subscription(uid):
        limit=VIP_SEARCHES_LIMIT if has_vip(uid) else PREMIUM_SEARCHES_LIMIT
        return u.get("daily_searches_used",0)<limit
    return u.get("free",0)+u.get("extra_searches",0)>0

def use_search(uid):
    _reset_daily_if_needed(uid); conn=sqlite3.connect(DB); c=conn.cursor()
    if uid in ADMIN_IDS: c.execute("UPDATE users SET searches=searches+1 WHERE uid=?",(uid,))
    elif has_subscription(uid): c.execute("UPDATE users SET searches=searches+1,daily_searches_used=daily_searches_used+1 WHERE uid=?",(uid,))
    else:
        u=get_user(uid)
        if u.get("extra_searches",0)>0: c.execute("UPDATE users SET extra_searches=MAX(extra_searches-1,0),searches=searches+1 WHERE uid=?",(uid,))
        else: c.execute("UPDATE users SET free=MAX(free-1,0),searches=searches+1 WHERE uid=?",(uid,))
    conn.commit(); conn.close()

def get_search_count(uid):
    if uid in ADMIN_IDS: return 6
    if has_vip(uid): return VIP_COUNT
    return PREMIUM_COUNT if has_subscription(uid) else FREE_COUNT

def get_max_searches(uid):
    if uid in ADMIN_IDS: return 999
    _reset_daily_if_needed(uid); u=get_user(uid)
    if has_subscription(uid):
        limit=VIP_SEARCHES_LIMIT if has_vip(uid) else PREMIUM_SEARCHES_LIMIT
        return max(0,limit-u.get("daily_searches_used",0))
    return u.get("free",0)+u.get("extra_searches",0)

def set_free_searches(uid, count):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET free=? WHERE uid=?",(max(0,count),uid)); conn.commit(); conn.close()
def add_extra_searches(uid, count):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET extra_searches=extra_searches+? WHERE uid=?",(count,uid)); conn.commit(); conn.close()
def set_balance(uid, amount):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET balance=? WHERE uid=?",(max(0,amount),uid)); conn.commit(); conn.close()
def add_balance(uid, amount):
    ensure_user(uid); conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET balance=balance+? WHERE uid=?",(amount,uid)); conn.commit(); conn.close()
def get_balance(uid): return get_user(uid).get("balance",0.0)
def add_monitor_slots(uid, count):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET monitor_slots=monitor_slots+? WHERE uid=?",(count,uid)); conn.commit(); conn.close()
def add_template_uses(uid, count):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET template_uses=template_uses+? WHERE uid=?",(count,uid)); conn.commit(); conn.close()

def process_referral(new_uid, ref_uid):
    if new_uid==ref_uid: return False
    u=get_user(new_uid)
    if u.get("referred_by",0)!=0: return False
    ensure_user(ref_uid); uname=get_user(new_uid).get("uname","")
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?",(ref_uid,new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1,extra_searches=extra_searches+? WHERE uid=?",(REF_BONUS,ref_uid))
    c.execute("INSERT INTO referrals (referrer_uid,referred_uid,referred_uname,created) VALUES (?,?,?,?)",
              (ref_uid,new_uid,uname,datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()
    log_action(ref_uid,"ref_bonus",f"+{REF_BONUS} from {new_uid}")
    return True

def get_user_referrals(uid, limit=50):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT referred_uid,referred_uname,created FROM referrals WHERE referrer_uid=? ORDER BY id DESC LIMIT ?",(uid,limit))
    rows=c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"created":r[2]} for r in rows]

def get_ref_top(limit=10):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid,uname,ref_count FROM users WHERE ref_count>0 ORDER BY ref_count DESC LIMIT ?",(limit,))
    rows=c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"ref_count":r[2]} for r in rows]

def get_ref_top_by_period(start_date, limit=10):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT referrer_uid,COUNT(*) as cnt FROM referrals WHERE created>=? GROUP BY referrer_uid ORDER BY cnt DESC LIMIT ?",(start_date,limit))
    rows=c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":get_user(r[0]).get("uname",""),"ref_count":r[1]} for r in rows]

def check_referral_fraud(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT referred_uid,created FROM referrals WHERE referrer_uid=? ORDER BY created",(uid,))
    rows=c.fetchall(); conn.close()
    if len(rows)<3: return {"fraud":False,"reason":""}
    sus=0
    for i in range(1,len(rows)):
        try:
            p=datetime.strptime(rows[i-1][1],"%Y-%m-%d %H:%M"); cu=datetime.strptime(rows[i][1],"%Y-%m-%d %H:%M")
            if (cu-p).total_seconds()<60: sus+=1
        except: pass
    if sus>=3: return {"fraud":True,"reason":"Много рефералов за короткое время"}
    return {"fraud":False,"reason":""}

def get_my_ref_place(uid):
    u=get_user(uid); my=u.get("ref_count",0)
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE ref_count>?",(my,)); above=c.fetchone()[0]; conn.close()
    return above+1, my

def set_pending_ref(uid, ref_uid):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET pending_ref=? WHERE uid=?",(ref_uid,uid)); conn.commit(); conn.close()
def get_pending_ref(uid): return get_user(uid).get("pending_ref",0)
def set_captcha_passed(uid):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET captcha_passed=1 WHERE uid=?",(uid,)); conn.commit(); conn.close()

def activate_key(uid, key_text):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT days,ktype FROM keys WHERE key=? AND used=0",(key_text.strip(),)); row=c.fetchone()
    if not row: conn.close(); return None
    days,ktype=row; c.execute("UPDATE keys SET used=1,used_by=? WHERE key=?",(uid,key_text.strip()))
    conn.commit(); conn.close(); return {"days":days,"end":give_subscription(uid,days)}

def generate_key(days, ktype="MANUAL"):
    key=f"HUNT-{ktype}-{secrets.token_hex(4).upper()}"
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO keys (key,days,ktype,created) VALUES (?,?,?,?)",(key,days,ktype,datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close(); return key

def set_auto_renew(uid, enabled, plan=""):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE users SET auto_renew=?,auto_renew_plan=? WHERE uid=?",(1 if enabled else 0,plan,uid)); conn.commit(); conn.close()
def get_auto_renew(uid):
    u=get_user(uid); return bool(u.get("auto_renew",0)), u.get("auto_renew_plan","")

def save_history(uid, username, mode, length=5):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO history (uid,username,found_at,mode,length) VALUES (?,?,?,?,?)",
              (uid,username,datetime.now().strftime("%Y-%m-%d %H:%M"),mode,length)); conn.commit(); conn.close()
def get_history(uid, limit=20):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT username,found_at,mode FROM history WHERE uid=? ORDER BY id DESC LIMIT ?",(uid,limit))
    rows=c.fetchall(); conn.close(); return rows
def delete_history_pattern(uid, pattern):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("DELETE FROM history WHERE uid=? AND username LIKE ?",(uid,f"%{pattern}%"))
    deleted=c.rowcount; conn.commit(); conn.close(); return deleted

def create_withdrawal(uid, amount):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO withdrawals (uid,amount,status,created) VALUES (?,?,'pending',?)",
              (uid,amount,datetime.now().strftime("%Y-%m-%d %H:%M"))); wid=c.lastrowid; conn.commit(); conn.close(); return wid
def get_pending_withdrawals():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,uid,amount,created FROM withdrawals WHERE status='pending'"); rows=c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"amount":r[2],"created":r[3]} for r in rows]
def process_withdrawal(wid, admin_uid, approve=True):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid,amount FROM withdrawals WHERE id=? AND status='pending'",(wid,)); row=c.fetchone()
    if not row: conn.close(); return None
    uid,amount=row
    if approve: c.execute("UPDATE withdrawals SET status='approved',processed_by=? WHERE id=?",(admin_uid,wid)); c.execute("UPDATE users SET balance=MAX(balance-?,0) WHERE uid=?",(amount,uid))
    else: c.execute("UPDATE withdrawals SET status='rejected',processed_by=? WHERE id=?",(admin_uid,wid))
    conn.commit(); conn.close(); return {"uid":uid,"amount":amount}

def create_promotion(name, ptype, button_text="", data=None):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO promotions (name,ptype,button_text,data,created) VALUES (?,?,?,?,?)",
              (name,ptype,button_text,json.dumps(data or {}),datetime.now().strftime("%Y-%m-%d %H:%M")))
    pid=c.lastrowid; conn.commit(); conn.close(); return pid
def get_active_promotions():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,name,ptype,data,created,button_text FROM promotions WHERE active=1")
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"name":r[1],"ptype":r[2],"data":json.loads(r[3] or "{}"),"created":r[4],
             "button_text":r[5] if r[5] else r[1]} for r in rows]
def end_promotion(pid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE promotions SET active=0,ended=? WHERE id=?",(datetime.now().strftime("%Y-%m-%d %H:%M"),pid)); conn.commit(); conn.close()

def get_premium_users():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid,uname,sub_end FROM users WHERE sub_end>? AND sub_end!=''",(datetime.now().strftime("%Y-%m-%d %H:%M"),))
    rows=c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"sub_end":r[2]} for r in rows]

def tiktok_can_submit(uid):
    today=datetime.now().strftime("%Y-%m-%d"); conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE uid=? AND created LIKE ?",(uid,today+"%"))
    cnt=c.fetchone()[0]; conn.close(); return cnt<TIKTOK_DAILY_LIMIT
def task_create(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id FROM tasks WHERE uid=? AND status='pending'",(uid,)); ex=c.fetchone()
    if ex: conn.close(); return ex[0]
    c.execute("INSERT INTO tasks (uid,status,created) VALUES (?,'pending',?)",(uid,datetime.now().strftime("%Y-%m-%d %H:%M")))
    tid=c.lastrowid; conn.commit(); conn.close(); return tid
def task_approve(tid, admin_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'",(tid,)); r=c.fetchone()
    if not r: conn.close(); return None
    c.execute("UPDATE tasks SET status='approved',reviewed_by=? WHERE id=?",(admin_uid,tid)); conn.commit(); conn.close(); return r[0]
def task_reject(tid, admin_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'",(tid,)); r=c.fetchone()
    if not r: conn.close(); return None
    c.execute("UPDATE tasks SET status='rejected',reviewed_by=? WHERE id=?",(admin_uid,tid)); conn.commit(); conn.close(); return r[0]
def get_pending_tasks():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,uid,created,photo_count FROM tasks WHERE status='pending'"); rows=c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"created":r[2],"photos":r[3]} for r in rows]

def add_blacklist(username, admin_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    try: c.execute("INSERT INTO blacklist (username,added_by,created) VALUES (?,?,?)",(username.lower(),admin_uid,datetime.now().strftime("%Y-%m-%d %H:%M")))
    except: pass
    conn.commit(); conn.close()
def remove_blacklist(username):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("DELETE FROM blacklist WHERE username=?",(username.lower(),)); conn.commit(); conn.close()
def is_blacklisted(username):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("SELECT username FROM blacklist WHERE username=?",(username.lower(),))
    r=c.fetchone(); conn.close(); return r is not None
def get_blacklist():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT username,added_by,created FROM blacklist ORDER BY created DESC"); rows=c.fetchall(); conn.close()
    return [{"username":r[0],"added_by":r[1],"created":r[2]} for r in rows]

def add_monitor(uid, username):
    conn=sqlite3.connect(DB); c=conn.cursor()
    expires=(datetime.now()+timedelta(days=7)).strftime("%Y-%m-%d %H:%M")
    c.execute("INSERT INTO monitors (uid,username,status,created,expires) VALUES (?,?,?,?,?)",
              (uid,username.lower(),'active',datetime.now().strftime("%Y-%m-%d %H:%M"),expires))
    mid=c.lastrowid; conn.commit(); conn.close(); return mid
def get_user_monitors(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,username,status,created,expires,last_status FROM monitors WHERE uid=? AND status='active'",(uid,))
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"username":r[1],"status":r[2],"created":r[3],"expires":r[4],"last_status":r[5]} for r in rows]
def remove_monitor(mid, uid):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE monitors SET status='removed' WHERE id=? AND uid=?",(mid,uid)); conn.commit(); conn.close()
def get_active_monitors():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,uid,username,expires FROM monitors WHERE status='active' AND expires>?",(datetime.now().strftime("%Y-%m-%d %H:%M"),))
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"username":r[2],"expires":r[3]} for r in rows]
def update_monitor_status(mid, status):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE monitors SET last_check=?,last_status=? WHERE id=?",(datetime.now().strftime("%Y-%m-%d %H:%M"),status,mid)); conn.commit(); conn.close()
def expire_monitors():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE monitors SET status='expired' WHERE status='active' AND expires<=?",(datetime.now().strftime("%Y-%m-%d %H:%M"),)); conn.commit(); conn.close()
def get_monitor_count(uid): return len(get_user_monitors(uid))
def get_monitor_limit(uid):
    if uid in ADMIN_IDS: return 99
    u=get_user(uid); base=MONITOR_MAX_PREMIUM if has_subscription(uid) else MONITOR_MAX_FREE
    return base+u.get("monitor_slots",0)

def get_stats():
    conn=sqlite3.connect(DB); c=conn.cursor()
    now_s=datetime.now().strftime("%Y-%m-%d %H:%M"); today=datetime.now().strftime("%Y-%m-%d")
    r={"users":c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
       "subs":c.execute("SELECT COUNT(*) FROM users WHERE sub_end>?",(now_s,)).fetchone()[0],
       "searches":c.execute("SELECT COALESCE(SUM(searches),0) FROM users").fetchone()[0],
       "tasks":c.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0],
       "today_users":c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?",(today+"%",)).fetchone()[0],
       "today_searches":c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?",(today+"%",)).fetchone()[0],
       "banned":c.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0],
       "withdrawals":c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0],
       "promos":c.execute("SELECT COUNT(*) FROM promotions WHERE active=1").fetchone()[0],
       "monitors":c.execute("SELECT COUNT(*) FROM monitors WHERE status='active'").fetchone()[0],
       "blacklist":c.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0]}
    conn.close(); return r

def find_user(inp):
    inp=inp.strip().replace("@","")
    if inp.isdigit(): return int(inp)
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("SELECT uid FROM users WHERE uname=?",(inp,))
    r=c.fetchone(); conn.close(); return r[0] if r else None

def set_last_roulette(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE users SET last_roulette=? WHERE uid=?",(datetime.now().strftime("%Y-%m-%d %H:%M"),uid)); conn.commit(); conn.close()
def can_roulette(uid):
    u=get_user(uid); lr=u.get("last_roulette","")
    if not lr: return True
    try: return (datetime.now()-datetime.strptime(lr,"%Y-%m-%d %H:%M")).days>=7
    except: return True
def get_expiring_users(days_before):
    conn=sqlite3.connect(DB); c=conn.cursor(); t=datetime.now()+timedelta(days=days_before)
    c.execute("SELECT uid,sub_end,auto_renew,auto_renew_plan,last_reminder FROM users WHERE sub_end BETWEEN ? AND ? AND sub_end!=''",(t.strftime("%Y-%m-%d 00:00"),t.strftime("%Y-%m-%d 23:59")))
    rows=c.fetchall(); conn.close()
    return [{"uid":r[0],"sub_end":r[1],"auto_renew":r[2],"auto_renew_plan":r[3],"last_reminder":r[4] or ""} for r in rows]
def set_last_reminder(uid, ds):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE users SET last_reminder=? WHERE uid=?",(ds,uid)); conn.commit(); conn.close()
def remove_referral(referrer_uid, referred_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("DELETE FROM referrals WHERE referrer_uid=? AND referred_uid=?",(referrer_uid,referred_uid))
    c.execute("UPDATE users SET ref_count=MAX(ref_count-1,0),extra_searches=MAX(extra_searches-?,0) WHERE uid=?",(REF_BONUS,referrer_uid))
    c.execute("UPDATE users SET referred_by=0 WHERE uid=?",(referred_uid,)); conn.commit(); conn.close()

# ═══════════════════════ МАРКЕТПЛЕЙС ФУНКЦИИ ═══════════════════════

def market_create_lot(seller_uid, mtype, title, description, price, is_nft=0, fragment_url=""):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO market (seller_uid,mtype,title,description,price,status,created,is_nft,fragment_url) VALUES (?,?,?,?,?,'pending',?,?,?)",
              (seller_uid,mtype,title,description,price,datetime.now().strftime("%Y-%m-%d %H:%M"),is_nft,fragment_url))
    lot_id=c.lastrowid; conn.commit(); conn.close(); return lot_id

def market_approve_lot(lot_id, admin_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET status='active',moderated_by=? WHERE id=? AND status='pending'",(admin_uid,lot_id))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

def market_reject_lot(lot_id, admin_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET status='rejected',moderated_by=? WHERE id=? AND status='pending'",(admin_uid,lot_id))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

def market_get_active_lots(limit=20, nft_only=False):
    conn=sqlite3.connect(DB); c=conn.cursor()
    if nft_only: c.execute("SELECT id,seller_uid,mtype,title,description,price,created,promoted,is_nft,fragment_url FROM market WHERE status='active' AND is_nft=1 ORDER BY promoted DESC, id DESC LIMIT ?",(limit,))
    else: c.execute("SELECT id,seller_uid,mtype,title,description,price,created,promoted,is_nft,fragment_url FROM market WHERE status='active' AND is_nft=0 ORDER BY promoted DESC, id DESC LIMIT ?",(limit,))
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"type":r[2],"title":r[3],"desc":r[4],"price":int(r[5]),
             "created":r[6],"promoted":r[7],"is_nft":r[8],"fragment_url":r[9]} for r in rows]

def market_get_lot(lot_id):
    conn=sqlite3.connect(DB); conn.row_factory=sqlite3.Row; c=conn.cursor()
    c.execute("SELECT * FROM market WHERE id=?",(lot_id,)); row=c.fetchone(); conn.close()
    if not row: return None
    d=dict(row); d["price"]=int(d.get("price",0)); return d

def market_get_user_lots(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,mtype,title,price,status,promoted,is_nft FROM market WHERE seller_uid=? AND status IN ('pending','active','escrow') ORDER BY id DESC",(uid,))
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"type":r[1],"title":r[2],"price":int(r[3]),"status":r[4],"promoted":r[5],"is_nft":r[6]} for r in rows]

def market_get_pending():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,seller_uid,mtype,title,price,created,is_nft,fast_mod FROM market WHERE status='pending' ORDER BY fast_mod DESC, id")
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"type":r[2],"title":r[3],"price":int(r[4]),"created":r[5],"is_nft":r[6],"fast":r[7]} for r in rows]

def market_buy_lot(lot_id, buyer_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    deadline=(datetime.now()+timedelta(hours=MARKET_ESCROW_HOURS)).strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE market SET status='escrow',buyer_uid=?,sold_at=?,escrow_deadline=? WHERE id=? AND status='active' AND seller_uid!=?",
              (buyer_uid,datetime.now().strftime("%Y-%m-%d %H:%M"),deadline,lot_id,buyer_uid))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

def market_confirm_seller(lot_id):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET seller_confirmed=1 WHERE id=?",(lot_id,)); conn.commit(); conn.close()
    return _check_deal_complete(lot_id)

def market_confirm_buyer(lot_id):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET buyer_confirmed=1 WHERE id=?",(lot_id,)); conn.commit(); conn.close()
    return _check_deal_complete(lot_id)

def _check_deal_complete(lot_id):
    lot=market_get_lot(lot_id)
    if not lot: return False
    if lot["seller_confirmed"] and lot["buyer_confirmed"]:
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("UPDATE market SET status='completed' WHERE id=?",(lot_id,))
        payout=int(lot["price"]*(1-MARKET_COMMISSION))
        c.execute("UPDATE users SET balance=balance+? WHERE uid=?",(payout,lot["seller_uid"]))
        conn.commit(); conn.close(); return True
    return False

def market_promote_lot(lot_id):
    conn=sqlite3.connect(DB); c=conn.cursor()
    until=(datetime.now()+timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE market SET promoted=1,promoted_until=? WHERE id=?",(until,lot_id)); conn.commit(); conn.close()

def market_cancel_lot(lot_id, uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET status='cancelled' WHERE id=? AND seller_uid=? AND status IN ('pending','active')",(lot_id,uid))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

def market_count_user_lots(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT COUNT(*) FROM market WHERE seller_uid=? AND status IN ('pending','active')",(uid,))
    cnt=c.fetchone()[0]; conn.close(); return cnt

def market_get_max_lots(uid):
    base=MARKET_VIP_MAX_LOTS if has_vip(uid) else MARKET_MAX_LOTS
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT extra_slots FROM market_slots WHERE uid=?",(uid,)); row=c.fetchone(); conn.close()
    return base+(row[0] if row else 0)

def market_add_slot(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT OR REPLACE INTO market_slots (uid,extra_slots) VALUES (?,COALESCE((SELECT extra_slots FROM market_slots WHERE uid=?),0)+1)",(uid,uid))
    conn.commit(); conn.close()

def market_open_dispute(lot_id, reason, by_uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE market SET dispute=1,dispute_reason=?,status='dispute' WHERE id=?",(f"{by_uid}: {reason}",lot_id))
    conn.commit(); conn.close()

def market_resolve_dispute(lot_id, winner, admin_uid):
    lot=market_get_lot(lot_id)
    if not lot: return False, None
    conn=sqlite3.connect(DB); c=conn.cursor()
    if winner=="buyer":
        c.execute("UPDATE market SET status='refunded',moderated_by=? WHERE id=?",(admin_uid,lot_id))
        c.execute("SELECT charge_id FROM market WHERE id=?",(lot_id,))
        row=c.fetchone(); charge_id=row[0] if row and row[0] else None
        conn.commit(); conn.close(); return True, charge_id
    else:
        payout=int(lot["price"]*(1-MARKET_COMMISSION))
        c.execute("UPDATE users SET balance=balance+? WHERE uid=?",(payout,lot["seller_uid"]))
        c.execute("UPDATE market SET status='completed',moderated_by=? WHERE id=?",(admin_uid,lot_id))
        conn.commit(); conn.close(); return True, None

def market_get_disputes():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,seller_uid,buyer_uid,title,price,dispute_reason FROM market WHERE status='dispute'")
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"buyer":r[2],"title":r[3],"price":int(r[4]),"reason":r[5]} for r in rows]

def market_set_fast_mod(lot_id):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE market SET fast_mod=1 WHERE id=?",(lot_id,)); conn.commit(); conn.close()

def lootbox_can_open(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT created FROM lootbox_history WHERE uid=? ORDER BY id DESC LIMIT 1",(uid,))
    row=c.fetchone(); conn.close()
    if not row: return True
    try: return (datetime.now()-datetime.strptime(row[0],"%Y-%m-%d %H:%M")).total_seconds()>=LOOTBOX_COOLDOWN
    except: return True

def lootbox_open(uid):
    roll=random.randint(1,100)
    if roll<=5: prize_type="premium"; days=random.choice([3,7]); give_subscription(uid,days); prize=f"💎 Premium {days} дней!"
    elif roll<=15: prize_type="vip"; days=random.choice([1,3]); give_vip(uid,days); prize=f"🌟 VIP {days} дней!"
    elif roll<=35: prize_type="stars"; amount=random.choice([5,10,15,20,25,50]); add_balance(uid,amount); prize=f"⭐ {amount} звёзд!"
    elif roll<=60: prize_type="searches"; count=random.choice([2,3,5,7,10]); add_extra_searches(uid,count); prize=f"🔍 {count} поисков!"
    elif roll<=80: prize_type="slot"; market_add_slot(uid); prize="📦 +1 слот маркета!"
    else: prize_type="emoji"; prize=random.choice(["🧸 Плюшевый мишка!","🃏 Джокер!","🎭 Маска!","🎉 Ура!"])
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO lootbox_history (uid,prize,prize_type,created) VALUES (?,?,?,?)",
              (uid,prize,prize_type,datetime.now().strftime("%Y-%m-%d %H:%M"))); conn.commit(); conn.close()
    return prize, prize_type

def create_promocode(code, discount_percent=0, discount_stars=0, max_uses=1, min_purchase=0, applies_to="all", created_by=0, expires=""):
    conn=sqlite3.connect(DB); c=conn.cursor()
    try:
        c.execute("INSERT INTO promocodes (code,discount_percent,discount_stars,max_uses,min_purchase,applies_to,created_by,created,expires) VALUES (?,?,?,?,?,?,?,?,?)",
                  (code.upper(),discount_percent,discount_stars,max_uses,min_purchase,applies_to,created_by,datetime.now().strftime("%Y-%m-%d %H:%M"),expires))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def check_promocode(code, uid, purchase_amount=0, purchase_type="all"):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT * FROM promocodes WHERE code=? AND active=1",(code.upper(),)); row=c.fetchone()
    if not row: conn.close(); return {"valid":False,"reason":"Не найден"}
    cols=[d[0] for d in c.description]; promo=dict(zip(cols,row))
    if promo.get("expires",""):
        try:
            if datetime.strptime(promo["expires"],"%Y-%m-%d %H:%M")<datetime.now(): conn.close(); return {"valid":False,"reason":"Истёк"}
        except: pass
    if promo["used_count"]>=promo["max_uses"]: conn.close(); return {"valid":False,"reason":"Исчерпан"}
    c.execute("SELECT COUNT(*) FROM promocode_uses WHERE code=? AND uid=?",(code.upper(),uid))
    if c.fetchone()[0]>0: conn.close(); return {"valid":False,"reason":"Уже использован"}
    if promo["min_purchase"]>0 and purchase_amount<promo["min_purchase"]: conn.close(); return {"valid":False,"reason":f"Мин {promo['min_purchase']}⭐"}
    discount=0
    if promo["discount_percent"]>0: discount=int(purchase_amount*promo["discount_percent"]/100)
    elif promo["discount_stars"]>0: discount=min(promo["discount_stars"],purchase_amount-1)
    conn.close(); return {"valid":True,"discount":max(1,discount),"promo":promo}

def use_promocode(code, uid, discount_amount):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE promocodes SET used_count=used_count+1 WHERE code=?",(code.upper(),))
    c.execute("INSERT INTO promocode_uses (code,uid,used_at,discount_amount) VALUES (?,?,?,?)",
              (code.upper(),uid,datetime.now().strftime("%Y-%m-%d %H:%M"),discount_amount)); conn.commit(); conn.close()

def get_all_promocodes():
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT code,discount_percent,discount_stars,max_uses,used_count,expires,active FROM promocodes ORDER BY id DESC")
    rows=c.fetchall(); conn.close()
    return [{"code":r[0],"percent":r[1],"stars":r[2],"max":r[3],"used":r[4],"expires":r[5],"active":r[6]} for r in rows]

def deactivate_promocode(code):
    conn=sqlite3.connect(DB); c=conn.cursor(); c.execute("UPDATE promocodes SET active=0 WHERE code=?",(code.upper(),)); conn.commit(); conn.close()

def add_review(from_uid, to_uid, rating, text, deal_id=0, deal_type="market"):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO reviews (from_uid,to_uid,rating,text,deal_id,deal_type,created) VALUES (?,?,?,?,?,?,?)",
              (from_uid,to_uid,min(5,max(1,rating)),text[:200],deal_id,deal_type,datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()

def get_user_rating(uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT AVG(rating),COUNT(*) FROM reviews WHERE to_uid=?",(uid,))
    row=c.fetchone(); conn.close()
    return {"avg":round(row[0],1) if row[0] else 0,"count":row[1] or 0}

def get_user_reviews(uid, limit=10):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT from_uid,rating,text,created FROM reviews WHERE to_uid=? ORDER BY id DESC LIMIT ?",(uid,limit))
    rows=c.fetchall(); conn.close()
    return [{"from":r[0],"rating":r[1],"text":r[2],"created":r[3]} for r in rows]

def exchange_create(uid, offer):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("INSERT INTO exchanges (initiator_uid,initiator_offer,status,created) VALUES (?,?,'open',?)",
              (uid,offer,datetime.now().strftime("%Y-%m-%d %H:%M")))
    eid=c.lastrowid; conn.commit(); conn.close(); return eid

def exchange_accept(eid, partner_uid, partner_offer):
    conn=sqlite3.connect(DB); c=conn.cursor()
    deadline=(datetime.now()+timedelta(hours=MARKET_ESCROW_HOURS)).strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE exchanges SET partner_uid=?,partner_offer=?,status='escrow',escrow_deadline=? WHERE id=? AND status='open' AND initiator_uid!=?",
              (partner_uid,partner_offer,deadline,eid,partner_uid))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

def exchange_confirm(eid, uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT initiator_uid,partner_uid FROM exchanges WHERE id=?",(eid,)); row=c.fetchone()
    if not row: conn.close(); return False
    if uid==row[0]: c.execute("UPDATE exchanges SET initiator_confirmed=1 WHERE id=?",(eid,))
    elif uid==row[1]: c.execute("UPDATE exchanges SET partner_confirmed=1 WHERE id=?",(eid,))
    else: conn.close(); return False
    conn.commit(); conn.close()
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT initiator_confirmed,partner_confirmed FROM exchanges WHERE id=?",(eid,))
    r=c.fetchone()
    if r and r[0] and r[1]:
        c.execute("UPDATE exchanges SET status='completed',completed_at=? WHERE id=?",(datetime.now().strftime("%Y-%m-%d %H:%M"),eid))
        conn.commit(); conn.close(); return True
    conn.close(); return False

def exchange_get_open(limit=20):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,initiator_uid,initiator_offer,created FROM exchanges WHERE status='open' ORDER BY id DESC LIMIT ?",(limit,))
    rows=c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"offer":r[2],"created":r[3]} for r in rows]

def exchange_get(eid):
    conn=sqlite3.connect(DB); conn.row_factory=sqlite3.Row; c=conn.cursor()
    c.execute("SELECT * FROM exchanges WHERE id=?",(eid,)); row=c.fetchone(); conn.close()
    return dict(row) if row else None

def exchange_cancel(eid, uid):
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("UPDATE exchanges SET status='cancelled' WHERE id=? AND initiator_uid=? AND status='open'",(eid,uid))
    changed=c.rowcount; conn.commit(); conn.close(); return changed>0

# ═══════════════════════ HELPERS ═══════════════════════

async def notify_admins(text, exclude=None, kb=None):
    for aid in ADMIN_IDS:
        if exclude and aid==exclude: continue
        try: await bot.send_message(aid, text, reply_markup=kb, parse_mode="HTML")
        except: pass

def build_sub_kb(channels):
    text = "📢 <b>Подпишитесь:</b>\n\n"
    kb = InlineKeyboardBuilder()
    for ch in channels: text += f"  ❌ @{ch}\n"; kb.button(text=f"📢 @{ch}", url=f"https://t.me/{ch}")
    text += "\n✅ За подписку <b>+2 поиска!</b>"
    kb.button(text="✅ Проверить", callback_data="check_sub"); kb.adjust(1)
    return text, kb.as_markup()
    # ═══════════════════════ МЕНЮ ═══════════════════════

def build_menu(uid):
    u = get_user(uid); ps = pool.stats()
    is_prem = has_subscription(uid); is_admin = uid in ADMIN_IDS
    is_vip_user = has_vip(uid)
    config = load_bot_config()
    if is_admin: si,st,sub_info = "👑","ADMIN","♾"
    elif is_vip_user: si,st = "🌟","VIP"; sub_info = f"до {u.get('vip_end','?')}"
    elif is_prem: si,st = "💎","PREMIUM"; sub_info = f"до {u.get('sub_end','?')}"
    else:
        total_free = u.get("free",0)+u.get("extra_searches",0)
        if total_free > 0: si,st = "🆓","FREE"; sub_info = f"{total_free} поисков"
        else: si,st,sub_info = "⛔️","ЛИМИТ","закончились"
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    bal = u.get("balance",0.0); promos = get_active_promotions()
    sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
    text = (f"🔍 <b>USERNAME HUNTER</b> {si}\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>{st}</b> | 🎯 <code>{cnt}</code> юзов/поиск\n📊 {sub_info}")
    if is_prem and not is_admin:
        used = u.get("daily_searches_used",0)
        limit = VIP_SEARCHES_LIMIT if is_vip_user else PREMIUM_SEARCHES_LIMIT
        text += f" | Сегодня: <code>{used}/{limit}</code>"
    text += f"\n🔢 Всего: <code>{u.get('searches',0)}</code>\n🔄 {sl}\n💰 Баланс: <code>{bal:.1f}</code> ⭐\n"
    if promos:
        text += "\n🎉 <b>Акции:</b>\n"
        for pr in promos: text += f"  • {pr['name']}\n"
    text += "\n━━━━━━━━━━━━━━━━━━━━━━━"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Поиск", callback_data="cmd_search")
    kb.button(text="📊 Оценка", callback_data="cmd_evaluate")
    kb.button(text="🔧 Утилиты", callback_data="cmd_utils")
    kb.button(text="👤 Профиль", callback_data="cmd_profile")
    if is_button_enabled("shop"): kb.button(text="🏪 Магазин", callback_data="cmd_shop")
    if is_button_enabled("referral"): kb.button(text="👥 Рефералы", callback_data="cmd_referral")
    if is_button_enabled("tiktok"): kb.button(text="🎁 TikTok", callback_data="cmd_tiktok")
    for pr in promos: kb.button(text=pr.get("button_text") or pr["name"], callback_data=f"pv_{pr['id']}")
    if is_button_enabled("support"): kb.button(text="🤖 Поддержать", callback_data="cmd_support")
    if is_admin: kb.button(text="👑 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    return text, kb.as_markup()

async def show_user_panel(msg_or_cb, target_uid):
    u=get_user(target_uid); is_prem=has_subscription(target_uid); is_vip_user=has_vip(target_uid)
    _reset_daily_if_needed(target_uid)
    if target_uid in ADMIN_IDS: status="👑 ADMIN"
    elif u.get("banned",0)==1: status="🚫 BANNED"
    elif is_vip_user: status="🌟 VIP"
    elif is_prem: status="💎 PREMIUM"
    else: status="🆓 FREE"
    limit=VIP_SEARCHES_LIMIT if is_vip_user else PREMIUM_SEARCHES_LIMIT
    text=(f"👤 <b>Панель юзера</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🆔 <code>{target_uid}</code> | @{u.get('uname','-') or '-'}\n📌 {status}\n\n"
          f"🔍 Free: <code>{u.get('free',0)}</code> | Extra: <code>{u.get('extra_searches',0)}</code>\n"
          f"📊 Всего: <code>{u.get('searches',0)}</code> | Сегодня: <code>{u.get('daily_searches_used',0)}</code>\n"
          f"💰 Баланс: <code>{u.get('balance',0):.1f}</code>⭐ | 👥 Реф: <code>{u.get('ref_count',0)}</code>\n"
          f"💎 Sub: <code>{u.get('sub_end','-') or '-'}</code>\n🌟 VIP: <code>{u.get('vip_end','-') or '-'}</code>")
    kb=InlineKeyboardBuilder()
    kb.button(text="🔍 +поиски",callback_data=f"au_adds_{target_uid}")
    kb.button(text="🔍 =поиски",callback_data=f"au_sets_{target_uid}")
    kb.button(text="💰 =баланс",callback_data=f"au_setb_{target_uid}")
    kb.button(text="💎 +подписка",callback_data=f"au_addd_{target_uid}")
    kb.button(text="💎 Убрать",callback_data=f"au_remd_{target_uid}")
    kb.button(text="🌟 +VIP",callback_data=f"au_addv_{target_uid}")
    kb.button(text="🌟 Убрать VIP",callback_data=f"au_remv_{target_uid}")
    kb.button(text="🔄 Сброс дневных",callback_data=f"au_resetd_{target_uid}")
    if u.get("banned",0)==1: kb.button(text="✅ Разбан",callback_data=f"au_unban_{target_uid}")
    else: kb.button(text="🚫 Бан",callback_data=f"au_ban_{target_uid}")
    kb.button(text="📜 История",callback_data=f"au_hist_{target_uid}")
    kb.button(text="📤 Написать",callback_data=f"au_msg_{target_uid}")
    kb.button(text="🔙 Админ",callback_data="cmd_admin")
    kb.adjust(2)
    if hasattr(msg_or_cb,'message'): await edit_msg(msg_or_cb.message, text, kb.as_markup())
    else: await msg_or_cb.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

# ═══════════════════════ КОМАНДЫ ═══════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, command: CommandObject):
    uid=msg.from_user.id; uname=msg.from_user.username or ""
    is_new=get_user(uid).get("searches",0)==0
    ensure_user(uid,uname); log_action(uid,"start",command.args or "")
    if is_banned(uid): await msg.answer("🚫 Заблокированы."); return
    if command.args and command.args.startswith("ref_") and is_new:
        try:
            ref_id=int(command.args.replace("ref_",""))
            if ref_id!=uid:
                set_pending_ref(uid,ref_id)
                kb=InlineKeyboardBuilder(); kb.button(text="Не робот 🌟",callback_data="captcha_ok")
                await msg.answer("🤖 <b>Подтвердите:</b>",reply_markup=kb.as_markup(),parse_mode="HTML"); return
        except: pass
    ns=await check_subscribed(uid)
    if ns: t,k=build_sub_kb(ns)
    else: t,k=build_menu(uid)
    await msg.answer(t,reply_markup=k,parse_mode="HTML",disable_web_page_preview=True)

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_menu")
    await msg.answer(f"📖 /check username\n/similar username\n/balance\n/id\n\n📩 @{ADMIN_CONTACT}",reply_markup=kb.as_markup(),parse_mode="HTML")

@dp.message(Command("id"))
async def cmd_id(msg: Message): await msg.answer(f"🆔 <code>{msg.from_user.id}</code>",parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check_cmd(msg: Message, command: CommandObject):
    uid=msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid,msg.from_user.username)
    un=(command.args or "").strip().replace("@","").lower()
    if not validate_username(un): await msg.answer("❌ <code>/check username</code>",parse_mode="HTML"); return
    log_action(uid,"check",un); wm=await msg.answer("⏳...")
    tg=await check_username(un)
    st={"free":"✅ Свободен!","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
    try: await wm.delete()
    except: pass
    kb=InlineKeyboardBuilder(); kb.button(text="📊 Оценка",callback_data=f"eval_{un}"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(2)
    await msg.answer(f"🔍 <b>@{un}</b> — {st}\n\n📱 <a href='https://t.me/{un}'>Telegram</a> · 💎 <a href='https://fragment.com/username/{un}'>Fragment</a>",
                     reply_markup=kb.as_markup(),parse_mode="HTML",disable_web_page_preview=True)

@dp.message(Command("similar"))
async def cmd_similar_cmd(msg: Message, command: CommandObject):
    uid=msg.from_user.id
    if is_banned(uid): return
    if not has_subscription(uid) and uid not in ADMIN_IDS: await msg.answer("🔒 Нужен Premium"); return
    un=(command.args or "").strip().replace("@","").lower()
    if not validate_username(un): await msg.answer("❌"); return
    wm=await msg.answer(f"🔄 @{un}...")
    found,stats=await do_similar_search(un,5,wm,uid)
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(wm,format_results(found,stats,f"Похожие на @{un}"),kb.as_markup())

@dp.message(Command("balance"))
async def cmd_balance(msg: Message):
    uid=msg.from_user.id; ensure_user(uid,msg.from_user.username); bal=get_balance(uid)
    kb=InlineKeyboardBuilder(); kb.button(text="🏪",callback_data="cmd_shop"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await msg.answer(f"💰 <code>{bal:.1f}</code> ⭐",reply_markup=kb.as_markup(),parse_mode="HTML")

# ═══════════════════════ БАЗОВЫЕ CALLBACKS ═══════════════════════

@dp.callback_query(F.data == "captcha_ok")
async def cb_captcha(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); ensure_user(uid,cb.from_user.username)
    ref_uid=get_pending_ref(uid)
    if ref_uid and ref_uid!=uid:
        ok=process_referral(uid,ref_uid); set_pending_ref(uid,0); set_captcha_passed(uid)
        if ok:
            try: await bot.send_message(ref_uid,f"🎉 Новый реферал! <b>+{REF_BONUS} поисков</b>",parse_mode="HTML")
            except: pass
    else: set_captcha_passed(uid)
    t,k=build_menu(uid); await edit_msg(cb.message,t,k)

@dp.callback_query(F.data == "check_sub")
async def cb_cs(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    ns=await check_subscribed(uid)
    if ns: t,k=build_sub_kb(ns); await edit_msg(cb.message,t,k); return
    u=get_user(uid)
    if u.get("sub_bonus",0)==0:
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("UPDATE users SET free=free+2,sub_bonus=1 WHERE uid=?",(uid,)); conn.commit(); conn.close()
    t,k=build_menu(uid); await edit_msg(cb.message,t,k)

@dp.callback_query(F.data == "cmd_menu")
async def cb_menu(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    user_states.pop(uid,None); t,k=build_menu(uid); await edit_msg(cb.message,t,k)

# ═══════════════════════ ПОИСК ═══════════════════════

@dp.callback_query(F.data == "cmd_search")
async def cb_search(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    if not can_search(uid):
        kb=InlineKeyboardBuilder(); kb.button(text="🏪",callback_data="cmd_shop"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message,"⛔️ <b>Поиски закончились!</b>",kb.as_markup()); return
    is_prem=uid in ADMIN_IDS or has_subscription(uid); is_vip_user=has_vip(uid)
    cnt=get_search_count(uid); mx=get_max_searches(uid)
    kb=InlineKeyboardBuilder(); mt=""
    for key,m in SEARCH_MODES.items():
        if m.get("disabled"): continue
        if m["premium"] and not is_prem:
            kb.button(text=f"🔒 {m['emoji']} {m['name']}",callback_data="need_prem"); lk="🔒"
        else: kb.button(text=f"{m['emoji']} {m['name']}",callback_data=f"go_{key}"); lk="✅"
        mt+=f"{lk} <b>{m['emoji']} {m['name']}</b> — {m['desc']}\n"
    if is_vip_user: kb.button(text="🎯 По слову (VIP)",callback_data="cmd_thematic"); mt+="✅ <b>🎯 По слову</b> — VIP\n"
    else: kb.button(text="🔒🌟 По слову (VIP)",callback_data="need_vip"); mt+="🔒 <b>🎯 По слову</b> — VIP\n"
    if is_prem:
        kb.button(text="🎯 Шаблон",callback_data="cmd_template"); kb.button(text="🔄 Похожие",callback_data="cmd_similar_cb")
    else:
        kb.button(text="🔒 Шаблон",callback_data="need_prem"); kb.button(text="🔒 Похожие",callback_data="need_prem")
    kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(2)
    fl="♾" if uid in ADMIN_IDS else str(mx)
    await edit_msg(cb.message,f"🔍 <b>Режим:</b>\n\n{mt}\n🎯 <code>{cnt}</code> юзов | Осталось: <b>{fl}</b>",kb.as_markup())

@dp.callback_query(F.data == "need_prem")
async def cb_np(cb: CallbackQuery): await answer_cb(cb,"🔒 Нужен Premium!",show_alert=True)
@dp.callback_query(F.data == "need_vip")
async def cb_nv(cb: CallbackQuery): await answer_cb(cb,"🌟 Нужен VIP!",show_alert=True)

@dp.callback_query(F.data.startswith("go_"))
async def cb_go(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    if not can_search(uid):
        kb=InlineKeyboardBuilder(); kb.button(text="🏪",callback_data="cmd_shop"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message,"⛔️ <b>Закончились!</b>",kb.as_markup()); return
    mode=cb.data[3:]; mi=SEARCH_MODES.get(mode)
    if not mi or mi.get("disabled"): return
    if mi["premium"] and not (uid in ADMIN_IDS or has_subscription(uid)): return
    if uid not in ADMIN_IDS:
        if uid in searching_users: await answer_cb(cb,"⏳ Уже идёт поиск!",show_alert=True); return
        cd=user_search_cooldown.get(uid,0); rem=SEARCH_COOLDOWN-(time.time()-cd)
        if rem>0: await answer_cb(cb,f"⏳ {int(rem)} сек.",show_alert=True); return
    searching_users.add(uid)
    try:
        await edit_msg(cb.message,f"🚀 <b>{mi['emoji']} {mi['name']}</b>\n\n⏳ Ищу...")
        use_search(uid); log_action(uid,"search",mode)
        found,stats=await do_search(get_search_count(uid),mi["func"],cb.message,mi["name"],uid)
        kb=InlineKeyboardBuilder()
        if can_search(uid): kb.button(text="🔄 Ещё",callback_data=cb.data)
        kb.button(text="🔍 Режимы",callback_data="cmd_search"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message,format_results(found,stats,mi["name"]),kb.as_markup())
    finally:
        searching_users.discard(uid)
        if uid not in ADMIN_IDS: user_search_cooldown[uid]=time.time()

@dp.callback_query(F.data == "cmd_thematic")
async def cb_thematic(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if not has_vip(uid) and uid not in ADMIN_IDS:
        kb=InlineKeyboardBuilder(); kb.button(text="🏪",callback_data="cmd_shop"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message,"🌟 <b>Нужен VIP!</b>",kb.as_markup()); return
    user_states[uid]={"action":"thematic_search"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_search")
    await edit_msg(cb.message,"🎯 <b>Тематический поиск</b>\n\nВведите слово:",kb.as_markup())

@dp.callback_query(F.data == "cmd_template")
async def cb_template(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        u=get_user(uid)
        if u.get("template_uses",0)<=0:
            kb=InlineKeyboardBuilder(); kb.button(text="🏪",callback_data="cmd_shop"); kb.adjust(1)
            await edit_msg(cb.message,"🔒 Нужен Premium",kb.as_markup()); return
    user_states[uid]={"action":"template_search"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_search")
    await edit_msg(cb.message,"🎯 <b>Шаблон</b>\n\n<code>*</code> = 2-4\n<code>?</code> = 1\n\nПример: <code>max_*</code>",kb.as_markup())

@dp.callback_query(F.data == "cmd_similar_cb")
async def cb_similar_cb(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message,"🔒 Нужен Premium",kb.as_markup()); return
    user_states[uid]={"action":"similar_search"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_search")
    await edit_msg(cb.message,"🔄 <b>Введите юзернейм:</b>",kb.as_markup())

# ═══════════════════════ ОЦЕНКА / УТИЛИТЫ ═══════════════════════

@dp.callback_query(F.data == "cmd_evaluate")
async def cb_eval(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"evaluate"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_menu")
    await edit_msg(cb.message,"📊 <b>Введите юзернейм:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("eval_"))
async def cb_eval_direct(cb: CallbackQuery):
    await answer_cb(cb); un=cb.data[5:]
    tg=await check_username(un); fr=await check_fragment(un)
    tgs={"free":"✅ Свободен","taken":"❌ Занят"}.get(tg,"❓")
    frs={"fragment":"💎 Fragment","sold":"✅ Продан","unavailable":"—"}.get(fr,"❓")
    ev=evaluate_username(un); fac="\n".join("  "+f for f in ev["factors"])
    kb=InlineKeyboardBuilder()
    if tg=="free": kb.button(text="👁 Мониторинг",callback_data=f"mon_add_{un}")
    kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,f"📊 <b>@{un}</b>\n\n📱 {tgs}\n💎 {frs}\n\n🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}\n\n📱 <a href='https://t.me/{un}'>TG</a> · 💎 <a href='https://fragment.com/username/{un}'>Fragment</a>",kb.as_markup())

@dp.callback_query(F.data == "cmd_utils")
async def cb_utils(cb: CallbackQuery):
    await answer_cb(cb); kb=InlineKeyboardBuilder()
    kb.button(text="🔍 Проверка",callback_data="util_check"); kb.button(text="📋 Массовая",callback_data="util_mass")
    kb.button(text="📜 История",callback_data="util_hist")
    if is_button_enabled("monitor"): kb.button(text="👁 Мониторинг",callback_data="cmd_monitors")
    kb.button(text="📥 Экспорт",callback_data="util_export"); kb.button(text="🗑 Удалить",callback_data="util_delete_pattern")
    kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(2)
    await edit_msg(cb.message,"🔧 <b>Утилиты</b>",kb.as_markup())

@dp.callback_query(F.data == "util_check")
async def cb_uc(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"quick_check"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_utils")
    await edit_msg(cb.message,"🔍 <b>Юзернейм:</b>",kb.as_markup())

@dp.callback_query(F.data == "util_mass")
async def cb_um(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"mass_check"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_utils")
    await edit_msg(cb.message,"📋 <b>По строке (макс 20):</b>",kb.as_markup())

@dp.callback_query(F.data == "util_hist")
async def cb_uh(cb: CallbackQuery):
    await answer_cb(cb); uid=cb.from_user.id; hist=get_history(uid)
    text=f"📜 <b>({len(hist)})</b>\n\n" if hist else "📜 Пусто"
    for h in hist[:15]: text+=f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
    kb=InlineKeyboardBuilder(); kb.button(text="📥 TXT",callback_data="util_export"); kb.button(text="🔙",callback_data="cmd_utils"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "util_export")
async def cb_ue(cb: CallbackQuery):
    await answer_cb(cb); uid=cb.from_user.id; hist=get_history(uid,100)
    if not hist: return
    content="ИСТОРИЯ\n\n"
    for i,h in enumerate(hist,1): content+=f"{i}. @{h[0]} | {h[2]} | {h[1]}\n"
    await bot.send_document(uid,BufferedInputFile(content.encode(),filename=f"history_{uid}.txt"),caption="📥")

@dp.callback_query(F.data == "util_delete_pattern")
async def cb_del_pat(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"delete_pattern"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_utils")
    await edit_msg(cb.message,"🗑 <b>Введите часть юзернейма для удаления:</b>",kb.as_markup())

# ═══════════════════════ МОНИТОРИНГ ═══════════════════════

@dp.callback_query(F.data == "cmd_monitors")
async def cb_monitors(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    mons=get_user_monitors(uid); limit=get_monitor_limit(uid)
    text=f"👁 <b>Мониторинг</b> <code>{len(mons)}/{limit}</code>\n\n"
    kb=InlineKeyboardBuilder()
    for m in mons:
        si="✅" if m["last_status"]=="free" else "❌"
        text+=f"{si} <code>@{m['username']}</code> до {m['expires'][:10]}\n"
        kb.button(text=f"❌ {m['username']}",callback_data=f"mon_del_{m['id']}")
    if not mons: text+="<i>Пусто</i>"
    kb.button(text="➕ Добавить",callback_data="mon_add_new")
    kb.button(text="🔙",callback_data="cmd_utils"); kb.adjust(2,1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "mon_add_new")
async def cb_mon_new(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if get_monitor_count(uid)>=get_monitor_limit(uid):
        await answer_cb(cb,"❌ Лимит",show_alert=True); return
    user_states[uid]={"action":"monitor_add"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_monitors")
    await edit_msg(cb.message,"👁 <b>Юзернейм:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("mon_add_"))
async def cb_mon_add(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); un=cb.data[8:]
    if un=="new": return
    if get_monitor_count(uid)>=get_monitor_limit(uid): await answer_cb(cb,"❌",show_alert=True); return
    add_monitor(uid,un); log_action(uid,"monitor_add",un)
    kb=InlineKeyboardBuilder(); kb.button(text="👁 Мои",callback_data="cmd_monitors"); kb.adjust(1)
    await edit_msg(cb.message,f"✅ @{un} на мониторинге",kb.as_markup())

@dp.callback_query(F.data.startswith("mon_del_"))
async def cb_mon_del(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); remove_monitor(int(cb.data[8:]),uid); await cb_monitors(cb)

# ═══════════════════════ МАГАЗИН ═══════════════════════

@dp.callback_query(F.data == "cmd_shop")
async def cb_shop(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    u=get_user(uid); extra=u.get("extra_searches",0); bal=get_balance(uid)
    text=(f"🏪 <b>Магазин</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"💰 Баланс: <code>{bal:.1f}⭐</code>\n🔍 Доп. поисков: <code>{extra}</code>\n\n"
          f"💎 <b>Premium:</b>\n")
    for p in PRICES.values(): text+=f"• {p['label']} — <code>{p['stars']}⭐</code>/<code>{p['rub']}₽</code>\n"
    kb=InlineKeyboardBuilder()
    kb.button(text="🔍 Купить поиски",callback_data="shop_buy_searches")
    kb.button(text="💎 Premium",callback_data="shop_premium")
    kb.button(text="🌟 VIP",callback_data="shop_vip")
    kb.button(text="📦 Бандл Premium+VIP",callback_data="shop_bundle")
    kb.button(text="🏪 Маркет",callback_data="cmd_market")
    kb.button(text="🔙 Меню",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "shop_buy_searches")
async def cb_shop_buy(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); bal=get_balance(uid)
    user_states[uid]={"action":"shop_custom_amount"}
    kb=InlineKeyboardBuilder()
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})",url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="❌",callback_data="cmd_shop"); kb.adjust(1)
    await edit_msg(cb.message,f"🔍 <b>Купить поиски</b>\n\nЦена: <code>{SEARCH_PRICE_STARS}⭐</code>/<code>{int(SEARCH_PRICE_STARS*STAR_TO_RUB)}₽</code>/шт\n💰 Баланс: <code>{bal:.1f}⭐</code>\n💳 Рубли — @{PAY_CONTACT}\n\nВведите количество (1-1000):",kb.as_markup())

@dp.callback_query(F.data == "shop_premium")
async def cb_shop_prem(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); bal=get_balance(uid)
    text=(f"💎 <b>Premium</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
          f"• {PREMIUM_COUNT} юзов/поиск\n• {PREMIUM_SEARCHES_LIMIT} поисков/день\n• Все режимы\n• Шаблон + Похожие\n• Мониторинг {MONITOR_MAX_PREMIUM} юзов\n\n<b>Цены:</b>\n\n")
    for p in PRICES.values(): text+=f"• {p['label']} — <code>{p['stars']}⭐</code>/<code>{p['rub']}₽</code>\n"
    text+=f"\n💰 Баланс: <code>{bal:.1f}⭐</code>\n💳 Рубли — @{PAY_CONTACT}"
    kb=InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐/{p['rub']}₽",callback_data=f"buy_{k}")
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})",url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="🔙 Магазин",callback_data="cmd_shop"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "shop_vip")
async def cb_shop_vip(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); bal=get_balance(uid)
    text=(f"🌟 <b>VIP</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
          f"• {VIP_COUNT} юзов/поиск\n• {VIP_SEARCHES_LIMIT} поисков/день\n• 🎯 Тематический поиск\n\n")
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        text+="⚠️ <b>Сначала нужен Premium!</b>"
        kb=InlineKeyboardBuilder(); kb.button(text="💎 Premium",callback_data="shop_premium"); kb.button(text="📦 Бандл",callback_data="shop_bundle"); kb.button(text="🔙",callback_data="cmd_shop"); kb.adjust(1)
    else:
        text+="<b>Цены:</b>\n\n"
        for k,vp in VIP_PRICES.items():
            rub=int(vp['stars']*STAR_TO_RUB); text+=f"• {vp['label']} — <code>{vp['stars']}⭐</code>/<code>{rub}₽</code>\n"
        text+=f"\n💰 Баланс: <code>{bal:.1f}⭐</code>"
        kb=InlineKeyboardBuilder()
        for k,vp in VIP_PRICES.items():
            rub=int(vp['stars']*STAR_TO_RUB); kb.button(text=f"{vp['label']} — {vp['stars']}⭐/{rub}₽",callback_data=f"buyvip_{k}")
        kb.button(text=f"💳 Рубли (@{PAY_CONTACT})",url=f"https://t.me/{PAY_CONTACT}")
        kb.button(text="🔙",callback_data="cmd_shop"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "shop_bundle")
async def cb_shop_bundle(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); bal=get_balance(uid)
    text=f"📦 <b>Бандл Premium+VIP</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n💎+🌟 Скидка 5%\n\n"
    for k,bp in BUNDLE_PRICES.items():
        rub=int(bp['stars']*STAR_TO_RUB); text+=f"• {bp['label']} — <code>{bp['stars']}⭐</code>/<code>{rub}₽</code>\n"
    text+=f"\n💰 Баланс: <code>{bal:.1f}⭐</code>"
    kb=InlineKeyboardBuilder()
    for k,bp in BUNDLE_PRICES.items():
        rub=int(bp['stars']*STAR_TO_RUB); kb.button(text=f"{bp['label']} — {bp['stars']}⭐/{rub}₽",callback_data=f"buybundle_{k}")
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})",url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="🔙",callback_data="cmd_shop"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

# ═══ ВЫБОР ОПЛАТЫ ═══

@dp.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery):
    uid=cb.from_user.id; k=cb.data[4:]; p=PRICES.get(k)
    if not p: return
    await answer_cb(cb); bal=get_balance(uid)
    kb=InlineKeyboardBuilder()
    if bal>=p["stars"]: kb.button(text=f"💰 С баланса ({p['stars']}⭐)",callback_data=f"paybal_prem_{k}")
    kb.button(text=f"⭐ Telegram Stars ({p['stars']}⭐)",callback_data=f"paystars_prem_{k}")
    kb.button(text="❌",callback_data="shop_premium"); kb.adjust(1)
    await edit_msg(cb.message,f"💎 <b>{p['label']}</b>\n💰 {p['stars']}⭐ | Баланс: {bal:.1f}⭐\n\nСпособ оплаты:",kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_prem_"))
async def cb_paybal_prem(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[12:]; p=PRICES.get(k)
    if not p: return
    bal=get_balance(uid)
    if bal<p["stars"]: await answer_cb(cb,"❌ Мало средств",show_alert=True); return
    set_balance(uid,bal-p["stars"]); end=give_subscription(uid,p["days"])
    log_action(uid,"buy_prem_bal",f"{k} -{p['stars']}⭐")
    display=f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb=InlineKeyboardBuilder(); kb.button(text="👤 Профиль",callback_data="cmd_profile")
    await edit_msg(cb.message,f"✅ 💎 {p['label']} до {end}\n💰 -{p['stars']}⭐",kb.as_markup())
    await notify_admins(f"🛒 {display} — Premium {p['label']} за баланс (-{p['stars']}⭐)")

@dp.callback_query(F.data.startswith("paystars_prem_"))
async def cb_paystars_prem(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[14:]; p=PRICES.get(k)
    if not p: return
    await bot.send_invoice(uid,title=f"💎 {p['label']}",description=f"Premium {p['label']}",
        payload=f"sub_{k}_{uid}",provider_token="",currency="XTR",prices=[LabeledPrice(label=p["label"],amount=p["stars"])])

@dp.callback_query(F.data.startswith("buyvip_"))
async def cb_buyvip(cb: CallbackQuery):
    uid=cb.from_user.id; k=cb.data[7:]; vp=VIP_PRICES.get(k)
    if not vp: return
    if not has_subscription(uid) and uid not in ADMIN_IDS: await answer_cb(cb,"⚠️ Сначала Premium!",show_alert=True); return
    await answer_cb(cb); bal=get_balance(uid)
    kb=InlineKeyboardBuilder()
    if bal>=vp["stars"]: kb.button(text=f"💰 С баланса ({vp['stars']}⭐)",callback_data=f"paybal_vip_{k}")
    kb.button(text=f"⭐ Stars ({vp['stars']}⭐)",callback_data=f"paystars_vip_{k}")
    kb.button(text="❌",callback_data="shop_vip"); kb.adjust(1)
    await edit_msg(cb.message,f"🌟 <b>{vp['label']}</b>\n💰 {vp['stars']}⭐ | Баланс: {bal:.1f}⭐",kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_vip_"))
async def cb_paybal_vip(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[11:]; vp=VIP_PRICES.get(k)
    if not vp: return
    bal=get_balance(uid)
    if bal<vp["stars"]: await answer_cb(cb,"❌ Мало",show_alert=True); return
    set_balance(uid,bal-vp["stars"]); end=give_vip(uid,vp["days"])
    log_action(uid,"buy_vip_bal",f"{k} -{vp['stars']}⭐")
    display=f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb=InlineKeyboardBuilder(); kb.button(text="👤",callback_data="cmd_profile")
    await edit_msg(cb.message,f"✅ 🌟 {vp['label']} до {end}\n💰 -{vp['stars']}⭐",kb.as_markup())
    await notify_admins(f"🛒 {display} — VIP {vp['label']} за баланс (-{vp['stars']}⭐)")

@dp.callback_query(F.data.startswith("paystars_vip_"))
async def cb_paystars_vip(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[13:]; vp=VIP_PRICES.get(k)
    if not vp: return
    await bot.send_invoice(uid,title=f"🌟 {vp['label']}",description=f"VIP {vp['label']}",
        payload=f"vip_{k}_{uid}",provider_token="",currency="XTR",prices=[LabeledPrice(label=vp["label"],amount=vp["stars"])])

@dp.callback_query(F.data.startswith("buybundle_"))
async def cb_buybundle(cb: CallbackQuery):
    uid=cb.from_user.id; k=cb.data[10:]; bp=BUNDLE_PRICES.get(k)
    if not bp: return
    await answer_cb(cb); bal=get_balance(uid)
    kb=InlineKeyboardBuilder()
    if bal>=bp["stars"]: kb.button(text=f"💰 С баланса ({bp['stars']}⭐)",callback_data=f"paybal_bundle_{k}")
    kb.button(text=f"⭐ Stars ({bp['stars']}⭐)",callback_data=f"paystars_bundle_{k}")
    kb.button(text="❌",callback_data="shop_bundle"); kb.adjust(1)
    await edit_msg(cb.message,f"📦 <b>{bp['label']}</b>\n💰 {bp['stars']}⭐ | Баланс: {bal:.1f}⭐",kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_bundle_"))
async def cb_paybal_bundle(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[14:]; bp=BUNDLE_PRICES.get(k); p=PRICES.get(k)
    if not bp or not p: return
    bal=get_balance(uid)
    if bal<bp["stars"]: await answer_cb(cb,"❌ Мало",show_alert=True); return
    set_balance(uid,bal-bp["stars"]); end_p=give_subscription(uid,p["days"]); end_v=give_vip(uid,p["days"])
    log_action(uid,"buy_bundle_bal",f"{k} -{bp['stars']}⭐")
    display=f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb=InlineKeyboardBuilder(); kb.button(text="👤",callback_data="cmd_profile")
    await edit_msg(cb.message,f"✅ 💎 до {end_p}\n🌟 до {end_v}\n💰 -{bp['stars']}⭐",kb.as_markup())
    await notify_admins(f"🛒 {display} — Бандл {bp['label']} за баланс (-{bp['stars']}⭐)")

@dp.callback_query(F.data.startswith("paystars_bundle_"))
async def cb_paystars_bundle(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); k=cb.data[16:]; bp=BUNDLE_PRICES.get(k)
    if not bp: return
    await bot.send_invoice(uid,title=f"📦 {bp['label']}",description=f"Premium+VIP {bp['label']}",
        payload=f"bundle_{k}_{uid}",provider_token="",currency="XTR",prices=[LabeledPrice(label=bp["label"],amount=bp["stars"])])

# ═══════════════════════ ПРОФИЛЬ ═══════════════════════

@dp.callback_query(F.data == "cmd_profile")
async def cb_profile(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    u=get_user(uid); is_prem=has_subscription(uid); is_vip_user=has_vip(uid)
    _reset_daily_if_needed(uid)
    if uid in ADMIN_IDS: status="👑 Админ ♾"
    elif is_vip_user: status=f"🌟 VIP до {u.get('vip_end','?')}"
    elif is_prem: status=f"💎 Premium до {u.get('sub_end','?')}"
    elif u.get("free",0)+u.get("extra_searches",0)>0: status=f"🆓 {u.get('free',0)+u.get('extra_searches',0)} поисков"
    else: status="⛔️ Лимит"
    bal=u.get("balance",0.0); ar_on,ar_plan=get_auto_renew(uid)
    text=(f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🆔 <code>{uid}</code>\n📌 {status}\n"
          f"🎯 {get_search_count(uid)} юзов | 🔄 {get_max_searches(uid)} осталось\n"
          f"📊 Всего: <code>{u.get('searches',0)}</code>\n"
          f"💰 Баланс: <code>{bal:.1f}</code>⭐\n"
          f"🔄 Авто: {'<b>ВКЛ</b> ('+ar_plan+')' if ar_on else 'ВЫКЛ'}")
    kb=InlineKeyboardBuilder()
    if ar_on: kb.button(text="🔄 Выкл авто",callback_data="toggle_renew")
    else: kb.button(text="🔄 Вкл авто",callback_data="toggle_renew")
    kb.button(text="📜 История",callback_data="util_hist")
    kb.button(text="🔑 Ключ",callback_data="cmd_activate")
    kb.button(text="🎁 Подарить",callback_data="gift_prem")
    if bal>=MIN_WITHDRAW: kb.button(text=f"💸 Вывести ({bal:.1f}⭐)",callback_data="cmd_withdraw")
    kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "toggle_renew")
async def cb_tr(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    ar_on,_=get_auto_renew(uid)
    if ar_on: set_auto_renew(uid,False,""); await cb_profile(cb)
    else:
        kb=InlineKeyboardBuilder()
        for k,p in PRICES.items():
            if p["days"]<99999: kb.button(text=f"{p['label']} ({p['stars']}⭐)",callback_data=f"sr_{k}")
        kb.button(text="❌",callback_data="cmd_profile"); kb.adjust(1)
        await edit_msg(cb.message,"🔄 <b>Тариф:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("sr_"))
async def cb_sr(cb: CallbackQuery):
    plan=cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    set_auto_renew(cb.from_user.id,True,plan); await cb_profile(cb)

@dp.callback_query(F.data == "cmd_activate")
async def cb_act(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"activate"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_menu")
    await edit_msg(cb.message,"🔑 <b>Ключ:</b>",kb.as_markup())

@dp.callback_query(F.data == "cmd_withdraw")
async def cb_wd(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); bal=get_balance(uid)
    if bal<MIN_WITHDRAW: return
    user_states[uid]={"action":"withdraw_amount"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_profile")
    await edit_msg(cb.message,f"💸 Баланс: {bal:.1f}⭐\nМин: {MIN_WITHDRAW}⭐\n\nСумма:",kb.as_markup())

@dp.callback_query(F.data == "cmd_referral")
async def cb_ref(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    u=get_user(uid); bu=bot_info.username if bot_info else "bot"
    link=f"https://t.me/{bu}?start=ref_{uid}"
    kb=InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться",url=f"https://t.me/share/url?url={link}&text=🔍 Найди юзернейм!")
    kb.button(text="👥 Мои",callback_data="my_refs"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,f"👥 <b>Рефералы</b>\n\n👥 <code>{u.get('ref_count',0)}</code>\n+{REF_BONUS} поисков за друга\n\n🔗 <code>{link}</code>",kb.as_markup())

@dp.callback_query(F.data == "my_refs")
async def cb_my_refs(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); refs=get_user_referrals(uid,20)
    text=f"👥 <b>({len(refs)})</b>\n\n"
    for r in refs:
        name=f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"; text+=f"• {name} — {r['created']}\n"
    if not refs: text+="<i>Пусто</i>"
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_referral"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "gift_prem")
async def cb_gift(cb: CallbackQuery):
    await answer_cb(cb); kb=InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐",callback_data=f"gp_{k}")
    kb.button(text="🔙",callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message,"🎁 <b>Подарить Premium</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("gp_"))
async def cb_gp(cb: CallbackQuery):
    plan=cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    user_states[cb.from_user.id]={"action":"gift_username","plan":plan}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="gift_prem")
    await edit_msg(cb.message,f"🎁 @username получателя:",kb.as_markup())

@dp.callback_query(F.data == "cmd_support")
async def cb_support(cb: CallbackQuery):
    await answer_cb(cb); kb=InlineKeyboardBuilder()
    for amt in DONATE_OPTIONS: kb.button(text=f"⭐ {amt}",callback_data=f"don_{amt}")
    kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(3,3,1,1)
    await edit_msg(cb.message,"🤖 <b>Поддержать</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("don_"))
async def cb_don(cb: CallbackQuery):
    amt=int(cb.data[4:]); await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id,title=f"🤖 {amt}⭐",description="Донат",
        payload=f"donate_{amt}_{cb.from_user.id}",provider_token="",currency="XTR",
        prices=[LabeledPrice(label=f"{amt}⭐",amount=amt)])

# ═══════════════════════ TIKTOK ═══════════════════════

@dp.callback_query(F.data == "cmd_tiktok")
async def cb_tt(cb: CallbackQuery):
    await answer_cb(cb); kb=InlineKeyboardBuilder()
    kb.button(text="📸 Начать",callback_data="tt_go"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,f"🎁 <b>TikTok</b>\n\n{TIKTOK_SCREENSHOTS_NEEDED} комментов\n🎁 {TIKTOK_REWARD_GIFT}",kb.as_markup())

@dp.callback_query(F.data == "tt_go")
async def cb_tg_tt(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if not tiktok_can_submit(uid):
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_tiktok")
        await edit_msg(cb.message,"❌ Лимит",kb.as_markup()); return
    tid=task_create(uid); user_states[uid]={"action":"tiktok_proof","task_id":tid,"photos":0}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="tt_cancel")
    await edit_msg(cb.message,f"📸 <b>#{tid}</b>\n<code>0/{TIKTOK_SCREENSHOTS_NEEDED}</code>",kb.as_markup())

@dp.callback_query(F.data == "tt_cancel")
async def cb_tc(cb: CallbackQuery):
    await answer_cb(cb); user_states.pop(cb.from_user.id,None)
    t,k=build_menu(cb.from_user.id); await edit_msg(cb.message,t,k)

@dp.callback_query(F.data.startswith("ta_"))
async def cb_ta(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); tid=int(cb.data[3:]); uid=task_approve(tid,cb.from_user.id)
    if uid:
        try: await cb.message.edit_text(f"✅ #{tid}")
        except: pass
        try: await bot.send_message(uid,f"🎉 Одобрено! {TIKTOK_REWARD_GIFT}")
        except: pass

@dp.callback_query(F.data.startswith("tr_"))
async def cb_trj(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); tid=int(cb.data[3:]); uid=task_reject(tid,cb.from_user.id)
    try: await cb.message.edit_text(f"❌ #{tid}")
    except: pass
    if uid:
        try: await bot.send_message(uid,"❌ Отклонено")
        except: pass

# ═══════════════════════ МАРКЕТ CALLBACKS ═══════════════════════

@dp.callback_query(F.data == "cmd_market")
async def cb_market(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    lots=market_get_active_lots(5); nft_lots=market_get_active_lots(3,nft_only=True)
    rating=get_user_rating(uid); bal=get_balance(uid)
    text=(f"🏪 <b>Маркет</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
          f"💰 {bal:.0f}⭐ | ⭐ {'⭐'*int(rating['avg'])+'☆'*(5-int(rating['avg']))} ({rating['count']})\n"
          f"📊 Комиссия: {int(MARKET_COMMISSION*100)}%\n")
    if lots:
        text+="\n<b>📦 Лоты:</b>\n"
        for lot in lots:
            promo="🔥 " if lot["promoted"] else ""; text+=f"{promo}<b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"
    if nft_lots:
        text+="\n<b>💎 NFT:</b>\n"
        for lot in nft_lots: text+=f"💎 <b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"
    kb=InlineKeyboardBuilder()
    kb.button(text="📋 Все лоты",callback_data="market_browse"); kb.button(text="💎 NFT",callback_data="market_nft")
    kb.button(text="➕ Продать",callback_data="market_sell"); kb.button(text="📦 Мои лоты",callback_data="market_my")
    kb.button(text="🛒 Покупки",callback_data="market_my_purchases"); kb.button(text="🔄 Обменник",callback_data="market_exchange")
    kb.button(text="📦 Лутбокс",callback_data="market_lootbox"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "market_browse")
async def cb_market_browse(cb: CallbackQuery):
    await answer_cb(cb); lots=market_get_active_lots(20)
    text=f"📋 <b>Лоты ({len(lots)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for lot in lots:
        sr=get_user_rating(lot["seller"]); promo="🔥 " if lot["promoted"] else ""
        text+=f"{promo}<b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"
        kb.button(text=f"🛒 {lot['title'][:20]} ({lot['price']}⭐)",callback_data=f"mlot_{lot['id']}")
    if not lots: text+="<i>Пусто</i>"
    kb.button(text="🔙 Маркет",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "market_nft")
async def cb_market_nft(cb: CallbackQuery):
    await answer_cb(cb); lots=market_get_active_lots(20,nft_only=True)
    text=f"💎 <b>NFT</b>\n\n"; kb=InlineKeyboardBuilder()
    for lot in lots:
        text+=f"💎 <b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"
        kb.button(text=f"💎 {lot['title'][:20]} ({lot['price']}⭐)",callback_data=f"mlot_{lot['id']}")
    if not lots: text+="<i>Нет</i>"
    kb.button(text="➕ Продать NFT",callback_data="market_sell_nft")
    kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("mlot_"))
async def cb_mlot(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[5:])
    lot=market_get_lot(lot_id)
    if not lot or lot["status"]!="active":
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="market_browse")
        await edit_msg(cb.message,"❌ Не найден",kb.as_markup()); return
    seller=get_user(lot["seller_uid"]); sr=get_user_rating(lot["seller_uid"])
    name=f"@{seller.get('uname','')}" if seller.get('uname') else f"ID:{lot['seller_uid']}"
    price=int(lot["price"]); rub=int(price*STAR_TO_RUB)
    text=(f"📦 <b>#{lot_id}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🏷 <b>{lot['title']}</b>\n📝 {lot['description']}\n\n"
          f"💰 <code>{price}⭐</code> ({rub}₽)\n👤 {name}\n⭐ {'⭐'*int(sr['avg'])+'☆'*(5-int(sr['avg']))} ({sr['count']})\n📅 {lot['created']}")
    if lot.get("fragment_url"): text+=f"\n🔗 <a href='{lot['fragment_url']}'>Fragment</a>"
    kb=InlineKeyboardBuilder()
    if lot["seller_uid"]!=uid:
        kb.button(text=f"🛒 Купить {price}⭐",callback_data=f"mbuy_{lot_id}")
        kb.button(text="🏷 Промокод",callback_data=f"mpromo_{lot_id}")
    else:
        kb.button(text=f"🔥 Продвинуть ({MARKET_PROMOTE_PRICE}⭐)",callback_data=f"mpromote_{lot_id}")
        kb.button(text="❌ Снять",callback_data=f"mcancel_{lot_id}")
    kb.button(text="👤 Отзывы",callback_data=f"mrev_{lot['seller_uid']}")
    kb.button(text="🔙",callback_data="market_browse"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("mbuy_"))
async def cb_mbuy(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[5:])
    lot=market_get_lot(lot_id)
    if not lot or lot["status"]!="active": await answer_cb(cb,"❌ Продан!",show_alert=True); return
    if lot["seller_uid"]==uid: await answer_cb(cb,"❌ Свой!",show_alert=True); return
    price=int(lot["price"]); bal=get_balance(uid); rub=int(price*STAR_TO_RUB)
    kb=InlineKeyboardBuilder()
    if bal>=price: kb.button(text=f"💰 С баланса ({price}⭐)",callback_data=f"paybal_lot_{lot_id}")
    kb.button(text=f"⭐ Stars ({price}⭐)",callback_data=f"paystars_lot_{lot_id}")
    kb.button(text="❌",callback_data=f"mlot_{lot_id}"); kb.adjust(1)
    await edit_msg(cb.message,f"🛒 <b>#{lot_id}</b>\n📦 {lot['title']}\n💰 {price}⭐ ({rub}₽)\n\nБаланс: {bal:.1f}⭐\n\nСпособ:",kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_lot_"))
async def cb_paybal_lot(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[11:])
    lot=market_get_lot(lot_id)
    if not lot or lot["status"]!="active": await answer_cb(cb,"❌",show_alert=True); return
    price=int(lot["price"]); bal=get_balance(uid)
    if bal<price: await answer_cb(cb,"❌ Мало",show_alert=True); return
    set_balance(uid,bal-price); ok=market_buy_lot(lot_id,uid)
    if not ok: add_balance(uid,price); await answer_cb(cb,"❌",show_alert=True); return
    log_action(uid,"market_buy_bal",f"lot={lot_id}"); display=f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb=InlineKeyboardBuilder(); kb.button(text="✅ Получил",callback_data=f"mbuyerok_{lot_id}"); kb.button(text="⚠️ Спор",callback_data=f"mdispute_{lot_id}"); kb.adjust(1)
    await edit_msg(cb.message,f"✅ <b>Оплачено!</b>\n📦 {lot['title']}\n💰 -{price}⭐\n🔒 Эскроу {MARKET_ESCROW_HOURS}ч",kb.as_markup())
    skb=InlineKeyboardBuilder(); skb.button(text="✅ Передал",callback_data=f"msellerok_{lot_id}")
    try: await bot.send_message(lot["seller_uid"],f"🛒 <b>Куплен #{lot_id}!</b>\n📦 {lot['title']}\n👤 {display}\n⏰ {MARKET_ESCROW_HOURS}ч",reply_markup=skb.as_markup(),parse_mode="HTML")
    except: pass
    commission=int(price*MARKET_COMMISSION)
    await notify_admins(f"🛒 <b>ПРОДАЖА (баланс)</b>\n📦 {lot['title']}\n💰 {price}⭐ | Комиссия: {commission}⭐\n👤 {display}")

@dp.callback_query(F.data.startswith("paystars_lot_"))
async def cb_paystars_lot(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[13:])
    lot=market_get_lot(lot_id)
    if not lot or lot["status"]!="active": return
    await bot.send_invoice(uid,title=f"🛒 {lot['title'][:50]}",description=f"Маркет: {lot['title']}",
        payload=f"market_{lot_id}_{uid}_0",provider_token="",currency="XTR",
        prices=[LabeledPrice(label=lot["title"][:50],amount=int(lot["price"]))])

@dp.callback_query(F.data.startswith("mpromo_"))
async def cb_mpromo(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[7:])
    user_states[uid]={"action":"market_enter_promo","lot_id":lot_id}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data=f"mlot_{lot_id}")
    await edit_msg(cb.message,"🏷 <b>Промокод:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("mpromote_"))
async def cb_mpromote(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[9:])
    lot=market_get_lot(lot_id)
    if not lot or lot["seller_uid"]!=uid: return
    await bot.send_invoice(uid,title=f"🔥 Продвижение #{lot_id}",description="24 часа наверху",
        payload=f"promote_{lot_id}_{uid}",provider_token="",currency="XTR",
        prices=[LabeledPrice(label="Продвижение",amount=MARKET_PROMOTE_PRICE)])

@dp.callback_query(F.data.startswith("mcancel_"))
async def cb_mcancel(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); market_cancel_lot(int(cb.data[8:]),uid); await cb_market(cb)

@dp.callback_query(F.data.startswith("msellerok_"))
async def cb_msellerok(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[10:]); lot=market_get_lot(lot_id)
    if not lot or lot["seller_uid"]!=uid: return
    completed=market_confirm_seller(lot_id)
    if completed:
        payout=int(int(lot["price"])*(1-MARKET_COMMISSION))
        await edit_msg(cb.message,f"✅ <b>Сделка завершена!</b>\n+{payout}⭐")
        try: await bot.send_message(lot["buyer_uid"],"✅ Сделка завершена!",parse_mode="HTML")
        except: pass
    else: await edit_msg(cb.message,"✅ Ждём покупателя")

@dp.callback_query(F.data.startswith("mbuyerok_"))
async def cb_mbuyerok(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[9:]); lot=market_get_lot(lot_id)
    if not lot or lot["buyer_uid"]!=uid: return
    completed=market_confirm_buyer(lot_id)
    if completed:
        payout=int(int(lot["price"])*(1-MARKET_COMMISSION))
        await edit_msg(cb.message,"✅ <b>Завершено!</b> Спасибо!")
        try: await bot.send_message(lot["seller_uid"],f"✅ Сделка завершена!\n💰 +{payout}⭐",parse_mode="HTML")
        except: pass
    else: await edit_msg(cb.message,"✅ Ждём продавца")

@dp.callback_query(F.data.startswith("mdispute_"))
async def cb_mdispute(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lot_id=int(cb.data[9:])
    user_states[uid]={"action":"market_dispute_reason","lot_id":lot_id}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="market_my_purchases")
    await edit_msg(cb.message,"⚠️ <b>Причина спора:</b>",kb.as_markup())

@dp.callback_query(F.data == "market_sell")
async def cb_market_sell(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    max_lots=market_get_max_lots(uid); current=market_count_user_lots(uid)
    text=f"➕ <b>Продать</b>\n📦 {current}/{max_lots}\n💰 Размещение: {MARKET_LISTING_FEE}⭐\n"
    kb=InlineKeyboardBuilder()
    if current>=max_lots:
        kb.button(text=f"📦 +1 слот ({MARKET_EXTRA_SLOT_PRICE}⭐)",callback_data="market_buy_slot")
    else:
        kb.button(text="👤 Юзернейм",callback_data="msell_username"); kb.button(text="💎 Premium",callback_data="msell_premium")
        kb.button(text="🔧 Услуга",callback_data="msell_service"); kb.button(text="📦 Другое",callback_data="msell_other")
    kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "market_sell_nft")
async def cb_sell_nft(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); user_states[uid]={"action":"msell_nft_title"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="market_nft")
    await edit_msg(cb.message,f"💎 <b>NFT юзернейм</b>\nРазмещение: {MARKET_NFT_LISTING_FEE}⭐\n\nВведите @юзернейм:",kb.as_markup())

@dp.callback_query(F.data.startswith("msell_"))
async def cb_msell_type(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); mtype=cb.data[6:]
    user_states[uid]={"action":"msell_title","mtype":mtype}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="market_sell")
    await edit_msg(cb.message,"📝 <b>Название:</b>",kb.as_markup())

@dp.callback_query(F.data == "market_buy_slot")
async def cb_buy_slot(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    await bot.send_invoice(uid,title="📦 +1 слот",description="Доп слот",
        payload=f"mslot_{uid}",provider_token="",currency="XTR",
        prices=[LabeledPrice(label="+1 слот",amount=MARKET_EXTRA_SLOT_PRICE)])

@dp.callback_query(F.data == "market_my")
async def cb_market_my(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); lots=market_get_user_lots(uid)
    text=f"📦 <b>Мои лоты ({len(lots)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for lot in lots:
        st={"pending":"⏳","active":"✅","escrow":"🔒"}.get(lot["status"],"❓")
        text+=f"{st} <b>{lot['title']}</b> — {lot['price']}⭐\n"
        if lot["status"]=="escrow": kb.button(text=f"✅ Передал #{lot['id']}",callback_data=f"msellerok_{lot['id']}")
        elif lot["status"] in ("pending","active"): kb.button(text=f"❌ #{lot['id']}",callback_data=f"mcancel_{lot['id']}")
    if not lots: text+="<i>Нет</i>"
    kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "market_my_purchases")
async def cb_market_purchases(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT id,title,price,status,buyer_confirmed FROM market WHERE buyer_uid=? AND status IN ('escrow','completed','dispute') ORDER BY id DESC LIMIT 20",(uid,))
    rows=c.fetchall(); conn.close()
    text=f"🛒 <b>Покупки</b>\n\n"; kb=InlineKeyboardBuilder()
    for r in rows:
        st={"escrow":"🔒","completed":"✅","dispute":"⚠️"}.get(r[3],"❓"); text+=f"{st} <b>{r[1]}</b> — {r[2]}⭐\n"
        if r[3]=="escrow" and not r[4]:
            kb.button(text=f"✅ Получил #{r[0]}",callback_data=f"mbuyerok_{r[0]}")
            kb.button(text=f"⚠️ Спор #{r[0]}",callback_data=f"mdispute_{r[0]}")
    if not rows: text+="<i>Нет</i>"
    kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("mrev_"))
async def cb_mrev(cb: CallbackQuery):
    uid_target=int(cb.data[5:]); await answer_cb(cb)
    reviews=get_user_reviews(uid_target); rating=get_user_rating(uid_target); u=get_user(uid_target)
    name=f"@{u.get('uname','')}" if u.get('uname') else f"ID:{uid_target}"
    text=f"⭐ <b>{name}</b> {'⭐'*int(rating['avg'])+'☆'*(5-int(rating['avg']))} ({rating['count']})\n\n"
    for r in reviews:
        fn=f"@{get_user(r['from']).get('uname','')}" if get_user(r['from']).get('uname') else f"ID:{r['from']}"
        text+=f"{'⭐'*r['rating']} {fn}\n{r['text']}\n\n"
    if not reviews: text+="<i>Нет отзывов</i>"
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "market_lootbox")
async def cb_lootbox(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); can=lootbox_can_open(uid)
    text=(f"📦 <b>Лутбокс</b>\n\n💰 {LOOTBOX_PRICE}⭐ | ⏰ 1 час\n\n"
          f"🎁 Призы:\n  💎 Premium (5%)\n  🌟 VIP (10%)\n  ⭐ Звёзды (20%)\n  🔍 Поиски (25%)\n  📦 Слот (20%)\n  🧸 Сувенир (20%)")
    kb=InlineKeyboardBuilder()
    if can: kb.button(text=f"📦 Открыть ({LOOTBOX_PRICE}⭐)",callback_data="lootbox_open")
    else: kb.button(text="⏰ Подождите...",callback_data="cmd_market")
    kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "lootbox_open")
async def cb_lootbox_open(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb)
    if not lootbox_can_open(uid): await answer_cb(cb,"⏰",show_alert=True); return
    await bot.send_invoice(uid,title="📦 Лутбокс",description="Случайный приз!",
        payload=f"lootbox_{uid}",provider_token="",currency="XTR",
        prices=[LabeledPrice(label="Лутбокс",amount=LOOTBOX_PRICE)])

@dp.callback_query(F.data == "market_exchange")
async def cb_exchange(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); exs=exchange_get_open(10)
    text=f"🔄 <b>Обменник</b>\n\n"; kb=InlineKeyboardBuilder()
    for ex in exs:
        u=get_user(ex["uid"]); name=f"@{u.get('uname','')}" if u.get('uname') else f"ID:{ex['uid']}"
        text+=f"🔄 #{ex['id']} {name}: <b>{ex['offer']}</b>\n"
        if ex["uid"]!=uid: kb.button(text=f"🔄 #{ex['id']}",callback_data=f"exview_{ex['id']}")
    if not exs: text+="<i>Нет</i>"
    kb.button(text="➕ Создать",callback_data="exchange_new"); kb.button(text="🔙",callback_data="cmd_market"); kb.adjust(2,1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "exchange_new")
async def cb_ex_new(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); user_states[uid]={"action":"exchange_offer"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="market_exchange")
    await edit_msg(cb.message,"🔄 <b>Что отдаёте?</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("exview_"))
async def cb_exview(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); eid=int(cb.data[7:]); ex=exchange_get(eid)
    if not ex or ex["status"]!="open":
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="market_exchange")
        await edit_msg(cb.message,"❌ Закрыт",kb.as_markup()); return
    u=get_user(ex["initiator_uid"]); name=f"@{u.get('uname','')}" if u.get('uname') else f"ID:{ex['initiator_uid']}"
    kb=InlineKeyboardBuilder()
    if ex["initiator_uid"]!=uid: kb.button(text="🔄 Предложить",callback_data=f"exaccept_{eid}")
    kb.button(text="🔙",callback_data="market_exchange")
    await edit_msg(cb.message,f"🔄 <b>#{eid}</b>\n👤 {name}\n📦 <b>{ex['initiator_offer']}</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("exaccept_"))
async def cb_exaccept(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); eid=int(cb.data[9:])
    user_states[uid]={"action":"exchange_counter","eid":eid}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data=f"exview_{eid}")
    await edit_msg(cb.message,"🔄 <b>Что предлагаете?</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("exconfirm_"))
async def cb_exconfirm(cb: CallbackQuery):
    uid=cb.from_user.id; await answer_cb(cb); eid=int(cb.data[10:])
    completed=exchange_confirm(eid,uid)
    if completed:
        ex=exchange_get(eid); await edit_msg(cb.message,f"✅ <b>Обмен #{eid} завершён!</b>")
        other=ex["partner_uid"] if uid==ex["initiator_uid"] else ex["initiator_uid"]
        try: await bot.send_message(other,f"✅ Обмен #{eid} завершён!",parse_mode="HTML")
        except: pass
    else: await edit_msg(cb.message,"✅ Ждём другую сторону")

@dp.callback_query(F.data.startswith("pv_"))
async def cb_pv(cb: CallbackQuery):
    await answer_cb(cb); pid=int(cb.data[3:]); promos=get_active_promotions()
    promo=next((p for p in promos if p["id"]==pid),None)
    if not promo:
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_menu")
        await edit_msg(cb.message,"❌ Завершена",kb.as_markup()); return
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,f"🎉 <b>{promo['name']}</b>",kb.as_markup())
