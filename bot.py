"""
USERNAME HUNTER v25.0 — VIP + ТЕМАТИЧЕСКИЙ ПОИСК + ССЫЛКИ
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
from aiogram.types import FSInputFile

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
VIP_COUNT = 5  # ═ НОВОЕ ═
PREMIUM_SEARCHES_LIMIT = 7
VIP_SEARCHES_LIMIT = 15  # ═ НОВОЕ ═
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
RATE_SEARCH_PER_MIN = 3
RATE_CHECK_PER_HOUR = 50
TEMP_BAN_MINUTES = 30
STAR_TO_RUB = 1.25  # ═ НОВОЕ: курс звезды к рублям ═

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

# ═══ НОВОЕ: VIP ЦЕНЫ ═══
# VIP = 50% от Premium (апгрейд)
VIP_PRICES = {}
for _k, _p in PRICES.items():
    VIP_PRICES[_k] = {
        "days": _p["days"],
        "stars": max(1, _p["stars"] // 2),
        "label": f"VIP {_p['label']}"
    }

# Бандл Premium+VIP сразу = (Premium + VIP) * 0.95 (скидка 5%)
BUNDLE_PRICES = {}
for _k, _p in PRICES.items():
    _vip_stars = VIP_PRICES[_k]["stars"]
    BUNDLE_PRICES[_k] = {
        "days": _p["days"],
        "stars": int((_p["stars"] + _vip_stars) * 0.95),
        "label": f"Premium+VIP {_p['label']}"
    }

DONATE_OPTIONS = [20, 50, 100, 200, 300, 500, 1000]

# ═══════════════════════ МАРКЕТПЛЕЙС ═══════════════════════

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
WHEEL_FREE_DAILY = 1
WHEEL_EXTRA_PRICE = 10
WITHDRAW_MIN = 50
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

async def edit_msg(msg, text, kb=None):
    try: await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
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
    "btn_tiktok": True, "btn_roulette": True, "btn_monitor": True,
    "btn_shop": True, "btn_support": True, "btn_referral": True,
    "mode_default": True, "mode_beautiful": True, "mode_meaningful": True,
    "mode_anyword": True, "mode_mat": True, "mode_telegram": True,
    "mode_default_premium": False, "mode_beautiful_premium": True,
    "mode_meaningful_premium": True, "mode_anyword_premium": True,
    "mode_mat_premium": True, "mode_telegram_premium": True,
    "prices": {}, "daily_report": True, "daily_report_hour": 23,
    "notify_purchases": True, "notify_milestones": True,
}

def load_bot_config():
    config = dict(DEFAULT_CONFIG)
    if os.path.exists(BOT_CONFIG_FILE):
        try:
            with open(BOT_CONFIG_FILE, "r") as f:
                config.update(json.load(f))
        except: pass
    return config

def save_bot_config(config):
    with open(BOT_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

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
    if "prices" in config:
        for k, v in config["prices"].items():
            if k in PRICES: PRICES[k]["stars"] = v

def is_button_enabled(name):
    return load_bot_config().get(f"btn_{name}", True)

# ═══════════════════════ RATE LIMITER (ОТКЛЮЧЁН) ═══════════════════════

class RateLimiter:
    def __init__(self):
        self.search_times = {}
        self.check_times = {}
        self.warnings = {}
        self.temp_bans = {}

    def is_temp_banned(self, uid):
        return False  # 🟢 Никогда не забанен

    def temp_ban(self, uid):
        pass  # 🟢 Ничего не делает

    def check_search(self, uid):
        return True, ""  # 🟢 Всегда разрешает поиск

    def check_action(self, uid):
        return True, ""  # 🟢 Всегда разрешает действия

    def get_ban_remaining(self, uid):
        return 0

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
    try:
        c.execute("SELECT uid,action,details,created FROM action_log ORDER BY id DESC LIMIT ?", (limit,))
        rows = c.fetchall()
    except: rows = []
    conn.close()
    return [{"uid": r[0], "action": r[1], "details": r[2], "created": r[3]} for r in rows]

# ═══════════════════════ ПУЛ АККАУНТОВ ═══════════════════════

class AccountPool:
    def __init__(self):
        self.clients = []
        self.accounts_data = []
        self.lock = asyncio.Lock()
        self.status = {}
        self.cooldown_until = {}
        self.last_used = {}
        self.error_streak = {}
        self.total_errors = {}
        self.req_count = {}
        self.window_start = {}
        self.flood_times = {}
        self.adaptive_delay = {}
        self.BASE_DELAY = 2.0
        self.MAX_DELAY = 20.0
        self.BUDGET_PER_MIN = 12
        self.MAX_ERROR_STREAK = 3
        self.FLOOD_REST_TIME = 600
        self.WARMUP_EXTRA_DELAY = 10.0
        self.total_checks = 0
        self.caught_by_botapi = 0
        self.caught_by_recheck = 0
        self.reconnect_count = 0
        self.active_users = {}
        self.max_users_per_account = 3
        self._health_task = None
        self._monitor_task = None

    async def init(self, accounts):
        if not HAS_TELETHON or not accounts:
            logger.info("Bot API mode"); return
        self.accounts_data = list(accounts)
        try:
            if os.path.exists("added_sessions.json"):
                with open("added_sessions.json") as f:
                    self.accounts_data += json.load(f)
        except: pass
        for acc in self.accounts_data:
            phone = acc["phone"].replace("+","").replace(" ","")
            try:
                c = TelegramClient(f"sessions/s_{phone}", acc["api_id"], acc["api_hash"],
                                   connection_retries=5, retry_delay=3, timeout=15, request_retries=2)
                await c.connect()
                if not await c.is_user_authorized(): await c.start(phone=acc["phone"])
                self.clients.append(c)
                idx = len(self.clients)-1
                self._init(idx)
                logger.info(f"✅ #{idx+1}: {acc['phone']}")
            except Exception as e:
                logger.error(f"❌ {acc['phone']}: {e}")
        logger.info(f"Пул: {len(self.clients)}")
        self._health_task = asyncio.create_task(self._health_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    def _init(self, i):
        self.status[i]='warming'; self.cooldown_until[i]=0; self.last_used[i]=0
        self.error_streak[i]=0; self.total_errors[i]=0; self.req_count[i]=0
        self.window_start[i]=time.time(); self.flood_times[i]=[]; self.adaptive_delay[i]=self.BASE_DELAY

    def has_sessions(self):
        return any(self.status.get(i) in ('healthy','warming') for i in range(len(self.clients)))

    async def _health_loop(self):
        while True:
            try:
                await asyncio.sleep(60); now=time.time()
                for i in range(len(self.clients)):
                    st=self.status.get(i,'dead')
                    if st=='dead': await self._try_reconnect(i)
                    elif st=='cooldown' and now>=self.cooldown_until.get(i,0):
                        self.status[i]='warming'; self.error_streak[i]=0; self.adaptive_delay[i]=self.BASE_DELAY+2
                    elif st=='warming' and now-self.last_used.get(i,0)>30 and self.error_streak.get(i,0)==0:
                        self.status[i]='healthy'; self.adaptive_delay[i]=self.BASE_DELAY
                    if now-self.window_start.get(i,0)>60: self.req_count[i]=0; self.window_start[i]=now
                    if i in self.flood_times: self.flood_times[i]=[t for t in self.flood_times[i] if now-t<3600]
                alive=sum(1 for j in range(len(self.clients)) if self.status.get(j) in ('healthy','warming'))
                if 0<alive<=2:
                    for i in range(len(self.clients)):
                        if self.status.get(i) in ('healthy','warming'):
                            self.adaptive_delay[i]=max(self.adaptive_delay[i], self.BASE_DELAY*2)
            except Exception as e: logger.error(f"Health: {e}")

    async def _monitor_loop(self):
        while True:
            await asyncio.sleep(300)
            try:
                s=self.stats()
                logger.info(f"📊 {s['active']}/{s['total']} checks={s['checks']}")
            except: pass

    async def _try_reconnect(self, i):
        try:
            c=self.clients[i]
            if c.is_connected(): await c.disconnect()
            await asyncio.sleep(2); await c.connect()
            if await c.is_user_authorized():
                self.status[i]='warming'; self.error_streak[i]=0
                self.adaptive_delay[i]=self.BASE_DELAY+3; self.reconnect_count+=1
                logger.info(f"🔄 #{i+1} reconnected")
        except Exception as e: logger.error(f"Reconnect #{i+1}: {e}")

    def _best(self, uid=None):
        now=time.time(); cands=[]
        for i in range(len(self.clients)):
            st=self.status.get(i,'dead')
            if st=='dead': continue
            if st=='cooldown' and now<self.cooldown_until.get(i,0): continue
            if now-self.window_start.get(i,0)>60: self.req_count[i]=0; self.window_start[i]=now
            b=self.BUDGET_PER_MIN
            alive=sum(1 for j in range(len(self.clients)) if self.status.get(j) in ('healthy','warming'))
            if alive<=2: b=max(5,b//2)
            if self.req_count.get(i,0)>=b: continue
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
            self.cooldown_until[i]=time.time()+secs+random.randint(20,45)
            self.status[i]='cooldown'
            self.flood_times.setdefault(i,[]).append(time.time())
            if len([t for t in self.flood_times[i] if time.time()-t<3600])>=3:
                self.cooldown_until[i]=time.time()+self.FLOOD_REST_TIME
        elif self.error_streak[i]>=self.MAX_ERROR_STREAK:
            self.status[i]='dead'
        else:
            self.adaptive_delay[i]=min(self.adaptive_delay.get(i,self.BASE_DELAY)*1.5, self.MAX_DELAY)
            self.cooldown_until[i]=time.time()+3; self.status[i]='cooldown'

    async def _resolve(self, u, uid=None):
        i,c=await self._acquire(uid,30)
        if c is None: return "no_session",-1
        try:
            await c(ResolveUsernameRequest(u)); self._ok(i); return "taken",i
        except UsernameNotOccupiedError: self._ok(i); return "free",i
        except UsernameInvalidError: self._ok(i); return "invalid",i
        except FloodWaitError as e: self._err(i,True,e.seconds); return "flood",i
        except: self._err(i); return "error",i

    async def _check_avail(self, u, uid=None):
        i,c=await self._acquire(uid,20)
        if c is None: return "no_session",-1
        try:
            ok=await c(AccountCheckUsername(u)); self._ok(i); return ("free" if ok else "taken"),i
        except FloodWaitError as e: self._err(i,True,e.seconds); return "flood",i
        except: self._err(i); return "error",i

    async def _botapi(self, u):
        try:
            await bot.get_chat(f"@{u}"); return "taken"
        except TelegramBadRequest as e:
            return "taken" if "not found" not in str(e).lower() else "not_found"
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

# ═══════════════════════ ГЕНЕРАТОРЫ v5 ═══════════════════════

_V = "aeiou"
_C = "bcdfghjklmnprstvwxyz"

# ═ НОВОЕ: красивые digraphs для начала слова ═
_DIGRAPHS = ["bl","br","ch","cl","cr","dr","fl","fr","gl","gr","kn","pl","pr",
             "sc","sh","sk","sl","sm","sn","sp","st","sw","th","tr","tw","wr","zh"]
_ENDINGS = ["ax","ex","ix","ox","en","on","an","er","or","ar","in","yn","us",
            "os","is","al","el","il","ol","ul","ay","ey","oy","ry","ly","ny","zy"]

def _pronounceable(n):
    w=[]; sc=random.choice([True,False])
    for i in range(n):
        w.append(random.choice(_C) if (i%2==0)==sc else random.choice(_V))
    return "".join(w)

# ═ ИЗМЕНЕНО: gen_default теперь генерирует красивые 5-буквенные ═
def gen_default():
    s=random.randint(1,5)
    if s==1:
        return _pronounceable(5)
    elif s==2:
        # digraph + гласная + согласная + гласная = 5 букв
        d=random.choice(_DIGRAPHS)
        return d+random.choice(_V)+random.choice(_C)+random.choice(_V)
    elif s==3:
        # согласная + красивое окончание = 5 букв (подгоняем)
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        end=random.choice(_V)+random.choice(_C)
        return (start+end)[:5]
    elif s==4:
        # CVCVC — классика
        return random.choice(_C)+random.choice(_V)+random.choice(_C)+random.choice(_V)+random.choice(_C)
    else:
        # 3 буквы + красивое окончание
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        end=random.choice(_ENDINGS)
        r=start+end
        return r[:5] if len(r)>5 else r

# ═ ИЗМЕНЕНО: gen_beautiful с красивыми комбинациями ═
def gen_beautiful():
    s=random.randint(1,6)
    if s==1:
        # digraph + vowel + consonant + vowel
        d=random.choice(_DIGRAPHS)
        return d+random.choice("aeiou")+random.choice(_C)+random.choice("aeiou")
    elif s==2:
        # красивый паттерн без повторов
        pats=["cvcvc","cvccv","ccvcv","vcvcv"]
        pat=random.choice(pats); w=[]; uc=set()
        for ch in pat:
            if ch=="c":
                p=[x for x in _C if x not in uc]; l=random.choice(p if p else list(_C))
                w.append(l); uc.add(l)
            else: w.append(random.choice(_V))
        return "".join(w)
    elif s==3:
        # consonant + ending
        start=random.choice(_C)+random.choice(_V)+random.choice(_C)
        return start+random.choice(_ENDINGS[:14])  # 2-буквенные окончания
    elif s==4:
        # красивые слоги
        s1=random.choice(["ka","ki","ko","ku","ra","ri","ro","ru","na","ni","no","nu",
                          "ma","mi","mo","mu","ta","ti","to","la","li","lo","sa","si"])
        s2=random.choice(["zen","rix","lex","vin","ren","lin","rex","nox","vex","lux",
                          "ryn","lyn","nyx","fox","pax","dex","max","jax"])
        r=s1+s2
        return r[:5]
    elif s==5:
        # модные комбинации
        starts=["ky","zy","xy","vy","ry","ly","ny","my"]
        mids=["ro","ra","le","li","no","na","ve","vi","re","ri"]
        ends=["n","x","s","r","l","k","t","d"]
        r=random.choice(starts)+random.choice(mids)+random.choice(ends)
        return r[:5]
    else:
        # vowel-centered
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

def gen_anyword():
    return _pronounceable(random.randint(5,7))

def gen_mat():
    roots=["blyad","suka","nahui","pizdec","huy","pizd","ebat","mraz","gand","shlyuh",
           "pidor","mudak","chmo","loh","xyilo","padla","ueban","tvar","blya","fck",
           "wtf","dmn","btch","azz","dck","fuk","shyt","pzdc","nah","suk","ebl","pid","lox",
           "zalupa","dermo","gavno","zhopa","bzdun","obsos","gnida","ublud","padonok"]
    suf=["_pro","_god","_king","_boss","_man","_boy","_top","_go","_gg","_xx",
         "_69","_228","_666","_13","_1","_x","ka","on","ik","er","ok","ec",
         "ych","an","off","iz","ov","in","chik","ina","stvo","nik"]
    pre=["mr_","el_","big_","top_","my_","x_","da_","not_","im_","ya_","the_","super_",
         "mega_","ultra_","king_","real_","true_","best_","cool_",""]
    s=random.randint(1,6)
    if s==1: r=random.choice(pre)+random.choice(roots)
    elif s==2: r=random.choice(roots)+random.choice(suf)
    elif s==3: r=random.choice(pre)+random.choice(roots)+random.choice(suf)
    elif s==4: r=random.choice(roots)+random.choice(roots[:15])
    elif s==5: r=random.choice(roots)+str(random.randint(1,999))
    else: r=random.choice(pre)+random.choice(roots)+str(random.randint(0,99))
    r=r.replace("__","_").strip("_")
    if len(r)<5: r+=random.choice(suf).strip("_")
    if len(r)>15: r=r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$',r): return gen_default()
    return r

def gen_telegram():
    tg_core = ["tg","telegram","telega","durov","pavel","ton","gram","channel",
               "chat","group","sticker","emoji","gif","media","voice","video",
               "story","stories","premium","stars","boost","topic"]
    tg_features = ["bot","admin","mod","owner","reply","forward","pin","edit",
                   "delete","mute","ban","kick","invite","link","username",
                   "bio","photo","pfp","status","online","typing","seen"]
    tg_slang = ["tgk","tgram","telga","telegam","durik","pasha","tgbot",
                "tgadm","tgmod","tgpro","tgod","tgking","tgboss","tglife",
                "chatik","gruppa","kanal","sticer","smailik","gifka"]
    roles = ["admin","mod","owner","god","king","boss","pro","master","guru",
             "ninja","dev","hacker","geek","fan","lover","addict","lord",
             "chief","head","main","super","mega","top","best","real"]
    verbs = ["send","post","share","like","join","spam","flood","read",
             "type","call","msg","dm","pm","text","write","chat","talk"]
    suf = ["_pro","_god","_king","_x","_gg","_1","_top","_go","_bot",
           "_dev","_fan","_life","_love","_man","_boy","er","ist",
           "ik","ka","off","on","_channel","_chat","_group"]
    pre = ["my_","the_","im_","mr_","x_","ya_","not_","just_","only_",
           "real_","true_","best_","top_","pro_","super_","mega_",""]
    num = ["","","","1","2","3","69","228","777","_01","_02","_99"]
    s = random.randint(1, 12)
    if s == 1: r = random.choice(pre) + random.choice(tg_core) + random.choice(suf)
    elif s == 2: r = random.choice(tg_core) + "_" + random.choice(roles)
    elif s == 3: r = random.choice(verbs) + "_" + random.choice(tg_core)
    elif s == 4: r = random.choice(tg_slang) + random.choice(num)
    elif s == 5: r = random.choice(pre) + random.choice(tg_slang)
    elif s == 6: r = random.choice(tg_features) + "_" + random.choice(roles)
    elif s == 7: r = random.choice(tg_core) + random.choice(tg_features)
    elif s == 8: r = random.choice(roles) + "_" + random.choice(tg_core)
    elif s == 9: r = random.choice(tg_slang) + random.choice(suf)
    elif s == 10: r = random.choice(pre) + random.choice(tg_features)
    elif s == 11: r = random.choice(tg_core) + "_" + random.choice(verbs)
    else: r = random.choice(verbs) + random.choice(tg_features)
    r = r.replace("__", "_").strip("_")
    if len(r) < 5: r += random.choice(["_pro", "_bot", "_tg", "er", "ik", "_go"])
    if len(r) > 15: r = r[:15].rstrip("_")
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r): return "tg_" + _pronounceable(4)
    return r

# ═══ НОВОЕ: ТЕМАТИЧЕСКИЙ ГЕНЕРАТОР (VIP) ═══
def gen_thematic_variations(word):
    """Чисто алгоритмические вариации — никаких словарей"""
    word = word.lower().replace("@","").replace(" ","")
    if len(word) < 2: return []
    results = set()
    
    # 1. Leetspeak (все комбинации по одной замене)
    leet = {"a":"4","e":"3","i":"1","o":"0","s":"5","t":"7","b":"8","g":"9"}
    for orig, rep in leet.items():
        if orig in word:
            # заменить первое вхождение
            results.add(word.replace(orig, rep, 1))
            # заменить все
            full = word.replace(orig, rep)
            if full != word: results.add(full)
    
    # 2. Удвоение каждой буквы
    for i in range(len(word)):
        r = word[:i] + word[i]*2 + word[i+1:]
        results.add(r)
    
    # 3. Удаление каждой буквы
    for i in range(len(word)):
        r = word[:i] + word[i+1:]
        if len(r) >= 4: results.add(r)
    
    # 4. Замена каждой буквы на соседнюю по клавиатуре
    keyboard_neighbors = {
        'q':'wa','w':'qeas','e':'wrds','r':'etf','t':'ryg','y':'tuh','u':'yij',
        'i':'uok','o':'ipl','p':'o','a':'qwsz','s':'awedxz','d':'serfcx',
        'f':'drtgvc','g':'ftyhbv','h':'gyujnb','j':'huiknm','k':'jiol',
        'l':'kop','z':'asx','x':'zsdc','c':'xdfv','v':'cfgb','b':'vghn',
        'n':'bhjm','m':'njk'
    }
    for i in range(len(word)):
        if word[i] in keyboard_neighbors:
            for c in keyboard_neighbors[word[i]]:
                r = word[:i] + c + word[i+1:]
                results.add(r)
    
    # 5. Свап соседних букв
    for i in range(len(word)-1):
        r = word[:i] + word[i+1] + word[i] + word[i+2:]
        results.add(r)
    
    # 6. Вставка подчёркивания
    for i in range(2, len(word)-1):
        r = word[:i] + "_" + word[i:]
        results.add(r)
    
    # 7. Вставка буквы/цифры в каждую позицию
    for i in range(1, len(word)+1):
        for c in "xzkvwy01234":
            r = word[:i] + c + word[i:]
            results.add(r)
    
    # 8. Реверс
    rev = word[::-1]
    if rev != word and len(rev) >= 5:
        results.add(rev)
    
    # 9. Похожие символы
    similar = {"c":"k","k":"c","f":"ph","ph":"f","s":"z","z":"s",
               "v":"w","w":"v","i":"y","y":"i","x":"ks","ks":"x",
               "ck":"k","ee":"i","oo":"u","ss":"s"}
    for orig, rep in similar.items():
        if orig in word:
            results.add(word.replace(orig, rep, 1))
    
    # 10. Цифры в конце
    for n in range(10):
        results.add(word + str(n))
    for n in [11,13,23,42,69,77,99,100,228,666,777]:
        results.add(word + str(n))
    
    # 11. Буква/цифра в начале (только если валидно)
    for c in "xzkvwymr":
        results.add(c + word)
    
    # 12. Обрезка + символ
    for n in range(3, len(word)):
        part = word[:n]
        for c in "xzo0123456789":
            results.add(part + c)
        results.add(part)
    
    # 13. Задняя обрезка + символ
    for n in range(3, len(word)):
        part = word[-n:]
        if part[0].isalpha():
            for c in "x1o0":
                results.add(part + c)
            results.add(part)
    
    # 14. Слово + слово (первые 3 + последние 3)
    if len(word) >= 6:
        results.add(word[:3] + word[-3:])
        results.add(word[-3:] + word[:3])
    
    # 15. Удаление гласных
    no_v = word[0] + "".join(c for c in word[1:] if c not in "aeiou")
    if len(no_v) >= 4 and no_v != word:
        results.add(no_v)
        for c in "xo0123":
            results.add(no_v + c)
    
    # 16. Удаление согласных (оставляем скелет)
    only_v = "".join(c for c in word if c in "aeiou")
    if len(only_v) >= 3:
        # обрамить согласными
        results.add(word[0] + only_v)
    
    # 17. Удвоение всего слова (если короткое)
    if len(word) <= 5:
        results.add(word + word)
        results.add(word + word[::-1])
    
    # 18. Капитализация через _ (разбивка)
    if len(word) >= 6:
        for i in range(3, len(word)-2):
            results.add(word[:i] + "_" + word[i:])
    
    # 19. Первые буквы + цифры для длинных
    if len(word) >= 4:
        for rep in ["er","or","ar","ix","ox","ax","us","is"]:
            r = word[:-2] + rep
            if r != word: results.add(r)
    
    # 20. Замена окончания
    if len(word) >= 4:
        for rep in ["o","i","a","y","x"]:
            r = word[:-1] + rep
            if r != word: results.add(r)
    
    # Фильтр валидных
    valid = []
    seen = set()
    for r in results:
        r = r.replace("__","_").strip("_")
        if (r not in seen and r != word and 
            re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r) and 
            5 <= len(r) <= 15):
            valid.append(r)
            seen.add(r)
    
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
    for i in range(len(base)):
        muts.append(base[:i]+random.choice(_C+_V)+base[i+1:])
    for i in range(len(base)+1):
        m=base[:i]+random.choice(_C+_V)+base[i:]
        if len(m)<=15: muts.append(m)
    for i in range(len(base)):
        m=base[:i]+base[i]+base[i:]
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
    "default":    {"name":"Дефолт",     "emoji":"🎲","desc":"Красивые (5 букв)",       "_default_premium":False,"premium":False,"func":gen_default,   "disabled":False},
    "beautiful":  {"name":"Красивые",   "emoji":"💎","desc":"Стильные паттерны",        "_default_premium":True, "premium":True, "func":gen_beautiful, "disabled":False},
    "meaningful": {"name":"Со смыслом", "emoji":"📖","desc":"Комбинации слов",          "_default_premium":True, "premium":True, "func":gen_meaningful,"disabled":False},
    "anyword":    {"name":"Любое слово","emoji":"🔤","desc":"Произносимые (5-7 букв)",  "_default_premium":True, "premium":True, "func":gen_anyword,   "disabled":False},
    "mat":        {"name":"Матерные",   "emoji":"🔞","desc":"18+ юзернеймы",            "_default_premium":True, "premium":True, "func":gen_mat,       "disabled":False},
    "telegram":   {"name":"Telegram",   "emoji":"📱","desc":"TG-тематика",              "_default_premium":True, "premium":True, "func":gen_telegram,  "disabled":False},
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
            headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as resp:
            if resp.status!=200: return "unavailable"
            text=await resp.text()
            if "Sold" in text or "sold" in text: r="sold"
            elif any(x in text for x in ["Available","Make an offer","Bid","auction"]): r="fragment"
            else: r="unavailable"
            _fragment_cache[u]=(r,now); return r
    except: return "unavailable"

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

# ═══════════════════════ ПОИСК v5 ═══════════════════════

async def do_search(count, gen_func, msg, mode_name, uid):
    found=[]; attempts=0; start=time.time(); last_update=0
    checked=set(); errors=0; fallback=not pool.has_sessions()
    warned_slow=False
    config=load_bot_config()
    
    if fallback:
        await edit_msg(msg, f"⚠️ <b>{mode_name}</b>\n\n⏳ Все сессии заняты — поиск будет дольше обычного...")
    
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
                    fr=await check_fragment(u)
                    found.append({"username":u,"fragment":fr})
                    save_history(uid,u,mode_name,len(u))
                await asyncio.sleep(2)  # ═ МЕДЛЕННЕЕ чтобы не ломать ═
            else:
                r=await pool.check(u,uid)
                if r=="taken": errors=0; await asyncio.sleep(0.2); continue
                if r=="skip":
                    errors+=1
                    if errors>=3 and not warned_slow:
                        warned_slow=True
                        fallback=True
                        await edit_msg(msg,
                            f"⚠️ <b>{mode_name}</b>\n\n"
                            f"⏳ Все сессии заняты — поиск будет дольше обычного...\n\n"
                            f"📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>")
                        errors=0
                    await asyncio.sleep(2); continue  # ═ МЕДЛЕННЕЕ ═
                errors=0
                if r=="maybe_free":
                    final=await pool.strong_check(u,uid)
                    if final=="taken": await asyncio.sleep(0.5); continue  # ═ чуть медленнее ═
                    if final=="skip": await asyncio.sleep(2); continue  # ═ МЕДЛЕННЕЕ ═
                    if final=="free":
                        fr=await check_fragment(u)
                        if fr!="fragment":
                            found.append({"username":u,"fragment":fr})
                            save_history(uid,u,mode_name,len(u))
                    await asyncio.sleep(0.8)  # ═ чуть медленнее ═

            now=time.time()
            if now-last_update>2.0:
                last_update=now; el=int(now-start)
                if fallback:
                    sl="⚠️ Сессии заняты"
                else:
                    ps=pool.stats()
                    sl=f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
                slow_txt = "\n⏳ Поиск дольше обычного..." if (warned_slow or fallback) else ""
                await edit_msg(msg,f"🔎 <b>{mode_name}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n🔄 {sl}\n⏱ {el}с{slow_txt}")
        return found, {"attempts":attempts,"elapsed":int(time.time()-start)}
    finally:
        pool.remove_user(uid)

async def do_template_search(template, count, msg, uid):
    found=[]; attempts=0; start=time.time(); last_update=0; checked=set()
    try:
        while len(found)<count and attempts<500:
            u=gen_from_template(template)
            if u is None or u.lower() in checked or not is_valid_username(u): attempts+=1; continue
            checked.add(u.lower()); attempts+=1
            r=await pool.check(u.lower(),uid)
            if r=="maybe_free":
                d=await pool.strong_check(u.lower(),uid)
                if d=="free":
                    fr=await check_fragment(u.lower())
                    if fr!="fragment":
                        found.append({"username":u.lower(),"fragment":fr})
                        save_history(uid,u.lower(),"Шаблон",len(u))
            await asyncio.sleep(1)
            now=time.time()
            if now-last_update>3:
                last_update=now
                await edit_msg(msg,f"🎯 <b>{template}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
        return found, {"attempts":attempts,"elapsed":int(time.time()-start)}
    finally: pass

async def do_similar_search(base, count, msg, uid):
    muts=gen_similar(base); found=[]; attempts=0; start=time.time(); last_update=0
    try:
        for u in muts:
            if len(found)>=count or attempts>=200: break
            attempts+=1
            r=await pool.check(u,uid)
            if r=="maybe_free":
                d=await pool.strong_check(u,uid)
                if d=="free":
                    fr=await check_fragment(u)
                    if fr!="fragment":
                        found.append({"username":u,"fragment":fr})
                        save_history(uid,u,"Похожие",len(u))
            await asyncio.sleep(1)
            now=time.time()
            if now-last_update>3:
                last_update=now
                await edit_msg(msg,f"🔄 <b>@{base}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
        return found, {"attempts":attempts,"elapsed":int(time.time()-start)}
    finally: pass

# ═══ НОВОЕ: Тематический поиск (VIP) ═══
async def do_thematic_search(word, count, msg, uid):
    """Поиск по слову — генерирует вариации и проверяет"""
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
                    if fr != "fragment":
                        found.append({"username": u, "fragment": fr})
                        save_history(uid, u, f"По слову: {word}", len(u))
            await asyncio.sleep(1)
            
            now = time.time()
            if now - last_update > 3:
                last_update = now
                await edit_msg(msg, f"🎯 <b>По слову: {word}</b>\n\n📊 <code>{attempts}</code>\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")
        
        return found, {"attempts": attempts, "elapsed": int(time.time()-start)}
    finally:
        pool.remove_user(uid)

# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '', joined TEXT DEFAULT '',
        free INTEGER DEFAULT 3, searches INTEGER DEFAULT 0, sub_end TEXT DEFAULT '',
        referred_by INTEGER DEFAULT 0, ref_count INTEGER DEFAULT 0,
        sub_bonus INTEGER DEFAULT 0,
        auto_renew INTEGER DEFAULT 0, auto_renew_plan TEXT DEFAULT '',
        last_reminder TEXT DEFAULT '', banned INTEGER DEFAULT 0,
        balance REAL DEFAULT 0.0, pending_ref INTEGER DEFAULT 0,
        captcha_passed INTEGER DEFAULT 0, last_roulette TEXT DEFAULT '',
        extra_searches INTEGER DEFAULT 0, monitor_slots INTEGER DEFAULT 0,
        template_uses INTEGER DEFAULT 0, daily_searches_used INTEGER DEFAULT 0,
        daily_searches_date TEXT DEFAULT '', vip_end TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY, days INTEGER, ktype TEXT, created TEXT,
        used INTEGER DEFAULT 0, used_by INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER,
        status TEXT DEFAULT 'pending', created TEXT,
        reviewed_by INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0)""")
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
    for col, default in [
        ("banned","0"),("balance","0.0"),("pending_ref","0"),("captcha_passed","0"),
        ("last_roulette","''"),("auto_renew","0"),("auto_renew_plan","''"),
        ("last_reminder","''"),("extra_searches","0"),("monitor_slots","0"),
        ("template_uses","0"),("daily_searches_used","0"),("daily_searches_date","''"),
        ("vip_end","''")  # ═ НОВОЕ ═
    ]:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except: pass
    try: c.execute("ALTER TABLE promotions ADD COLUMN button_text TEXT DEFAULT ''")
    except: pass

    # Фикс: пересоздаём таблицу market с правильной структурой
    try:
        c.execute("SELECT mtype FROM market LIMIT 1")
    except:
        c.execute("DROP TABLE IF EXISTS market")
    
    # ═══ МАРКЕТПЛЕЙС ТАБЛИЦЫ ═══
    c.execute("""CREATE TABLE IF NOT EXISTS market (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seller_uid INTEGER, mtype TEXT DEFAULT 'username',
        title TEXT DEFAULT '', description TEXT DEFAULT '',
        price INTEGER DEFAULT 0, status TEXT DEFAULT 'pending',
        buyer_uid INTEGER DEFAULT 0, created TEXT DEFAULT '',
        sold_at TEXT DEFAULT '', moderated_by INTEGER DEFAULT 0,
        escrow_deadline TEXT DEFAULT '',
        seller_confirmed INTEGER DEFAULT 0, buyer_confirmed INTEGER DEFAULT 0,
        dispute INTEGER DEFAULT 0, dispute_reason TEXT DEFAULT '',
        charge_id TEXT DEFAULT '', promoted INTEGER DEFAULT 0,
        promoted_until TEXT DEFAULT '', is_nft INTEGER DEFAULT 0,
        fragment_url TEXT DEFAULT '', fast_mod INTEGER DEFAULT 0,
        listing_paid INTEGER DEFAULT 0
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS promocodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE, discount_percent INTEGER DEFAULT 0,
        discount_stars INTEGER DEFAULT 0, max_uses INTEGER DEFAULT 1,
        used_count INTEGER DEFAULT 0, min_purchase INTEGER DEFAULT 0,
        applies_to TEXT DEFAULT 'all', created_by INTEGER DEFAULT 0,
        created TEXT DEFAULT '', expires TEXT DEFAULT '',
        active INTEGER DEFAULT 1
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS promocode_uses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT, uid INTEGER,
        used_at TEXT DEFAULT '', discount_amount INTEGER DEFAULT 0
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS exchanges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        initiator_uid INTEGER, partner_uid INTEGER DEFAULT 0,
        initiator_offer TEXT DEFAULT '', partner_offer TEXT DEFAULT '',
        status TEXT DEFAULT 'open', created TEXT DEFAULT '',
        completed_at TEXT DEFAULT '',
        initiator_confirmed INTEGER DEFAULT 0, partner_confirmed INTEGER DEFAULT 0,
        escrow_deadline TEXT DEFAULT '', dispute INTEGER DEFAULT 0
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_uid INTEGER, to_uid INTEGER,
        rating INTEGER DEFAULT 5, text TEXT DEFAULT '',
        deal_id INTEGER DEFAULT 0, deal_type TEXT DEFAULT 'market',
        created TEXT DEFAULT ''
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS lootbox_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, prize TEXT DEFAULT '',
        prize_type TEXT DEFAULT '', created TEXT DEFAULT ''
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS wheel_spins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, prize TEXT DEFAULT '',
        created TEXT DEFAULT ''
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS market_slots (
        uid INTEGER PRIMARY KEY,
        extra_slots INTEGER DEFAULT 0
    )""")

    # ALTER для market (старые БД)
    for col, default in [
        ("promoted","0"),("promoted_until","''"),("is_nft","0"),
        ("fragment_url","''"),("fast_mod","0"),("listing_paid","0"),
        ("charge_id","''")
    ]:
        try: c.execute(f"ALTER TABLE market ADD COLUMN {col} DEFAULT {default}")
        except: pass
    conn.commit(); conn.close()

def ensure_user(uid, uname=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM users WHERE uid=?", (uid,))
    if not c.fetchone():
        c.execute("INSERT INTO users (uid,uname,joined,free) VALUES (?,?,?,?)",
                  (uid, uname or "", datetime.now().strftime("%Y-%m-%d %H:%M"), FREE_SEARCHES))
    elif uname:
        c.execute("UPDATE users SET uname=? WHERE uid=?", (uname, uid))
    conn.commit(); conn.close()

def get_user(uid):
    ensure_user(uid)
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT * FROM users WHERE uid=?", (uid,))
    row = c.fetchone(); conn.close()
    if not row:
        return {"uid":uid,"uname":"","free":FREE_SEARCHES,"searches":0,"sub_end":"",
                "ref_count":0,"referred_by":0,"sub_bonus":0,"auto_renew":0,
                "auto_renew_plan":"","last_reminder":"","banned":0,"balance":0.0,
                "pending_ref":0,"captcha_passed":0,"last_roulette":"",
                "extra_searches":0,"monitor_slots":0,"template_uses":0,
                "daily_searches_used":0,"daily_searches_date":"","vip_end":""}
    d = dict(row)
    for k, v in [("auto_renew",0),("auto_renew_plan",""),("last_reminder",""),
                 ("banned",0),("balance",0.0),("pending_ref",0),("captcha_passed",0),
                 ("last_roulette",""),("extra_searches",0),("monitor_slots",0),
                 ("template_uses",0),("daily_searches_used",0),("daily_searches_date",""),
                 ("vip_end","")]:
        d.setdefault(k, v)
    return d

def is_banned(uid):
    if rate_limiter.is_temp_banned(uid): return True
    return get_user(uid).get("banned", 0) == 1

def ban_user(uid):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()

def unban_user(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=0 WHERE uid=?", (uid,)); conn.commit(); conn.close()
    rate_limiter.temp_bans.pop(uid, None); rate_limiter.warnings.pop(uid, None)

def has_subscription(uid):
    if uid in ADMIN_IDS: return True
    sub_end = get_user(uid).get("sub_end", "")
    if not sub_end: return False
    try: return datetime.strptime(sub_end, "%Y-%m-%d %H:%M") > datetime.now()
    except: return False

# ═══ НОВОЕ: VIP ФУНКЦИИ ═══
def has_vip(uid):
    """Проверка VIP (работает только при активном Premium)"""
    if uid in ADMIN_IDS: return True
    if not has_subscription(uid): return False
    vip_end = get_user(uid).get("vip_end", "")
    if not vip_end: return False
    try: return datetime.strptime(vip_end, "%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def give_vip(uid, days):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    now = datetime.now(); u = get_user(uid)
    vip_end = u.get("vip_end", ""); base = now
    if vip_end:
        try:
            cur = datetime.strptime(vip_end, "%Y-%m-%d %H:%M")
            if cur > now: base = cur
        except: pass
    new_end = base + timedelta(days=days)
    c.execute("UPDATE users SET vip_end=? WHERE uid=?", (new_end.strftime("%Y-%m-%d %H:%M"), uid))
    conn.commit(); conn.close()
    return new_end.strftime("%d.%m.%Y %H:%M")

def remove_vip(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET vip_end='' WHERE uid=?", (uid,)); conn.commit(); conn.close()

def _reset_daily_if_needed(uid):
    u = get_user(uid)
    today = datetime.now().strftime("%Y-%m-%d")
    if u.get("daily_searches_date", "") != today:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET daily_searches_used=0, daily_searches_date=? WHERE uid=?", (today, uid))
        conn.commit(); conn.close()

def can_search(uid):
    if uid in ADMIN_IDS: return True
    _reset_daily_if_needed(uid)
    u = get_user(uid)
    if has_subscription(uid):
        used_today = u.get("daily_searches_used", 0)
        limit = VIP_SEARCHES_LIMIT if has_vip(uid) else PREMIUM_SEARCHES_LIMIT  # ═ ИЗМЕНЕНО ═
        return used_today < limit
    else:
        return u.get("free", 0) + u.get("extra_searches", 0) > 0

def use_search(uid):
    _reset_daily_if_needed(uid)
    conn = sqlite3.connect(DB); c = conn.cursor()
    if uid in ADMIN_IDS:
        c.execute("UPDATE users SET searches=searches+1 WHERE uid=?", (uid,))
    elif has_subscription(uid):
        c.execute("UPDATE users SET searches=searches+1, daily_searches_used=daily_searches_used+1 WHERE uid=?", (uid,))
    else:
        u = get_user(uid)
        if u.get("extra_searches", 0) > 0:
            c.execute("UPDATE users SET extra_searches=MAX(extra_searches-1,0), searches=searches+1 WHERE uid=?", (uid,))
        else:
            c.execute("UPDATE users SET free=MAX(free-1,0), searches=searches+1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

# ═ ИЗМЕНЕНО: учитывает VIP ═
def get_search_count(uid):
    if uid in ADMIN_IDS: return 6
    if has_vip(uid): return VIP_COUNT
    return PREMIUM_COUNT if has_subscription(uid) else FREE_COUNT

def get_max_searches(uid):
    if uid in ADMIN_IDS: return 999
    _reset_daily_if_needed(uid)
    u = get_user(uid)
    if has_subscription(uid):
        used = u.get("daily_searches_used", 0)
        limit = VIP_SEARCHES_LIMIT if has_vip(uid) else PREMIUM_SEARCHES_LIMIT  # ═ ИЗМЕНЕНО ═
        return max(0, limit - used)
    else:
        return u.get("free", 0) + u.get("extra_searches", 0)

def give_subscription(uid, days):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    now = datetime.now(); u = get_user(uid)
    sub_end = u.get("sub_end", ""); base = now
    if sub_end:
        try:
            cur = datetime.strptime(sub_end, "%Y-%m-%d %H:%M")
            if cur > now: base = cur
        except: pass
    new_end = base + timedelta(days=days)
    c.execute("UPDATE users SET sub_end=? WHERE uid=?", (new_end.strftime("%Y-%m-%d %H:%M"), uid))
    conn.commit(); conn.close()
    return new_end.strftime("%d.%m.%Y %H:%M")

def remove_subscription(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET sub_end='' WHERE uid=?", (uid,)); conn.commit(); conn.close()

def set_free_searches(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET free=? WHERE uid=?", (max(0,count), uid)); conn.commit(); conn.close()

def add_extra_searches(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET extra_searches=extra_searches+? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

def set_balance(uid, amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=? WHERE uid=?", (max(0,amount), uid)); conn.commit(); conn.close()

def add_balance(uid, amount):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=balance+? WHERE uid=?", (amount, uid)); conn.commit(); conn.close()

def get_balance(uid): return get_user(uid).get("balance", 0.0)

def add_monitor_slots(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET monitor_slots=monitor_slots+? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

def add_template_uses(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET template_uses=template_uses+? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

def process_referral(new_uid, ref_uid):
    if new_uid == ref_uid: return False
    u = get_user(new_uid)
    if u.get("referred_by", 0) != 0: return False
    ensure_user(ref_uid)
    uname = get_user(new_uid).get("uname", "")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?", (ref_uid, new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1, extra_searches=extra_searches+? WHERE uid=?", (REF_BONUS, ref_uid))
    c.execute("INSERT INTO referrals (referrer_uid,referred_uid,referred_uname,created) VALUES (?,?,?,?)",
              (ref_uid, new_uid, uname, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()
    log_action(ref_uid, "ref_bonus", f"+{REF_BONUS} searches from {new_uid}")
    return True

def get_user_referrals(uid, limit=50):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referred_uid,referred_uname,created FROM referrals WHERE referrer_uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
    rows = c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"created":r[2]} for r in rows]

def get_ref_top_by_period(start_date, limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referrer_uid,COUNT(*) as cnt FROM referrals WHERE created>=? GROUP BY referrer_uid ORDER BY cnt DESC LIMIT ?", (start_date, limit))
    rows = c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":get_user(r[0]).get("uname",""),"ref_count":r[1]} for r in rows]

def check_referral_fraud(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referred_uid,created FROM referrals WHERE referrer_uid=? ORDER BY created", (uid,))
    rows = c.fetchall(); conn.close()
    if len(rows)<3: return {"fraud":False,"reason":""}
    sus = 0
    for i in range(1,len(rows)):
        try:
            p = datetime.strptime(rows[i-1][1], "%Y-%m-%d %H:%M")
            cu = datetime.strptime(rows[i][1], "%Y-%m-%d %H:%M")
            if (cu-p).total_seconds()<60: sus+=1
        except: pass
    if sus>=3: return {"fraud":True,"reason":"Много рефералов за короткое время"}
    return {"fraud":False,"reason":""}

def remove_referral(referrer_uid, referred_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM referrals WHERE referrer_uid=? AND referred_uid=?", (referrer_uid,referred_uid))
    c.execute("UPDATE users SET ref_count=MAX(ref_count-1,0),free=MAX(free-?,0) WHERE uid=?", (REF_BONUS,referrer_uid))
    c.execute("UPDATE users SET referred_by=0 WHERE uid=?", (referred_uid,))
    conn.commit(); conn.close()

def set_pending_ref(uid, ref_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET pending_ref=? WHERE uid=?", (ref_uid,uid)); conn.commit(); conn.close()

def get_pending_ref(uid): return get_user(uid).get("pending_ref", 0)

def set_captcha_passed(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET captcha_passed=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()

def activate_key(uid, key_text):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT days,ktype FROM keys WHERE key=? AND used=0", (key_text.strip(),))
    row = c.fetchone()
    if not row: conn.close(); return None
    days,ktype = row
    c.execute("UPDATE keys SET used=1,used_by=? WHERE key=?", (uid,key_text.strip()))
    conn.commit(); conn.close()
    return {"days":days,"end":give_subscription(uid,days)}

def generate_key(days, ktype="MANUAL"):
    key = f"HUNT-{ktype}-{secrets.token_hex(4).upper()}"
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO keys (key,days,ktype,created) VALUES (?,?,?,?)",
              (key,days,ktype,datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close(); return key

def set_auto_renew(uid, enabled, plan=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET auto_renew=?,auto_renew_plan=? WHERE uid=?",
              (1 if enabled else 0, plan, uid)); conn.commit(); conn.close()

def get_auto_renew(uid):
    u = get_user(uid)
    return bool(u.get("auto_renew",0)), u.get("auto_renew_plan","")

def set_last_reminder(uid, ds):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET last_reminder=? WHERE uid=?", (ds,uid)); conn.commit(); conn.close()

def get_expiring_users(days_before):
    conn = sqlite3.connect(DB); c = conn.cursor()
    t = datetime.now() + timedelta(days=days_before)
    c.execute("SELECT uid,sub_end,auto_renew,auto_renew_plan,last_reminder FROM users WHERE sub_end BETWEEN ? AND ? AND sub_end!=''",
              (t.strftime("%Y-%m-%d 00:00"), t.strftime("%Y-%m-%d 23:59")))
    rows = c.fetchall(); conn.close()
    return [{"uid":r[0],"sub_end":r[1],"auto_renew":r[2],"auto_renew_plan":r[3],"last_reminder":r[4] or ""} for r in rows]

def save_history(uid, username, mode, length=5):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO history (uid,username,found_at,mode,length) VALUES (?,?,?,?,?)",
              (uid,username,datetime.now().strftime("%Y-%m-%d %H:%M"),mode,length))
    conn.commit(); conn.close()

def get_history(uid, limit=20):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username,found_at,mode FROM history WHERE uid=? ORDER BY id DESC LIMIT ?", (uid,limit))
    rows = c.fetchall(); conn.close(); return rows

# ═══ НОВОЕ: удаление по шаблону ═══
def delete_history_pattern(uid, pattern):
    """Удаляет из истории юзернеймы содержащие pattern"""
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM history WHERE uid=? AND username LIKE ?", (uid, f"%{pattern}%"))
    deleted = c.rowcount
    conn.commit(); conn.close()
    return deleted

def set_last_roulette(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET last_roulette=? WHERE uid=?", (datetime.now().strftime("%Y-%m-%d %H:%M"),uid))
    conn.commit(); conn.close()

def can_roulette(uid):
    u = get_user(uid); lr = u.get("last_roulette","")
    if not lr: return True
    try: return (datetime.now()-datetime.strptime(lr,"%Y-%m-%d %H:%M")).days>=7
    except: return True

def create_withdrawal(uid, amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO withdrawals (uid,amount,status,created) VALUES (?,?,'pending',?)",
              (uid,amount,datetime.now().strftime("%Y-%m-%d %H:%M")))
    wid = c.lastrowid; conn.commit(); conn.close(); return wid

def get_pending_withdrawals():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,uid,amount,created FROM withdrawals WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"amount":r[2],"created":r[3]} for r in rows]

def process_withdrawal(wid, admin_uid, approve=True):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,amount FROM withdrawals WHERE id=? AND status='pending'", (wid,))
    row = c.fetchone()
    if not row: conn.close(); return None
    uid, amount = row
    if approve:
        c.execute("UPDATE withdrawals SET status='approved',processed_by=? WHERE id=?", (admin_uid,wid))
        c.execute("UPDATE users SET balance=MAX(balance-?,0) WHERE uid=?", (amount,uid))
    else:
        c.execute("UPDATE withdrawals SET status='rejected',processed_by=? WHERE id=?", (admin_uid,wid))
    conn.commit(); conn.close()
    return {"uid":uid,"amount":amount}

def create_promotion(name, ptype, button_text="", data=None):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO promotions (name,ptype,button_text,data,created) VALUES (?,?,?,?,?)",
              (name,ptype,button_text,json.dumps(data or {}),datetime.now().strftime("%Y-%m-%d %H:%M")))
    pid = c.lastrowid; conn.commit(); conn.close(); return pid

def get_active_promotions():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,name,ptype,data,created,button_text FROM promotions WHERE active=1")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"name":r[1],"ptype":r[2],"data":json.loads(r[3] or "{}"),"created":r[4],
             "button_text":r[5] if r[5] else r[1]} for r in rows]

def end_promotion(pid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE promotions SET active=0,ended=? WHERE id=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"),pid)); conn.commit(); conn.close()

def get_ref_top(limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,uname,ref_count FROM users WHERE ref_count>0 ORDER BY ref_count DESC LIMIT ?", (limit,))
    rows = c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"ref_count":r[2]} for r in rows]

def get_my_ref_place(uid):
    u = get_user(uid); my = u.get("ref_count",0)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE ref_count>?", (my,))
    above = c.fetchone()[0]; conn.close()
    return above+1, my

def get_premium_users():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    c.execute("SELECT uid,uname,sub_end FROM users WHERE sub_end>? AND sub_end!=''", (now_s,))
    rows = c.fetchall(); conn.close()
    return [{"uid":r[0],"uname":r[1],"sub_end":r[2]} for r in rows]

def tiktok_can_submit(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE uid=? AND created LIKE ?", (uid,today+"%"))
    cnt = c.fetchone()[0]; conn.close(); return cnt < TIKTOK_DAILY_LIMIT

def task_create(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id FROM tasks WHERE uid=? AND status='pending'", (uid,))
    ex = c.fetchone()
    if ex: conn.close(); return ex[0]
    c.execute("INSERT INTO tasks (uid,status,created) VALUES (?,'pending',?)",
              (uid,datetime.now().strftime("%Y-%m-%d %H:%M")))
    tid = c.lastrowid; conn.commit(); conn.close(); return tid

def task_approve(tid, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'", (tid,))
    r = c.fetchone()
    if not r: conn.close(); return None
    c.execute("UPDATE tasks SET status='approved',reviewed_by=? WHERE id=?", (admin_uid,tid))
    conn.commit(); conn.close(); return r[0]

def task_reject(tid, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'", (tid,))
    r = c.fetchone()
    if not r: conn.close(); return None
    c.execute("UPDATE tasks SET status='rejected',reviewed_by=? WHERE id=?", (admin_uid,tid))
    conn.commit(); conn.close(); return r[0]

def get_pending_tasks():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,uid,created,photo_count FROM tasks WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"created":r[2],"photos":r[3]} for r in rows]

def add_blacklist(username, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try: c.execute("INSERT INTO blacklist (username,added_by,created) VALUES (?,?,?)",
                   (username.lower(),admin_uid,datetime.now().strftime("%Y-%m-%d %H:%M")))
    except: pass
    conn.commit(); conn.close()

def remove_blacklist(username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM blacklist WHERE username=?", (username.lower(),)); conn.commit(); conn.close()

def is_blacklisted(username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username FROM blacklist WHERE username=?", (username.lower(),))
    r = c.fetchone(); conn.close(); return r is not None

def get_blacklist():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username,added_by,created FROM blacklist ORDER BY created DESC")
    rows = c.fetchall(); conn.close()
    return [{"username":r[0],"added_by":r[1],"created":r[2]} for r in rows]

def add_monitor(uid, username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    expires = (datetime.now()+timedelta(days=7)).strftime("%Y-%m-%d %H:%M")
    c.execute("INSERT INTO monitors (uid,username,status,created,expires) VALUES (?,?,?,?,?)",
              (uid,username.lower(),'active',datetime.now().strftime("%Y-%m-%d %H:%M"),expires))
    mid = c.lastrowid; conn.commit(); conn.close(); return mid

def get_user_monitors(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,username,status,created,expires,last_status FROM monitors WHERE uid=? AND status='active'", (uid,))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"username":r[1],"status":r[2],"created":r[3],"expires":r[4],"last_status":r[5]} for r in rows]

def remove_monitor(mid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET status='removed' WHERE id=? AND uid=?", (mid,uid)); conn.commit(); conn.close()

def get_active_monitors():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    c.execute("SELECT id,uid,username,expires FROM monitors WHERE status='active' AND expires>?", (now_s,))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"username":r[2],"expires":r[3]} for r in rows]

def update_monitor_status(mid, status):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET last_check=?,last_status=? WHERE id=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"),status,mid)); conn.commit(); conn.close()

def expire_monitors():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET status='expired' WHERE status='active' AND expires<=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"),)); conn.commit(); conn.close()

def get_monitor_count(uid): return len(get_user_monitors(uid))

def get_monitor_limit(uid):
    if uid in ADMIN_IDS: return 99
    u = get_user(uid)
    base = MONITOR_MAX_PREMIUM if has_subscription(uid) else MONITOR_MAX_FREE
    return base + u.get("monitor_slots", 0)

def get_stats():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M"); today = datetime.now().strftime("%Y-%m-%d")
    r = {
        "users": c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "subs": c.execute("SELECT COUNT(*) FROM users WHERE sub_end>?", (now_s,)).fetchone()[0],
        "searches": c.execute("SELECT COALESCE(SUM(searches),0) FROM users").fetchone()[0],
        "tasks": c.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0],
        "today_users": c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?", (today+"%",)).fetchone()[0],
        "today_searches": c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?", (today+"%",)).fetchone()[0],
        "banned": c.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0],
        "withdrawals": c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0],
        "promos": c.execute("SELECT COUNT(*) FROM promotions WHERE active=1").fetchone()[0],
        "monitors": c.execute("SELECT COUNT(*) FROM monitors WHERE status='active'").fetchone()[0],
        "blacklist": c.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0],
    }
    conn.close(); return r

def find_user(inp):
    inp = inp.strip().replace("@","")
    if inp.isdigit(): return int(inp)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM users WHERE uname=?", (inp,))
    r = c.fetchone(); conn.close()
    return r[0] if r else None

# ═══════════════════════ МАРКЕТПЛЕЙС ФУНКЦИИ ═══════════════════════

def market_create_lot(seller_uid, mtype, title, description, price, is_nft=0, fragment_url=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("""INSERT INTO market (seller_uid,mtype,title,description,price,status,created,is_nft,fragment_url)
                 VALUES (?,?,?,?,?,'pending',?,?,?)""",
              (seller_uid, mtype, title, description, price,
               datetime.now().strftime("%Y-%m-%d %H:%M"), is_nft, fragment_url))
    lot_id = c.lastrowid; conn.commit(); conn.close(); return lot_id

def market_approve_lot(lot_id, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET status='active',moderated_by=? WHERE id=? AND status='pending'", (admin_uid, lot_id))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

def market_reject_lot(lot_id, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET status='rejected',moderated_by=? WHERE id=? AND status='pending'", (admin_uid, lot_id))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

def market_get_active_lots(limit=20, nft_only=False):
    conn = sqlite3.connect(DB); c = conn.cursor()
    if nft_only:
        c.execute("SELECT id,seller_uid,mtype,title,description,price,created,promoted,is_nft,fragment_url FROM market WHERE status='active' AND is_nft=1 ORDER BY promoted DESC, id DESC LIMIT ?", (limit,))
    else:
        c.execute("SELECT id,seller_uid,mtype,title,description,price,created,promoted,is_nft,fragment_url FROM market WHERE status='active' AND is_nft=0 ORDER BY promoted DESC, id DESC LIMIT ?", (limit,))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"type":r[2],"title":r[3],"desc":r[4],"price":r[5],
             "created":r[6],"promoted":r[7],"is_nft":r[8],"fragment_url":r[9]} for r in rows]

def market_get_lot(lot_id):
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT * FROM market WHERE id=?", (lot_id,))
    row = c.fetchone(); conn.close()
    return dict(row) if row else None

def market_get_user_lots(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,mtype,title,price,status,promoted,is_nft FROM market WHERE seller_uid=? AND status IN ('pending','active','escrow') ORDER BY id DESC", (uid,))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"type":r[1],"title":r[2],"price":r[3],"status":r[4],"promoted":r[5],"is_nft":r[6]} for r in rows]

def market_get_pending():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,seller_uid,mtype,title,price,created,is_nft,fast_mod FROM market WHERE status='pending' ORDER BY fast_mod DESC, id")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"type":r[2],"title":r[3],"price":r[4],"created":r[5],"is_nft":r[6],"fast":r[7]} for r in rows]

def market_buy_lot(lot_id, buyer_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    deadline = (datetime.now() + timedelta(hours=MARKET_ESCROW_HOURS)).strftime("%Y-%m-%d %H:%M")
    c.execute("""UPDATE market SET status='escrow',buyer_uid=?,sold_at=?,escrow_deadline=?
                 WHERE id=? AND status='active' AND seller_uid!=?""",
              (buyer_uid, datetime.now().strftime("%Y-%m-%d %H:%M"), deadline, lot_id, buyer_uid))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

def market_confirm_seller(lot_id):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET seller_confirmed=1 WHERE id=?", (lot_id,))
    conn.commit(); conn.close()
    return _check_deal_complete(lot_id)

def market_confirm_buyer(lot_id):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET buyer_confirmed=1 WHERE id=?", (lot_id,))
    conn.commit(); conn.close()
    return _check_deal_complete(lot_id)

def _check_deal_complete(lot_id):
    lot = market_get_lot(lot_id)
    if not lot: return False
    if lot["seller_confirmed"] and lot["buyer_confirmed"]:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE market SET status='completed' WHERE id=?", (lot_id,))
        payout = int(lot["price"] * (1 - MARKET_COMMISSION))
        c.execute("UPDATE users SET balance=balance+? WHERE uid=?", (payout, lot["seller_uid"]))
        conn.commit(); conn.close()
        return True
    return False

def market_promote_lot(lot_id):
    conn = sqlite3.connect(DB); c = conn.cursor()
    until = (datetime.now() + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE market SET promoted=1,promoted_until=? WHERE id=?", (until, lot_id))
    conn.commit(); conn.close()

def market_cancel_lot(lot_id, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET status='cancelled' WHERE id=? AND seller_uid=? AND status IN ('pending','active')", (lot_id, uid))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

def market_count_user_lots(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM market WHERE seller_uid=? AND status IN ('pending','active')", (uid,))
    cnt = c.fetchone()[0]; conn.close(); return cnt

def market_get_max_lots(uid):
    base = MARKET_VIP_MAX_LOTS if has_vip(uid) else MARKET_MAX_LOTS
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT extra_slots FROM market_slots WHERE uid=?", (uid,))
    row = c.fetchone(); conn.close()
    extra = row[0] if row else 0
    return base + extra

def market_add_slot(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO market_slots (uid, extra_slots) VALUES (?, COALESCE((SELECT extra_slots FROM market_slots WHERE uid=?),0)+1)", (uid, uid))
    conn.commit(); conn.close()

def market_open_dispute(lot_id, reason, by_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET dispute=1,dispute_reason=?,status='dispute' WHERE id=?", (f"{by_uid}: {reason}", lot_id))
    conn.commit(); conn.close()

def market_resolve_dispute(lot_id, winner, admin_uid):
    lot = market_get_lot(lot_id)
    if not lot: return False, None
    conn = sqlite3.connect(DB); c = conn.cursor()
    if winner == "buyer":
        c.execute("UPDATE market SET status='refunded',moderated_by=? WHERE id=?", (admin_uid, lot_id))
        c.execute("SELECT charge_id FROM market WHERE id=?", (lot_id,))
        row = c.fetchone(); charge_id = row[0] if row and row[0] else None
        conn.commit(); conn.close(); return True, charge_id
    else:
        payout = int(lot["price"] * (1 - MARKET_COMMISSION))
        c.execute("UPDATE users SET balance=balance+? WHERE uid=?", (payout, lot["seller_uid"]))
        c.execute("UPDATE market SET status='completed',moderated_by=? WHERE id=?", (admin_uid, lot_id))
        conn.commit(); conn.close(); return True, None

def market_get_disputes():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,seller_uid,buyer_uid,title,price,dispute_reason FROM market WHERE status='dispute'")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"seller":r[1],"buyer":r[2],"title":r[3],"price":r[4],"reason":r[5]} for r in rows]

def market_set_fast_mod(lot_id):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE market SET fast_mod=1 WHERE id=?", (lot_id,))
    conn.commit(); conn.close()

# ═══ ЛУТБОКС ═══

def lootbox_can_open(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT created FROM lootbox_history WHERE uid=? ORDER BY id DESC LIMIT 1", (uid,))
    row = c.fetchone(); conn.close()
    if not row: return True
    try:
        last = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
        return (datetime.now() - last).total_seconds() >= LOOTBOX_COOLDOWN
    except: return True

def lootbox_open(uid):
    roll = random.randint(1, 100)
    if roll <= 5:
        prize_type = "premium"; days = random.choice([3, 7])
        give_subscription(uid, days)
        prize = f"💎 Premium {days} дней!"
    elif roll <= 15:
        prize_type = "vip"; days = random.choice([1, 3])
        give_vip(uid, days)
        prize = f"🌟 VIP {days} дней!"
    elif roll <= 35:
        prize_type = "stars"; amount = random.choice([5, 10, 15, 20, 25, 50])
        add_balance(uid, amount)
        prize = f"⭐ {amount} звёзд!"
    elif roll <= 60:
        prize_type = "searches"; count = random.choice([2, 3, 5, 7, 10])
        add_extra_searches(uid, count)
        prize = f"🔍 {count} поисков!"
    elif roll <= 80:
        prize_type = "slot"; market_add_slot(uid)
        prize = f"📦 +1 слот маркета!"
    else:
        prize_type = "emoji"
        prize = random.choice(["🧸 Плюшевый мишка!", "🃏 Джокер!", "🎭 Маска!", "🎉 Ура!"])
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO lootbox_history (uid,prize,prize_type,created) VALUES (?,?,?,?)",
              (uid, prize, prize_type, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()
    return prize, prize_type

# ═══ КОЛЕСО ФОРТУНЫ ═══

def wheel_free_spins_today(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM wheel_spins WHERE uid=? AND created LIKE ?", (uid, today+"%"))
    cnt = c.fetchone()[0]; conn.close()
    return cnt

def wheel_spin(uid):
    prizes = [
        (30, "stars", 5, "⭐ 5 звёзд"),
        (20, "stars", 10, "⭐ 10 звёзд"),
        (15, "searches", 2, "🔍 2 поиска"),
        (10, "searches", 5, "🔍 5 поисков"),
        (8, "stars", 25, "⭐ 25 звёзд"),
        (5, "premium", 1, "💎 1 день Premium"),
        (5, "stars", 50, "⭐ 50 звёзд"),
        (3, "vip", 1, "🌟 1 день VIP"),
        (2, "premium", 3, "💎 3 дня Premium"),
        (1, "stars", 100, "⭐ 100 звёзд !!!"),
        (1, "premium", 7, "💎 7 дней PREMIUM!!!")
    ]
    roll = random.randint(1, 100); cumulative = 0
    for chance, ptype, value, text in prizes:
        cumulative += chance
        if roll <= cumulative:
            if ptype == "stars": add_balance(uid, value)
            elif ptype == "searches": add_extra_searches(uid, value)
            elif ptype == "premium": give_subscription(uid, value)
            elif ptype == "vip": give_vip(uid, value)
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("INSERT INTO wheel_spins (uid,prize,created) VALUES (?,?,?)",
                      (uid, text, datetime.now().strftime("%Y-%m-%d %H:%M")))
            conn.commit(); conn.close()
            return text, ptype
    return "🧸 Утешительный приз!", "nothing"

# ═══ ПРОМОКОДЫ ═══

def create_promocode(code, discount_percent=0, discount_stars=0, max_uses=1,
                     min_purchase=0, applies_to="all", created_by=0, expires=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("""INSERT INTO promocodes (code,discount_percent,discount_stars,max_uses,
                     min_purchase,applies_to,created_by,created,expires) VALUES (?,?,?,?,?,?,?,?,?)""",
                  (code.upper(), discount_percent, discount_stars, max_uses, min_purchase,
                   applies_to, created_by, datetime.now().strftime("%Y-%m-%d %H:%M"), expires))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def check_promocode(code, uid, purchase_amount=0, purchase_type="all"):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT * FROM promocodes WHERE code=? AND active=1", (code.upper(),))
    row = c.fetchone()
    if not row: conn.close(); return {"valid":False,"reason":"Не найден"}
    cols = [d[0] for d in c.description]; promo = dict(zip(cols, row))
    if promo.get("expires",""):
        try:
            if datetime.strptime(promo["expires"],"%Y-%m-%d %H:%M") < datetime.now():
                conn.close(); return {"valid":False,"reason":"Истёк"}
        except: pass
    if promo["used_count"] >= promo["max_uses"]:
        conn.close(); return {"valid":False,"reason":"Исчерпан"}
    c.execute("SELECT COUNT(*) FROM promocode_uses WHERE code=? AND uid=?", (code.upper(), uid))
    if c.fetchone()[0] > 0:
        conn.close(); return {"valid":False,"reason":"Уже использован"}
    if promo["min_purchase"] > 0 and purchase_amount < promo["min_purchase"]:
        conn.close(); return {"valid":False,"reason":f"Мин. покупка {promo['min_purchase']}⭐"}
    discount = 0
    if promo["discount_percent"] > 0:
        discount = int(purchase_amount * promo["discount_percent"] / 100)
    elif promo["discount_stars"] > 0:
        discount = min(promo["discount_stars"], purchase_amount - 1)
    conn.close()
    return {"valid":True,"discount":max(1, discount),"promo":promo}

def use_promocode(code, uid, discount_amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE promocodes SET used_count=used_count+1 WHERE code=?", (code.upper(),))
    c.execute("INSERT INTO promocode_uses (code,uid,used_at,discount_amount) VALUES (?,?,?,?)",
              (code.upper(), uid, datetime.now().strftime("%Y-%m-%d %H:%M"), discount_amount))
    conn.commit(); conn.close()

def get_all_promocodes():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT code,discount_percent,discount_stars,max_uses,used_count,expires,active FROM promocodes ORDER BY id DESC")
    rows = c.fetchall(); conn.close()
    return [{"code":r[0],"percent":r[1],"stars":r[2],"max":r[3],"used":r[4],"expires":r[5],"active":r[6]} for r in rows]

def deactivate_promocode(code):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE promocodes SET active=0 WHERE code=?", (code.upper(),))
    conn.commit(); conn.close()

# ═══ ОТЗЫВЫ ═══

def add_review(from_uid, to_uid, rating, text, deal_id=0, deal_type="market"):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO reviews (from_uid,to_uid,rating,text,deal_id,deal_type,created) VALUES (?,?,?,?,?,?,?)",
              (from_uid, to_uid, min(5,max(1,rating)), text[:200], deal_id, deal_type,
               datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()

def get_user_rating(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT AVG(rating),COUNT(*) FROM reviews WHERE to_uid=?", (uid,))
    row = c.fetchone(); conn.close()
    return {"avg":round(row[0],1) if row[0] else 0, "count":row[1] or 0}

def get_user_reviews(uid, limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT from_uid,rating,text,created FROM reviews WHERE to_uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
    rows = c.fetchall(); conn.close()
    return [{"from":r[0],"rating":r[1],"text":r[2],"created":r[3]} for r in rows]

# ═══ ОБМЕННИК ═══

def exchange_create(uid, offer):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO exchanges (initiator_uid,initiator_offer,status,created) VALUES (?,?,'open',?)",
              (uid, offer, datetime.now().strftime("%Y-%m-%d %H:%M")))
    eid = c.lastrowid; conn.commit(); conn.close(); return eid

def exchange_accept(eid, partner_uid, partner_offer):
    conn = sqlite3.connect(DB); c = conn.cursor()
    deadline = (datetime.now() + timedelta(hours=MARKET_ESCROW_HOURS)).strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE exchanges SET partner_uid=?,partner_offer=?,status='escrow',escrow_deadline=? WHERE id=? AND status='open' AND initiator_uid!=?",
              (partner_uid, partner_offer, deadline, eid, partner_uid))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

def exchange_confirm(eid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT initiator_uid,partner_uid FROM exchanges WHERE id=?", (eid,))
    row = c.fetchone()
    if not row: conn.close(); return False
    if uid == row[0]: c.execute("UPDATE exchanges SET initiator_confirmed=1 WHERE id=?", (eid,))
    elif uid == row[1]: c.execute("UPDATE exchanges SET partner_confirmed=1 WHERE id=?", (eid,))
    else: conn.close(); return False
    conn.commit(); conn.close()
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT initiator_confirmed,partner_confirmed FROM exchanges WHERE id=?", (eid,))
    r = c.fetchone()
    if r and r[0] and r[1]:
        c.execute("UPDATE exchanges SET status='completed',completed_at=? WHERE id=?",
                  (datetime.now().strftime("%Y-%m-%d %H:%M"), eid))
        conn.commit(); conn.close(); return True
    conn.close(); return False

def exchange_get_open(limit=20):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,initiator_uid,initiator_offer,created FROM exchanges WHERE status='open' ORDER BY id DESC LIMIT ?", (limit,))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"offer":r[2],"created":r[3]} for r in rows]

def exchange_get(eid):
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT * FROM exchanges WHERE id=?", (eid,))
    row = c.fetchone(); conn.close()
    return dict(row) if row else None

def exchange_cancel(eid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE exchanges SET status='cancelled' WHERE id=? AND initiator_uid=? AND status='open'", (eid, uid))
    changed = c.rowcount; conn.commit(); conn.close(); return changed > 0

# ═══ NFT ПРОВЕРКА ═══

async def check_fragment_nft(username):
    try:
        async with http_session.get(
            f"https://fragment.com/username/{username.lower()}",
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        ) as resp:
            if resp.status != 200: return {"is_nft": False, "status": "unknown"}
            text = await resp.text()
            if any(x in text for x in ["Sold", "sold", "Assigned", "assigned", "Owner", "owner",
                                        "Status:&nbsp;Taken", "tm-value-sold"]):
                return {"is_nft": True, "status": "sold"}
            elif any(x in text for x in ["Available", "Make an offer", "Bid", "auction",
                                          "Minimum bid", "Buy now"]):
                return {"is_nft": True, "status": "on_sale"}
            else:
                return {"is_nft": False, "status": "not_found"}
    except:
        return {"is_nft": False, "status": "error"}

async def verify_nft_owner(uid, username):
    try:
        chat = await bot.get_chat(uid)
        if chat.username and chat.username.lower() == username.lower():
            return True
    except: pass
    if pool.has_sessions():
        try:
            i, client = await pool._acquire(uid, 10)
            if client:
                try:
                    user = await client.get_entity(uid)
                    pool._ok(i)
                    if user.username and user.username.lower() == username.lower():
                        return True
                except:
                    pool._err(i)
        except: pass
    return False

# ═══════════════════════ HELPERS ═══════════════════════

def _d(uid_val, uname_val):
    return "@"+uname_val if uname_val else "ID:"+str(uid_val)

async def notify_admins(text, exclude=None, kb=None):
    for aid in ADMIN_IDS:
        if exclude and aid==exclude: continue
        try: await bot.send_message(aid, text, reply_markup=kb, parse_mode="HTML")
        except: pass

def build_sub_kb(channels):
    text = "📢 <b>Подпишитесь:</b>\n\n"
    kb = InlineKeyboardBuilder()
    for ch in channels:
        text += f"  ❌ @{ch}\n"
        kb.button(text=f"📢 @{ch}", url=f"https://t.me/{ch}")
    text += "\n✅ За подписку <b>+2 поиска!</b>"
    kb.button(text="✅ Проверить", callback_data="check_sub"); kb.adjust(1)
    return text, kb.as_markup()

# ═ ИЗМЕНЕНО: build_menu показывает VIP ═
def build_menu(uid):
    u = get_user(uid); ps = pool.stats()
    is_prem = has_subscription(uid); is_admin = uid in ADMIN_IDS
    is_vip_user = has_vip(uid)
    config = load_bot_config()
    
    if is_admin: si,st,sub_info = "👑","ADMIN","♾"
    elif is_vip_user:
        si,st = "🌟","VIP"
        sub_info = f"до {u.get('vip_end','?')}"
    elif is_prem:
        si,st = "💎","PREMIUM"
        sub_info = f"до {u.get('sub_end','?')}"
    else:
        total_free = u.get("free",0)+u.get("extra_searches",0)
        if total_free > 0:
            si,st = "🆓","FREE"
            sub_info = f"{total_free} поисков"
        else: si,st,sub_info = "⛔️","ЛИМИТ","закончились"
    
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    bal = u.get("balance",0.0); promos = get_active_promotions()
    sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
    
    custom_welcome = config.get("text_welcome","")
    
    text = (f"🔍 <b>USERNAME HUNTER</b> {si}\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>{st}</b> | 🎯 <code>{cnt}</code> юзов/поиск\n"
            f"📊 {sub_info}")
    
    if is_prem and not is_admin:
        used = u.get("daily_searches_used",0)
        limit = VIP_SEARCHES_LIMIT if is_vip_user else PREMIUM_SEARCHES_LIMIT
        text += f" | Сегодня: <code>{used}/{limit}</code>"
    
    text += (f"\n🔢 Всего поисков: <code>{u.get('searches',0)}</code>\n"
             f"🔄 Сессии: {sl}\n💰 Баланс: <code>{bal:.1f}</code> ⭐\n")
    
    if custom_welcome: text += f"\n{custom_welcome}\n"
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
    kb.button(text="🏪 Маркет", callback_data="cmd_market")
    if is_button_enabled("tiktok"): kb.button(text="🎁 TikTok", callback_data="cmd_tiktok")
    for pr in promos:
        kb.button(text=pr.get("button_text") or pr["name"], callback_data=f"pv_{pr['id']}")
    if is_button_enabled("support"): kb.button(text="🤖 Поддержать", callback_data="cmd_support")
    if is_admin: kb.button(text="👑 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    return text, kb.as_markup()

async def show_user_panel(msg_or_cb, target_uid):
    u = get_user(target_uid)
    is_prem = has_subscription(target_uid)
    is_ban = u.get("banned",0)==1
    is_vip_user = has_vip(target_uid)
    _reset_daily_if_needed(target_uid)
    
    if target_uid in ADMIN_IDS: status = "👑 ADMIN"
    elif is_ban: status = "🚫 BANNED"
    elif is_vip_user: status = "🌟 VIP"
    elif is_prem: status = "💎 PREMIUM"
    else: status = "🆓 FREE"
    
    daily_used = u.get("daily_searches_used",0)
    limit = VIP_SEARCHES_LIMIT if is_vip_user else PREMIUM_SEARCHES_LIMIT
    
    text = (
        f"👤 <b>Панель юзера</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <code>{target_uid}</code>\n"
        f"👤 @{u.get('uname','-') or '-'}\n"
        f"📌 {status}\n\n"
        f"🔍 Free поисков: <code>{u.get('free',0)}</code>\n"
        f"🔍 Extra поисков: <code>{u.get('extra_searches',0)}</code>\n"
        f"📊 Всего поисков: <code>{u.get('searches',0)}</code>\n"
        f"📅 Сегодня поисков: <code>{daily_used}</code>/{limit if is_prem else '-'}\n"
        f"💰 Баланс: <code>{u.get('balance',0):.1f}</code> ⭐\n"
        f"👥 Рефералов: <code>{u.get('ref_count',0)}</code>\n"
        f"💎 Подписка: <code>{u.get('sub_end','-') or '-'}</code>\n"
        f"🌟 VIP: <code>{u.get('vip_end','-') or '-'}</code>\n"
        f"📅 Рег: <code>{u.get('joined','-')}</code>\n"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 +поиски", callback_data=f"au_adds_{target_uid}")
    kb.button(text="🔍 =поиски", callback_data=f"au_sets_{target_uid}")
    kb.button(text="💰 =баланс", callback_data=f"au_setb_{target_uid}")
    kb.button(text="💎 +подписка", callback_data=f"au_addd_{target_uid}")
    kb.button(text="💎 Убрать", callback_data=f"au_remd_{target_uid}")
    kb.button(text="🌟 +VIP", callback_data=f"au_addv_{target_uid}")
    kb.button(text="🌟 Убрать VIP", callback_data=f"au_remv_{target_uid}")
    kb.button(text="🔄 Сброс дневных", callback_data=f"au_resetd_{target_uid}")
    if is_ban: kb.button(text="✅ Разбан", callback_data=f"au_unban_{target_uid}")
    else: kb.button(text="🚫 Бан", callback_data=f"au_ban_{target_uid}")
    kb.button(text="📜 История", callback_data=f"au_hist_{target_uid}")
    kb.button(text="👥 Рефералы", callback_data=f"au_refs_{target_uid}")
    kb.button(text="📤 Написать", callback_data=f"au_msg_{target_uid}")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    
    if hasattr(msg_or_cb, 'message'):
        await edit_msg(msg_or_cb.message, text, kb.as_markup())
    else:
        await msg_or_cb.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

# ═══ НОВОЕ: формат результатов со ссылками ═══
def format_results(found, stats, mode_name, config=None):
    """Формирует текст результатов со ссылками в одном сообщении"""
    config = config or load_bot_config()
    
    if found:
        custom_found = config.get("text_found","")
        text = custom_found + "\n\n" if custom_found else ""
        text += f"✅ <b>{mode_name} — найдено {len(found)}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, item in enumerate(found, 1):
            ev = evaluate_username(item["username"])
            fri = ""
            if item.get("fragment") == "fragment": fri = " 💎"
            elif item.get("fragment") == "sold": fri = " 🏷"
            
            text += (f"{i}. <code>@{item['username']}</code> — {ev['rarity']}{fri}\n"
                     f"   📱 <a href='https://t.me/{item['username']}'>Telegram</a>"
                     f" · 💎 <a href='https://fragment.com/username/{item['username']}'>Fragment</a>\n\n")
        text += f"📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"
    else:
        custom_empty = config.get("text_empty","")
        text = custom_empty if custom_empty else f"😔 <b>Не найдено</b>"
        text += f"\n\n📊 <code>{stats['attempts']}</code> ⏱ <code>{stats['elapsed']}с</code>"
    
    return text


# ═══════════════════════ КОМАНДЫ ═══════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, command: CommandObject):
    uid = msg.from_user.id; uname = msg.from_user.username or ""
    is_new = get_user(uid).get("searches",0)==0
    ensure_user(uid, uname); log_action(uid, "start", command.args or "")
    if is_banned(uid):
        rem = rate_limiter.get_ban_remaining(uid)
        if rem>0: await msg.answer(f"🚫 Блокировка. Подождите {rem} мин.")
        else: await msg.answer("🚫 Заблокированы.")
        return
    if command.args and command.args.startswith("ref_") and is_new:
        try:
            ref_id = int(command.args.replace("ref_",""))
            if ref_id!=uid:
                set_pending_ref(uid, ref_id)
                kb = InlineKeyboardBuilder(); kb.button(text="Не робот 🌟", callback_data="captcha_ok")
                await msg.answer("🤖 <b>Подтвердите:</b>", reply_markup=kb.as_markup(), parse_mode="HTML"); return
        except: pass
    ns = await check_subscribed(uid)
    if ns: t,k = build_sub_kb(ns)
    else: t,k = build_menu(uid)
    await msg.answer(t, reply_markup=k, parse_mode="HTML", disable_web_page_preview=True)

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    if is_banned(msg.from_user.id): return
    kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_menu")
    await msg.answer(f"📖 <b>v25.0</b>\n/check username\n/similar username\n/balance\n/id\n\n📩 @{ADMIN_CONTACT}",
                     reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.message(Command("id"))
async def cmd_id(msg: Message):
    await msg.answer(f"🆔 <code>{msg.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check_cmd(msg: Message, command: CommandObject):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    ok,reason = rate_limiter.check_action(uid)
    if not ok:
        await msg.answer("⚠️ Слишком быстро!" if reason!="ban" else f"🚫 Блокировка {TEMP_BAN_MINUTES} мин."); return
    un = (command.args or "").strip().replace("@","").lower()
    if not validate_username(un):
        await msg.answer("❌ <code>/check username</code>", parse_mode="HTML"); return
    log_action(uid,"check",un)
    wm = await msg.answer("⏳...")
    tg = await check_username(un)
    st = {"free":"✅ Свободен!","taken":"❌ Занят","error":"⚠️ Ошибка","skip":"⚠️"}.get(tg,"❓")
    try: await wm.delete()
    except: pass
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Оценка", callback_data=f"eval_{un}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(2)
    text = (f"🔍 <b>@{un}</b> — {st}\n\n"
            f"📱 <a href='https://t.me/{un}'>Открыть в Telegram</a>\n"
            f"💎 <a href='https://fragment.com/username/{un}'>Открыть на Fragment</a>")
    await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(Command("similar"))
async def cmd_similar_cmd(msg: Message, command: CommandObject):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        await msg.answer("🔒 Нужен Premium"); return
    un = (command.args or "").strip().replace("@","").lower()
    if not validate_username(un):
        await msg.answer("❌ <code>/similar username</code>", parse_mode="HTML"); return
    log_action(uid,"similar",un)
    wm = await msg.answer(f"🔄 Ищу похожие на @{un}...")
    found, stats = await do_similar_search(un, 5, wm, uid)
    kb = InlineKeyboardBuilder()
    text = format_results(found, stats, f"Похожие на @{un}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(wm, text, kb.as_markup())

@dp.message(Command("balance"))
async def cmd_balance(msg: Message):
    uid = msg.from_user.id; ensure_user(uid, msg.from_user.username)
    bal = get_balance(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="🏪 Магазин", callback_data="cmd_shop")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await msg.answer(f"💰 <b>Баланс:</b> <code>{bal:.1f}</code> ⭐", reply_markup=kb.as_markup(), parse_mode="HTML")

from aiogram.types import Message
from aiogram.filters import Command

@dp.message(Command("getdb"))
async def send_db(message: Message):
    # Проверка ID
    if message.from_user.id != 5969266721:
        return

    file_path = "hunter.db" # Проверьте, что файл лежит в папке с ботом

    # Проверяем, существует ли файл физически
    if not os.path.exists(file_path):
        await message.answer(f"❌ Файл {file_path} не найден!")
        return

    await message.answer("📦 Отправляю базу...")

    try:
        # В aiogram 3.x используем FSInputFile
        document = FSInputFile(path=file_path, filename="hunter.db")
        await message.answer_document(document, caption="Ваша база данных")
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке: {e}")
        
# ═══════════════════════ CALLBACKS: Базовые ═══════════════════════

@dp.callback_query(F.data == "captcha_ok")
async def cb_captcha(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ensure_user(uid, cb.from_user.username)
    ref_uid = get_pending_ref(uid)
    if ref_uid and ref_uid!=uid:
        ok = process_referral(uid, ref_uid)
        set_pending_ref(uid,0); set_captcha_passed(uid)
        if ok:
            log_action(uid,"referral_join",str(ref_uid))
            try: await bot.send_message(ref_uid, f"🎉 Новый реферал! <b>+{REF_BONUS}</b>", parse_mode="HTML")
            except: pass
    else: set_captcha_passed(uid)
    t,k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "check_sub")
async def cb_cs(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ns = await check_subscribed(uid)
    if ns: t,k = build_sub_kb(ns); await edit_msg(cb.message, t, k); return
    u = get_user(uid)
    if u.get("sub_bonus",0)==0:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET free=free+2,sub_bonus=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()
    t,k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "cmd_menu")
async def cb_menu(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    user_states.pop(uid, None)
    t,k = build_menu(uid); await edit_msg(cb.message, t, k)


# ═══════════════════════ CALLBACKS: Поиск ═══════════════════════

@dp.callback_query(F.data == "cmd_search")
async def cb_search(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    config = load_bot_config()
    ok,reason = rate_limiter.check_search(uid)
    if not ok:
        await edit_msg(cb.message, f"🚫 Блокировка {TEMP_BAN_MINUTES} мин." if reason=="ban" else "⚠️ Подождите."); return
    if not can_search(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="🏪 Магазин", callback_data="cmd_shop")
        kb.button(text="👥 Рефералы", callback_data="cmd_referral")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>", kb.as_markup()); return

    is_prem = uid in ADMIN_IDS or has_subscription(uid)
    is_vip_user = has_vip(uid)
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    fl = "♾" if uid in ADMIN_IDS else str(mx)

    kb = InlineKeyboardBuilder(); mt = ""
    for key, m in SEARCH_MODES.items():
        if m.get("disabled",False): continue
        if m["premium"] and not is_prem:
            kb.button(text=f"🔒 {m['emoji']} {m['name']}", callback_data="need_prem"); lk="🔒"
        else:
            kb.button(text=f"{m['emoji']} {m['name']}", callback_data=f"go_{key}"); lk="✅"
        mt += f"{lk} <b>{m['emoji']} {m['name']}</b> — {m['desc']}\n"

        # Тематический поиск — VIP
    if is_vip_user:
        kb.button(text="🎯 По слову (VIP)", callback_data="cmd_thematic")
        mt += "✅ <b>🎯 По слову</b> — VIP тематический поиск\n"
    else:
        kb.button(text="🔒🌟 По слову (VIP)", callback_data="need_vip")
        mt += "🔒 <b>🎯 По слову</b> — нужен VIP\n"

    if is_prem:
        kb.button(text="🎯 По шаблону", callback_data="cmd_template")
        kb.button(text="🔄 Похожие", callback_data="cmd_similar_cb")
        mt += "✅ <b>🎯 По шаблону</b>\n✅ <b>🔄 Похожие</b>\n"
    else:
        kb.button(text="🔒 🎯 Шаблон", callback_data="need_prem")
        kb.button(text="🔒 🔄 Похожие", callback_data="need_prem")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(2)
    await edit_msg(cb.message, f"🔍 <b>Режим:</b>\n\n{mt}\n🎯 <code>{cnt}</code> юзов | Осталось: <b>{fl}</b>", kb.as_markup())

@dp.callback_query(F.data == "need_prem")
async def cb_np(cb: CallbackQuery): await answer_cb(cb, "🔒 Нужен Premium!", show_alert=True)

@dp.callback_query(F.data == "need_vip")
async def cb_nv(cb: CallbackQuery): await answer_cb(cb, "🌟 Нужен VIP! Купи в магазине.", show_alert=True)

@dp.callback_query(F.data.startswith("go_"))
async def cb_go(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    ok,reason = rate_limiter.check_search(uid)
    if not ok: await edit_msg(cb.message, "⚠️ Подождите."); return
    if not can_search(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Закончились!</b>", kb.as_markup()); return
    mode = cb.data[3:]; mi = SEARCH_MODES.get(mode)
    if not mi or mi.get("disabled"): return
    is_prem = uid in ADMIN_IDS or has_subscription(uid)
    if mi["premium"] and not is_prem: return
    if uid not in ADMIN_IDS:
        if uid in searching_users:
            try: await bot.send_message(uid, "⏳ Уже идёт поиск!")
            except: pass; return
        cd = user_search_cooldown.get(uid,0); rem = SEARCH_COOLDOWN-(time.time()-cd)
        if rem>0:
            try: await bot.send_message(uid, f"⏳ {int(rem)} сек.")
            except: pass; return
    searching_users.add(uid)
    try:
        ps = pool.stats()
        sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
        await edit_msg(cb.message, f"🚀 <b>{mi['emoji']} {mi['name']}</b>\n\n🔄 {sl}\n⏳ Ищу...")
        use_search(uid); log_action(uid,"search",mode)
        count = get_search_count(uid)
        found, stats = await do_search(count, mi["func"], cb.message, mi["name"], uid)
        config = load_bot_config()
        kb = InlineKeyboardBuilder()
        text = format_results(found, stats, mi["name"], config)
        if can_search(uid): kb.button(text="🔄 Ещё", callback_data=cb.data)
        kb.button(text="🔍 Режимы", callback_data="cmd_search")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())
    finally:
        searching_users.discard(uid)
        if uid not in ADMIN_IDS: user_search_cooldown[uid]=time.time()

# ═══ НОВОЕ: Тематический поиск (VIP) ═══
@dp.callback_query(F.data == "cmd_thematic")
async def cb_thematic(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_vip(uid) and uid not in ADMIN_IDS:
        kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "🌟 <b>Нужен VIP!</b>\n\nТематический поиск доступен с VIP подпиской.", kb.as_markup()); return
    user_states[uid] = {"action":"thematic_search"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_search")
    await edit_msg(cb.message,
        "🎯 <b>Тематический поиск (VIP)</b>\n\n"
        "Введите любое слово или никнейм:\n\n"
        "Примеры:\n"
        "• <code>fluger</code> — вариации на fluger\n"
        "• <code>vortex</code> — vortex_x, voortex, xvortex...\n"
        "• <code>ninja</code> — mr_ninja, ninjax, n1nja...\n"
        "• <code>shadow</code> — sh4dow, shadow_go, shadw...\n\n"
        "💡 Бот создаст 100+ вариаций и проверит свободные!", kb.as_markup())

@dp.callback_query(F.data == "cmd_template")
async def cb_template(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        u = get_user(uid)
        if u.get("template_uses",0)<=0:
            kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
            await edit_msg(cb.message, "🔒 Нужен Premium или купите в магазине", kb.as_markup()); return
    user_states[uid] = {"action":"template_search"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_search")
    await edit_msg(cb.message, "🎯 <b>Шаблон</b>\n\n<code>*</code> = 2-4 символа\n<code>?</code> = 1 символ\n\nПример: <code>max_*</code>, <code>*_dev</code>", kb.as_markup())

@dp.callback_query(F.data == "cmd_similar_cb")
async def cb_similar_cb(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "🔒 Нужен Premium", kb.as_markup()); return
    user_states[uid] = {"action":"similar_search"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_search")
    await edit_msg(cb.message, "🔄 <b>Введите юзернейм:</b>", kb.as_markup())


# ═══════════════════════ CALLBACKS: Оценка / Утилиты ═══════════════════════

@dp.callback_query(F.data == "cmd_evaluate")
async def cb_eval(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action":"evaluate"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "📊 <b>Введите юзернейм:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("eval_"))
async def cb_eval_direct(cb: CallbackQuery):
    await answer_cb(cb); un = cb.data[5:]
    await edit_msg(cb.message, "⏳...")
    tg = await check_username(un); fr = await check_fragment(un)
    tgs = {"free":"✅ Свободен","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
    frs = {"fragment":"💎 Fragment","sold":"✅ Продан","unavailable":"—"}.get(fr,"❓")
    ev = evaluate_username(un)
    fac = "\n".join("  "+f for f in ev["factors"]) or "  —"
    kb = InlineKeyboardBuilder()
    if tg=="free": kb.button(text="👁 Мониторинг", callback_data=f"mon_add_{un}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    text = (f"📊 <b>@{un}</b>\n\n📱 {tgs}\n💎 {frs}\n\n"
            f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
            f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}\n\n"
            f"📱 <a href='https://t.me/{un}'>Telegram</a> · "
            f"💎 <a href='https://fragment.com/username/{un}'>Fragment</a>")
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "cmd_utils")
async def cb_utils(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверка", callback_data="util_check")
    kb.button(text="📋 Массовая", callback_data="util_mass")
    kb.button(text="📜 История", callback_data="util_hist")
    if is_button_enabled("monitor"): kb.button(text="👁 Мониторинг", callback_data="cmd_monitors")
    kb.button(text="📥 Экспорт", callback_data="util_export")
    kb.button(text="🗑 Удалить по шаблону", callback_data="util_delete_pattern")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(2)
    await edit_msg(cb.message, "🔧 <b>Утилиты</b>", kb.as_markup())

@dp.callback_query(F.data == "util_check")
async def cb_uc(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action":"quick_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "🔍 <b>Юзернейм:</b>", kb.as_markup())

@dp.callback_query(F.data == "util_mass")
async def cb_um(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action":"mass_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "📋 <b>По строке (макс 20):</b>", kb.as_markup())

@dp.callback_query(F.data == "util_hist")
async def cb_uh(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; hist = get_history(uid)
    kb = InlineKeyboardBuilder()
    text = f"📜 <b>({len(hist)})</b>\n\n" if hist else "📜 Пусто"
    for h in hist[:15]: text += f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
    kb.button(text="📥 TXT", callback_data="util_export")
    kb.button(text="🗑 Удалить", callback_data="util_delete_pattern")
    kb.button(text="🔙", callback_data="cmd_utils"); kb.adjust(2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "util_export")
async def cb_ue(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; hist = get_history(uid,100)
    if not hist: return
    content = "ИСТОРИЯ\n\n"
    for i,h in enumerate(hist,1): content += f"{i}. @{h[0]} | {h[2]} | {h[1]}\n"
    await bot.send_document(uid, BufferedInputFile(content.encode(), filename=f"history_{uid}.txt"), caption="📥")

# ═══ НОВОЕ: Удаление по шаблону ═══
@dp.callback_query(F.data == "util_delete_pattern")
async def cb_del_pat(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action":"delete_pattern"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message,
        "🗑 <b>Удаление по шаблону</b>\n\n"
        "Введите часть юзернейма для удаления из истории:\n\n"
        "Примеры:\n"
        "• <code>craft</code> — удалит все с 'craft'\n"
        "• <code>_pro</code> — удалит все с '_pro'\n"
        "• <code>tg</code> — удалит все с 'tg'", kb.as_markup())

# ═══════════════════════ МАРКЕТПЛЕЙС ХЕНДЛЕРЫ ═══════════════════════

@dp.callback_query(F.data == "cmd_market")
async def cb_market(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    lots = market_get_active_lots(5)
    nft_lots = market_get_active_lots(3, nft_only=True)
    rating = get_user_rating(uid)
    bal = get_balance(uid)

    text = (f"🏪 <b>Маркет</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Баланс: <code>{bal:.0f}⭐</code>\n"
            f"⭐ Рейтинг: {'⭐'*int(rating['avg'])+'☆'*(5-int(rating['avg']))} ({rating['count']})\n"
            f"📊 Комиссия: {int(MARKET_COMMISSION*100)}%\n")

    if lots:
        text += "\n<b>📦 Последние лоты:</b>\n"
        for lot in lots:
            promo = "🔥 " if lot["promoted"] else ""
            text += f"{promo}<b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"

    if nft_lots:
        text += "\n<b>💎 NFT юзернеймы:</b>\n"
        for lot in nft_lots:
            text += f"💎 <b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"

    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Все лоты", callback_data="market_browse")
    kb.button(text="💎 NFT юзы", callback_data="market_nft")
    kb.button(text="➕ Продать", callback_data="market_sell")
    kb.button(text="📦 Мои лоты", callback_data="market_my")
    kb.button(text="🛒 Мои покупки", callback_data="market_my_purchases")
    kb.button(text="🔄 Обменник", callback_data="market_exchange")
    kb.button(text="🎡 Колесо", callback_data="market_wheel")
    kb.button(text="📦 Лутбокс", callback_data="market_lootbox")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(2)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "market_browse")
async def cb_market_browse(cb: CallbackQuery):
    await answer_cb(cb)
    lots = market_get_active_lots(20)
    text = f"📋 <b>Лоты ({len(lots)})</b>\n\n"
    kb = InlineKeyboardBuilder()
    for lot in lots:
        sr = get_user_rating(lot["seller"])
        promo = "🔥 " if lot["promoted"] else ""
        stars = f"⭐{sr['avg']}" if sr['avg'] else "🆕"
        text += f"{promo}<b>{lot['title']}</b> — <code>{lot['price']}⭐</code> ({stars})\n"
        kb.button(text=f"🛒 {lot['title'][:20]} ({lot['price']}⭐)", callback_data=f"mlot_{lot['id']}")
    if not lots: text += "<i>Пусто</i>"
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "market_nft")
async def cb_market_nft(cb: CallbackQuery):
    await answer_cb(cb)
    lots = market_get_active_lots(20, nft_only=True)
    text = (f"💎 <b>NFT Юзернеймы</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Юзернеймы купленные на Fragment.\n"
            f"Размещение: <code>{MARKET_NFT_LISTING_FEE}⭐</code>\n\n")
    kb = InlineKeyboardBuilder()
    for lot in lots:
        promo = "🔥 " if lot["promoted"] else ""
        text += f"{promo}💎 <b>{lot['title']}</b> — <code>{lot['price']}⭐</code>\n"
        if lot.get("fragment_url"): text += f"   🔗 <a href='{lot['fragment_url']}'>Fragment</a>\n"
        text += "\n"
        kb.button(text=f"💎 {lot['title'][:20]} ({lot['price']}⭐)", callback_data=f"mlot_{lot['id']}")
    if not lots: text += "<i>Нет NFT лотов</i>"
    kb.button(text="➕ Продать NFT", callback_data="market_sell_nft")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("mlot_"))
async def cb_mlot(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[5:])
    lot = market_get_lot(lot_id)
    if not lot or lot["status"] != "active":
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="market_browse")
        await edit_msg(cb.message, "❌ Лот не найден", kb.as_markup()); return

    seller = get_user(lot["seller_uid"])
    sr = get_user_rating(lot["seller_uid"])
    name = f"@{seller.get('uname','')}" if seller.get('uname') else f"ID:{lot['seller_uid']}"
    stars_d = "⭐"*int(sr['avg'])+"☆"*(5-int(sr['avg'])) if sr['avg'] else "🆕"
    nft_badge = "💎 NFT | " if lot.get("is_nft") else ""
    promo_badge = "🔥 ПРОМО | " if lot.get("promoted") else ""
    type_names = {"username":"👤 Юзернейм","service":"🔧 Услуга","premium":"💎 Premium","other":"📦 Другое","nft":"💎 NFT"}

    text = (f"📦 <b>Лот #{lot_id}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{promo_badge}{nft_badge}{type_names.get(lot['mtype'],'📦')}\n"
            f"🏷 <b>{lot['title']}</b>\n"
            f"📝 {lot['description']}\n\n"
            f"💰 Цена: <code>{lot['price']}⭐</code> ({int(lot['price']*STAR_TO_RUB)}₽)\n\n"
            f"👤 {name}\n"
            f"⭐ {stars_d} ({sr['count']} отзывов)\n"
            f"📅 {lot['created']}")

    if lot.get("fragment_url"):
        text += f"\n🔗 <a href='{lot['fragment_url']}'>Fragment</a>"

    kb = InlineKeyboardBuilder()
    if lot["seller_uid"] != uid:
        kb.button(text=f"🛒 Купить за {lot['price']}⭐", callback_data=f"mbuy_{lot_id}")
        kb.button(text="🏷 Есть промокод", callback_data=f"mpromo_{lot_id}")
    else:
        kb.button(text=f"🔥 Продвинуть ({MARKET_PROMOTE_PRICE}⭐)", callback_data=f"mpromote_{lot_id}")
        kb.button(text="❌ Снять", callback_data=f"mcancel_{lot_id}")
    kb.button(text="👤 Отзывы", callback_data=f"mrev_{lot['seller_uid']}")
    back = "market_nft" if lot.get("is_nft") else "market_browse"
    kb.button(text="🔙", callback_data=back); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("mbuy_"))
async def cb_mbuy(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[5:])
    lot = market_get_lot(lot_id)
    if not lot or lot["status"] != "active":
        await answer_cb(cb, "❌ Лот уже продан!", show_alert=True); return
    if lot["seller_uid"] == uid:
        await answer_cb(cb, "❌ Нельзя купить свой лот!", show_alert=True); return
    
    bal = get_balance(uid)
    price = lot["price"]
    rub = int(price * STAR_TO_RUB)
    
    kb = InlineKeyboardBuilder()
    if bal >= price:
        kb.button(text=f"💰 С баланса ({price}⭐)", callback_data=f"paybal_lot_{lot_id}")
    kb.button(text=f"⭐ Telegram Stars ({price}⭐)", callback_data=f"paystars_lot_{lot_id}")
    kb.button(text="❌ Отмена", callback_data=f"mlot_{lot_id}")
    kb.adjust(1)
    
    await edit_msg(cb.message,
        f"🛒 <b>Покупка лота #{lot_id}</b>\n\n"
        f"📦 {lot['title']}\n"
        f"💰 Цена: <code>{price}⭐</code> (<code>{rub}₽</code>)\n\n"
        f"💰 Ваш баланс: <code>{bal:.1f}⭐</code>\n\n"
        f"Выберите способ оплаты:", kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_lot_"))
async def cb_paybal_lot(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[11:])
    lot = market_get_lot(lot_id)
    if not lot or lot["status"] != "active":
        await answer_cb(cb, "❌ Лот продан!", show_alert=True); return
    if lot["seller_uid"] == uid:
        await answer_cb(cb, "❌ Свой лот!", show_alert=True); return
    
    price = lot["price"]
    bal = get_balance(uid)
    if bal < price:
        await answer_cb(cb, f"❌ Нужно {price}⭐, у вас {bal:.1f}⭐", show_alert=True); return
    
    # Списываем с баланса
    set_balance(uid, bal - price)
    
    # Переводим в эскроу
    ok = market_buy_lot(lot_id, uid)
    if not ok:
        add_balance(uid, price)  # возврат
        await answer_cb(cb, "❌ Ошибка покупки!", show_alert=True); return
    
    log_action(uid, "market_buy_bal", f"lot={lot_id} paid={price}")
    display = f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    
    # Кнопки покупателю
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Получил товар", callback_data=f"mbuyerok_{lot_id}")
    kb.button(text="⚠️ Открыть спор", callback_data=f"mdispute_{lot_id}")
    kb.adjust(1)
    await edit_msg(cb.message,
        f"✅ <b>Оплачено с баланса!</b>\n\n"
        f"📦 {lot['title']}\n"
        f"💰 Списано: {price}⭐\n"
        f"💰 Остаток: {bal-price:.1f}⭐\n\n"
        f"🔒 Деньги на эскроу\n"
        f"⏰ Срок: {MARKET_ESCROW_HOURS} часов\n\n"
        f"Когда получите товар — нажмите <b>✅ Получил товар</b>",
        kb.as_markup())
    
    # Уведомление продавцу
    skb = InlineKeyboardBuilder()
    skb.button(text="✅ Я передал товар", callback_data=f"msellerok_{lot_id}")
    try: await bot.send_message(lot["seller_uid"],
        f"🛒 <b>Ваш лот куплен!</b>\n\n"
        f"📦 {lot['title']}\n"
        f"💰 {price}⭐\n"
        f"👤 Покупатель: {display}\n\n"
        f"⏰ Передайте товар в течение {MARKET_ESCROW_HOURS} часов",
        reply_markup=skb.as_markup(), parse_mode="HTML")
    except: pass
    
    # Уведомление админу
    commission = int(price * MARKET_COMMISSION)
    payout = price - commission
    await notify_admins(
        f"🛒 <b>ПРОДАЖА (баланс)</b>\n"
        f"📦 {lot['title']}\n"
        f"💰 {price}⭐ | Комиссия: {commission}⭐\n"
        f"👤 Покупатель: {display}\n"
        f"👤 Продавец: <code>{lot['seller_uid']}</code>")

@dp.callback_query(F.data.startswith("paystars_lot_"))
async def cb_paystars_lot(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[13:])
    lot = market_get_lot(lot_id)
    if not lot or lot["status"] != "active":
        await answer_cb(cb, "❌ Лот продан!", show_alert=True); return
    if lot["seller_uid"] == uid:
        await answer_cb(cb, "❌ Свой лот!", show_alert=True); return
    
    await bot.send_invoice(uid,
        title=f"🛒 {lot['title'][:50]}",
        description=f"Маркет: {lot['description'][:100]}" if lot['description'] else f"Маркет: {lot['title']}",
        payload=f"market_{lot_id}_{uid}_0",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=lot["title"][:50], amount=lot["price"])])

@dp.callback_query(F.data.startswith("mpromo_"))
async def cb_mpromo(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[7:])
    user_states[uid] = {"action":"market_enter_promo","lot_id":lot_id}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"mlot_{lot_id}")
    await edit_msg(cb.message, "🏷 <b>Введите промокод:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("mpromote_"))
async def cb_mpromote(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[9:])
    lot = market_get_lot(lot_id)
    if not lot or lot["seller_uid"] != uid: return
    await bot.send_invoice(uid,
        title=f"🔥 Продвижение лота #{lot_id}",
        description=f"Лот будет наверху списка 24 часа",
        payload=f"promote_{lot_id}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="Продвижение 24ч", amount=MARKET_PROMOTE_PRICE)])

@dp.callback_query(F.data.startswith("mcancel_"))
async def cb_mcancel(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[8:])
    market_cancel_lot(lot_id, uid); log_action(uid, "market_cancel", str(lot_id))
    await cb_market(cb)

@dp.callback_query(F.data.startswith("msellerok_"))
async def cb_msellerok(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[10:]); lot = market_get_lot(lot_id)
    if not lot or lot["seller_uid"] != uid: return
    completed = market_confirm_seller(lot_id)
    if completed:
        payout = int(lot["price"] * (1 - MARKET_COMMISSION))
        await edit_msg(cb.message, f"✅ <b>Сделка завершена!</b>\n+{payout}⭐ на баланс")
        try: await bot.send_message(lot["buyer_uid"], "✅ Сделка завершена!", parse_mode="HTML")
        except: pass
    else:
        await edit_msg(cb.message, "✅ Подтверждено! Ждём покупателя")

@dp.callback_query(F.data.startswith("mbuyerok_"))
async def cb_mbuyerok(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[9:]); lot = market_get_lot(lot_id)
    if not lot or lot["buyer_uid"] != uid: return
    completed = market_confirm_buyer(lot_id)
    if completed:
        await edit_msg(cb.message, "✅ <b>Сделка завершена!</b>\nСпасибо за покупку!")
        payout = int(lot["price"] * (1 - MARKET_COMMISSION))
        try: await bot.send_message(lot["seller_uid"], f"✅ Сделка завершена!\n💰 +{payout}⭐", parse_mode="HTML")
        except: pass
    else:
        await edit_msg(cb.message, "✅ Подтверждено! Ждём продавца")

@dp.callback_query(F.data.startswith("mdispute_"))
async def cb_mdispute(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lot_id = int(cb.data[9:])
    user_states[uid] = {"action":"market_dispute_reason","lot_id":lot_id}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="market_my_purchases")
    await edit_msg(cb.message, "⚠️ <b>Причина спора:</b>", kb.as_markup())

@dp.callback_query(F.data == "market_sell")
async def cb_market_sell(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    max_lots = market_get_max_lots(uid)
    current = market_count_user_lots(uid)
    text = (f"➕ <b>Продать</b>\n\n"
            f"📦 Лотов: {current}/{max_lots}\n"
            f"💰 Размещение: {MARKET_LISTING_FEE}⭐\n"
            f"📊 Комиссия: {int(MARKET_COMMISSION*100)}%\n")
    kb = InlineKeyboardBuilder()
    if current >= max_lots:
        text += f"\n❌ Лимит! Купите доп слот ({MARKET_EXTRA_SLOT_PRICE}⭐)"
        kb.button(text=f"📦 +1 слот ({MARKET_EXTRA_SLOT_PRICE}⭐)", callback_data="market_buy_slot")
    else:
        kb.button(text="👤 Юзернейм", callback_data="msell_username")
        kb.button(text="💎 Premium", callback_data="msell_premium")
        kb.button(text="🔧 Услуга", callback_data="msell_service")
        kb.button(text="📦 Другое", callback_data="msell_other")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(2,2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "market_sell_nft")
async def cb_sell_nft(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    user_states[uid] = {"action":"msell_nft_title"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="market_nft")
    await edit_msg(cb.message,
        f"💎 <b>Продать NFT юзернейм</b>\n\n"
        f"💰 Размещение: <code>{MARKET_NFT_LISTING_FEE}⭐</code>\n\n"
        f"Введите @юзернейм:", kb.as_markup())

@dp.callback_query(F.data.startswith("msell_"))
async def cb_msell_type(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    mtype = cb.data[6:]
    user_states[uid] = {"action":"msell_title","mtype":mtype}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="market_sell")
    await edit_msg(cb.message, "📝 <b>Название лота:</b>", kb.as_markup())

@dp.callback_query(F.data == "market_buy_slot")
async def cb_buy_slot(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    await bot.send_invoice(uid,
        title="📦 +1 слот маркета",
        description="Дополнительный слот для размещения",
        payload=f"mslot_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="+1 слот", amount=MARKET_EXTRA_SLOT_PRICE)])

@dp.callback_query(F.data == "market_my")
async def cb_market_my(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    lots = market_get_user_lots(uid)
    max_lots = market_get_max_lots(uid)
    text = f"📦 <b>Мои лоты ({len(lots)}/{max_lots})</b>\n\n"
    kb = InlineKeyboardBuilder()
    for lot in lots:
        st = {"pending":"⏳","active":"✅","escrow":"🔒"}.get(lot["status"],"❓")
        nft = "💎" if lot["is_nft"] else ""
        promo = "🔥" if lot["promoted"] else ""
        text += f"{st}{nft}{promo} <b>{lot['title']}</b> — {lot['price']}⭐\n"
        if lot["status"] == "escrow":
            kb.button(text=f"✅ Передал #{lot['id']}", callback_data=f"msellerok_{lot['id']}")
        elif lot["status"] in ("pending","active"):
            kb.button(text=f"❌ #{lot['id']}", callback_data=f"mcancel_{lot['id']}")
    if not lots: text += "<i>Нет</i>"
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "market_my_purchases")
async def cb_market_purchases(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,title,price,status,buyer_confirmed FROM market WHERE buyer_uid=? AND status IN ('escrow','completed','dispute') ORDER BY id DESC LIMIT 20", (uid,))
    rows = c.fetchall(); conn.close()
    text = f"🛒 <b>Мои покупки</b>\n\n"
    kb = InlineKeyboardBuilder()
    for r in rows:
        st = {"escrow":"🔒","completed":"✅","dispute":"⚠️"}.get(r[3],"❓")
        text += f"{st} <b>{r[1]}</b> — {r[2]}⭐\n"
        if r[3] == "escrow" and not r[4]:
            kb.button(text=f"✅ Получил #{r[0]}", callback_data=f"mbuyerok_{r[0]}")
            kb.button(text=f"⚠️ Спор #{r[0]}", callback_data=f"mdispute_{r[0]}")
    if not rows: text += "<i>Нет</i>"
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("mrev_"))
async def cb_mrev(cb: CallbackQuery):
    uid_target = int(cb.data[5:]); await answer_cb(cb)
    reviews = get_user_reviews(uid_target)
    rating = get_user_rating(uid_target)
    u = get_user(uid_target)
    name = f"@{u.get('uname','')}" if u.get('uname') else f"ID:{uid_target}"
    text = f"⭐ <b>{name}</b>\n{'⭐'*int(rating['avg'])+'☆'*(5-int(rating['avg']))} ({rating['avg']}/5, {rating['count']})\n\n"
    for r in reviews:
        fn = f"@{get_user(r['from']).get('uname','')}" if get_user(r['from']).get('uname') else f"ID:{r['from']}"
        text += f"{'⭐'*r['rating']} {fn}\n{r['text']}\n\n"
    if not reviews: text += "<i>Нет отзывов</i>"
    kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_market"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

# ═══ КОЛЕСО ═══

@dp.callback_query(F.data == "market_wheel")
async def cb_wheel(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    free_used = wheel_free_spins_today(uid)
    free_left = max(0, WHEEL_FREE_DAILY - free_used)
    text = (f"🎡 <b>Колесо фортуны</b>\n\n"
            f"🎟 Бесплатных: <code>{free_left}</code>\n"
            f"💰 Доп крутка: <code>{WHEEL_EXTRA_PRICE}⭐</code>\n\n"
            f"🎁 Призы: звёзды, поиски, Premium, VIP!")
    kb = InlineKeyboardBuilder()
    if free_left > 0:
        kb.button(text="🎡 Крутить бесплатно!", callback_data="wheel_spin_free")
    kb.button(text=f"🎡 Крутить за {WHEEL_EXTRA_PRICE}⭐", callback_data="wheel_spin_paid")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "wheel_spin_free")
async def cb_wheel_free(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if wheel_free_spins_today(uid) >= WHEEL_FREE_DAILY:
        await answer_cb(cb, "❌ Бесплатные кончились!", show_alert=True); return
    for e in ["🎡","🔍","🎰","🌟","✨","🎯"]:
        await edit_msg(cb.message, f"{e} Крутим...")
        await asyncio.sleep(0.3)
    prize, ptype = wheel_spin(uid)
    log_action(uid, "wheel_free", prize)
    kb = InlineKeyboardBuilder()
    kb.button(text="🎡 Ещё!", callback_data="market_wheel")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, f"🎉 <b>{prize}</b>", kb.as_markup())

@dp.callback_query(F.data == "wheel_spin_paid")
async def cb_wheel_paid(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    await bot.send_invoice(uid,
        title="🎡 Колесо фортуны",
        description="Крутка колеса — выиграй звёзды и Premium!",
        payload=f"wheel_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="Крутка", amount=WHEEL_EXTRA_PRICE)])

# ═══ ЛУТБОКС ═══

@dp.callback_query(F.data == "market_lootbox")
async def cb_lootbox(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    can = lootbox_can_open(uid)
    text = (f"📦 <b>Лутбокс</b>\n\n"
            f"💰 Цена: <code>{LOOTBOX_PRICE}⭐</code>\n"
            f"⏰ КД: 1 час\n\n"
            f"🎁 Возможные призы:\n"
            f"  💎 Premium (5%)\n  🌟 VIP (10%)\n  ⭐ Звёзды (20%)\n"
            f"  🔍 Поиски (25%)\n  📦 Слот маркета (20%)\n  🧸 Сувенир (20%)")
    kb = InlineKeyboardBuilder()
    if can:
        kb.button(text=f"📦 Открыть ({LOOTBOX_PRICE}⭐)", callback_data="lootbox_open")
    else:
        kb.button(text="⏰ Подождите...", callback_data="cmd_market")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "lootbox_open")
async def cb_lootbox_open(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not lootbox_can_open(uid):
        await answer_cb(cb, "⏰ Подождите!", show_alert=True); return
    await bot.send_invoice(uid,
        title="📦 Лутбокс",
        description="Случайный приз: Premium, VIP, звёзды...",
        payload=f"lootbox_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label="Лутбокс", amount=LOOTBOX_PRICE)])

# ═══ ОБМЕННИК ═══

@dp.callback_query(F.data == "market_exchange")
async def cb_exchange(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    exs = exchange_get_open(10)
    text = f"🔄 <b>Обменник:</b>\n\n"
    kb = InlineKeyboardBuilder()
    for ex in exs:
        u = get_user(ex["uid"])
        name = f"@{u.get('uname','')}" if u.get('uname') else f"ID:{ex['uid']}"
        text += f"🔄 #{ex['id']} {name}: <b>{ex['offer']}</b>\n"
        if ex["uid"] != uid:
            kb.button(text=f"🔄 #{ex['id']}", callback_data=f"exview_{ex['id']}")
    if not exs: text += "<i>Нет</i>\n"
    kb.button(text="➕ Создать", callback_data="exchange_new")
    kb.button(text="🔙 Маркет", callback_data="cmd_market")
    kb.adjust(2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "exchange_new")
async def cb_ex_new(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    user_states[uid] = {"action":"exchange_offer"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="market_exchange")
    await edit_msg(cb.message, "🔄 <b>Что отдаёте?</b>\n\nНапример: <code>@myusername</code>", kb.as_markup())

@dp.callback_query(F.data.startswith("exview_"))
async def cb_exview(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    eid = int(cb.data[7:]); ex = exchange_get(eid)
    if not ex or ex["status"] != "open":
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="market_exchange")
        await edit_msg(cb.message, "❌ Обмен закрыт", kb.as_markup()); return
    u = get_user(ex["initiator_uid"])
    name = f"@{u.get('uname','')}" if u.get('uname') else f"ID:{ex['initiator_uid']}"
    text = f"🔄 <b>#{eid}</b>\n\n👤 {name}\n📦 <b>{ex['initiator_offer']}</b>"
    kb = InlineKeyboardBuilder()
    if ex["initiator_uid"] != uid:
        kb.button(text="🔄 Предложить обмен", callback_data=f"exaccept_{eid}")
    kb.button(text="🔙", callback_data="market_exchange")
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("exaccept_"))
async def cb_exaccept(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    eid = int(cb.data[9:])
    user_states[uid] = {"action":"exchange_counter","eid":eid}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"exview_{eid}")
    await edit_msg(cb.message, "🔄 <b>Что предлагаете взамен?</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("exconfirm_"))
async def cb_exconfirm(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    eid = int(cb.data[10:])
    completed = exchange_confirm(eid, uid)
    if completed:
        ex = exchange_get(eid)
        await edit_msg(cb.message, f"✅ <b>Обмен #{eid} завершён!</b>")
        other = ex["partner_uid"] if uid == ex["initiator_uid"] else ex["initiator_uid"]
        try: await bot.send_message(other, f"✅ Обмен #{eid} завершён!", parse_mode="HTML")
        except: pass
    else:
        await edit_msg(cb.message, "✅ Подтверждено! Ждём другую сторону")


# ═══════════════════════ CALLBACKS: Мониторинг ═══════════════════════

@dp.callback_query(F.data == "cmd_monitors")
async def cb_monitors(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    mons = get_user_monitors(uid); limit = get_monitor_limit(uid)
    kb = InlineKeyboardBuilder()
    text = f"👁 <b>Мониторинг</b>\n\n📊 <code>{len(mons)}/{limit}</code>\n\n"
    if mons:
        for m in mons:
            si = "✅" if m["last_status"]=="free" else "❌"
            text += f"{si} <code>@{m['username']}</code> до {m['expires'][:10]}\n"
            kb.button(text=f"❌ {m['username']}", callback_data=f"mon_del_{m['id']}")
    else: text += "<i>Пусто</i>\n"
    text += "\n💡 Проверяет каждые 30 мин"
    kb.button(text="➕ Добавить", callback_data="mon_add_new")
    kb.button(text="🔙", callback_data="cmd_utils"); kb.adjust(2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "mon_add_new")
async def cb_mon_new(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if get_monitor_count(uid)>=get_monitor_limit(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_monitors"); kb.adjust(1)
        await edit_msg(cb.message, "❌ Лимит слотов", kb.as_markup()); return
    user_states[uid] = {"action":"monitor_add"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_monitors")
    await edit_msg(cb.message, "👁 <b>Введите занятый юзернейм:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("mon_add_"))
async def cb_mon_add(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    un = cb.data[8:]
    if un=="new": return
    if get_monitor_count(uid)>=get_monitor_limit(uid):
        await answer_cb(cb,"❌ Лимит",show_alert=True); return
    mid = add_monitor(uid,un); log_action(uid,"monitor_add",un)
    kb = InlineKeyboardBuilder(); kb.button(text="👁 Мои", callback_data="cmd_monitors"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, f"✅ @{un} на мониторинге", kb.as_markup())

@dp.callback_query(F.data.startswith("mon_del_"))
async def cb_mon_del(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    remove_monitor(int(cb.data[8:]),uid); await cb_monitors(cb)


# ═══════════════════════ CALLBACKS: Магазин ═══════════════════════

@dp.callback_query(F.data == "cmd_shop")
async def cb_shop(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    u = get_user(uid); extra = u.get("extra_searches",0)
    bal = get_balance(uid)
    
    text = (f"🏪 <b>Магазин</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Баланс: <code>{bal:.1f}⭐</code>\n"
            f"🔍 Доп. поисков: <code>{extra}</code>\n\n"
            f"💎 <b>Premium:</b>\n")
    
    for p in PRICES.values():
        text += f"• {p['label']} — <code>{p['stars']}⭐</code>/<code>{p['rub']}₽</code>\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Купить поиски", callback_data="shop_buy_searches")
    kb.button(text="💎 Premium", callback_data="shop_premium")
    kb.button(text="🌟 VIP", callback_data="shop_vip")
    kb.button(text="📦 Бандл Premium+VIP", callback_data="shop_bundle")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "shop_buy_searches")
async def cb_shop_buy(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bal = get_balance(uid)
    
    text = (f"🔍 <b>Купить поиски</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Цена: <code>{SEARCH_PRICE_STARS}⭐</code>/<code>{int(SEARCH_PRICE_STARS*STAR_TO_RUB)}₽</code> за 1 поиск\n\n"
            f"💰 Баланс: <code>{bal:.1f}⭐</code>\n"
            f"💳 Рубли — @{PAY_CONTACT}\n\n"
            f"Введите количество (1-1000):")
    
    user_states[uid] = {"action":"shop_custom_amount"}
    kb = InlineKeyboardBuilder()
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})", url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="❌ Отмена", callback_data="cmd_shop")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "shop_premium")
async def cb_shop_prem(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bal = get_balance(uid)
    
    text = (f"💎 <b>Premium</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Преимущества:\n\n"
            f"• {PREMIUM_COUNT} юзов/поиск\n"
            f"• {PREMIUM_SEARCHES_LIMIT} поисков/день\n"
            f"• Все режимы\n"
            f"• Шаблон + Похожие\n"
            f"• Мониторинг {MONITOR_MAX_PREMIUM} юзов\n"
            f"• Рулетка\n\n"
            f"<b>Цены:</b>\n\n")
    
    for p in PRICES.values():
        text += f"• {p['label']} — <code>{p['stars']}⭐</code>/<code>{p['rub']}₽</code>\n"
    
    text += f"\n💰 Баланс: <code>{bal:.1f}⭐</code>\n💳 Рубли — @{PAY_CONTACT}"
    
    kb = InlineKeyboardBuilder()
    for k,p in PRICES.items():
        kb.button(text=f"{p['label']} — {p['stars']}⭐/{p['rub']}₽", callback_data=f"buy_{k}")
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})", url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="🔙 Магазин", callback_data="cmd_shop")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

# ═══ НОВОЕ: VIP магазин ═══
@dp.callback_query(F.data == "shop_vip")
async def cb_shop_vip(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    is_prem = has_subscription(uid)
    bal = get_balance(uid)
    
    text = (f"🌟 <b>VIP</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Преимущества:\n\n"
            f"• {VIP_COUNT} юзов/поиск (вместо {PREMIUM_COUNT})\n"
            f"• {VIP_SEARCHES_LIMIT} поисков/день (вместо {PREMIUM_SEARCHES_LIMIT})\n"
            f"• 🎯 Тематический поиск по слову\n\n")
    
    if not is_prem and uid not in ADMIN_IDS:
        text += "⚠️ <b>Сначала нужен Premium!</b>\nИли купите бандл Premium+VIP.\n"
        kb = InlineKeyboardBuilder()
        kb.button(text="💎 Купить Premium", callback_data="shop_premium")
        kb.button(text="📦 Бандл", callback_data="shop_bundle")
        kb.button(text="🔙 Магазин", callback_data="cmd_shop")
        kb.adjust(1)
    else:
        text += f"<b>Цены апгрейда:</b>\n\n"
        for k, vp in VIP_PRICES.items():
            rub = int(vp['stars'] * STAR_TO_RUB)
            text += f"• {vp['label']} — <code>{vp['stars']}⭐</code>/<code>{rub}₽</code>\n"
        text += f"\n💰 Баланс: <code>{bal:.1f}⭐</code>\n💳 Рубли — @{PAY_CONTACT}"
        kb = InlineKeyboardBuilder()
        for k, vp in VIP_PRICES.items():
            rub = int(vp['stars'] * STAR_TO_RUB)
            kb.button(text=f"{vp['label']} — {vp['stars']}⭐/{rub}₽", callback_data=f"buyvip_{k}")
        kb.button(text=f"💳 Рубли (@{PAY_CONTACT})", url=f"https://t.me/{PAY_CONTACT}")
        kb.button(text="🔙 Магазин", callback_data="cmd_shop")
        kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())

# ═══ НОВОЕ: Бандл Premium+VIP ═══
@dp.callback_query(F.data == "shop_bundle")
async def cb_shop_bundle(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bal = get_balance(uid)
    
    text = (f"📦 <b>Бандл Premium+VIP</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💎 Premium + 🌟 VIP в одном!\n"
            f"<b>Скидка 5%</b> от суммарной цены\n\n"
            f"<b>Цены:</b>\n\n")
    
    for k, bp in BUNDLE_PRICES.items():
        p = PRICES[k]; vp = VIP_PRICES[k]
        full = p['stars'] + vp['stars']
        rub = int(bp['stars'] * STAR_TO_RUB)
        text += f"• {bp['label']} — <code>{bp['stars']}⭐</code>/<code>{rub}₽</code> <s>{full}⭐</s>\n"
    
    text += f"\n💰 Баланс: <code>{bal:.1f}⭐</code>\n💳 Рубли — @{PAY_CONTACT}"
    
    kb = InlineKeyboardBuilder()
    for k, bp in BUNDLE_PRICES.items():
        rub = int(bp['stars'] * STAR_TO_RUB)
        kb.button(text=f"{bp['label']} — {bp['stars']}⭐/{rub}₽", callback_data=f"buybundle_{k}")
    kb.button(text=f"💳 Рубли (@{PAY_CONTACT})", url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="🔙 Магазин", callback_data="cmd_shop")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

# ═══ ВЫБОР ОПЛАТЫ: БАЛАНС ═══

@dp.callback_query(F.data.startswith("paybal_searches_"))
async def cb_paybal_searches(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    count = int(cb.data[16:])
    cost = count * SEARCH_PRICE_STARS
    bal = get_balance(uid)
    if bal < cost:
        await answer_cb(cb, f"❌ Нужно {cost}⭐, у вас {bal:.1f}⭐", show_alert=True); return
    set_balance(uid, bal - cost)
    add_extra_searches(uid, count)
    log_action(uid, "buy_searches_bal", f"{count}шт -{cost}⭐")
    display = f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb = InlineKeyboardBuilder(); kb.button(text="🔙 Магазин", callback_data="cmd_shop")
    await edit_msg(cb.message,
        f"✅ <b>Куплено!</b>\n\n"
        f"🔍 +{count} поисков\n"
        f"💰 Списано: {cost}⭐\n"
        f"💰 Остаток: {bal-cost:.1f}⭐", kb.as_markup())
    await notify_admins(f"🛒 {display} купил {count} поисков за баланс (-{cost}⭐)")

@dp.callback_query(F.data.startswith("paystars_searches_"))
async def cb_paystars_searches(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    count = int(cb.data[18:])
    total = count * SEARCH_PRICE_STARS
    await bot.send_invoice(uid, title=f"🔍 +{count} поисков",
        description=f"{count} поисков",
        payload=f"searches_{count}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"+{count}", amount=total)])

# ═══ ВЫБОР ОПЛАТЫ: PREMIUM ═══

@dp.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery):
    uid = cb.from_user.id; k = cb.data[4:]; p = PRICES.get(k)
    if not p: await answer_cb(cb,"❌",show_alert=True); return
    await answer_cb(cb)
    bal = get_balance(uid)
    kb = InlineKeyboardBuilder()
    if bal >= p["stars"]:
        kb.button(text=f"💰 С баланса ({p['stars']}⭐)", callback_data=f"paybal_prem_{k}")
    kb.button(text=f"⭐ Telegram Stars ({p['stars']}⭐)", callback_data=f"paystars_prem_{k}")
    kb.button(text="❌ Отмена", callback_data="shop_premium")
    kb.adjust(1)
    await edit_msg(cb.message,
        f"💎 <b>Premium {p['label']}</b>\n\n"
        f"💰 Цена: {p['stars']}⭐\n"
        f"💰 Баланс: {bal:.1f}⭐\n\n"
        f"Способ оплаты:", kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_prem_"))
async def cb_paybal_prem(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[12:]; p = PRICES.get(k)
    if not p: return
    bal = get_balance(uid)
    if bal < p["stars"]:
        await answer_cb(cb, "❌ Недостаточно средств", show_alert=True); return
    set_balance(uid, bal - p["stars"])
    end = give_subscription(uid, p["days"])
    log_action(uid, "buy_prem_bal", f"{k} -{p['stars']}⭐")
    display = f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb = InlineKeyboardBuilder(); kb.button(text="👤 Профиль", callback_data="cmd_profile")
    await edit_msg(cb.message,
        f"✅ <b>Куплено!</b>\n\n"
        f"💎 Premium {p['label']}\n"
        f"📅 До: {end}\n"
        f"💰 Списано: {p['stars']}⭐", kb.as_markup())
    await notify_admins(f"🛒 <b>ПОКУПКА ЗА БАЛАНС</b>\n👤 {display}\n💎 Premium {p['label']}\n💰 -{p['stars']}⭐")

@dp.callback_query(F.data.startswith("paystars_prem_"))
async def cb_paystars_prem(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[14:]; p = PRICES.get(k)
    if not p: return
    await bot.send_invoice(uid, title=f"💎 {p['label']}",
        description=f"Premium {p['label']}",
        payload=f"sub_{k}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=p["label"], amount=p["stars"])])

# ═══ ВЫБОР ОПЛАТЫ: VIP ═══

@dp.callback_query(F.data.startswith("buyvip_"))
async def cb_buyvip(cb: CallbackQuery):
    uid = cb.from_user.id; k = cb.data[7:]; vp = VIP_PRICES.get(k)
    if not vp: await answer_cb(cb,"❌",show_alert=True); return
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        await answer_cb(cb,"⚠️ Сначала купите Premium!",show_alert=True); return
    await answer_cb(cb)
    bal = get_balance(uid)
    kb = InlineKeyboardBuilder()
    if bal >= vp["stars"]:
        kb.button(text=f"💰 С баланса ({vp['stars']}⭐)", callback_data=f"paybal_vip_{k}")
    kb.button(text=f"⭐ Telegram Stars ({vp['stars']}⭐)", callback_data=f"paystars_vip_{k}")
    kb.button(text="❌ Отмена", callback_data="shop_vip")
    kb.adjust(1)
    await edit_msg(cb.message,
        f"🌟 <b>VIP {vp['label']}</b>\n\n"
        f"💰 Цена: {vp['stars']}⭐\n"
        f"💰 Баланс: {bal:.1f}⭐\n\n"
        f"Способ оплаты:", kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_vip_"))
async def cb_paybal_vip(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[11:]; vp = VIP_PRICES.get(k)
    if not vp: return
    bal = get_balance(uid)
    if bal < vp["stars"]:
        await answer_cb(cb, "❌ Недостаточно средств", show_alert=True); return
    set_balance(uid, bal - vp["stars"])
    end = give_vip(uid, vp["days"])
    log_action(uid, "buy_vip_bal", f"{k} -{vp['stars']}⭐")
    display = f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb = InlineKeyboardBuilder(); kb.button(text="👤 Профиль", callback_data="cmd_profile")
    await edit_msg(cb.message,
        f"✅ <b>Куплено!</b>\n\n"
        f"🌟 VIP {vp['label']}\n"
        f"📅 До: {end}\n"
        f"💰 Списано: {vp['stars']}⭐", kb.as_markup())
    await notify_admins(f"🛒 <b>VIP ЗА БАЛАНС</b>\n👤 {display}\n🌟 {vp['label']}\n💰 -{vp['stars']}⭐")

@dp.callback_query(F.data.startswith("paystars_vip_"))
async def cb_paystars_vip(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[13:]; vp = VIP_PRICES.get(k)
    if not vp: return
    await bot.send_invoice(uid, title=f"🌟 {vp['label']}",
        description=f"VIP {vp['label']}",
        payload=f"vip_{k}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=vp["label"], amount=vp["stars"])])

# ═══ ВЫБОР ОПЛАТЫ: БАНДЛ ═══

@dp.callback_query(F.data.startswith("buybundle_"))
async def cb_buybundle(cb: CallbackQuery):
    uid = cb.from_user.id; k = cb.data[10:]; bp = BUNDLE_PRICES.get(k)
    if not bp: await answer_cb(cb,"❌",show_alert=True); return
    await answer_cb(cb)
    bal = get_balance(uid)
    kb = InlineKeyboardBuilder()
    if bal >= bp["stars"]:
        kb.button(text=f"💰 С баланса ({bp['stars']}⭐)", callback_data=f"paybal_bundle_{k}")
    kb.button(text=f"⭐ Telegram Stars ({bp['stars']}⭐)", callback_data=f"paystars_bundle_{k}")
    kb.button(text="❌ Отмена", callback_data="shop_bundle")
    kb.adjust(1)
    await edit_msg(cb.message,
        f"📦 <b>{bp['label']}</b>\n\n"
        f"💰 Цена: {bp['stars']}⭐\n"
        f"💰 Баланс: {bal:.1f}⭐\n\n"
        f"Способ оплаты:", kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_bundle_"))
async def cb_paybal_bundle(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[14:]; bp = BUNDLE_PRICES.get(k); p = PRICES.get(k)
    if not bp or not p: return
    bal = get_balance(uid)
    if bal < bp["stars"]:
        await answer_cb(cb, "❌ Недостаточно средств", show_alert=True); return
    set_balance(uid, bal - bp["stars"])
    end_prem = give_subscription(uid, p["days"])
    end_vip = give_vip(uid, p["days"])
    log_action(uid, "buy_bundle_bal", f"{k} -{bp['stars']}⭐")
    display = f"@{cb.from_user.username}" if cb.from_user.username else f"ID:{uid}"
    kb = InlineKeyboardBuilder(); kb.button(text="👤 Профиль", callback_data="cmd_profile")
    await edit_msg(cb.message,
        f"✅ <b>Куплено!</b>\n\n"
        f"💎 Premium до {end_prem}\n"
        f"🌟 VIP до {end_vip}\n"
        f"💰 Списано: {bp['stars']}⭐", kb.as_markup())
    await notify_admins(f"🛒 <b>БАНДЛ ЗА БАЛАНС</b>\n👤 {display}\n📦 {bp['label']}\n💰 -{bp['stars']}⭐")

@dp.callback_query(F.data.startswith("paystars_bundle_"))
async def cb_paystars_bundle(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    k = cb.data[16:]; bp = BUNDLE_PRICES.get(k)
    if not bp: return
    await bot.send_invoice(uid, title=f"📦 {bp['label']}",
        description=f"Premium+VIP {bp['label']}",
        payload=f"bundle_{k}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=bp["label"], amount=bp["stars"])])


# ═══════════════════════ CALLBACKS: Профиль ═══════════════════════

@dp.callback_query(F.data == "cmd_profile")
async def cb_profile(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    u = get_user(uid); config = load_bot_config()
    is_admin = uid in ADMIN_IDS; is_prem = has_subscription(uid)
    is_vip_user = has_vip(uid)
    _reset_daily_if_needed(uid)
    
    if is_admin: status = "👑 Админ ♾"
    elif is_vip_user:
        used = u.get("daily_searches_used",0)
        status = f"🌟 VIP до {u.get('vip_end','?')} | Сегодня: {used}/{VIP_SEARCHES_LIMIT}"
    elif is_prem:
        used = u.get("daily_searches_used",0)
        status = f"💎 Premium до {u.get('sub_end','?')} | Сегодня: {used}/{PREMIUM_SEARCHES_LIMIT}"
    elif u.get("free",0)+u.get("extra_searches",0)>0:
        status = f"🆓 {u.get('free',0)+u.get('extra_searches',0)} поисков"
    else: status = "⛔️ Лимит"
    
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    ar_on,ar_plan = get_auto_renew(uid)
    bal = u.get("balance",0.0); uname = u.get("uname","")
    mons = get_monitor_count(uid); mon_limit = get_monitor_limit(uid)
    custom_header = config.get("text_profile_header","")
    
    text = f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    if custom_header: text += f"{custom_header}\n\n"
    text += (f"🆔 <code>{uid}</code>" + (f" | @{uname}" if uname else "") + "\n"
             f"📌 {status}\n"
             f"🎯 {cnt} юзов/поиск | 🔄 {mx} осталось\n"
             f"📊 Всего: <code>{u.get('searches',0)}</code>\n"
             f"👥 Рефералов: <code>{u.get('ref_count',0)}</code>\n"
             f"👁 Мониторинг: <code>{mons}/{mon_limit}</code>\n"
             f"💰 Баланс: <code>{bal:.1f}</code> ⭐ (<code>{bal*STAR_TO_RUB:.0f}₽</code>)\n\n"
             f"🔄 Авто: {'<b>ВКЛ</b> ('+ar_plan+')' if ar_on else 'ВЫКЛ'}")
    
    kb = InlineKeyboardBuilder()
    if ar_on: kb.button(text="🔄 Выкл авто", callback_data="toggle_renew")
    else: kb.button(text="🔄 Вкл авто", callback_data="toggle_renew")
    kb.button(text="📜 История", callback_data="util_hist")
    kb.button(text="🔑 Ключ", callback_data="cmd_activate")
    kb.button(text="🎁 Подарить", callback_data="gift_prem")
    if bal>=MIN_WITHDRAW: kb.button(text=f"💸 Вывести ({bal:.1f}⭐)", callback_data="cmd_withdraw")
    if is_prem and is_button_enabled("roulette"): kb.button(text="🎰 Рулетка", callback_data="cmd_roulette")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "toggle_renew")
async def cb_tr(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ar_on,_ = get_auto_renew(uid)
    if ar_on: set_auto_renew(uid,False,""); await cb_profile(cb)
    else:
        kb = InlineKeyboardBuilder()
        for k,p in PRICES.items():
            if p["days"]<99999: kb.button(text=f"{p['label']} ({p['stars']}⭐)", callback_data=f"sr_{k}")
        kb.button(text="❌", callback_data="cmd_profile"); kb.adjust(1)
        await edit_msg(cb.message, "🔄 <b>Тариф:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("sr_"))
async def cb_sr(cb: CallbackQuery):
    plan = cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    set_auto_renew(cb.from_user.id,True,plan); await cb_profile(cb)

@dp.callback_query(F.data == "cmd_activate")
async def cb_act(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action":"activate"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "🔑 <b>Введите ключ:</b>", kb.as_markup())

@dp.callback_query(F.data == "cmd_withdraw")
async def cb_wd(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bal = get_balance(uid)
    if bal<MIN_WITHDRAW: return
    user_states[uid] = {"action":"withdraw_amount"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_profile")
    await edit_msg(cb.message, f"💸 <b>Вывод</b>\n\n💰 {bal:.1f}⭐ ({bal*STAR_TO_RUB:.0f}₽)\nМин: {MIN_WITHDRAW}⭐\n\nСумма:", kb.as_markup())


# ═══════════════════════ CALLBACKS: Рулетка ═══════════════════════

@dp.callback_query(F.data == "cmd_roulette")
async def cb_roulette(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    if not can_roulette(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_profile")
        await edit_msg(cb.message, "⏳ Через неделю", kb.as_markup()); return
    kb = InlineKeyboardBuilder()
    kb.button(text="🎰 Крутить!", callback_data="roulette_spin")
    kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message, "🎰 <b>Рулетка</b>\n\nРаз в неделю 🍀", kb.as_markup())

@dp.callback_query(F.data == "roulette_spin")
async def cb_rspin(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    if not can_roulette(uid): return
    set_last_roulette(uid); log_action(uid,"roulette","spin")
    for e in ["🎰","🔄","💫","🌟","✨","🎯"]:
        await edit_msg(cb.message, f"{e} Крутим..."); await asyncio.sleep(0.4)
    roll = random.randint(1,100)
    if roll<=40: days=random.choice([1,2]); give_subscription(uid,days); prize=f"💎 +{days} дн!"
    elif roll<=70: s=random.choice([1,2,3,5]); add_extra_searches(uid,s); prize=f"🔍 +{s} поисков!"
    elif roll<=90: st=random.choice([5,10,15]); add_balance(uid,st); prize=f"⭐ +{st} звёзд!"
    else: days=random.choice([3,5,7]); give_subscription(uid,days); prize=f"🎉 ДЖЕКПОТ! +{days} дн!"
    kb = InlineKeyboardBuilder(); kb.button(text="👤", callback_data="cmd_profile"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, f"🎉 <b>Поздравляем!</b>\n\n{prize}\n\nЧерез 7 дней 🍀", kb.as_markup())


# ═══════════════════════ CALLBACKS: Рефералы / Подарить / Донат ═══════════════════════

@dp.callback_query(F.data == "cmd_referral")
async def cb_ref(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    u = get_user(uid); bu = bot_info.username if bot_info else "bot"
    link = f"https://t.me/{bu}?start=ref_{uid}"
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться", url=f"https://t.me/share/url?url={link}&text=🔍 Найди юзернейм!")
    kb.button(text="👥 Мои рефералы", callback_data="my_refs")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,
        f"👥 <b>Рефералы</b>\n\n👥 <code>{u.get('ref_count',0)}</code>\n💰 <code>{u.get('balance',0):.1f}</code> ⭐ (4%)\n+{REF_BONUS} поиска за друга\n\n🔗 <code>{link}</code>",
        kb.as_markup())

@dp.callback_query(F.data == "my_refs")
async def cb_my_refs(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    refs = get_user_referrals(uid,20)
    text = f"👥 <b>Рефералы ({len(refs)})</b>\n\n"
    for r in refs:
        name = f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"
        text += f"• {name} — {r['created']}\n"
    if not refs: text += "<i>Пусто</i>"
    kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_referral"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "gift_prem")
async def cb_gift(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐", callback_data=f"gp_{k}")
    kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message, "🎁 <b>Подарить Premium</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("gp_"))
async def cb_gp(cb: CallbackQuery):
    plan = cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    user_states[cb.from_user.id] = {"action":"gift_username","plan":plan}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="gift_prem")
    await edit_msg(cb.message, f"🎁 <b>{PRICES[plan]['label']}</b>\n\n@username получателя:", kb.as_markup())

@dp.callback_query(F.data == "cmd_support")
async def cb_support(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for amt in DONATE_OPTIONS: kb.button(text=f"⭐ {amt}", callback_data=f"don_{amt}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(3,3,1,1)
    await edit_msg(cb.message, "🤖 <b>Поддержать</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("don_"))
async def cb_don(cb: CallbackQuery):
    amt = int(cb.data[4:]); await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"🤖 {amt}⭐", description="Донат",
        payload=f"donate_{amt}_{cb.from_user.id}", provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"{amt}⭐", amount=amt)])


# ═══════════════════════ CALLBACKS: Акции ═══════════════════════

@dp.callback_query(F.data.startswith("pv_"))
async def cb_pv(cb: CallbackQuery):
    await answer_cb(cb); pid = int(cb.data[3:])
    promos = get_active_promotions()
    promo = next((p for p in promos if p["id"]==pid), None)
    if not promo:
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_menu")
        await edit_msg(cb.message, "❌ Завершена", kb.as_markup()); return
    if promo["ptype"]=="ref_contest":
        text = "🏆 <b>Реферальный конкурс!</b>\n\nПриглашайте друзей!\n\n🎁 Призы ТОП-5"
        kb = InlineKeyboardBuilder()
        kb.button(text="🏆 Топ", callback_data=f"pt_{pid}"); kb.button(text="👥 Ссылка", callback_data="cmd_referral")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    else:
        text = f"🎉 <b>{promo['name']}</b>"
        kb = InlineKeyboardBuilder(); kb.button(text="🏪", callback_data="cmd_shop"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("pt_"))
async def cb_pt(cb: CallbackQuery):
    await answer_cb(cb); pid = int(cb.data[3:]); uid = cb.from_user.id
    promos = get_active_promotions(); sd = "2024-01-01 00:00"
    for p in promos:
        if p["id"]==pid and p["ptype"]=="ref_contest": sd=p["created"]; break
    top = get_ref_top_by_period(sd,20)
    text = "🏆 <b>Топ</b>\n\n"
    for i,t in enumerate(top,1):
        medals = {1:"🥇",2:"🥈",3:"🥉",4:"🏅",5:"🏅"}
        m = medals.get(i,f"{i}.")
        name = f"@{t['uname']}" if t["uname"] else f"ID:{t['uid']}"
        you = " ← <b>ты</b>" if t["uid"]==uid else ""
        text += f"{m} {name} — <code>{t['ref_count']}</code>{you}\n"
    if not top: text += "<i>Пусто</i>"
    place,refs = get_my_ref_place(uid)
    text += f"\n\n📍 #{place} | 👥 {refs}"
    kb = InlineKeyboardBuilder(); kb.button(text="👥 Ссылка", callback_data="cmd_referral"); kb.button(text="🔙", callback_data=f"pv_{pid}"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())


# ═══════════════════════ CALLBACKS: TikTok ═══════════════════════

@dp.callback_query(F.data == "cmd_tiktok")
async def cb_tt(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Начать", callback_data="tt_go")
    kb.button(text="📹 Видео", callback_data="tt_video")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,
        f"🎁 <b>TikTok</b>\n\n1️⃣ Найди видео\n2️⃣ {TIKTOK_SCREENSHOTS_NEEDED} комментов\n💬 <code>{TIKTOK_COMMENT_TEXT}</code>\n3️⃣ Скрины\n4️⃣ 🎁 {TIKTOK_REWARD_GIFT}",
        kb.as_markup())

@dp.callback_query(F.data == "tt_video")
async def cb_ttv(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    kb.button(text="📲 @SwordUserTiktok", url="https://t.me/SwordUserTiktok")
    kb.button(text="🔙", callback_data="cmd_tiktok"); kb.adjust(1)
    await edit_msg(cb.message, "📹 <b>Снимай видео!</b>\n\n💸 1000 просмотров = 1$", kb.as_markup())

@dp.callback_query(F.data == "tt_go")
async def cb_tg_tt(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not tiktok_can_submit(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_tiktok")
        await edit_msg(cb.message, "❌ Лимит на сегодня", kb.as_markup()); return
    tid = task_create(uid)
    user_states[uid] = {"action":"tiktok_proof","task_id":tid,"photos":0}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="tt_cancel")
    await edit_msg(cb.message, f"📸 <b>#{tid}</b>\n\n<code>0/{TIKTOK_SCREENSHOTS_NEEDED}</code>", kb.as_markup())

@dp.callback_query(F.data == "tt_cancel")
async def cb_tc(cb: CallbackQuery):
    await answer_cb(cb); user_states.pop(cb.from_user.id, None)
    t,k = build_menu(cb.from_user.id); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data.startswith("ta_"))
async def cb_ta(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); tid = int(cb.data[3:])
    uid = task_approve(tid, cb.from_user.id)
    if uid:
        log_action(cb.from_user.id,"task_approve",str(tid))
        try: await cb.message.edit_text(f"✅ #{tid} одобрено")
        except: pass
        try: await bot.send_message(uid, f"🎉 Одобрено! 🎁 {TIKTOK_REWARD_GIFT}")
        except: pass

# ═══════════════════════ ФОТО ═══════════════════════

@dp.message(F.photo)
async def handle_photo(msg: Message):
    uid = msg.from_user.id; state = user_states.get(uid,{})
    if state.get("action")!="tiktok_proof": return
    tid = state.get("task_id"); photos = state.get("photos",0)+1
    user_states[uid]["photos"] = photos
    if "file_ids" not in user_states[uid]: user_states[uid]["file_ids"]=[]
    user_states[uid]["file_ids"].append(msg.photo[-1].file_id)
    if photos<TIKTOK_SCREENSHOTS_NEEDED:
        await msg.answer(f"📸 {photos}/{TIKTOK_SCREENSHOTS_NEEDED}"); return
    file_ids = user_states[uid].get("file_ids",[]); user_states.pop(uid, None)
    await msg.answer(f"✅ Все {TIKTOK_SCREENSHOTS_NEEDED}! Ожидайте.")
    display = f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"
    from aiogram.types import InputMediaPhoto
    for aid in ADMIN_IDS:
        try:
            akb = InlineKeyboardBuilder()
            akb.button(text="✅", callback_data=f"ta_{tid}"); akb.button(text="❌", callback_data=f"tr_{tid}"); akb.adjust(2)
            await bot.send_message(aid, f"📱 <b>#{tid}</b>\n👤 {display} (<code>{uid}</code>)\n📸 {photos}", reply_markup=akb.as_markup(), parse_mode="HTML")
            for i in range(0,len(file_ids),10):
                batch = file_ids[i:i+10]
                media = [InputMediaPhoto(media=fid) for fid in batch]
                if i==0: media[0].caption = f"#{tid} | {display}"
                await bot.send_media_group(aid, media)
        except Exception as e: logger.error(f"TT {aid}: {e}")

        
# ═══════════════════════ ОПЛАТА ═══════════════════════

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery): await q.answer(ok=True)

@dp.message(F.successful_payment)
async def succ_pay(msg: Message):
    payload = msg.successful_payment.invoice_payload
    parts = payload.split("_")
    amount_paid = msg.successful_payment.total_amount
    uid = msg.from_user.id
    config = load_bot_config()
    uname = msg.from_user.username or ""
    display = f"@{uname}" if uname else f"ID:{uid}"

    if parts[0]=="sub" and len(parts)>=3:
        k=parts[1]; tuid=int(parts[2]); p=PRICES.get(k)
        if p:
            end = give_subscription(tuid, p["days"])
            log_action(tuid,"purchase_sub",f"{k}={p['stars']}⭐")
            await msg.answer(f"🎉 <b>Оплачено!</b> 💎 {p['label']} до {end}", parse_mode="HTML")
            ref_uid = get_user(tuid).get("referred_by",0)
            if ref_uid and ref_uid!=tuid:
                comm = round(amount_paid*REFERRAL_COMMISSION,1)
                if comm>0: add_balance(ref_uid,comm)
                try: await bot.send_message(ref_uid, f"💰 +<code>{comm}</code>⭐ реф. бонус", parse_mode="HTML")
                except: pass
            await notify_admins(f"💰 <b>ПОКУПКА</b>\n👤 {display} (<code>{tuid}</code>)\n💎 Premium {p['label']}\n⭐ {p['stars']}⭐")

    elif parts[0]=="vip" and len(parts)>=3:
        k=parts[1]; tuid=int(parts[2]); vp=VIP_PRICES.get(k)
        if vp:
            end = give_vip(tuid, vp["days"])
            log_action(tuid,"purchase_vip",f"{k}={vp['stars']}⭐")
            await msg.answer(f"🎉 <b>Оплачено!</b> 🌟 VIP {vp['label']} до {end}", parse_mode="HTML")
            await notify_admins(f"💰 <b>ПОКУПКА</b>\n👤 {display} (<code>{tuid}</code>)\n🌟 VIP {vp['label']}\n⭐ {vp['stars']}⭐")

    elif parts[0]=="bundle" and len(parts)>=3:
        k=parts[1]; tuid=int(parts[2])
        p=PRICES.get(k); bp=BUNDLE_PRICES.get(k)
        if p and bp:
            end_prem = give_subscription(tuid, p["days"])
            end_vip = give_vip(tuid, p["days"])
            log_action(tuid,"purchase_bundle",f"{k}={bp['stars']}⭐")
            await msg.answer(f"🎉 <b>Оплачено!</b>\n💎 Premium до {end_prem}\n🌟 VIP до {end_vip}", parse_mode="HTML")
            ref_uid = get_user(tuid).get("referred_by",0)
            if ref_uid and ref_uid!=tuid:
                comm = round(amount_paid*REFERRAL_COMMISSION,1)
                if comm>0: add_balance(ref_uid,comm)
                try: await bot.send_message(ref_uid, f"💰 +<code>{comm}</code>⭐ реф. бонус", parse_mode="HTML")
                except: pass
            await notify_admins(f"💰 <b>ПОКУПКА</b>\n👤 {display} (<code>{tuid}</code>)\n📦 Бандл {bp['label']}\n⭐ {bp['stars']}⭐")

    elif parts[0]=="gift" and len(parts)>=4:
        k=parts[1]; tuid=int(parts[2]); p=PRICES.get(k)
        if p:
            end = give_subscription(tuid, p["days"])
            log_action(uid,"gift",f"{tuid} {k}")
            await msg.answer(f"🎁 <b>Отправлено!</b>", parse_mode="HTML")
            try: await bot.send_message(tuid, f"🎁 Вам подарили <b>{p['label']}</b>!\nДо: {end}", parse_mode="HTML")
            except: pass
            await notify_admins(f"🎁 <b>ПОДАРОК</b>\n👤 {display} → <code>{tuid}</code>\n💎 {p['label']}\n⭐ {p['stars']}⭐")

    elif parts[0]=="topup" and len(parts)>=3:
        amount=int(parts[1]); tuid=int(parts[2])
        add_balance(tuid, amount); log_action(tuid,"topup",str(amount))
        await msg.answer(f"✅ <b>+{amount}⭐</b>", parse_mode="HTML")
        await notify_admins(f"💰 <b>ПОПОЛНЕНИЕ</b>\n👤 {display}\n⭐ +{amount}⭐")

    elif parts[0]=="searches" and len(parts)>=3:
        count=int(parts[1]); tuid=int(parts[2])
        add_extra_searches(tuid, count); log_action(tuid,"buy_searches",str(count))
        await msg.answer(f"✅ <b>+{count} поисков!</b>", parse_mode="HTML")
        await notify_admins(f"💰 <b>ПОКУПКА</b>\n👤 {display}\n🔍 +{count} поисков\n⭐ {count*SEARCH_PRICE_STARS}⭐")

    elif parts[0]=="donate" and len(parts)>=3:
        amt=int(parts[1]); log_action(uid,"donate",str(amt))
        await msg.answer("❤️ <b>Спасибо!</b>", parse_mode="HTML")
        await notify_admins(f"❤️ <b>ДОНАТ</b>\n👤 {display}\n⭐ {amt}⭐")

    # ═══════════════════════ МАРКЕТПЛЕЙС ОПЛАТА ═══════════════════════

    elif parts[0]=="market" and len(parts)>=3:
        lot_id=int(parts[1]); buyer_uid=int(parts[2])
        discount=int(parts[3]) if len(parts)>=4 else 0
        lot=market_get_lot(lot_id)
        if not lot or lot["status"]!="active":
            try:
                await bot.refund_star_payment(uid, msg.successful_payment.telegram_payment_charge_id)
                await msg.answer("❌ Лот уже продан! Возврат средств")
            except: await msg.answer("❌ Лот продан! Напишите админу для возврата.")
            return
        ok=market_buy_lot(lot_id, buyer_uid)
        if not ok:
            try:
                await bot.refund_star_payment(uid, msg.successful_payment.telegram_payment_charge_id)
                await msg.answer("❌ Ошибка покупки! Возврат средств")
            except: await msg.answer("❌ Ошибка! Напишите админу.")
            return
        # Сохраняем charge_id для возможного рефанда при споре
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("UPDATE market SET charge_id=? WHERE id=?",
                  (msg.successful_payment.telegram_payment_charge_id, lot_id))
        conn.commit(); conn.close()
        log_action(buyer_uid,"market_buy",f"lot={lot_id} paid={amount_paid}")
        disc_text=f"\n🏷 Скидка: -{discount}⭐" if discount else ""
        # Кнопки для покупателя
        kb=InlineKeyboardBuilder()
        kb.button(text="✅ Получил товар", callback_data=f"mbuyerok_{lot_id}")
        kb.button(text="⚠️ Открыть спор", callback_data=f"mdispute_{lot_id}")
        kb.adjust(1)
        await msg.answer(
            f"✅ <b>Оплачено!</b>\n"
            f"📦 {lot['title']}\n"
            f"💰 {amount_paid}⭐{disc_text}\n\n"
            f"🔒 Деньги на эскроу — продавец получит после подтверждения\n"
            f"⏰ Срок: {MARKET_ESCROW_HOURS} часов\n\n"
            f"Когда получите товар — нажмите <b>✅ Получил товар</b>",
            reply_markup=kb.as_markup(), parse_mode="HTML")
        # Уведомление продавцу
        bname=f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"
        skb=InlineKeyboardBuilder()
        skb.button(text="✅ Я передал товар", callback_data=f"msellerok_{lot_id}")
        try: await bot.send_message(lot["seller_uid"],
            f"🛒 <b>Ваш лот куплен!</b>\n\n"
            f"📦 {lot['title']}\n"
            f"💰 {lot['price']}⭐\n"
            f"👤 Покупатель: {bname}\n\n"
            f"⏰ Передайте товар в течение {MARKET_ESCROW_HOURS} часов\n"
            f"После передачи нажмите <b>✅ Я передал товар</b>",
            reply_markup=skb.as_markup(), parse_mode="HTML")
        except: pass
        # Уведомление админу
        commission = int(lot['price'] * MARKET_COMMISSION)
        payout = lot['price'] - commission
        await notify_admins(
            f"🛒 <b>ПРОДАЖА НА МАРКЕТЕ</b>\n"
            f"📦 {lot['title']}\n"
            f"💰 Цена: {lot['price']}⭐\n"
            f"📊 Комиссия: {commission}⭐ (твой доход)\n"
            f"💸 Продавцу: {payout}⭐\n"
            f"👤 Продавец: <code>{lot['seller_uid']}</code>\n"
            f"👤 Покупатель: {bname} (<code>{uid}</code>)")

    elif parts[0]=="promote" and len(parts)>=2:
        lot_id=int(parts[1])
        market_promote_lot(lot_id)
        log_action(uid,"promote",str(lot_id))
        await msg.answer("🔥 <b>Лот продвинут на 24 часа!</b>", parse_mode="HTML")
        await notify_admins(f"🔥 <b>ПРОДВИЖЕНИЕ</b>\n👤 {display}\n📦 Лот #{lot_id}\n⭐ {MARKET_PROMOTE_PRICE}⭐")

    elif parts[0]=="listing" and len(parts)>=2:
        lot_id=int(parts[1])
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("UPDATE market SET listing_paid=1 WHERE id=?", (lot_id,))
        conn.commit(); conn.close()
        log_action(uid,"listing_paid",str(lot_id))
        await msg.answer("✅ <b>Лот оплачен!</b>\nОжидайте модерации администратором", parse_mode="HTML")
        # Уведомление админу на модерацию
        lot=market_get_lot(lot_id)
        if lot:
            akb=InlineKeyboardBuilder()
            akb.button(text="✅ Одобрить",callback_data=f"mmod_ok_{lot_id}")
            akb.button(text="❌ Отклонить",callback_data=f"mmod_no_{lot_id}")
            akb.adjust(2)
            fast="⚡ СРОЧНО! " if lot.get("fast_mod") else ""
            nft="💎 NFT " if lot.get("is_nft") else ""
            seller = get_user(lot['seller_uid'])
            sname = f"@{seller.get('uname','')}" if seller.get('uname') else f"ID:{lot['seller_uid']}"
            await notify_admins(
                f"📦 <b>НОВЫЙ ЛОТ НА МОДЕРАЦИИ</b>\n\n"
                f"{fast}{nft}#{lot_id}\n"
                f"📌 {lot['title']}\n"
                f"📝 {lot.get('description','')}\n"
                f"💰 {lot['price']}⭐\n"
                f"👤 {sname}\n"
                f"💳 Размещение оплачено ✅",
                kb=akb.as_markup())

    elif parts[0]=="fastmod" and len(parts)>=2:
        lot_id=int(parts[1])
        market_set_fast_mod(lot_id)
        log_action(uid,"fast_mod",str(lot_id))
        await msg.answer("⚡ <b>Быстрая модерация активирована!</b>\nАдмин получил уведомление", parse_mode="HTML")
        lot=market_get_lot(lot_id)
        if lot:
            akb=InlineKeyboardBuilder()
            akb.button(text="✅ Одобрить",callback_data=f"mmod_ok_{lot_id}")
            akb.button(text="❌ Отклонить",callback_data=f"mmod_no_{lot_id}")
            akb.adjust(2)
            await notify_admins(
                f"⚡⚡⚡ <b>СРОЧНАЯ МОДЕРАЦИЯ!</b>\n\n"
                f"#{lot_id} — {lot['title']}\n"
                f"💰 {lot['price']}⭐\n"
                f"👤 <code>{lot['seller_uid']}</code>",
                kb=akb.as_markup())
        await notify_admins(f"⚡ Быстрая модерация: {MARKET_FAST_MOD_PRICE}⭐ от {display}")

    elif parts[0]=="mslot":
        market_add_slot(uid)
        log_action(uid,"buy_slot","")
        await msg.answer("📦 <b>+1 слот маркета!</b>\nТеперь можете размещать больше лотов", parse_mode="HTML")
        await notify_admins(f"📦 <b>ПОКУПКА СЛОТА</b>\n👤 {display}\n⭐ {MARKET_EXTRA_SLOT_PRICE}⭐")

    elif parts[0]=="wheel":
        prize, ptype = wheel_spin(uid)
        log_action(uid,"wheel_paid",prize)
        # Анимация
        for e in ["🎡","🔄","💫","🌟","✨","🎯"]:
            try: await bot.send_message(uid, f"{e} Крутим...")
            except: break
            await asyncio.sleep(0.3)
        await msg.answer(f"🎡 <b>{prize}</b>", parse_mode="HTML")
        await notify_admins(f"🎡 <b>КОЛЕСО</b>\n👤 {display}\n🎁 {prize}\n⭐ {WHEEL_EXTRA_PRICE}⭐")

    elif parts[0]=="lootbox":
        prize, ptype = lootbox_open(uid)
        log_action(uid,"lootbox",prize)
        # Анимация
        for e in ["📦","🔍","🎰","🌟","✨"]:
            try: await bot.send_message(uid, f"{e}")
            except: break
            await asyncio.sleep(0.3)
        await msg.answer(f"🎉 <b>{prize}</b>", parse_mode="HTML")
        await notify_admins(f"📦 <b>ЛУТБОКС</b>\n👤 {display}\n🎁 {prize}\n⭐ {LOOTBOX_PRICE}⭐")

# ═══════════════════════ ВЫВОДЫ ═══════════════════════

@dp.callback_query(F.data.startswith("wd_ok_"))
async def cb_wdo(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wid=int(cb.data[6:]); r=process_withdrawal(wid,cb.from_user.id,True)
    if r:
        log_action(cb.from_user.id,"wd_approve",str(wid))
        await edit_msg(cb.message, f"✅ #{wid} ({r['amount']:.1f}⭐)")
        try: await bot.send_message(r["uid"], f"✅ #{wid} {r['amount']:.1f}⭐ одобрен!")
        except: pass

@dp.callback_query(F.data.startswith("wd_no_"))
async def cb_wdn(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wid=int(cb.data[6:]); r=process_withdrawal(wid,cb.from_user.id,False)
    if r:
        log_action(cb.from_user.id,"wd_reject",str(wid))
        await edit_msg(cb.message, f"❌ #{wid}")
        try: await bot.send_message(r["uid"], f"❌ #{wid} отклонён")
        except: pass


# ═══════════════════════ ТЕКСТ ═══════════════════════

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: Message):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    state = user_states.get(uid, {}); action = state.get("action")

    if action=="activate":
        user_states.pop(uid,None); r=activate_key(uid,msg.text.strip())
        if r: log_action(uid,"key",msg.text.strip()); await msg.answer(f"🎉 {r['days']}дн до {r['end']}", parse_mode="HTML")
        else: await msg.answer("❌ Неверный ключ")
        t,k=build_menu(uid); await msg.answer(t,reply_markup=k,parse_mode="HTML",disable_web_page_preview=True); return

    if action=="evaluate":
        user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌ Мин 5 символов"); return
        wm=await msg.answer("⏳..."); tg=await check_username(un); fr=await check_fragment(un)
        tgs={"free":"✅ Свободен","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
        frs={"fragment":"💎","sold":"✅ Продан","unavailable":"—"}.get(fr,"❓")
        ev=evaluate_username(un); fac="\n".join("  "+f for f in ev["factors"]) or "  —"
        kb=InlineKeyboardBuilder()
        if tg=="free": kb.button(text="👁 Мониторинг",callback_data=f"mon_add_{un}")
        kb.button(text="📊 Ещё",callback_data="cmd_evaluate"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        text = (f"📊 <b>@{un}</b>\n\n📱 {tgs}\n💎 {frs}\n\n"
                f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
                f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}\n\n"
                f"📱 <a href='https://t.me/{un}'>Telegram</a> · "
                f"💎 <a href='https://fragment.com/username/{un}'>Fragment</a>")
        await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True); return

    if action=="quick_check":
        user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌"); return
        wm=await msg.answer("⏳..."); tg=await check_username(un)
        st={"free":"✅ Свободен!","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
        kb=InlineKeyboardBuilder()
        if tg=="free": kb.button(text="👁 Мониторинг",callback_data=f"mon_add_{un}")
        kb.button(text="🔍 Ещё",callback_data="util_check"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        text = (f"🔍 <b>@{un}</b> — {st}\n\n"
                f"📱 <a href='https://t.me/{un}'>Telegram</a> · "
                f"💎 <a href='https://fragment.com/username/{un}'>Fragment</a>")
        await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True); return

    if action=="mass_check":
        user_states.pop(uid,None)
        names=[n.strip().replace("@","").lower() for n in msg.text.split("\n") if validate_username(n.strip().replace("@","").lower())][:20]
        if not names: await msg.answer("❌"); return
        wm=await msg.answer(f"⏳ {len(names)}...")
        results=[]
        for n in names: r=await pool.check(n,uid); results.append(r); await asyncio.sleep(0.3)
        fc=sum(1 for r in results if r in ("free","maybe_free"))
        text=f"📋 <b>({len(names)})</b> ✅{fc}\n\n"
        for i,r in enumerate(results):
            icon={"free":"✅","maybe_free":"✅","taken":"❌"}.get(r,"⚠️")
            text+=f"{icon} <code>@{names[i]}</code>"
            if r in ("free","maybe_free"):
                text+=f" <a href='https://t.me/{names[i]}'>📱</a> <a href='https://fragment.com/username/{names[i]}'>💎</a>"
            text+="\n"
        kb=InlineKeyboardBuilder(); kb.button(text="📋 Ещё",callback_data="util_mass"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(text,reply_markup=kb.as_markup(),parse_mode="HTML",disable_web_page_preview=True); return

    # ═══ НОВОЕ: Поиск по слову (Premium режимы: telegram, mat, meaningful) ═══
    if action=="word_search":
        user_states.pop(uid,None)
        word=msg.text.strip().lower().replace("@","")
        mode=state.get("mode","meaningful")
        mi=SEARCH_MODES.get(mode,{})
        if len(word)<2 or len(word)>20:
            await msg.answer("❌ Слово от 2 до 20 символов"); return
        if not re.match(r'^[a-zA-Z0-9_]+$', word):
            await msg.answer("❌ Только латиница, цифры, _"); return
        if not can_search(uid):
            await msg.answer("⛔️ Поиски закончились!"); return
        if uid not in ADMIN_IDS:
            if uid in searching_users:
                await msg.answer("⏳ Уже идёт поиск!"); return
            cd = user_search_cooldown.get(uid,0); rem = SEARCH_COOLDOWN-(time.time()-cd)
            if rem>0: await msg.answer(f"⏳ {int(rem)} сек."); return
        searching_users.add(uid)
        try:
            mode_name = mi.get("name", mode)
            log_action(uid,"word_search",f"{mode}:{word}"); use_search(uid)
            wm=await msg.answer(f"{mi.get('emoji','🎯')} <b>{mode_name}: {word}</b>\n\n⏳ Генерирую вариации...")
            count = get_search_count(uid)
            found, stats = await do_thematic_search(word, count, wm, uid)
            config = load_bot_config()
            kb = InlineKeyboardBuilder()
            text = format_results(found, stats, f"{mode_name}: {word}", config)
            if can_search(uid): kb.button(text="🔄 Ещё", callback_data=f"go_{mode}")
            kb.button(text="🔍 Режимы", callback_data="cmd_search")
            kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
            await edit_msg(wm, text, kb.as_markup())
        finally:
            searching_users.discard(uid)
            if uid not in ADMIN_IDS: user_search_cooldown[uid]=time.time()
        return
    
    # ═══ НОВОЕ: Тематический поиск ═══
    if action=="thematic_search":
        user_states.pop(uid,None); word=msg.text.strip().lower().replace("@","")
        if len(word)<2 or len(word)>20:
            await msg.answer("❌ Слово от 2 до 20 символов"); return
        if not re.match(r'^[a-zA-Z0-9_]+$', word):
            await msg.answer("❌ Только латиница, цифры, _"); return
        if not has_vip(uid) and uid not in ADMIN_IDS:
            await msg.answer("🌟 Нужен VIP!"); return
        if not can_search(uid):
            await msg.answer("⛔️ Поиски закончились!"); return
        if uid not in ADMIN_IDS:
            if uid in searching_users:
                await msg.answer("⏳ Уже идёт поиск!"); return
            cd = user_search_cooldown.get(uid,0); rem = SEARCH_COOLDOWN-(time.time()-cd)
            if rem>0: await msg.answer(f"⏳ {int(rem)} сек."); return
        searching_users.add(uid)
        try:
            log_action(uid,"thematic",word); use_search(uid)
            wm=await msg.answer(f"🎯 <b>По слову: {word}</b>\n\n⏳ Генерирую вариации...")
            count = get_search_count(uid)
            found, stats = await do_thematic_search(word, count, wm, uid)
            config = load_bot_config()
            kb = InlineKeyboardBuilder()
            text = format_results(found, stats, f"По слову: {word}", config)
            if can_search(uid): kb.button(text="🔄 Ещё", callback_data="cmd_thematic")
            kb.button(text="🔍 Режимы", callback_data="cmd_search")
            kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
            await edit_msg(wm, text, kb.as_markup())
        finally:
            searching_users.discard(uid)
            if uid not in ADMIN_IDS: user_search_cooldown[uid]=time.time()
        return

    # ═══ МАРКЕТПЛЕЙС ТЕКСТОВЫЕ ХЕНДЛЕРЫ ═══
    if action=="msell_title":
        title=msg.text.strip()
        if len(title)<3 or len(title)>100: await msg.answer("❌ 3-100 символов"); return
        user_states[uid]={"action":"msell_desc","mtype":state["mtype"],"title":title}
        await msg.answer("📝 <b>Описание:</b>", parse_mode="HTML"); return

    if action=="msell_desc":
        desc=msg.text.strip()[:500]
        user_states[uid]={"action":"msell_price","mtype":state["mtype"],"title":state["title"],"desc":desc}
        await msg.answer(f"💰 <b>Цена (⭐):</b>\n{MARKET_MIN_PRICE} до {MARKET_MAX_PRICE}", parse_mode="HTML"); return

    if action=="msell_price":
        try: price=int(msg.text.strip()); assert MARKET_MIN_PRICE<=price<=MARKET_MAX_PRICE
        except: await msg.answer(f"❌ {MARKET_MIN_PRICE}-{MARKET_MAX_PRICE}"); return
        user_states.pop(uid,None)
        lot_id=market_create_lot(uid,state["mtype"],state["title"],state["desc"],price)
        commission=int(price*MARKET_COMMISSION)
        log_action(uid,"market_create",str(lot_id))
        await bot.send_invoice(uid,
            title=f"📦 Размещение лота #{lot_id}",
            description=f"{state['title']} — {price}⭐ (получите {price-commission}⭐)",
            payload=f"listing_{lot_id}_{uid}",
            provider_token="",currency="XTR",
            prices=[LabeledPrice(label="Размещение", amount=MARKET_LISTING_FEE)]); return

    if action=="msell_nft_title":
        username = msg.text.strip().replace("@","").lower()
        if not validate_username(username):
            await msg.answer("❌ Неверный юзернейм"); return
        wm = await msg.answer("🔍 Проверяю на Fragment...")
        frag = await check_fragment_nft(username)
        if not frag["is_nft"]:
            try: await wm.delete()
            except: pass
            await msg.answer("❌ <b>Этот юзернейм не NFT на Fragment!</b>\n\nМожно продавать только NFT юзернеймы купленные на Fragment.", parse_mode="HTML"); return
        if frag["status"] == "on_sale":
            try: await wm.delete()
            except: pass
            await msg.answer("⚠️ <b>Этот юз ещё на продаже на Fragment!</b>\n\nСначала купите его, потом продавайте здесь.", parse_mode="HTML"); return
        is_owner = await verify_nft_owner(uid, username)
        try: await wm.delete()
        except: pass
        if not is_owner:
            await msg.answer(
                f"⚠️ <b>Поставьте @{username} себе в профиль!</b>\n\n"
                f"1. Зайдите в настройки Telegram\n"
                f"2. Поставьте @{username} как юзернейм\n"
                f"3. Попробуйте снова\n\n"
                f"Это нужно для подтверждения владения.", parse_mode="HTML"); return
        user_states[uid] = {"action":"msell_nft_desc","title":username,"url":f"https://fragment.com/username/{username}"}
        await msg.answer(
            f"✅ <b>Подтверждено!</b>\n\n"
            f"💎 @{username} — NFT юзернейм\n"
            f"👤 Владелец: вы\n\n"
            f"📝 <b>Описание лота:</b>", parse_mode="HTML"); return

    if action=="msell_nft_desc":
        desc=msg.text.strip()[:500]
        user_states[uid]={"action":"msell_nft_price","title":state["title"],"url":state["url"],"desc":desc}
        await msg.answer(f"💰 <b>Цена (⭐):</b>\n{MARKET_MIN_PRICE} до {MARKET_MAX_PRICE}", parse_mode="HTML"); return

    if action=="msell_nft_price":
        try: price=int(msg.text.strip()); assert MARKET_MIN_PRICE<=price<=MARKET_MAX_PRICE
        except: await msg.answer(f"❌ {MARKET_MIN_PRICE}-{MARKET_MAX_PRICE}"); return
        user_states.pop(uid,None)
        lot_id=market_create_lot(uid,"nft",state["title"],state["desc"],price,is_nft=1,fragment_url=state["url"])
        log_action(uid,"nft_create",str(lot_id))
        await bot.send_invoice(uid,
            title=f"💎 Размещение NFT #{lot_id}",
            description=f"NFT @{state['title']} — {price}⭐",
            payload=f"listing_{lot_id}_{uid}",
            provider_token="",currency="XTR",
            prices=[LabeledPrice(label="NFT размещение", amount=MARKET_NFT_LISTING_FEE)]); return

    if action=="market_enter_promo":
        user_states.pop(uid,None)
        lot_id=state["lot_id"]; lot=market_get_lot(lot_id)
        if not lot: await msg.answer("❌"); return
        result=check_promocode(msg.text.strip(),uid,lot["price"],"market")
        if not result["valid"]: await msg.answer(f"❌ {result.get('reason','')}"); return
        discount=result["discount"]; final=max(1,lot["price"]-discount)
        use_promocode(msg.text.strip(),uid,discount)
        log_action(uid,"promo",f"{msg.text.strip()} -{discount}")
        await bot.send_invoice(uid,
            title=f"🛒 {lot['title'][:50]} (скидка!)",
            description=f"Было {lot['price']}⭐, скидка -{discount}⭐",
            payload=f"market_{lot_id}_{uid}_{discount}",
            provider_token="",currency="XTR",
            prices=[LabeledPrice(label=lot["title"][:50],amount=final)]); return

    if action=="market_dispute_reason":
        user_states.pop(uid,None)
        market_open_dispute(state["lot_id"],msg.text.strip(),uid)
        log_action(uid,"dispute",str(state["lot_id"]))
        await msg.answer("⚠️ <b>Спор открыт!</b>\nАдмин рассмотрит", parse_mode="HTML")
        await notify_admins(f"⚠️ Спор #{state['lot_id']}: {msg.text.strip()}"); return

    if action=="exchange_offer":
        user_states.pop(uid,None)
        if len(msg.text.strip())<3: await msg.answer("❌ Минимум 3 символа"); return
        eid=exchange_create(uid,msg.text.strip())
        log_action(uid,"exchange",str(eid))
        await msg.answer(f"✅ <b>Обмен #{eid}!</b>\n📦 {msg.text.strip()}\n⏳ Ждите", parse_mode="HTML"); return

    if action=="exchange_counter":
        user_states.pop(uid,None); eid=state["eid"]
        if len(msg.text.strip())<3: await msg.answer("❌ Минимум 3 символа"); return
        ok=exchange_accept(eid,uid,msg.text.strip())
        if not ok: await msg.answer("❌ Обмен закрыт"); return
        ex=exchange_get(eid)
        name=f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"
        ikb=InlineKeyboardBuilder()
        ikb.button(text="✅ Подтвердить", callback_data=f"exconfirm_{eid}")
        ikb.adjust(1)
        try: await bot.send_message(ex["initiator_uid"],
            f"🔄 Предложение обмена!\n👤 {name}: <b>{msg.text.strip()}</b>\nЗа: <b>{ex['initiator_offer']}</b>",
            reply_markup=ikb.as_markup(), parse_mode="HTML")
        except: pass
        await msg.answer("✅ Отправлено!\n⏳ Ждём"); return
    
    if action=="template_search":
        user_states.pop(uid,None); template=msg.text.strip().lower()
        if len(template)<3 or ("*" not in template and "?" not in template):
            await msg.answer("❌ Нужен * или ?"); return
        u=get_user(uid)
        if uid not in ADMIN_IDS and not has_subscription(uid):
            if u.get("template_uses",0)<=0:
                await msg.answer("❌ Нет использований"); return
            conn=sqlite3.connect(DB); c=conn.cursor()
            c.execute("UPDATE users SET template_uses=template_uses-1 WHERE uid=?",(uid,)); conn.commit(); conn.close()
        log_action(uid,"template",template)
        wm=await msg.answer(f"🎯 <code>{template}</code>...",parse_mode="HTML")
        found,stats=await do_template_search(template,3,wm,uid)
        kb=InlineKeyboardBuilder()
        text = format_results(found, stats, f"Шаблон: {template}")
        kb.button(text="🎯 Ещё",callback_data="cmd_template"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(wm,text,kb.as_markup()); return

    if action=="similar_search":
        user_states.pop(uid,None); base=msg.text.strip().replace("@","").lower()
        if not validate_username(base): await msg.answer("❌"); return
        log_action(uid,"similar",base)
        wm=await msg.answer(f"🔄 @{base}...")
        found,stats=await do_similar_search(base,5,wm,uid)
        kb=InlineKeyboardBuilder()
        text = format_results(found, stats, f"Похожие на @{base}")
        kb.button(text="🔄 Ещё",callback_data="cmd_similar_cb"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(wm,text,kb.as_markup()); return

    if action=="monitor_add":
        user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌"); return
        if get_monitor_count(uid)>=get_monitor_limit(uid): await msg.answer("❌ Лимит"); return
        mid=add_monitor(uid,un); log_action(uid,"monitor_add",un)
        await msg.answer(f"✅ @{un} на мониторинге"); return

    # ═══ НОВОЕ: Удаление по шаблону ═══
    if action=="delete_pattern":
        user_states.pop(uid,None); pattern=msg.text.strip().lower().replace("@","")
        if len(pattern)<2: await msg.answer("❌ Минимум 2 символа"); return
        deleted = delete_history_pattern(uid, pattern)
        kb=InlineKeyboardBuilder(); kb.button(text="📜 История",callback_data="util_hist"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
        await msg.answer(f"🗑 Удалено <code>{deleted}</code> записей по шаблону <code>{pattern}</code>",
                         reply_markup=kb.as_markup(), parse_mode="HTML"); return

    if action=="withdraw_amount":
        user_states.pop(uid,None)
        try: amount=float(msg.text.strip()); assert amount>=MIN_WITHDRAW
        except: await msg.answer(f"❌ Мин {MIN_WITHDRAW}⭐"); return
        bal=get_balance(uid)
        if amount>bal: await msg.answer("❌ Мало"); return
        wid=create_withdrawal(uid,amount); log_action(uid,"withdraw",str(amount))
        await msg.answer(f"✅ #{wid} на {amount:.1f}⭐ ({amount*STAR_TO_RUB:.0f}₽)")
        for aid in ADMIN_IDS:
            try:
                akb=InlineKeyboardBuilder(); akb.button(text="✅",callback_data=f"wd_ok_{wid}"); akb.button(text="❌",callback_data=f"wd_no_{wid}"); akb.adjust(2)
                await bot.send_message(aid,f"💰 #{wid} {amount:.1f}⭐ от {uid}",reply_markup=akb.as_markup())
            except: pass
        return

    if action=="gift_username":
        user_states.pop(uid,None); tu=msg.text.strip().replace("@","")
        tuid=find_user(tu)
        if not tuid: await msg.answer("❌ Не найден"); return
        plan=state.get("plan"); p=PRICES.get(plan)
        if not p: return
        await bot.send_invoice(uid,title=f"🎁 {p['label']} для @{tu}",description=f"Premium для @{tu}",
            payload=f"gift_{plan}_{tuid}_{uid}",provider_token="",currency="XTR",
            prices=[LabeledPrice(label=p["label"],amount=p["stars"])]); return

    if action=="shop_custom_amount":
        user_states.pop(uid,None)
        try: count=int(msg.text.strip()); assert 1<=count<=1000
        except: await msg.answer("❌ 1-1000"); return
        total=count*SEARCH_PRICE_STARS
        bal=get_balance(uid)
        kb=InlineKeyboardBuilder()
        if bal>=total:
            kb.button(text=f"💰 С баланса ({total}⭐)", callback_data=f"paybal_searches_{count}")
        kb.button(text=f"⭐ Telegram Stars ({total}⭐)", callback_data=f"paystars_searches_{count}")
        kb.button(text="❌ Отмена", callback_data="cmd_shop")
        kb.adjust(1)
        await msg.answer(
            f"🔍 <b>{count} поисков = {total}⭐</b>\n\n"
            f"💰 Баланс: <code>{bal:.1f}⭐</code>\n\n"
            f"Выберите способ оплаты:",
            reply_markup=kb.as_markup(), parse_mode="HTML"); return

    # ─── АДМИНСКИЕ СОСТОЯНИЯ ───
    if action=="admin_broadcast_text":
        if uid not in ADMIN_IDS: user_states.pop(uid,None); return
        user_states.pop(uid,None); bt=msg.text.strip()
        conn=sqlite3.connect(DB); c=conn.cursor()
        c.execute("SELECT uid FROM users WHERE banned=0"); aus=[r[0] for r in c.fetchall()]; conn.close()
        s,f=0,0; sm=await msg.answer(f"📤 0/{len(aus)}")
        for i,tu in enumerate(aus):
            try: await bot.send_message(tu,bt,parse_mode="HTML"); s+=1
            except: f+=1
            if (i+1)%50==0:
                try: await sm.edit_text(f"📤 {i+1}/{len(aus)} ✅{s} ❌{f}")
                except: pass
            await asyncio.sleep(0.05)
        log_action(uid,"broadcast",f"s={s} f={f}")
        try: await sm.edit_text(f"✅ ✅{s} ❌{f}")
        except: pass; return

    if action=="admin_give_user":
        target=find_user(msg.text.strip())
        if not target: await msg.answer("❌"); return
        user_states[uid]={"action":"admin_give_days","target":target}
        await msg.answer(f"📅 Дней для <code>{target}</code>?",parse_mode="HTML"); return

    if action=="admin_give_days":
        try: days=int(msg.text.strip()); assert days>0
        except: await msg.answer("❌ Число!"); return
        target=state["target"]; user_states.pop(uid,None)
        end=give_subscription(target,days); log_action(uid,"admin_give",f"{target}+{days}d")
        await msg.answer(f"✅ {days}дн до {end}",parse_mode="HTML")
        try: await bot.send_message(target,f"🎉 <b>{days}дн</b> до <b>{end}</b>!",parse_mode="HTML")
        except: pass; return

    if action=="admin_key_days":
        try: days=int(msg.text.strip()); assert days>0
        except: await msg.answer("❌"); return
        user_states.pop(uid,None); key=generate_key(days,f"D{days}"); log_action(uid,"key_gen",key)
        await msg.answer(f"🔑 <code>{key}</code> — {days}дн",parse_mode="HTML"); return

    if action=="admin_ban_input":
        user_states.pop(uid,None); target=find_user(msg.text.strip())
        if not target: await msg.answer("❌"); return
        ban_user(target); log_action(uid,"ban",str(target)); await msg.answer("🚫 Забанен"); return

    if action=="admin_unban_input":
        user_states.pop(uid,None); target=find_user(msg.text.strip())
        if not target: await msg.answer("❌"); return
        unban_user(target); log_action(uid,"unban",str(target)); await msg.answer("✅ Разбанен"); return

    if action=="admin_blacklist_add":
        user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌"); return
        add_blacklist(un,uid); log_action(uid,"bl_add",un); await msg.answer(f"⛔ @{un}"); return

    if action=="admin_promo_name":
        user_states[uid]={"action":"admin_promo_btn","name":msg.text.strip()}
        await msg.answer("🔘 Текст кнопки:"); return
    if action=="admin_promo_btn":
        user_states[uid]={"action":"admin_promo_type","name":state.get("name"),"btn":msg.text.strip()}
        await msg.answer("📋 Тип: discount/holiday/ref_contest/custom"); return
    if action=="admin_promo_type":
        user_states.pop(uid,None); pid=create_promotion(state.get("name"),msg.text.strip().lower(),state.get("btn",""))
        log_action(uid,"promo",str(pid)); await msg.answer(f"✅ #{pid}"); return

    if action=="admin_user_search":
        user_states.pop(uid,None); target=find_user(msg.text.strip())
        if not target: await msg.answer("❌"); return
        await show_user_panel(msg,target); return

    if action=="admin_user_set_free":
        user_states.pop(uid,None)
        try: count=int(msg.text.strip())
        except: await msg.answer("❌"); return
        set_free_searches(state["target"],count); log_action(uid,"set_free",f"{state['target']}={count}")
        await msg.answer(f"✅ {count}"); return

    if action=="admin_user_add_searches":
        user_states.pop(uid,None)
        try: count=int(msg.text.strip())
        except: await msg.answer("❌"); return
        add_extra_searches(state["target"],count); log_action(uid,"add_searches",f"{state['target']}+{count}")
        await msg.answer(f"✅ +{count}"); return

    if action=="admin_user_set_balance":
        user_states.pop(uid,None)
        try: amount=float(msg.text.strip())
        except: await msg.answer("❌"); return
        set_balance(state["target"],amount); log_action(uid,"set_bal",f"{state['target']}={amount}")
        await msg.answer(f"✅ {amount:.1f}⭐"); return

    if action=="admin_user_add_days":
        user_states.pop(uid,None)
        try: days=int(msg.text.strip())
        except: await msg.answer("❌"); return
        end=give_subscription(state["target"],days); log_action(uid,"add_days",f"{state['target']}+{days}")
        await msg.answer(f"✅ +{days}дн до {end}"); return

    # ═══ НОВОЕ: Админ VIP ═══
    if action=="admin_user_add_vip_days":
        user_states.pop(uid,None)
        try: days=int(msg.text.strip())
        except: await msg.answer("❌"); return
        end=give_vip(state["target"],days); log_action(uid,"add_vip",f"{state['target']}+{days}")
        await msg.answer(f"✅ 🌟 +{days}дн VIP до {end}"); return

    if action=="admin_user_msg":
        user_states.pop(uid,None)
        try: await bot.send_message(state["target"],msg.text.strip(),parse_mode="HTML"); await msg.answer("✅")
        except Exception as e: await msg.answer(f"❌ {e}")
        return

    if action=="admin_create_promo_code":
        code = msg.text.strip().upper()
        if code == "AUTO":
            code = f"PROMO-{secrets.token_hex(3).upper()}"
        if len(code) < 3:
            await msg.answer("❌ Минимум 3 символа"); return
        user_states[uid] = {"action":"admin_create_promo_discount","code":code}
        await msg.answer(f"🏷 Код: <code>{code}</code>\n\nВведите скидку:\n• <code>10%</code> — процент\n• <code>50</code> — фикс в ⭐", parse_mode="HTML"); return

    if action=="admin_create_promo_discount":
        text_val = msg.text.strip()
        percent = 0; stars_disc = 0
        if text_val.endswith("%"):
            try: percent = int(text_val[:-1]); assert 1 <= percent <= 99
            except: await msg.answer("❌ 1-99%"); return
        else:
            try: stars_disc = int(text_val); assert stars_disc > 0
            except: await msg.answer("❌ Число > 0"); return
        user_states[uid] = {"action":"admin_create_promo_uses","code":state["code"],
                            "percent":percent,"stars":stars_disc}
        await msg.answer("🔢 Макс использований (число):"); return

    if action=="admin_create_promo_uses":
        try: max_uses = int(msg.text.strip()); assert max_uses > 0
        except: await msg.answer("❌ Число > 0"); return
        user_states.pop(uid, None)
        ok = create_promocode(state["code"], discount_percent=state["percent"],
                              discount_stars=state["stars"], max_uses=max_uses,
                              created_by=uid)
        if ok:
            disc = f"-{state['percent']}%" if state['percent'] else f"-{state['stars']}⭐"
            log_action(uid, "promo_create", state["code"])
            await msg.answer(f"✅ <b>Промокод создан!</b>\n\n<code>{state['code']}</code>\n{disc}\nМакс: {max_uses}", parse_mode="HTML")
        else:
            await msg.answer("❌ Код уже существует")
        return
    
    if action=="admin_add_session_api_id":
        try: api_id=int(msg.text.strip())
        except: await msg.answer("❌ Число"); return
        user_states[uid]={"action":"admin_add_session_api_hash","api_id":api_id}
        await msg.answer("<b>api_hash:</b>",parse_mode="HTML"); return

    if action=="admin_add_session_api_hash":
        ah=msg.text.strip()
        if len(ah)<10: await msg.answer("❌"); return
        user_states[uid]={"action":"admin_add_session_phone","api_id":state["api_id"],"api_hash":ah}
        await msg.answer("<b>Телефон (с +):</b>",parse_mode="HTML"); return

    if action=="admin_add_session_phone":
        phone=msg.text.strip()
        if not phone.startswith("+"): await msg.answer("❌ С +"); return
        user_states.pop(uid,None)
        wm=await msg.answer("🔄...")
        try:
            cp=phone.replace("+","").replace(" ","")
            cl=TelegramClient(f"sessions/s_{cp}",state["api_id"],state["api_hash"],
                              connection_retries=5,retry_delay=3,timeout=15,request_retries=2)
            await cl.connect()
            if not await cl.is_user_authorized(): await cl.start(phone=phone)
            idx=len(pool.clients); pool.clients.append(cl); pool._init(idx)
            new_acc={"api_id":state["api_id"],"api_hash":state["api_hash"],"phone":phone}
            try:
                existing=[]
                if os.path.exists("added_sessions.json"):
                    with open("added_sessions.json") as f: existing=json.load(f)
                existing.append(new_acc)
                with open("added_sessions.json","w") as f: json.dump(existing,f,indent=2)
            except: pass
            log_action(uid,"add_session",phone)
            try: await wm.delete()
            except: pass
            await msg.answer(f"✅ #{idx+1} {phone}",parse_mode="HTML")
        except Exception as e:
            try: await wm.delete()
            except: pass
            await msg.answer(f"❌ <code>{str(e)[:200]}</code>",parse_mode="HTML")
        return

    if action=="admin_refs_check_input":
        user_states.pop(uid,None); target=find_user(msg.text.strip())
        if not target: await msg.answer("❌"); return
        fraud=check_referral_fraud(target); refs=get_user_referrals(target,10); u=get_user(target)
        name=f"@{u.get('uname','')}" if u.get('uname') else f"ID:{target}"
        text=f"🔍 <b>{name}</b>\n\n👥 {u.get('ref_count',0)}\n\n"
        if fraud["fraud"]: text+=f"⚠️ <b>НАКРУТ!</b>\n{fraud['reason']}\n\n"
        else: text+="✅ Ок\n\n"
        for r in refs[:10]:
            rn=f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"; text+=f"• {rn} {r['created']}\n"
        kb=InlineKeyboardBuilder(); kb.button(text="👤",callback_data=f"au_back_{target}"); kb.button(text="🔙",callback_data="a_refs"); kb.adjust(1)
        await msg.answer(text,reply_markup=kb.as_markup(),parse_mode="HTML"); return

    # Конфиг
    if action=="ctl_set_value":
        user_states.pop(uid,None); key=state.get("key",""); value=msg.text.strip()
        config=load_bot_config()
        if key.startswith("price_"):
            pk=key[6:]
            if pk in PRICES:
                try: PRICES[pk]["stars"]=int(value); config.setdefault("prices",{})[pk]=int(value); save_bot_config(config); await msg.answer(f"✅ {value}⭐")
                except: await msg.answer("❌ Число")
            return
        int_keys=["free_searches","free_count","premium_count","vip_count","premium_searches_limit","vip_searches_limit","search_price_stars","ref_bonus","search_cooldown","min_withdraw"]
        if key in int_keys:
            try: config[key]=int(value)
            except: await msg.answer("❌ Число"); return
        else: config[key]=value
        save_bot_config(config); apply_config(config); log_action(uid,"config",f"{key}={value}")
        await msg.answer(f"✅ <code>{key}</code> = <code>{value}</code>",parse_mode="HTML"); return

    if action=="ctl_set_text":
        user_states.pop(uid,None); key=state.get("key",""); config=load_bot_config()
        config[key]=msg.text.strip(); save_bot_config(config); log_action(uid,"config_text",key)
        await msg.answer("✅ Текст обновлён"); return

    if action=="ctl_add_channel":
        user_states.pop(uid,None); ch=msg.text.strip().replace("@","")
        if not ch: await msg.answer("❌"); return
        config=load_bot_config(); channels=config.get("required_channels",[])
        if ch not in channels: channels.append(ch)
        config["required_channels"]=channels; save_bot_config(config); apply_config(config)
        log_action(uid,"ch_add",ch); await msg.answer(f"✅ @{ch}"); return

    # Дефолт
    ns=await check_subscribed(uid)
    if ns: t,k=build_sub_kb(ns)
    else: t,k=build_menu(uid)
    await msg.answer(t,reply_markup=k,parse_mode="HTML",disable_web_page_preview=True)


# ═══════════════════════ АДМИНКА ═══════════════════════

@dp.callback_query(F.data == "cmd_admin")
async def cb_admin(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    s = get_stats(); ps = pool.stats()
    sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
    text = (
        f"👑 <b>v25.0</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 <code>{s['users']}</code> | 💎 <code>{s['subs']}</code> | 🚫 <code>{s['banned']}</code>\n"
        f"🔍 <code>{s['searches']}</code> | 📅 👤<code>{s['today_users']}</code> 🔍<code>{s['today_searches']}</code>\n\n"
        f"🔄 {sl}\n📊 <code>{ps['checks']}</code> | 🛡 <code>{ps['botapi_saves']+ps.get('recheck_saves',0)}</code>\n\n"
        f"👁 <code>{s['monitors']}</code> | ⛔ <code>{s['blacklist']}</code> | 📱 <code>{s['tasks']}</code> | 💸 <code>{s['withdrawals']}</code>")
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Юзер", callback_data="a_user")
    kb.button(text="💎 Premium", callback_data="a_plist")
    kb.button(text="📩 Выдать", callback_data="a_give")
    kb.button(text="🔑 Ключ", callback_data="a_keys")
    kb.button(text="🚫 Бан", callback_data="a_ban")
    kb.button(text="✅ Разбан", callback_data="a_unban")
    kb.button(text="👥 Рефералы", callback_data="a_refs")
    kb.button(text="📢 Акции", callback_data="a_promos")
    kb.button(text="⛔ Чёрный список", callback_data="a_blacklist")
    kb.button(text="🎁 Розыгрыш", callback_data="a_raffle")
    kb.button(text=f"📱 TT ({s['tasks']})", callback_data="a_tt")
    kb.button(text=f"💸 Выводы ({s['withdrawals']})", callback_data="a_wd")
    kb.button(text="📤 Рассылка", callback_data="a_bcast")
    kb.button(text="📊 Экспорт", callback_data="a_export")
    kb.button(text=f"🔄 Сессии ({ps['active']}/{ps['total']})", callback_data="a_sessions")
    kb.button(text="📋 Лог", callback_data="a_log")
    kb.button(text="⚙️ Управление", callback_data="a_control")
    kb.button(text="📥 Обновить код", callback_data="a_update")
    kb.button(text="🔄 Перезапуск", callback_data="a_restart")
    kb.button(text="📋 Логи", callback_data="a_logs")
    kb.button(text="💻 Сервер", callback_data="a_server")
    pending_m = len(market_get_pending())
    disputes_m = len(market_get_disputes())
    kb.button(text=f"📦 Маркет ({pending_m})", callback_data="a_mmod")
    kb.button(text=f"⚠️ Споры ({disputes_m})", callback_data="a_mdisputes")
    kb.button(text="🏷 Промокоды", callback_data="a_promocodes")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(2)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_user")
async def cb_a_user(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action":"admin_user_search"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "👤 <b>ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("au_"))
async def cb_au(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    parts = cb.data.split("_"); action = parts[1]; target = int(parts[2])

    if action == "adds":
        user_states[cb.from_user.id] = {"action":"admin_user_add_searches","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "🔍 <b>Сколько добавить?</b>", kb.as_markup())
    elif action == "sets":
        user_states[cb.from_user.id] = {"action":"admin_user_set_free","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "🔍 <b>Установить free:</b>", kb.as_markup())
    elif action == "setb":
        user_states[cb.from_user.id] = {"action":"admin_user_set_balance","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "💰 <b>Баланс:</b>", kb.as_markup())
    elif action == "addd":
        user_states[cb.from_user.id] = {"action":"admin_user_add_days","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "💎 <b>Дней:</b>", kb.as_markup())
    elif action == "remd":
        remove_subscription(target); log_action(cb.from_user.id,"remove_sub",str(target))
        await show_user_panel(cb, target)
    # ═══ НОВОЕ: VIP управление ═══
    elif action == "addv":
        user_states[cb.from_user.id] = {"action":"admin_user_add_vip_days","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "🌟 <b>Дней VIP:</b>", kb.as_markup())
    elif action == "remv":
        remove_vip(target); log_action(cb.from_user.id,"remove_vip",str(target))
        await show_user_panel(cb, target)
    elif action == "resetd":
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET daily_searches_used=0 WHERE uid=?", (target,))
        conn.commit(); conn.close()
        log_action(cb.from_user.id,"reset_daily",str(target))
        await show_user_panel(cb, target)
    elif action == "ban":
        ban_user(target); log_action(cb.from_user.id,"ban",str(target))
        await show_user_panel(cb, target)
    elif action == "unban":
        unban_user(target); log_action(cb.from_user.id,"unban",str(target))
        await show_user_panel(cb, target)
    elif action == "hist":
        hist = get_history(target, 15)
        text = f"📜 <b>{target}</b>\n\n"
        for h in hist: text += f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
        if not hist: text += "<i>Пусто</i>"
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, text, kb.as_markup())
    elif action == "refs":
        refs = get_user_referrals(target, 15)
        text = f"👥 <b>{target}</b>\n\n"
        for r in refs:
            name = f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"
            text += f"• {name} {r['created']}\n"
        if not refs: text += "<i>Пусто</i>"
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, text, kb.as_markup())
    elif action == "msg":
        user_states[cb.from_user.id] = {"action":"admin_user_msg","target":target}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data=f"au_back_{target}")
        await edit_msg(cb.message, "📤 <b>Сообщение:</b>", kb.as_markup())
    elif action == "back":
        await show_user_panel(cb, target)

@dp.callback_query(F.data == "a_plist")
async def cb_aplist(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    users = get_premium_users()
    text = f"💎 <b>Premium ({len(users)})</b>\n\n"
    kb = InlineKeyboardBuilder()
    for u in users[:20]:
        name = f"@{u['uname']}" if u['uname'] else f"ID:{u['uid']}"
        vip_mark = " 🌟" if has_vip(u['uid']) else ""
        text += f"• {name}{vip_mark} — {u['sub_end']}\n"
        kb.button(text=f"👤 {name}", callback_data=f"au_back_{u['uid']}")
    if not users: text += "<i>Нет</i>"
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_sessions")
async def cb_asessions(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    ps = pool.stats(); detail = pool.detailed_status()
    text = (
        f"🔄 <b>Сессии</b>\n\n"
        f"🟢 {ps['active']-ps.get('warming',0)} | 🟡 {ps.get('warming',0)} | 🟠 {ps.get('cooldown',0)} | 🔴 {ps.get('dead',0)}\n"
        f"📊 {ps['checks']} | ❌ {ps.get('errors',0)} | 🛡 {ps['botapi_saves']+ps.get('recheck_saves',0)} | 🔄 {ps.get('reconnects',0)}\n\n"
        f"<pre>{detail}</pre>")
    kb = InlineKeyboardBuilder()
    for i in range(len(pool.clients)):
        st = pool.status.get(i,'dead')
        if st=='dead': kb.button(text=f"🔄 #{i+1}", callback_data=f"a_revive_{i}")
        else: kb.button(text=f"💀 #{i+1}", callback_data=f"a_kill_{i}")
    kb.button(text="➕ Добавить", callback_data="a_add_session")
    kb.button(text="⚡ Reconnect all", callback_data="a_reconnect_all")
    kb.button(text="🔄 Обновить", callback_data="a_sessions")
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(3)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("a_kill_"))
async def cb_kill(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); i=int(cb.data[7:])
    if i<len(pool.clients):
        pool.status[i]='dead'; pool.cooldown_until[i]=time.time()+9999
        try: await pool.clients[i].disconnect()
        except: pass
        log_action(cb.from_user.id,"kill_session",str(i))
    await cb_asessions(cb)

@dp.callback_query(F.data.startswith("a_revive_"))
async def cb_revive(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); i=int(cb.data[9:])
    if i<len(pool.clients):
        await pool._try_reconnect(i); log_action(cb.from_user.id,"revive",str(i))
    await cb_asessions(cb)

@dp.callback_query(F.data == "a_reconnect_all")
async def cb_reconnect(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); await edit_msg(cb.message, "🔄...")
    r=0
    for i in range(len(pool.clients)):
        if pool.status.get(i)=='dead':
            await pool._try_reconnect(i)
            if pool.status.get(i)!='dead': r+=1
            await asyncio.sleep(2)
    log_action(cb.from_user.id,"reconnect_all",str(r))
    await cb_asessions(cb)

@dp.callback_query(F.data == "a_add_session")
async def cb_add_sess(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action":"admin_add_session_api_id"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="a_sessions")
    await edit_msg(cb.message, "➕ <b>api_id:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_keys")
async def cb_akeys(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=p["label"], callback_data=f"gk_{p['days']}")
    kb.button(text="✏️ Своё", callback_data="gk_custom"); kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message, "🔑 <b>Срок:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("gk_"))
async def cb_gk(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); val=cb.data[3:]
    if val=="custom":
        user_states[cb.from_user.id]={"action":"admin_key_days"}
        kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="a_keys")
        await edit_msg(cb.message,"✏️ <b>Дней:</b>",kb.as_markup()); return
    days=int(val); key=generate_key(days,f"D{days}"); log_action(cb.from_user.id,"key",key)
    kb=InlineKeyboardBuilder(); kb.button(text="🔑 Ещё",callback_data="a_keys"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message, f"🔑 <code>{key}</code>\n{days}дн", kb.as_markup())

@dp.callback_query(F.data == "a_give")
async def cb_agive(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_give_user"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_admin")
    await edit_msg(cb.message,"📩 <b>ID/@username:</b>",kb.as_markup())

@dp.callback_query(F.data == "a_ban")
async def cb_aban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_ban_input"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_admin")
    await edit_msg(cb.message,"🚫 <b>ID/@username:</b>",kb.as_markup())

@dp.callback_query(F.data == "a_unban")
async def cb_aunban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_unban_input"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_admin")
    await edit_msg(cb.message,"✅ <b>ID/@username:</b>",kb.as_markup())

@dp.callback_query(F.data == "a_tt")
async def cb_att(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); tasks=get_pending_tasks()
    if not tasks:
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_admin")
        await edit_msg(cb.message,"📱 Нет",kb.as_markup()); return
    text=f"📱 <b>({len(tasks)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for t in tasks:
        text+=f"#{t['id']} | {t['uid']} | {t['created']}\n"
        kb.button(text=f"✅#{t['id']}",callback_data=f"ta_{t['id']}"); kb.button(text=f"❌#{t['id']}",callback_data=f"tr_{t['id']}")
    kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "a_wd")
async def cb_awd(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wds=get_pending_withdrawals()
    if not wds:
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_admin")
        await edit_msg(cb.message,"💸 Нет",kb.as_markup()); return
    text=f"💸 <b>({len(wds)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for w in wds:
        text+=f"#{w['id']} | {w['uid']} | {w['amount']:.1f}⭐\n"
        kb.button(text=f"✅#{w['id']}",callback_data=f"wd_ok_{w['id']}"); kb.button(text=f"❌#{w['id']}",callback_data=f"wd_no_{w['id']}")
    kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "a_bcast")
async def cb_abcast(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_broadcast_text"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="cmd_admin")
    await edit_msg(cb.message,"📤 <b>Текст (HTML):</b>",kb.as_markup())

@dp.callback_query(F.data == "a_export")
async def cb_aexport(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid,uname,joined,searches,sub_end,ref_count,banned,balance FROM users ORDER BY uid")
    rows=c.fetchall(); conn.close()
    content=f"USERS — {len(rows)}\n{'='*40}\n\n"
    for r in rows:
        name=f"@{r[1]}" if r[1] else f"ID:{r[0]}"
        content+=f"{name} | s:{r[3]} | ref:{r[5]} | bal:{r[7] or 0:.1f}⭐\n"
    await bot.send_document(cb.from_user.id,BufferedInputFile(content.encode(),filename=f"users_{datetime.now().strftime('%Y%m%d')}.txt"),caption=f"📊 {len(rows)}")

@dp.callback_query(F.data == "a_refs")
async def cb_arefs(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    top=get_ref_top(10); text="👥 <b>Топ рефералов</b>\n\n"
    for i,t in enumerate(top,1):
        medals={1:"🥇",2:"🥈",3:"🥉"}; m=medals.get(i,f"{i}.")
        name=f"@{t['uname']}" if t['uname'] else f"ID:{t['uid']}"
        fraud=check_referral_fraud(t['uid']); w=" ⚠️" if fraud['fraud'] else ""
        text+=f"{m} {name} — <code>{t['ref_count']}</code>{w}\n"
    if not top: text+="<i>Нет</i>"
    kb=InlineKeyboardBuilder()
    kb.button(text="🔍 Проверить",callback_data="a_refs_check")
    kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "a_refs_check")
async def cb_arefs_check(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_refs_check_input"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="a_refs")
    await edit_msg(cb.message,"🔍 <b>ID/@username:</b>",kb.as_markup())

@dp.callback_query(F.data == "a_promos")
async def cb_apromos(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); promos=get_active_promotions()
    text=f"📢 <b>({len(promos)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for pr in promos:
        text+=f"• #{pr['id']} {pr['name']} ({pr['ptype']})\n"
        kb.button(text=f"❌ #{pr['id']}",callback_data=f"a_endp_{pr['id']}")
    if not promos: text+="<i>Нет</i>"
    kb.button(text="➕",callback_data="a_addp"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "a_addp")
async def cb_aaddp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_promo_name"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="a_promos")
    await edit_msg(cb.message,"📢 <b>Название:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("a_endp_"))
async def cb_aendp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); end_promotion(int(cb.data[7:])); log_action(cb.from_user.id,"promo_end",cb.data[7:])
    await cb_apromos(cb)

@dp.callback_query(F.data == "a_blacklist")
async def cb_abl(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); bl=get_blacklist()
    text=f"⛔ <b>({len(bl)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for item in bl[:20]:
        text+=f"• <code>@{item['username']}</code>\n"
        kb.button(text=f"❌ {item['username']}",callback_data=f"bl_del_{item['username']}")
    if not bl: text+="<i>Пусто</i>"
    kb.button(text="➕",callback_data="bl_add"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(2,1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "bl_add")
async def cb_bl_add(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"admin_blacklist_add"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="a_blacklist")
    await edit_msg(cb.message,"⛔ <b>Username:</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("bl_del_"))
async def cb_bl_del(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); remove_blacklist(cb.data[7:]); log_action(cb.from_user.id,"bl_del",cb.data[7:])
    await cb_abl(cb)

@dp.callback_query(F.data == "a_raffle")
async def cb_araffle(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    conn=sqlite3.connect(DB); c=conn.cursor()
    total=c.execute("SELECT COUNT(*) FROM users WHERE banned=0").fetchone()[0]; conn.close()
    kb=InlineKeyboardBuilder()
    kb.button(text="🎁 1д",callback_data="raf_1"); kb.button(text="🎁 3д",callback_data="raf_3")
    kb.button(text="🎁 7д",callback_data="raf_7"); kb.button(text="🎁 30д",callback_data="raf_30")
    kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(2,2,1)
    await edit_msg(cb.message,f"🎁 <b>Розыгрыш</b>\n\n👥 {total}",kb.as_markup())

@dp.callback_query(F.data.startswith("raf_"))
async def cb_raf(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); days=int(cb.data[4:])
    conn=sqlite3.connect(DB); c=conn.cursor()
    c.execute("SELECT uid,uname FROM users WHERE banned=0"); au=c.fetchall(); conn.close()
    if not au: return
    w=random.choice(au); end=give_subscription(w[0],days)
    name=f"@{w[1]}" if w[1] else f"ID:{w[0]}"
    log_action(cb.from_user.id,"raffle",f"{w[0]}+{days}d")
    await edit_msg(cb.message,f"🎉 <b>{name}</b>\n🎁 {days}дн до {end}")
    try: await bot.send_message(w[0],f"🎉 <b>{days}дн Premium!</b> До: {end}",parse_mode="HTML")
    except: pass

@dp.callback_query(F.data == "a_log")
async def cb_alog(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); logs=get_action_log(30)
    text="📋 <b>Лог</b>\n\n"
    for l in logs:
        text+=f"<code>{l['created'][5:]}</code> {l['uid']} {l['action']}"
        if l['details']: text+=f" {l['details'][:15]}"
        text+="\n"
    if not logs: text+="<i>Пусто</i>"
    kb=InlineKeyboardBuilder(); kb.button(text="🔄",callback_data="a_log"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())


# ═══════════════════════ ПАНЕЛЬ УПРАВЛЕНИЯ ═══════════════════════

@dp.callback_query(F.data == "a_control")
async def cb_control(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    text = (f"⚙️ <b>Управление</b>\n\n"
            f"🆓 Free: {config['free_searches']} поисков, {config['free_count']} юзов\n"
            f"💎 Prem: {config['premium_count']} юзов, {config['premium_searches_limit']} поисков/день\n"
            f"🌟 VIP: {config.get('vip_count',5)} юзов, {config.get('vip_searches_limit',15)} поисков/день\n"
            f"💰 Цена: {config['search_price_stars']}⭐/поиск\n"
            f"👥 Реф: +{config['ref_bonus']}\n"
            f"💳 @{config['pay_contact']}")
    kb = InlineKeyboardBuilder()
    kb.button(text="🔢 Числа", callback_data="ctl_numbers")
    kb.button(text="📝 Тексты", callback_data="ctl_texts")
    kb.button(text="🔘 Кнопки", callback_data="ctl_buttons")
    kb.button(text="🔍 Режимы", callback_data="ctl_modes")
    kb.button(text="💎 Цены", callback_data="ctl_prices")
    kb.button(text="📢 Каналы", callback_data="ctl_channels")
    kb.button(text="📊 Статистика", callback_data="ctl_stats")
    kb.button(text="🔄 Сброс", callback_data="ctl_reset")
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "ctl_numbers")
async def cb_ctl_num(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Free поисков: {config['free_searches']}", callback_data="setv_free_searches")
    kb.button(text=f"Free юзов: {config['free_count']}", callback_data="setv_free_count")
    kb.button(text=f"Prem юзов: {config['premium_count']}", callback_data="setv_premium_count")
    kb.button(text=f"VIP юзов: {config.get('vip_count',5)}", callback_data="setv_vip_count")
    kb.button(text=f"Prem поисков: {config['premium_searches_limit']}", callback_data="setv_premium_searches_limit")
    kb.button(text=f"VIP поисков: {config.get('vip_searches_limit',15)}", callback_data="setv_vip_searches_limit")
    kb.button(text=f"Цена: {config['search_price_stars']}⭐", callback_data="setv_search_price_stars")
    kb.button(text=f"Реф: {config['ref_bonus']}", callback_data="setv_ref_bonus")
    kb.button(text=f"КД: {config['search_cooldown']}с", callback_data="setv_search_cooldown")
    kb.button(text=f"Вывод: {config['min_withdraw']}⭐", callback_data="setv_min_withdraw")
    kb.button(text=f"Контакт: @{config['pay_contact']}", callback_data="setv_pay_contact")
    kb.button(text="🔙", callback_data="a_control"); kb.adjust(1)
    await edit_msg(cb.message,"🔢 <b>Нажмите чтобы изменить</b>",kb.as_markup())

@dp.callback_query(F.data == "ctl_texts")
async def cb_ctl_texts(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    def pv(k):
        v=config.get(k,""); return f"<code>{v[:25]}...</code>" if len(v)>25 else (f"<code>{v}</code>" if v else "<i>—</i>")
    text=f"📝 <b>Тексты</b>\n\n🏠 {pv('text_welcome')}\n✅ {pv('text_found')}\n😔 {pv('text_empty')}\n👤 {pv('text_profile_header')}\n🏪 {pv('text_shop_header')}"
    kb=InlineKeyboardBuilder()
    kb.button(text="🏠 Приветствие",callback_data="sett_text_welcome")
    kb.button(text="✅ Найден",callback_data="sett_text_found")
    kb.button(text="😔 Пусто",callback_data="sett_text_empty")
    kb.button(text="👤 Профиль",callback_data="sett_text_profile_header")
    kb.button(text="🏪 Магазин",callback_data="sett_text_shop_header")
    kb.button(text="🗑 Сброс",callback_data="sett_reset_all")
    kb.button(text="🔙",callback_data="a_control"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "sett_reset_all")
async def cb_sett_reset(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    for k in ["text_welcome","text_found","text_empty","text_profile_header","text_shop_header"]: config[k]=""
    save_bot_config(config); await cb_ctl_texts(cb)

@dp.callback_query(F.data.startswith("sett_text_"))
async def cb_sett(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); key=cb.data[5:]
    user_states[cb.from_user.id]={"action":"ctl_set_text","key":key}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="ctl_texts")
    await edit_msg(cb.message,f"📝 <b>Новый текст (HTML):</b>",kb.as_markup())

@dp.callback_query(F.data == "ctl_buttons")
async def cb_ctl_btns(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    btns={"tiktok":"🎁 TikTok","roulette":"🎰 Рулетка","monitor":"👁 Мониторинг","shop":"🏪 Магазин","support":"🤖 Донат","referral":"👥 Рефералы"}
    text="🔘 <b>Кнопки</b>\n\n"; kb=InlineKeyboardBuilder()
    for k,n in btns.items():
        on=config.get(f"btn_{k}",True); icon="✅" if on else "❌"
        text+=f"{icon} {n}\n"; kb.button(text=f"{icon} {n}",callback_data=f"togbtn_{k}")
    kb.button(text="🔙",callback_data="a_control"); kb.adjust(2,2,2,1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("togbtn_"))
async def cb_togbtn(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); k=cb.data[7:]; config=load_bot_config()
    config[f"btn_{k}"]=not config.get(f"btn_{k}",True)
    save_bot_config(config); apply_config(config); await cb_ctl_btns(cb)

@dp.callback_query(F.data == "ctl_modes")
async def cb_ctl_modes(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    text="🔍 <b>Режимы</b>\n\n"; kb=InlineKeyboardBuilder()
    for key,m in SEARCH_MODES.items():
        on=config.get(f"mode_{key}",True); prem=config.get(f"mode_{key}_premium",m.get("_default_premium",False))
        ei="✅" if on else "❌"; pi="💎" if prem else "🆓"
        text+=f"{ei}{pi} {m['emoji']} {m['name']}\n"
        kb.button(text=f"{ei} {m['emoji']}",callback_data=f"togmode_{key}")
        kb.button(text=f"{pi}",callback_data=f"togprem_{key}")
    text+="\n✅❌ = вкл/выкл | 💎🆓 = prem/free"
    kb.button(text="🔙",callback_data="a_control"); kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("togmode_"))
async def cb_togmode(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); k=cb.data[8:]; config=load_bot_config()
    config[f"mode_{k}"]=not config.get(f"mode_{k}",True)
    save_bot_config(config); apply_config(config); await cb_ctl_modes(cb)

@dp.callback_query(F.data.startswith("togprem_"))
async def cb_togprem(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); k=cb.data[8:]; config=load_bot_config()
    config[f"mode_{k}_premium"]=not config.get(f"mode_{k}_premium",True)
    save_bot_config(config); apply_config(config); await cb_ctl_modes(cb)

@dp.callback_query(F.data == "ctl_prices")
async def cb_ctl_prices(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    text="💎 <b>Цены Premium</b>\n\n"; kb=InlineKeyboardBuilder()
    for k,p in PRICES.items():
        rub = int(p['stars'] * STAR_TO_RUB)
        text+=f"• {p['label']} — <code>{p['stars']}⭐</code> ({rub}₽)\n"
        kb.button(text=f"✏️ {p['label']}",callback_data=f"setv_price_{k}")
    kb.button(text="🔙",callback_data="a_control"); kb.adjust(2,1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "ctl_channels")
async def cb_ctl_ch(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config(); chs=config.get("required_channels",[])
    text=f"📢 <b>Каналы ({len(chs)})</b>\n\n"; kb=InlineKeyboardBuilder()
    for ch in chs: text+=f"• @{ch}\n"; kb.button(text=f"❌ {ch}",callback_data=f"ch_del_{ch}")
    if not chs: text+="<i>Нет</i>"
    kb.button(text="➕",callback_data="ch_add"); kb.button(text="🔙",callback_data="a_control"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data == "ch_add")
async def cb_ch_add(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id]={"action":"ctl_add_channel"}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="ctl_channels")
    await edit_msg(cb.message,"📢 <b>Username (без @):</b>",kb.as_markup())

@dp.callback_query(F.data.startswith("ch_del_"))
async def cb_ch_del(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); ch=cb.data[7:]; config=load_bot_config()
    chs=config.get("required_channels",[])
    if ch in chs: chs.remove(ch)
    config["required_channels"]=chs; save_bot_config(config); apply_config(config)
    await cb_ctl_ch(cb)

@dp.callback_query(F.data == "ctl_stats")
async def cb_ctl_stats(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); config=load_bot_config()
    conn=sqlite3.connect(DB); c=conn.cursor()
    text="📊 <b>7 дней</b>\n\n<pre>Дата     | 👤 | 🔍\n"
    for i in range(7):
        d=(datetime.now()-timedelta(days=i)).strftime("%Y-%m-%d")
        u=c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?",(d+"%",)).fetchone()[0]
        s=c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?",(d+"%",)).fetchone()[0]
        text+=f"{d[5:]} | {u:>3} | {s:>3}\n"
    text+="</pre>\n"
    conn.close()
    dr="✅" if config.get("daily_report",True) else "❌"
    np="✅" if config.get("notify_purchases",True) else "❌"
    text+=f"\n{dr} Ежедневный отчёт\n{np} Уведом. покупки"
    kb=InlineKeyboardBuilder()
    kb.button(text=f"{dr} Отчёт",callback_data="tog_daily_report")
    kb.button(text=f"{np} Покупки",callback_data="tog_notify_purchases")
    kb.button(text="📊 Полная",callback_data="ctl_stats_full")
    kb.button(text="🔙",callback_data="a_control"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("tog_"))
async def cb_tog(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); k=cb.data[4:]; config=load_bot_config()
    config[k]=not config.get(k,True); save_bot_config(config); await cb_ctl_stats(cb)

@dp.callback_query(F.data == "ctl_stats_full")
async def cb_stats_full(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    conn=sqlite3.connect(DB); c=conn.cursor()
    content="STATS\n"+"="*40+"\n\n"
    for i in range(30):
        d=(datetime.now()-timedelta(days=i)).strftime("%Y-%m-%d")
        u=c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?",(d+"%",)).fetchone()[0]
        s=c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?",(d+"%",)).fetchone()[0]
        content+=f"{d} | users:{u} | searches:{s}\n"
    conn.close()
    await bot.send_document(cb.from_user.id,BufferedInputFile(content.encode(),filename=f"stats_{datetime.now().strftime('%Y%m%d')}.txt"))

@dp.callback_query(F.data == "ctl_reset")
async def cb_ctl_reset(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    kb=InlineKeyboardBuilder(); kb.button(text="⚠️ Да",callback_data="ctl_reset_yes"); kb.button(text="❌",callback_data="a_control"); kb.adjust(1)
    await edit_msg(cb.message,"⚠️ <b>Сбросить ВСЕ?</b>",kb.as_markup())

@dp.callback_query(F.data == "ctl_reset_yes")
async def cb_ctl_reset_yes(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    if os.path.exists(BOT_CONFIG_FILE): os.remove(BOT_CONFIG_FILE)
    apply_config(DEFAULT_CONFIG); log_action(cb.from_user.id,"config_reset","all")
    kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="a_control")
    await edit_msg(cb.message,"✅ Сброшено",kb.as_markup())

@dp.callback_query(F.data.startswith("setv_"))
async def cb_setv(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); key=cb.data[5:]
    names={"free_searches":"Free поисков","free_count":"Free юзов","premium_count":"Prem юзов",
           "vip_count":"VIP юзов","premium_searches_limit":"Prem поисков/день",
           "vip_searches_limit":"VIP поисков/день","search_price_stars":"Цена/поиск ⭐",
           "pay_contact":"Контакт","ref_bonus":"Реф бонус","search_cooldown":"КД (сек)","min_withdraw":"Мин вывод ⭐"}
    if key.startswith("price_"):
        pk=key[6:]; p=PRICES.get(pk,{}); name=f"Цена {p.get('label',pk)} ⭐"
    else: name=names.get(key,key)
    config=load_bot_config()
    if key.startswith("price_"): current=PRICES.get(key[6:],{}).get("stars","?")
    elif key in config: current=config[key]
    else: current="?"
    user_states[cb.from_user.id]={"action":"ctl_set_value","key":key}
    kb=InlineKeyboardBuilder(); kb.button(text="❌",callback_data="ctl_numbers")
    await edit_msg(cb.message,f"✏️ <b>{name}</b>\n\nСейчас: <code>{current}</code>\n\nНовое значение:",kb.as_markup())


# ═══════════════════════ СЕРВЕР ═══════════════════════

@dp.callback_query(F.data == "a_update")
async def cb_update(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); await edit_msg(cb.message,"📥 Скачиваю...")
    try:
        result=subprocess.run(["curl","-s","https://raw.githubusercontent.com/yarmukhamedovsergey-creator/app.zip/main/bot.py","-o","/root/bot.py"],
                              capture_output=True,text=True,timeout=60)
        if result.returncode==0:
            kb=InlineKeyboardBuilder(); kb.button(text="🔄 Перезапуск",callback_data="a_restart"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
            await edit_msg(cb.message,"✅ <b>Обновлено!</b>",kb.as_markup())
        else:
            kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_admin")
            await edit_msg(cb.message,f"❌ {result.stderr[:300]}",kb.as_markup())
    except Exception as e:
        kb=InlineKeyboardBuilder(); kb.button(text="🔙",callback_data="cmd_admin")
        await edit_msg(cb.message,f"❌ {e}",kb.as_markup())

@dp.callback_query(F.data == "a_restart")
async def cb_restart(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); log_action(cb.from_user.id,"restart","")
    for aid in ADMIN_IDS:
        try: await bot.send_message(aid,"🔄 <b>Перезапуск...</b>",parse_mode="HTML")
        except: pass
    await asyncio.sleep(1)
    try: await pool.disconnect()
    except: pass
    try: await http_session.close()
    except: pass
    os.execv(sys.executable,[sys.executable]+sys.argv)

@dp.callback_query(F.data == "a_logs")
async def cb_logs(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    try:
        r=subprocess.run(["journalctl","-u","hunter","-n","40","--no-pager"],capture_output=True,text=True,timeout=10)
        logs=r.stdout[-3500:] if r.stdout else "Нет логов"
    except: logs="systemd не настроен"
    kb=InlineKeyboardBuilder(); kb.button(text="🔄",callback_data="a_logs"); kb.button(text="📥",callback_data="a_logs_full"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message,f"📋 <b>Логи</b>\n\n<pre>{logs}</pre>",kb.as_markup())

@dp.callback_query(F.data == "a_logs_full")
async def cb_logs_full(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    try: r=subprocess.run(["journalctl","-u","hunter","-n","500","--no-pager"],capture_output=True,text=True,timeout=15); c=r.stdout or "Пусто"
    except: c="Ошибка"
    await bot.send_document(cb.from_user.id,BufferedInputFile(c.encode(),filename=f"logs_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"))

@dp.callback_query(F.data == "a_server")
async def cb_server(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    try: up=subprocess.run(["uptime","-p"],capture_output=True,text=True,timeout=5).stdout.strip()
    except: up="N/A"
    try: mem=subprocess.run(["free","-h"],capture_output=True,text=True,timeout=5).stdout
    except: mem="N/A"
    try: disk=subprocess.run(["df","-h","/"],capture_output=True,text=True,timeout=5).stdout
    except: disk="N/A"
    try: load=subprocess.run(["cat","/proc/loadavg"],capture_output=True,text=True,timeout=5).stdout.strip()
    except: load="N/A"
    text=f"💻 <b>Сервер</b>\n\n⏱ {up}\n⚡ {load}\n\n💾\n<pre>{mem}</pre>\n💿\n<pre>{disk}</pre>"
    kb=InlineKeyboardBuilder(); kb.button(text="🔄",callback_data="a_server"); kb.button(text="🔙",callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message,text,kb.as_markup())

# ═══════════════════════ АДМИН МАРКЕТПЛЕЙС ═══════════════════════

@dp.callback_query(F.data == "a_mmod")
async def cb_a_mmod(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    lots=market_get_pending()
    text=f"📦 <b>Модерация ({len(lots)})</b>\n\n"
    kb=InlineKeyboardBuilder()
    for l in lots:
        fast="⚡" if l["fast"] else ""; nft="💎" if l["is_nft"] else ""
        text+=f"{fast}{nft}#{l['id']} {l['title']} ({l['price']}⭐)\n"
        kb.button(text=f"✅#{l['id']}",callback_data=f"mmod_ok_{l['id']}")
        kb.button(text=f"❌#{l['id']}",callback_data=f"mmod_no_{l['id']}")
    if not lots: text+="<i>Нет</i>"
    kb.button(text="🔙 Админ",callback_data="cmd_admin")
    kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("mmod_ok_"))
async def cb_mmod_ok(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); lot_id=int(cb.data[8:])
    market_approve_lot(lot_id,cb.from_user.id)
    lot=market_get_lot(lot_id)
    if lot:
        try: await bot.send_message(lot["seller_uid"],"✅ <b>Лот одобрен!</b>",parse_mode="HTML")
        except: pass
    await cb_a_mmod(cb)

@dp.callback_query(F.data.startswith("mmod_no_"))
async def cb_mmod_no(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); lot_id=int(cb.data[8:])
    market_reject_lot(lot_id,cb.from_user.id)
    lot=market_get_lot(lot_id)
    if lot:
        try: await bot.send_message(lot["seller_uid"],"❌ <b>Лот отклонён</b>",parse_mode="HTML")
        except: pass
    await cb_a_mmod(cb)

@dp.callback_query(F.data == "a_mdisputes")
async def cb_a_mdisputes(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    disputes=market_get_disputes()
    text=f"⚠️ <b>Споры ({len(disputes)})</b>\n\n"
    kb=InlineKeyboardBuilder()
    for d in disputes:
        text+=f"#{d['id']} {d['title']} ({d['price']}⭐)\n  {d['reason']}\n\n"
        kb.button(text=f"👤 Продавец #{d['id']}",callback_data=f"dwin_s_{d['id']}")
        kb.button(text=f"👤 Покупатель #{d['id']}",callback_data=f"dwin_b_{d['id']}")
    if not disputes: text+="<i>Нет</i>"
    kb.button(text="🔙 Админ",callback_data="cmd_admin")
    kb.adjust(2)
    await edit_msg(cb.message,text,kb.as_markup())

@dp.callback_query(F.data.startswith("dwin_s_"))
async def cb_dwin_s(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); lot_id=int(cb.data[7:])
    market_resolve_dispute(lot_id,"seller",cb.from_user.id)
    await answer_cb(cb,"✅ Продавцу",show_alert=True)
    await cb_a_mdisputes(cb)

@dp.callback_query(F.data.startswith("dwin_b_"))
async def cb_dwin_b(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); lot_id=int(cb.data[7:])
    lot=market_get_lot(lot_id)
    ok,charge_id=market_resolve_dispute(lot_id,"buyer",cb.from_user.id)
    if ok and charge_id and lot:
        try: await bot.refund_star_payment(lot["buyer_uid"],charge_id)
        except: add_balance(lot["buyer_uid"],lot["price"])
    elif ok and lot: add_balance(lot["buyer_uid"],lot["price"])
    await answer_cb(cb,"✅ Возврат покупателю",show_alert=True)
    await cb_a_mdisputes(cb)

@dp.callback_query(F.data == "a_promocodes")
async def cb_a_promocodes(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    promos = get_all_promocodes()
    text = f"🏷 <b>Промокоды ({len(promos)})</b>\n\n"
    kb = InlineKeyboardBuilder()
    for p in promos[:20]:
        active = "✅" if p["active"] else "❌"
        disc = f"-{p['percent']}%" if p['percent'] else f"-{p['stars']}⭐"
        text += f"{active} <code>{p['code']}</code> {disc} ({p['used']}/{p['max']})\n"
        if p["active"]:
            kb.button(text=f"❌ {p['code']}", callback_data=f"deact_promo_{p['code']}")
    if not promos: text += "<i>Нет</i>"
    kb.button(text="➕ Создать", callback_data="create_promo")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2,1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("deact_promo_"))
async def cb_deact_promo(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    code = cb.data[12:]
    deactivate_promocode(code)
    log_action(cb.from_user.id, "promo_deact", code)
    await cb_a_promocodes(cb)

@dp.callback_query(F.data == "create_promo")
async def cb_create_promo(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action":"admin_create_promo_code"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="a_promocodes")
    await edit_msg(cb.message,
        "🏷 <b>Создание промокода</b>\n\n"
        "Введите код (или <code>auto</code> для автогенерации):", kb.as_markup())

# ═══════════════════════ ФОНОВЫЕ ЗАДАЧИ ═══════════════════════

async def reminder_loop():
    while True:
        try:
            await asyncio.sleep(REMINDER_CHECK_INTERVAL)
            today=datetime.now().strftime("%Y-%m-%d")
            for db in REMINDER_DAYS:
                for u in get_expiring_users(db):
                    rk=f"{today}_d{db}"
                    if rk in u.get("last_reminder",""): continue
                    kb=InlineKeyboardBuilder(); kb.button(text="💰 Продлить",callback_data="shop_premium")
                    try:
                        await bot.send_message(u["uid"],f"🔔 Подписка через {db} дн!\n⏰ {u['sub_end']}",reply_markup=kb.as_markup(),parse_mode="HTML")
                        set_last_reminder(u["uid"],rk)
                    except: pass
        except Exception as e: logger.error(f"Reminder: {e}")

async def monitor_loop():
    await asyncio.sleep(60)
    while True:
        try:
            expire_monitors()
            for mon in get_active_monitors():
                try:
                    status=await pool.check(mon["username"])
                    if status in ("free","maybe_free"):
                        update_monitor_status(mon["id"],"free")
                        try:
                            kb=InlineKeyboardBuilder(); kb.button(text="📊",callback_data=f"eval_{mon['username']}")
                            await bot.send_message(mon["uid"],f"🎉 <b>@{mon['username']} свободен!</b>",reply_markup=kb.as_markup(),parse_mode="HTML")
                        except: pass
                    else: update_monitor_status(mon["id"],"taken")
                    await asyncio.sleep(3)
                except: pass
        except Exception as e: logger.error(f"Monitor: {e}")
        await asyncio.sleep(MONITOR_CHECK_INTERVAL)

async def session_watchdog():
    await asyncio.sleep(60); last_alert=0
    while True:
        try:
            ps=pool.stats(); dead=ps.get('dead',0)
            if dead>0:
                for i in range(len(pool.clients)):
                    if pool.status.get(i)=='dead': await pool._try_reconnect(i); await asyncio.sleep(10)
            ps2=pool.stats(); now=time.time()
            if ps2['active']<=max(ps['total']//2,1) and ps['total']>0 and now-last_alert>3600:
                last_alert=now
                await notify_admins(f"⚠️ Сессий: {ps2['active']}/{ps['total']}")
        except Exception as e: logger.error(f"Watchdog: {e}")
        await asyncio.sleep(900)

async def daily_report_loop():
    while True:
        try:
            now=datetime.now(); config=load_bot_config()
            if now.hour==config.get("daily_report_hour",23) and now.minute==0 and config.get("daily_report",True):
                s=get_stats(); ps=pool.stats(); today=now.strftime("%Y-%m-%d")
                conn=sqlite3.connect(DB); c=conn.cursor()
                ts=c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?",(today+"%",)).fetchone()[0]
                tu=c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?",(today+"%",)).fetchone()[0]
                conn.close()
                await notify_admins(
                    f"📊 <b>Отчёт {today}</b>\n\n👤 +{tu} | 🔍 {ts}\n👥 {s['users']} | 💎 {s['subs']}\n🔄 {ps['active']}/{ps['total']}")
                if config.get("notify_milestones",True):
                    for m in [50,100,250,500,1000,5000]:
                        if s['users']>=m and s['users']-tu<m:
                            await notify_admins(f"🎉 <b>{m} юзеров!</b>"); break
        except: pass
        await asyncio.sleep(60)


# ═══════════════════════ SYSTEMD ═══════════════════════

def setup_systemd():
    sp="/etc/systemd/system/hunter.service"
    if os.path.exists(sp): return
    if os.geteuid()!=0: logger.warning("Не root — systemd не настроен"); return
    bp=os.path.abspath(__file__); bd=os.path.dirname(bp); pp=sys.executable
    try:
        with open(sp,"w") as f:
            f.write(f"[Unit]\nDescription=Hunter Bot\nAfter=network.target\n\n[Service]\nType=simple\nUser=root\nWorkingDirectory={bd}\nExecStart={pp} {bp}\nRestart=always\nRestartSec=3\nEnvironment=PYTHONUNBUFFERED=1\n\n[Install]\nWantedBy=multi-user.target\n")
        subprocess.run(["systemctl","daemon-reload"],check=True)
        subprocess.run(["systemctl","enable","hunter"],check=True)
        logger.info("✅ Systemd OK")
    except Exception as e: logger.error(f"Systemd: {e}")


# ═══════════════════════ MAIN ═══════════════════════

async def main():
    global http_session, bot_info
    init_db(); setup_systemd()
    config=load_bot_config(); apply_config(config)
    if "prices" in config:
        for k,v in config["prices"].items():
            if k in PRICES: PRICES[k]["stars"]=v
    bot_info=await bot.get_me(); http_session=aiohttp.ClientSession()
    await pool.init(ACCOUNTS); ps=pool.stats()
    logger.info("━"*30)
    logger.info(f"🚀 @{bot_info.username} v25.0")
    logger.info(f"🔄 {ps['total']} сессий")
    logger.info("━"*30)
    asyncio.create_task(reminder_loop())
    asyncio.create_task(session_watchdog())
    asyncio.create_task(monitor_loop())
    asyncio.create_task(daily_report_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_session.close(); await pool.disconnect()

if __name__=="__main__":
    asyncio.run(main())
