"""
USERNAME HUNTER v21.0 — ПОЛНАЯ ПЕРЕРАБОТКА
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
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile, FSInputFile,
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
REQUIRED_CHANNELS = ["SwordUsers"]

ACCOUNTS = [
    {"api_id": 35094180, "api_hash": "8732d865063dadaf1cba0ace1ef87de9", "phone": "+959790770236"},
    {"api_id": 34992704, "api_hash": "d54449feb7289284c9e4598911d08068", "phone": "+959973228130"},
    {"api_id": 36284654, "api_hash": "1073109c2e1085dd601ad289a9a65562", "phone": "+67077454464"},
    {"api_id": 34792667, "api_hash": "fc2eb570576ddc72819a5ba22f8c0f5d", "phone": "+959980062721"},
    {"api_id": 36347986, "api_hash": "2ef08b03748cdf3b688efc18a1e540b7", "phone": "+13347793071"},
    {"api_id": 36037729, "api_hash": "c48c8326dfb577fd4b8d503cb7dce2a4", "phone": "+19316345068"},
    {"api_id": 36360664, "api_hash": "facb9902e2eafe009a2fb43c901c2328", "phone": "+959694410210"},
    {"api_id": 38047070, "api_hash": "8132d885c41d0db88c345a868de305e5", "phone": "+15029264416"},
    {"api_id": 37487174, "api_hash": "65168477b7764d3163d8a3b2bc3e9006", "phone": "+18633585097"},
    {"api_id": 36689382, "api_hash": "68e81cc3a654222750230c8bafff6c2c", "phone": "+18598807376"},
    {"api_id": 34009805, "api_hash": "64daace78ea1ca5935e8b1f73dfd0280", "phone": "+15029264472"},
]

FREE_SEARCHES = 3
FREE_COUNT = 1
PREMIUM_COUNT = 3
PREMIUM_SEARCHES_LIMIT = 7
REF_BONUS = 2
REFERRAL_COMMISSION = 0.04
SEARCH_COOLDOWN = 10
MIN_WITHDRAW = 50
ACCOUNT_MIN_DELAY = 3.0

TIKTOK_COMMENT_TEXT = "@SworuserN_bot бесплатные звёзды, найти юз, оценить юз"
TIKTOK_REWARD_GIFT = "🧸 Мишка (15⭐)"
TIKTOK_SCREENSHOTS_NEEDED = 35
TIKTOK_DAILY_LIMIT = 2
REMINDER_DAYS = [3, 1]
REMINDER_CHECK_INTERVAL = 3600

PRICES = {
    "1d":      {"days": 1,     "rub": 41,    "stars": 36,    "rub_orig": 45,    "stars_orig": 40,
                "label": "1 день",    "desc": "Попробуй Premium на 24 часа",
                "funpay": "https://funpay.com/lots/offer?id=65182705"},
    "3d":      {"days": 3,     "rub": 108,   "stars": 90,    "rub_orig": 120,   "stars_orig": 100,
                "label": "3 дня",     "desc": "Идеально для быстрого поиска",
                "funpay": "https://funpay.com/lots/offer?id=65182951"},
    "7d":      {"days": 7,     "rub": 225,   "stars": 180,   "rub_orig": 250,   "stars_orig": 200,
                "label": "7 дней",    "desc": "Неделя полного доступа",
                "funpay": "https://funpay.com/lots/offer?id=65182991"},
    "1m":      {"days": 30,    "rub": 720,   "stars": 585,   "rub_orig": 800,   "stars_orig": 650,
                "label": "1 месяц",   "desc": "Лучшее соотношение цена/качество",
                "funpay": "https://funpay.com/lots/offer?id=65183001"},
    "3m":      {"days": 90,    "rub": 1980,  "stars": 1800,  "rub_orig": 2200,  "stars_orig": 2000,
                "label": "3 месяца",  "desc": "Для серьёзных охотников",
                "funpay": "https://funpay.com/lots/offer?id=65183010"},
    "1y":      {"days": 365,   "rub": 7200,  "stars": 5850,  "rub_orig": 8000,  "stars_orig": 6500,
                "label": "1 год",     "desc": "Целый год без ограничений",
                "funpay": "https://funpay.com/lots/offer?id=65183025"},
    "forever": {"days": 99999, "rub": 11699, "stars": 8999,  "rub_orig": 12999, "stars_orig": 9999,
                "label": "Навсегда",  "desc": "Вечный доступ Premium",
                "funpay": "https://funpay.com/lots/offer?id=65183050"},
}

DONATE_OPTIONS = [20, 50, 100, 200, 300, 500, 1000]

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


# ═══════════════════════ EDIT / ANSWER ═══════════════════════

async def edit_msg(msg, text, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as e:
        logger.warning(f"edit fail: {e}")
    except Exception as e:
        logger.error(f"edit error: {e}")


async def answer_cb(cb, text=None, show_alert=False):
    try:
        await cb.answer(text, show_alert=show_alert)
    except:
        pass


# ═══════════════════════ ПУЛ АККАУНТОВ ═══════════════════════

class AccountPool:
    def __init__(self):
        self.clients = []
        self.index = 0
        self.lock = asyncio.Lock()
        self.cooldowns = {}
        self.last_used = {}
        self.check_counts = {}
        self.min_delay = ACCOUNT_MIN_DELAY
        self.total_checks = 0
        self.caught_by_botapi = 0
        self.caught_by_layer3 = 0
        self.active_users = {}
        self.max_users_per_account = 3

    async def init(self, accounts):
        if not HAS_TELETHON or not accounts:
            logger.info("Telethon not installed — Bot API mode")
            return
        for i, acc in enumerate(accounts):
            phone = acc["phone"].replace("+", "").replace(" ", "")
            try:
                client = TelegramClient(
                    f"sessions/s_{phone}", acc["api_id"], acc["api_hash"],
                    connection_retries=3, retry_delay=2)
                await client.connect()
                if not await client.is_user_authorized():
                    await client.start(phone=acc["phone"])
                self.clients.append(client)
                self.cooldowns[i] = 0
                self.last_used[i] = 0
                self.check_counts[i] = 0
                self.active_users[i] = set()
                logger.info(f"Session #{i+1}: {acc['phone']} OK")
            except Exception as e:
                logger.error(f"Session {acc['phone']}: {e}")
        logger.info(f"Sessions: {len(self.clients)}")

    def _get_available_account(self, uid):
        now = time.time()
        for i in range(len(self.clients)):
            idx = (self.index + i) % len(self.clients)
            if self.cooldowns.get(idx, 0) > now:
                continue
            if uid in self.active_users.get(idx, set()):
                return idx
            if len(self.active_users.get(idx, set())) < self.max_users_per_account:
                return idx
        return None

    def all_busy(self, uid=None):
        if not self.clients:
            return False
        return self._get_available_account(uid) is None

    def add_user(self, uid):
        idx = self._get_available_account(uid)
        if idx is not None:
            if idx not in self.active_users:
                self.active_users[idx] = set()
            self.active_users[idx].add(uid)
            return idx
        return None

    def remove_user(self, uid):
        for idx in self.active_users:
            self.active_users[idx].discard(uid)

    async def _get_client(self, uid=None):
        for _ in range(300):
            async with self.lock:
                now = time.time()
                idx = self._get_available_account(uid)
                if idx is not None:
                    since = now - self.last_used.get(idx, 0)
                    if since >= self.min_delay:
                        self.index = (idx + 1) % len(self.clients)
                        self.last_used[idx] = now
                        self.check_counts[idx] = self.check_counts.get(idx, 0) + 1
                        self.total_checks += 1
                        return idx, self.clients[idx]
            await asyncio.sleep(0.15 + random.random() * 0.1)
        return None, None

    def _set_cooldown(self, idx, seconds):
        self.cooldowns[idx] = time.time() + seconds

    async def _layer1(self, username, uid=None):
        if not self.clients:
            return "error"
        idx, client = await self._get_client(uid)
        if client is None:
            return "error"
        try:
            result = await client(ResolveUsernameRequest(username))
            return "taken"
        except UsernameNotOccupiedError:
            return "free"
        except UsernameInvalidError:
            return "taken"
        except FloodWaitError as e:
            self._set_cooldown(idx, e.seconds + 10)
            return "error"
        except:
            return "error"

    async def _layer2(self, username):
        try:
            chat = await bot.get_chat(f"@{username}")
            return "taken" if chat else "not_found"
        except:
            return "not_found"

    async def _layer3(self, username, uid=None):
        if not self.clients:
            return "error"
        idx, client = await self._get_client(uid)
        if client is None:
            return "error"
        try:
            ok = await client(AccountCheckUsername(username))
            return "free" if ok else "taken"
        except FloodWaitError as e:
            self._set_cooldown(idx, e.seconds + 10)
            return "error"
        except:
            return "error"

    async def check(self, username, uid=None):
        if not self.clients:
            r = await self._layer2(username)
            return "taken" if r == "taken" else "free"
        t1 = await self._layer1(username, uid)
        if t1 == "taken":
            return "taken"
        if t1 == "error":
            b = await self._layer2(username)
            return "taken" if b == "taken" else "error"
        b = await self._layer2(username)
        if b == "taken":
            self.caught_by_botapi += 1
            return "taken"
        return "free"

    async def strong_check(self, username, uid=None):
        if not self.clients:
            r = await self._layer2(username)
            return "taken" if r == "taken" else "free"
        t1 = await self._layer1(username, uid)
        if t1 == "taken":
            return "taken"
        b = await self._layer2(username)
        if b == "taken":
            self.caught_by_botapi += 1
            return "taken"
        if len(self.clients) >= 2:
            t2 = await self._layer1(username, uid)
            if t2 == "taken":
                self.caught_by_layer3 += 1
                return "taken"
            t3 = await self._layer3(username, uid)
            if t3 == "taken":
                self.caught_by_layer3 += 1
                return "taken"
        return "free" if t1 == "free" else "error"

    def stats(self):
        now = time.time()
        active = sum(1 for i in range(len(self.clients)) if self.cooldowns.get(i, 0) <= now)
        total_users = sum(len(users) for users in self.active_users.values())
        return {"total": len(self.clients), "active": active, "checks": self.total_checks,
                "botapi_saves": self.caught_by_botapi, "layer3_saves": self.caught_by_layer3,
                "active_users": total_users}

    async def disconnect(self):
        for c in self.clients:
            try:
                await c.disconnect()
            except:
                pass


pool = AccountPool()


# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '', joined TEXT DEFAULT '',
        free INTEGER DEFAULT 3, searches INTEGER DEFAULT 0, sub_end TEXT DEFAULT '',
        referred_by INTEGER DEFAULT 0, ref_count INTEGER DEFAULT 0,
        sub_bonus INTEGER DEFAULT 0, favorites TEXT DEFAULT '[]',
        auto_renew INTEGER DEFAULT 0, auto_renew_plan TEXT DEFAULT '',
        last_reminder TEXT DEFAULT '', banned INTEGER DEFAULT 0,
        balance REAL DEFAULT 0.0, pending_ref INTEGER DEFAULT 0,
        captcha_passed INTEGER DEFAULT 0, last_roulette TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY, days INTEGER, ktype TEXT, created TEXT,
        used INTEGER DEFAULT 0, used_by INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER,
        status TEXT DEFAULT 'pending', created TEXT,
        reviewed_by INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, username TEXT,
        found_at TEXT, mode TEXT, length INTEGER DEFAULT 5
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS promotions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, ptype TEXT,
        active INTEGER DEFAULT 1, data TEXT DEFAULT '{}',
        created TEXT, ended TEXT DEFAULT '', button_text TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, amount REAL,
        status TEXT DEFAULT 'pending', created TEXT,
        processed_by INTEGER DEFAULT 0
    )""")
    for col, default in [
        ("banned", "0"), ("balance", "0.0"), ("pending_ref", "0"),
        ("captcha_passed", "0"), ("last_roulette", "''"),
        ("auto_renew", "0"), ("auto_renew_plan", "''"), ("last_reminder", "''")
    ]:
        try:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except:
            pass
    try:
        c.execute("ALTER TABLE promotions ADD COLUMN button_text TEXT DEFAULT ''")
    except:
        pass
    conn.commit()
    conn.close()


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
        return {"uid": uid, "uname": "", "free": FREE_SEARCHES, "searches": 0,
                "sub_end": "", "ref_count": 0, "favorites": "[]", "referred_by": 0,
                "sub_bonus": 0, "auto_renew": 0, "auto_renew_plan": "",
                "last_reminder": "", "banned": 0, "balance": 0.0,
                "pending_ref": 0, "captcha_passed": 0, "last_roulette": ""}
    d = dict(row)
    for k, v in [("auto_renew", 0), ("auto_renew_plan", ""), ("last_reminder", ""),
                 ("banned", 0), ("balance", 0.0), ("pending_ref", 0),
                 ("captcha_passed", 0), ("last_roulette", "")]:
        d.setdefault(k, v)
    return d


def is_banned(uid): return get_user(uid).get("banned", 0) == 1
def ban_user(uid):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()
def unban_user(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=0 WHERE uid=?", (uid,)); conn.commit(); conn.close()

def has_subscription(uid):
    if uid in ADMIN_IDS: return True
    sub_end = get_user(uid).get("sub_end", "")
    if not sub_end: return False
    try: return datetime.strptime(sub_end, "%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def can_search(uid):
    if uid in ADMIN_IDS or has_subscription(uid): return True
    return get_user(uid).get("free", 0) > 0

def use_search(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    if uid in ADMIN_IDS or has_subscription(uid):
        c.execute("UPDATE users SET searches=searches+1 WHERE uid=?", (uid,))
    else:
        c.execute("UPDATE users SET free=MAX(free-1,0), searches=searches+1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def get_search_count(uid):
    if uid in ADMIN_IDS:
        return 6
    return PREMIUM_COUNT if has_subscription(uid) else FREE_COUNT

def get_max_searches(uid):
    if uid in ADMIN_IDS: return 999
    if has_subscription(uid): return PREMIUM_SEARCHES_LIMIT
    return get_user(uid).get("free", 0)

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

def add_balance(uid, amount):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=balance+? WHERE uid=?", (amount, uid))
    conn.commit(); conn.close()

def get_balance(uid): return get_user(uid).get("balance", 0.0)

def process_referral(new_uid, ref_uid):
    if new_uid == ref_uid: return False
    u = get_user(new_uid)
    if u.get("referred_by", 0) != 0: return False
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?", (ref_uid, new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1, free=free+? WHERE uid=?", (REF_BONUS, ref_uid))
    conn.commit(); conn.close(); return True

def set_pending_ref(uid, ref_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET pending_ref=? WHERE uid=?", (ref_uid, uid))
    conn.commit(); conn.close()

def get_pending_ref(uid): return get_user(uid).get("pending_ref", 0)

def set_captcha_passed(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET captcha_passed=1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def activate_key(uid, key_text):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT days, ktype FROM keys WHERE key=? AND used=0", (key_text.strip(),))
    row = c.fetchone()
    if not row: conn.close(); return None
    days, ktype = row
    c.execute("UPDATE keys SET used=1, used_by=? WHERE key=?", (uid, key_text.strip()))
    conn.commit(); conn.close()
    return {"days": days, "end": give_subscription(uid, days)}

def generate_key(days, ktype="MANUAL"):
    key = f"HUNT-{ktype}-{secrets.token_hex(4).upper()}"
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO keys (key,days,ktype,created) VALUES (?,?,?,?)",
              (key, days, ktype, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close(); return key

def set_auto_renew(uid, enabled, plan=""):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET auto_renew=?, auto_renew_plan=? WHERE uid=?",
              (1 if enabled else 0, plan, uid)); conn.commit(); conn.close()

def get_auto_renew(uid):
    u = get_user(uid)
    return bool(u.get("auto_renew", 0)), u.get("auto_renew_plan", "")

def set_last_reminder(uid, ds):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET last_reminder=? WHERE uid=?", (ds, uid))
    conn.commit(); conn.close()

def get_expiring_users(days_before):
    conn = sqlite3.connect(DB); c = conn.cursor()
    t = datetime.now() + timedelta(days=days_before)
    c.execute("SELECT uid,sub_end,auto_renew,auto_renew_plan,last_reminder FROM users WHERE sub_end BETWEEN ? AND ? AND sub_end!=''",
              (t.strftime("%Y-%m-%d 00:00"), t.strftime("%Y-%m-%d 23:59")))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "sub_end": r[1], "auto_renew": r[2], "auto_renew_plan": r[3], "last_reminder": r[4] or ""} for r in rows]

def add_favorite(uid, username):
    u = get_user(uid)
    try: favs = json.loads(u.get("favorites", "[]"))
    except: favs = []
    username = username.lower().strip()
    if username not in favs:
        favs.append(username)
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET favorites=? WHERE uid=?", (json.dumps(favs), uid))
        conn.commit(); conn.close()
    return favs

def get_favorites(uid):
    u = get_user(uid)
    try: return json.loads(u.get("favorites", "[]"))
    except: return []

def remove_favorite(uid, username):
    favs = get_favorites(uid); un = username.lower().strip()
    if un in favs:
        favs.remove(un)
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET favorites=? WHERE uid=?", (json.dumps(favs), uid))
        conn.commit(); conn.close()
    return favs

def save_history(uid, username, mode, length=5):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO history (uid,username,found_at,mode,length) VALUES (?,?,?,?,?)",
              (uid, username, datetime.now().strftime("%Y-%m-%d %H:%M"), mode, length))
    conn.commit(); conn.close()

def get_history(uid, limit=20):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username,found_at,mode FROM history WHERE uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
    rows = c.fetchall(); conn.close(); return rows

def set_last_roulette(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET last_roulette=? WHERE uid=?", (datetime.now().strftime("%Y-%m-%d %H:%M"), uid))
    conn.commit(); conn.close()

def can_roulette(uid):
    u = get_user(uid); lr = u.get("last_roulette", "")
    if not lr: return True
    try: return (datetime.now() - datetime.strptime(lr, "%Y-%m-%d %H:%M")).days >= 7
    except: return True

def create_withdrawal(uid, amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO withdrawals (uid,amount,status,created) VALUES (?,?,'pending',?)",
              (uid, amount, datetime.now().strftime("%Y-%m-%d %H:%M")))
    wid = c.lastrowid; conn.commit(); conn.close(); return wid

def get_pending_withdrawals():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,uid,amount,created FROM withdrawals WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "uid": r[1], "amount": r[2], "created": r[3]} for r in rows]

def process_withdrawal(wid, admin_uid, approve=True):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,amount FROM withdrawals WHERE id=? AND status='pending'", (wid,))
    row = c.fetchone()
    if not row: conn.close(); return None
    uid, amount = row
    if approve:
        c.execute("UPDATE withdrawals SET status='approved',processed_by=? WHERE id=?", (admin_uid, wid))
        c.execute("UPDATE users SET balance=MAX(balance-?,0) WHERE uid=?", (amount, uid))
    else:
        c.execute("UPDATE withdrawals SET status='rejected',processed_by=? WHERE id=?", (admin_uid, wid))
    conn.commit(); conn.close()
    return {"uid": uid, "amount": amount}

def create_promotion(name, ptype, button_text="", data=None):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO promotions (name,ptype,button_text,data,created) VALUES (?,?,?,?,?)",
              (name, ptype, button_text, json.dumps(data or {}), datetime.now().strftime("%Y-%m-%d %H:%M")))
    pid = c.lastrowid; conn.commit(); conn.close(); return pid

def get_active_promotions():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,name,ptype,data,created,button_text FROM promotions WHERE active=1")
    rows = c.fetchall(); conn.close()
    result = []
    for r in rows:
        result.append({"id": r[0], "name": r[1], "ptype": r[2],
                        "data": json.loads(r[3] or "{}"), "created": r[4],
                        "button_text": r[5] if r[5] else r[1]})
    return result

def end_promotion(pid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE promotions SET active=0,ended=? WHERE id=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"), pid))
    conn.commit(); conn.close()

def get_ref_top(limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,uname,ref_count FROM users WHERE ref_count>0 ORDER BY ref_count DESC LIMIT ?", (limit,))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "uname": r[1], "ref_count": r[2]} for r in rows]

def get_my_ref_place(uid):
    u = get_user(uid); my_refs = u.get("ref_count", 0)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE ref_count > ?", (my_refs,))
    above = c.fetchone()[0]; conn.close()
    return above + 1, my_refs

def get_premium_users():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    c.execute("SELECT uid,uname,sub_end FROM users WHERE sub_end>? AND sub_end!=''", (now_s,))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "uname": r[1], "sub_end": r[2]} for r in rows]

def tiktok_can_submit(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE uid=? AND created LIKE ?", (uid, today + "%"))
    cnt = c.fetchone()[0]; conn.close(); return cnt < TIKTOK_DAILY_LIMIT

def task_create(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id FROM tasks WHERE uid=? AND status='pending'", (uid,))
    ex = c.fetchone()
    if ex: conn.close(); return ex[0]
    c.execute("INSERT INTO tasks (uid,status,created) VALUES (?,'pending',?)",
              (uid, datetime.now().strftime("%Y-%m-%d %H:%M")))
    tid = c.lastrowid; conn.commit(); conn.close(); return tid

def task_approve(tid, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'", (tid,))
    r = c.fetchone()
    if not r: conn.close(); return None
    uid = r[0]
    c.execute("UPDATE tasks SET status='approved',reviewed_by=? WHERE id=?", (admin_uid, tid))
    conn.commit(); conn.close(); return uid

def task_reject(tid, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'", (tid,))
    r = c.fetchone()
    if not r: conn.close(); return None
    uid = r[0]
    c.execute("UPDATE tasks SET status='rejected',reviewed_by=? WHERE id=?", (admin_uid, tid))
    conn.commit(); conn.close(); return uid

def get_pending_tasks():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,uid,created,photo_count FROM tasks WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "uid": r[1], "created": r[2], "photos": r[3]} for r in rows]

def get_stats():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M"); today = datetime.now().strftime("%Y-%m-%d")
    r = {
        "users": c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "subs": c.execute("SELECT COUNT(*) FROM users WHERE sub_end>?", (now_s,)).fetchone()[0],
        "searches": c.execute("SELECT COALESCE(SUM(searches),0) FROM users").fetchone()[0],
        "tasks": c.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0],
        "today_users": c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?", (today + "%",)).fetchone()[0],
        "today_searches": c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?", (today + "%",)).fetchone()[0],
        "banned": c.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0],
        "withdrawals": c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0],
        "promos": c.execute("SELECT COUNT(*) FROM promotions WHERE active=1").fetchone()[0],
    }
    conn.close(); return r


# ═══════════════════════ ГЕНЕРАТОРЫ ═══════════════════════

_V = "aeiou"
_C = "bcdfghjklmnprstvwxyz"

def _pronounceable(length):
    w = []; sc = random.choice([True, False])
    for i in range(length):
        w.append(random.choice(_C) if (i % 2 == 0) == sc else random.choice(_V))
    return "".join(w)

def gen_default():
    return _pronounceable(5)

def gen_beautiful():
    """Красивые 5 букв — похожи на слова, произносимые, без повторов мусора"""
    v = "aeiou"
    c = "bcdfghjklmnprstvwxyz"
    
    patterns = [
        "cvcvc",  # wekse, botan, lifen
        "cvccv",  # bonta, kilma, werso  
        "ccvcv",  # skale, breno, travi
        "cvcvc",  # raxel, movin, dukes
        "vcvcv",  # amoke, eliva, uvano
        "cvccv",  # dorka, milza, nexpo
    ]
    
    pat = random.choice(patterns)
    word = []
    used = set()
    for ch in pat:
        if ch == "c":
            letter = random.choice(c)
            attempts = 0
            while letter in used and attempts < 10:
                letter = random.choice(c)
                attempts += 1
            word.append(letter)
            used.add(letter)
        else:
            letter = random.choice(v)
            word.append(letter)
    
    return "".join(word)

def gen_meaningful():
    pre = ["my","go","hi","ok","no","up","on","in","mr","dj","pro","top","hot","big",
           "old","new","red","max","neo","zen","ice","sun","sky","air","sea","own",
           "try","run","fly","win","get","set","fix","mix","pop","raw","now","day","one"]
    suf = ["bot","dev","pro","man","boy","cat","dog","fox","owl","god","war","run",
           "fly","win","fan","art","lab","hub","app","web","net","box","job","pay",
           "buy","car","map","log","key","pin","tag","tip","spy","doc","gem","ink"]
    mid = ["cool","fast","best","good","real","true","dark","wild","bold","epic",
           "mega","gold","blue","easy","mini","deep","kind","wise","calm","warm"]

    style = random.choice(["ps", "pm", "us", "um"])
    if style == "ps":
        r = random.choice(pre) + random.choice(suf)
    elif style == "pm":
        r = random.choice(pre) + random.choice(mid)
    elif style == "us":
        r = random.choice(pre) + "_" + random.choice(suf)
    else:
        r = random.choice(mid) + "_" + random.choice(suf)
    if len(r) < 5:
        r = r + random.choice(suf)
    if len(r) > 15:
        r = r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r):
        return _pronounceable(5)
    return r

def gen_anyword():
    return _pronounceable(random.randint(5, 7))

SEARCH_MODES = {
    "default":    {"name": "Дефолт",      "emoji": "🎲", "desc": "Произносимые (5 букв)",       "premium": False, "func": gen_default},
    "beautiful":  {"name": "Красивые",    "emoji": "💎", "desc": "Паттерны и симметрия (5 букв)","premium": True,  "func": gen_beautiful},
    "meaningful": {"name": "Со смыслом",  "emoji": "📖", "desc": "Комбинации слов",             "premium": True,  "func": gen_meaningful},
    "anyword":    {"name": "Любое слово", "emoji": "🔤", "desc": "Произносимые (5-7 букв)",     "premium": True,  "func": gen_anyword},
}

INVALID_WORDS = ["admin", "support", "help", "test", "telegram", "bot", "official", 
                 "service", "security", "account", "login", "password", "verify",
                 "moderator", "system", "null", "undefined", "root", "user"]

def is_valid_username(username):
    u = username.lower().replace("_", "")
    for word in INVALID_WORDS:
        if word in u:
            return False
    if "__" in username:
        return False
    if username.startswith("_") or username.endswith("_"):
        return False
    return True


# ═══════════════════════ ЧЕКЕРЫ ═══════════════════════

async def check_username(username):
    return await pool.strong_check(username)

async def check_fragment(username):
    url = f"https://fragment.com/username/{username.lower()}"
    try:
        async with http_session.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200: return "unavailable"
            text = await resp.text()
            if "Sold" in text or "sold" in text: return "sold"
            if any(x in text for x in ["Available", "Make an offer", "Bid", "auction"]): return "fragment"
            return "unavailable"
    except:
        return "unavailable"

async def check_subscribed(uid):
    if uid in ADMIN_IDS or not REQUIRED_CHANNELS: return []
    bad = []
    for ch in REQUIRED_CHANNELS:
        try:
            m = await bot.get_chat_member(f"@{ch}", uid)
            if m.status in ("left", "kicked"): bad.append(ch)
        except: pass
    return bad

def validate_username(u):
    if not u or len(u) < 5 or len(u) > 32: return False
    return bool(re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', u))

def evaluate_username(username):
    score = 0; factors = []; u = username.lower().replace("_", ""); ln = len(u)
    if ln <= 3: score += 95; factors.append("🔥 Ультракороткий")
    elif ln <= 4: score += 80; factors.append("💎 Очень короткий")
    elif ln <= 5: score += 60; factors.append("✨ Короткий")
    elif ln <= 6: score += 30; factors.append("📏 Средний")
    else: score += 10; factors.append("📦 Длинный")
    if len(set(u)) == 1 and ln >= 3: score += 90; factors.append("🔥 Моно")
    if u == u[::-1] and ln >= 3: score += 40; factors.append("🪞 Палиндром")
    if "_" in username: score += 5; factors.append("🔗 Разделитель")
    if u.isalpha(): score += 15; factors.append("🔤 Чистые буквы")
    if not any(ch.isdigit() for ch in u): score += 10; factors.append("🅰️ Без цифр")
    vc = sum(1 for c in u if c in _V)
    if 0.3 <= vc / max(len(u), 1) <= 0.6: score += 15; factors.append("🗣 Произносимый")
    score = min(score, 200)
    if score >= 150: pr, ra = "$3k+", "🔥🔥🔥 ЛЕГЕНДА"
    elif score >= 100: pr, ra = "$500-$3k", "💎💎 РЕДКИЙ"
    elif score >= 70: pr, ra = "$100-$500", "💎 ХОРОШИЙ"
    elif score >= 40: pr, ra = "$20-$100", "✨ СТАНДАРТ"
    else: pr, ra = "$1-$20", "📦 ОБЫЧНЫЙ"
    filled = min(score // 20, 10)
    return {"score": score, "bar": "▓" * filled + "░" * (10 - filled),
            "factors": factors, "price": pr, "rarity": ra}


# ═══════════════════════ ПОИСК ═══════════════════════

async def do_search(count, gen_func, msg, mode_name, uid):
    found = []
    attempts = 0
    start = time.time()
    last_update = 0
    checked = set()

    acc_idx = pool.add_user(uid)
    if acc_idx is None and pool.clients:
        await edit_msg(msg,
            f"🔄 <b>{mode_name}</b>\n\n"
            f"⏳ Все аккаунты заняты...\n"
            f"Подождите немного")
        for _ in range(30):
            await asyncio.sleep(1)
            acc_idx = pool.add_user(uid)
            if acc_idx is not None:
                break
        if acc_idx is None:
            return [], {"attempts": 0, "elapsed": 0}

    try:
        while len(found) < count and attempts < 5000:
            batch_size = max(len(pool.clients) * 2, 5)
            batch = []
            for _ in range(batch_size):
                u = gen_func()
                if len(u) >= 5 and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', u) and u.lower() not in checked:
                    if is_valid_username(u):
                        batch.append(u.lower())
                        checked.add(u.lower())
            if not batch:
                continue

            results = await asyncio.gather(*[pool.check(u, uid) for u in batch])
            attempts += len(batch)

            for username, status in zip(batch, results):
                if status == "free":
                    dc = await pool.strong_check(username, uid)
                    if dc != "free":
                        continue
                    fr = await check_fragment(username)
                    found.append({"username": username, "fragment": fr})
                    save_history(uid, username, mode_name, len(username))
                    if len(found) >= count:
                        break

            now = time.time()
            if now - last_update > 2.5:
                last_update = now
                el = int(now - start)
                ps = pool.stats()
                await edit_msg(msg,
                    f"🔎 <b>{mode_name}</b>\n\n"
                    f"📊 Проверено: <code>{attempts}</code>\n"
                    f"✅ Найдено: <code>{len(found)}/{count}</code>\n"
                    f"🔄 Сессій: <code>{ps['active']}/{ps['total']}</code>\n"
                    f"⏱ {el}с")

        return found, {"attempts": attempts, "elapsed": int(time.time() - start)}
    finally:
        pool.remove_user(uid)


# ═══════════════════════ HELPERS ═══════════════════════

def _d(uid_val, uname_val):
    return "@" + uname_val if uname_val else "ID:" + str(uid_val)

async def notify_admins(text, exclude=None, kb=None):
    for aid in ADMIN_IDS:
        if exclude and aid == exclude: continue
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

def build_menu(uid):
    u = get_user(uid); ps = pool.stats()
    is_prem = has_subscription(uid); is_admin = uid in ADMIN_IDS
    if is_admin: si, st, sub_info = "👑", "ADMIN", "♾"
    elif is_prem: si, st = "💎", "PREMIUM"; sub_info = "до " + u.get("sub_end", "?")
    elif u.get("free", 0) > 0: si, st = "🆓", "FREE"; sub_info = str(u.get("free", 0)) + " поисков"
    else: si, st, sub_info = "⛔️", "ЛИМИТ", "закончились"
    cnt = get_search_count(uid); mx = get_max_searches(uid); bal = u.get("balance", 0.0)
    promos = get_active_promotions()

    text = (f"🔍 <b>USERNAME HUNTER</b> {si}\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>{st}</b> | 🎯 <code>{cnt}</code> юзов/поиск | 🔄 <code>{mx}</code> поисков\n"
            f"📊 {sub_info}\n🔢 Поисков: <code>{u.get('searches', 0)}</code>\n"
            f"🔄 Аккаунтов: <code>{ps['active']}/{ps['total']}</code>\n"
            f"💰 Баланс: <code>{bal:.1f}</code> ⭐\n")
    if promos:
        text += "\n🎉 <b>Акции:</b>\n"
        for pr in promos: text += f"  • {pr['name']}\n"
    text += "\n━━━━━━━━━━━━━━━━━━━━━━━"

    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Поиск", callback_data="cmd_search")
    kb.button(text="📊 Оценка", callback_data="cmd_evaluate")
    kb.button(text="🔧 Утилиты", callback_data="cmd_utils")
    kb.button(text="👤 Профиль", callback_data="cmd_profile")
    kb.button(text="💰 Premium", callback_data="cmd_prices")
    kb.button(text="👥 Рефералы", callback_data="cmd_referral")
    kb.button(text="🎁 TikTok", callback_data="cmd_tiktok")
    kb.button(text="🔑 Ключ", callback_data="cmd_activate")
    for pr in promos:
        kb.button(text=pr.get("button_text") or pr["name"], callback_data=f"pv_{pr['id']}")
    kb.button(text="🤖 Поддержать", callback_data="cmd_support")
    if is_admin: kb.button(text="👑 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    return text, kb.as_markup()


# ═══════════════════════ COMMANDS ═══════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, command: CommandObject):
    uid = msg.from_user.id; uname = msg.from_user.username or ""
    is_new = get_user(uid).get("searches", 0) == 0
    ensure_user(uid, uname)
    if is_banned(uid): await msg.answer("🚫 Заблокированы."); return
    if command.args and command.args.startswith("ref_") and is_new:
        try:
            ref_id = int(command.args.replace("ref_", ""))
            if ref_id != uid:
                set_pending_ref(uid, ref_id)
                kb = InlineKeyboardBuilder(); kb.button(text="Не робот 🌟", callback_data="captcha_ok")
                await msg.answer("🤖 <b>Подтвердите что вы не робот:</b>\n\nНажмите кнопку 👇",
                                 reply_markup=kb.as_markup(), parse_mode="HTML"); return
        except: pass
    ns = await check_subscribed(uid)
    if ns: t, k = build_sub_kb(ns)
    else: t, k = build_menu(uid)
    await msg.answer(t, reply_markup=k, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    if is_banned(msg.from_user.id): return
    kb = InlineKeyboardBuilder(); kb.button(text="🔙 Меню", callback_data="cmd_menu")
    await msg.answer(f"📖 <b>Справка</b>\n\n📩 @{ADMIN_CONTACT}", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.message(Command("id"))
async def cmd_id(msg: Message):
    await msg.answer(f"🆔 <code>{msg.from_user.id}</code>", parse_mode="HTML")


# ═══════════════════════ ТЕКСТ ═══════════════════════

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: Message):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    state = user_states.get(uid, {})
    action = state.get("action")

    if action == "admin_broadcast_text":
        if uid not in ADMIN_IDS: user_states.pop(uid, None); return
        user_states.pop(uid, None)
        bt = msg.text.strip()
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("SELECT uid FROM users WHERE banned=0")
        aus = [r[0] for r in c.fetchall()]; conn.close()
        s, f = 0, 0; sm = await msg.answer(f"📤 0/{len(aus)}")
        for i, tu in enumerate(aus):
            try: await bot.send_message(tu, bt, parse_mode="HTML"); s += 1
            except: f += 1
            if (i + 1) % 50 == 0:
                try: await sm.edit_text(f"📤 {i+1}/{len(aus)} ✅{s} ❌{f}")
                except: pass
            await asyncio.sleep(0.05)
        try: await sm.edit_text(f"✅ Готово! ✅{s} ❌{f}")
        except: pass; return

    if action == "activate":
        user_states.pop(uid, None)
        r = activate_key(uid, msg.text.strip())
        if r: await msg.answer(f"🎉 <b>Активировано!</b> {r['days']} дн до {r['end']}", parse_mode="HTML")
        else: await msg.answer("❌ Неверный ключ")
        t, k = build_menu(uid); await msg.answer(t, reply_markup=k, parse_mode="HTML"); return

    if action == "evaluate":
        user_states.pop(uid, None)
        un = msg.text.strip().replace("@", "").lower()
        if not validate_username(un): await msg.answer("❌ Некорректный (мин 5 символов)"); return
        wm = await msg.answer("⏳ Проверка...")
        tg = await check_username(un); fr = await check_fragment(un)
        tgs = {"free": "✅ Свободен", "taken": "❌ Занят", "error": "⚠️ Ошибка"}.get(tg, "❓")
        frs = {"fragment": "💎 Fragment", "sold": "✅ Продан", "unavailable": "—"}.get(fr, "❓")
        ev = evaluate_username(un)
        fac = "\n".join(f"  {f}" for f in ev["factors"]) or "  —"
        kb = InlineKeyboardBuilder()
        if tg == "free": kb.button(text="⭐ В избранное", callback_data=f"fav_a_{un}")
        kb.button(text="📊 Ещё", callback_data="cmd_evaluate")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(2, 1)
        try: await wm.delete()
        except: pass
        await msg.answer(
            f"📊 <b>Оценка @{un}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📱 Telegram: {tgs}\n💎 Fragment: {frs}\n\n"
            f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
            f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}",
            reply_markup=kb.as_markup(), parse_mode="HTML"); return

    if action == "quick_check":
        user_states.pop(uid, None)
        un = msg.text.strip().replace("@", "").lower()
        if not validate_username(un): await msg.answer("❌ Некорректный"); return
        wm = await msg.answer("⏳ Проверка...")
        tg = await check_username(un)
        st = {"free": "✅ Свободен!", "taken": "❌ Занят", "error": "⚠️ Ошибка"}.get(tg, "❓")
        kb = InlineKeyboardBuilder()
        if tg == "free": kb.button(text="⭐ В избранное", callback_data=f"fav_a_{un}")
        kb.button(text="🔍 Ещё", callback_data="util_check"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(2, 1)
        try: await wm.delete()
        except: pass
        await msg.answer(f"🔍 <b>@{un}</b> — {st}", reply_markup=kb.as_markup(), parse_mode="HTML"); return

    if action == "mass_check":
        user_states.pop(uid, None)
        names = [n.strip().replace("@", "").lower() for n in msg.text.split("\n") if validate_username(n.strip().replace("@", "").lower())][:20]
        if not names: await msg.answer("❌ Нет валидных"); return
        wm = await msg.answer(f"⏳ Проверяю {len(names)}...")
        results = await asyncio.gather(*[check_username(n) for n in names])
        fc = sum(1 for r in results if r == "free"); tc = sum(1 for r in results if r == "taken")
        text = f"📋 <b>Массовая ({len(names)})</b> ✅{fc} ❌{tc}\n\n"
        for i, r in enumerate(results):
            icon = {"free": "✅", "taken": "❌", "error": "⚠️"}.get(r, "❓")
            text += f"{icon} @{names[i]}\n"
        kb = InlineKeyboardBuilder(); kb.button(text="📋 Ещё", callback_data="util_mass"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML"); return

    if action == "admin_give_user":
        inp = msg.text.strip(); target = None
        if inp.isdigit(): target = int(inp)
        else:
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("SELECT uid FROM users WHERE uname=?", (inp.replace("@", ""),)); r = c.fetchone(); conn.close()
            target = r[0] if r else None
        if not target: await msg.answer("❌ Не найден"); return
        user_states[uid] = {"action": "admin_give_days", "target": target}
        await msg.answer(f"📅 Дней для <code>{target}</code>?", parse_mode="HTML"); return

    if action == "admin_give_days":
        try: days = int(msg.text.strip()); assert days > 0
        except: await msg.answer("❌ Число!"); return
        target = state["target"]; user_states.pop(uid, None)
        end = give_subscription(target, days)
        await msg.answer(f"✅ {days}дн для <code>{target}</code> до {end}", parse_mode="HTML")
        try: await bot.send_message(target, f"🎉 Подписка <b>{days}дн</b> до <b>{end}</b>!", parse_mode="HTML")
        except: pass; return

    if action == "admin_key_days":
        try: days = int(msg.text.strip()); assert days > 0
        except: await msg.answer("❌ Число!"); return
        user_states.pop(uid, None); key = generate_key(days, f"D{days}")
        await msg.answer(f"🔑 <code>{key}</code> — {days} дн", parse_mode="HTML"); return

    if action == "admin_ban_input":
        user_states.pop(uid, None); inp = msg.text.strip(); target = None
        if inp.isdigit(): target = int(inp)
        else:
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("SELECT uid FROM users WHERE uname=?", (inp.replace("@", ""),)); r = c.fetchone(); conn.close()
            target = r[0] if r else None
        if not target: await msg.answer("❌ Не найден"); return
        ban_user(target); await msg.answer(f"🚫 Заблокирован"); return

    if action == "admin_unban_input":
        user_states.pop(uid, None); inp = msg.text.strip(); target = None
        if inp.isdigit(): target = int(inp)
        else:
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("SELECT uid FROM users WHERE uname=?", (inp.replace("@", ""),)); r = c.fetchone(); conn.close()
            target = r[0] if r else None
        if not target: await msg.answer("❌ Не найден"); return
        unban_user(target); await msg.answer(f"✅ Разблокирован"); return

    if action == "gift_username":
        user_states.pop(uid, None)
        tu = msg.text.strip().replace("@", "")
        if not tu: await msg.answer("❌ Введите @username"); return
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("SELECT uid FROM users WHERE uname=?", (tu,)); r = c.fetchone(); conn.close()
        if not r: await msg.answer("❌ Не найден в боте"); return
        plan = state.get("plan"); method = state.get("method"); p = PRICES.get(plan)
        if not p: return
        tuid = r[0]
        if method == "stars":
            await bot.send_invoice(uid, title=f"🎁 Подарок {p['label']} для @{tu}",
                description=f"Premium {p['label']} для @{tu}", payload=f"gift_{plan}_{tuid}_{uid}",
                provider_token="", currency="XTR", prices=[LabeledPrice(label=p["label"], amount=p["stars"])])
        return

    if action == "withdraw_amount":
        user_states.pop(uid, None)
        try: amount = float(msg.text.strip()); assert amount >= MIN_WITHDRAW
        except: await msg.answer(f"❌ Минимум {MIN_WITHDRAW}⭐"); return
        bal = get_balance(uid)
        if amount > bal: await msg.answer(f"❌ Недостаточно ({bal:.1f}⭐)"); return
        wid = create_withdrawal(uid, amount)
        await msg.answer(f"✅ Заявка #{wid} на {amount:.1f}⭐")
        for aid in ADMIN_IDS:
            try:
                akb = InlineKeyboardBuilder(); akb.button(text="✅", callback_data=f"wd_ok_{wid}"); akb.button(text="❌", callback_data=f"wd_no_{wid}"); akb.adjust(2)
                await bot.send_message(aid, f"💰 Вывод #{wid}\n{amount:.1f}⭐", reply_markup=akb.as_markup())
            except: pass
        return

    if action == "admin_promo_name":
        user_states[uid] = {"action": "admin_promo_btn", "name": msg.text.strip()}
        await msg.answer("🔘 Введите текст для кнопки в меню:\nНапример: 🌺 8 марта 🏆"); return

    if action == "admin_promo_btn":
        user_states[uid] = {"action": "admin_promo_type", "name": state.get("name", "Акция"), "btn": msg.text.strip()}
        await msg.answer("📋 Тип: discount / holiday / ref_contest / custom"); return

    if action == "admin_promo_type":
        ptype = msg.text.strip().lower(); name = state.get("name"); btn = state.get("btn", name)
        user_states.pop(uid, None)
        pid = create_promotion(name, ptype, button_text=btn)
        await msg.answer(f"✅ Акция <b>{name}</b> #{pid} создана!\nКнопка: <code>{btn}</code>", parse_mode="HTML"); return

    ns = await check_subscribed(uid)
    if ns: t, k = build_sub_kb(ns)
    else: t, k = build_menu(uid)
    await msg.answer(t, reply_markup=k, parse_mode="HTML")


@dp.message(F.photo)
async def handle_photo(msg: Message):
    uid = msg.from_user.id
    state = user_states.get(uid, {})
    if state.get("action") != "tiktok_proof":
        return
    
    tid = state.get("task_id")
    photos = state.get("photos", 0) + 1
    user_states[uid]["photos"] = photos
    
    if "file_ids" not in user_states[uid]:
        user_states[uid]["file_ids"] = []
    user_states[uid]["file_ids"].append(msg.photo[-1].file_id)
    
    if photos < TIKTOK_SCREENSHOTS_NEEDED:
        await msg.answer(f"📸 {photos}/{TIKTOK_SCREENSHOTS_NEEDED}")
        return
    
    # Все скрины — забираем данные ДО pop
    file_ids = user_states[uid].get("file_ids", [])
    user_states.pop(uid, None)
    
    await msg.answer(f"✅ Все {TIKTOK_SCREENSHOTS_NEEDED} скринов! Ожидайте.")
    
    uname = msg.from_user.username or ""
    display = f"@{uname}" if uname else f"ID:{uid}"
    
    from aiogram.types import InputMediaPhoto
    
    for aid in ADMIN_IDS:
        try:
            akb = InlineKeyboardBuilder()
            akb.button(text="✅ Одобрить", callback_data=f"ta_{tid}")
            akb.button(text="❌ Отклонить", callback_data=f"tr_{tid}")
            akb.adjust(2)
            await bot.send_message(
                aid,
                f"📱 <b>TikTok #{tid}</b>\n"
                f"👤 От: {display} (<code>{uid}</code>)\n"
                f"📸 Скринов: {photos}",
                reply_markup=akb.as_markup(),
                parse_mode="HTML"
            )
            
            for i in range(0, len(file_ids), 10):
                batch = file_ids[i:i+10]
                media = [InputMediaPhoto(media=fid) for fid in batch]
                if i == 0:
                    media[0].caption = f"TikTok #{tid} | {display}"
                await bot.send_media_group(aid, media)
        except Exception as e:
            logger.error(f"TikTok admin {aid}: {e}")


# ═══════════════════════ CALLBACKS ═══════════════════════

@dp.callback_query(F.data == "captcha_ok")
async def cb_captcha(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ensure_user(uid, cb.from_user.username)
    ref_uid = get_pending_ref(uid)
    if ref_uid and ref_uid != uid:
        ok = process_referral(uid, ref_uid); set_pending_ref(uid, 0); set_captcha_passed(uid)
        if ok:
            try: await bot.send_message(ref_uid, f"🎉 Новый реферал! <b>+{REF_BONUS}</b>", parse_mode="HTML")
            except: pass
    else: set_captcha_passed(uid)
    t, k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "check_sub")
async def cb_cs(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ns = await check_subscribed(uid)
    if ns: t, k = build_sub_kb(ns); await edit_msg(cb.message, t, k); return
    u = get_user(uid)
    if u.get("sub_bonus", 0) == 0:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET free=free+2,sub_bonus=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()
    t, k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "cmd_menu")
async def cb_menu(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    user_states.pop(uid, None); t, k = build_menu(uid); await edit_msg(cb.message, t, k)


# ─── ПОИСК ───

@dp.callback_query(F.data == "cmd_search")
async def cb_search(cb: CallbackQuery):
    uid = cb.from_user.id
    logger.info(f"SEARCH clicked by {uid}")
    await answer_cb(cb)
    if is_banned(uid): return
    if not can_search(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="👥 Рефералы", callback_data="cmd_referral")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>\n\n💰 Купите Premium", kb.as_markup())
        return

    is_prem = uid in ADMIN_IDS or has_subscription(uid)
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    fl = "♾" if uid in ADMIN_IDS else str(mx)

    kb = InlineKeyboardBuilder()
    for key, m in SEARCH_MODES.items():
        if m["premium"] and not is_prem:
            kb.button(text=f"🔒 {m['emoji']} {m['name']}", callback_data="need_prem")
        else:
            kb.button(text=f"{m['emoji']} {m['name']}", callback_data=f"go_{key}")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(2, 2, 1)

    mt = ""
    for key, m in SEARCH_MODES.items():
        lk = "🔒" if m["premium"] and not is_prem else "✅"
        mt += f"{lk} <b>{m['emoji']} {m['name']}</b> — {m['desc']}\n"

    await edit_msg(cb.message,
        f"🔍 <b>Выберите режим:</b>\n\n{mt}\n🎯 <code>{cnt}</code> юзов/поиск | Поисков: <b>{fl}</b>",
        kb.as_markup())


@dp.callback_query(F.data == "need_prem")
async def cb_np(cb: CallbackQuery):
    await answer_cb(cb, "🔒 Нужен Premium!", show_alert=True)


@dp.callback_query(F.data.startswith("go_"))
async def cb_go(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    if not can_search(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="💰 Premium", callback_data="cmd_prices"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>", kb.as_markup()); return

    mode = cb.data[3:]; mi = SEARCH_MODES.get(mode)
    if not mi: return
    is_prem = uid in ADMIN_IDS or has_subscription(uid)
    if mi["premium"] and not is_prem: return

    if uid not in ADMIN_IDS:
        if uid in searching_users:
            try: await bot.send_message(uid, "⏳ Уже идёт поиск!")
            except: pass; return
        cd = user_search_cooldown.get(uid, 0); rem = SEARCH_COOLDOWN - (time.time() - cd)
        if rem > 0:
            try: await bot.send_message(uid, f"⏳ Подождите {int(rem)} сек.")
            except: pass; return

    searching_users.add(uid)
    try:
        ps = pool.stats()
        await edit_msg(cb.message,
            f"🚀 <b>{mi['emoji']} {mi['name']}</b>\n\n"
            f"🔄 Сессий: <code>{ps['active']}/{ps['total']}</code>\n⏳ Ищу...")

        use_search(uid); count = get_search_count(uid)
        found, stats = await do_search(count, mi["func"], cb.message, mi["name"], uid)

        kb = InlineKeyboardBuilder()
        if found:
            text = f"✅ <b>Найдено {len(found)}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, item in enumerate(found, 1):
                ev = evaluate_username(item["username"])
                fri = ""
                if item["fragment"] == "fragment": fri = " 💎"
                elif item["fragment"] == "sold": fri = " 🏷"
                text += f"{i}. <code>@{item['username']}</code> — {ev['rarity']}{fri}\n"
                kb.button(text=f"⭐ @{item['username']}", callback_data=f"fav_a_{item['username']}")
            text += f"\n📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"
        else:
            text = f"😔 <b>Не найдено</b>\n\n📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"

        if can_search(uid): kb.button(text="🔄 Ещё", callback_data=cb.data)
        kb.button(text="🔍 Режимы", callback_data="cmd_search")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())
    finally:
        searching_users.discard(uid)
        if uid not in ADMIN_IDS: user_search_cooldown[uid] = time.time()


# ─── ОЦЕНКА / УТИЛИТЫ ───

@dp.callback_query(F.data == "cmd_evaluate")
async def cb_eval(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "evaluate"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "📊 <b>Введите юзернейм для оценки:</b>", kb.as_markup())

@dp.callback_query(F.data == "cmd_utils")
async def cb_utils(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверка", callback_data="util_check")
    kb.button(text="📋 Массовая", callback_data="util_mass")
    kb.button(text="⭐ Избранное", callback_data="util_favs")
    kb.button(text="📜 История", callback_data="util_hist")
    kb.button(text="📥 Экспорт", callback_data="util_export")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(2, 2, 1, 1)
    await edit_msg(cb.message, "🔧 <b>Утилиты</b>", kb.as_markup())

@dp.callback_query(F.data == "util_check")
async def cb_uc(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "quick_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "🔍 <b>Введите юзернейм:</b>", kb.as_markup())

@dp.callback_query(F.data == "util_mass")
async def cb_um(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "mass_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "📋 <b>Юзернеймы по строке (макс 20):</b>", kb.as_markup())

@dp.callback_query(F.data == "util_favs")
async def cb_uf(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; favs = get_favorites(uid); kb = InlineKeyboardBuilder()
    if favs:
        text = f"⭐ <b>Избранное ({len(favs)})</b>\n\n"
        for f in favs: text += f"• <code>@{f}</code>\n"; kb.button(text=f"❌ {f}", callback_data=f"fav_r_{f}")
    else: text = "⭐ Пусто"
    kb.button(text="🔙", callback_data="cmd_utils"); kb.adjust(2, 1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("fav_a_"))
async def cb_fa(cb: CallbackQuery):
    add_favorite(cb.from_user.id, cb.data[6:]); await answer_cb(cb, "⭐ Добавлено!", show_alert=True)

@dp.callback_query(F.data.startswith("fav_r_"))
async def cb_fr(cb: CallbackQuery):
    remove_favorite(cb.from_user.id, cb.data[6:]); await answer_cb(cb, "❌ Удалено"); await cb_uf(cb)

@dp.callback_query(F.data == "util_hist")
async def cb_uh(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; hist = get_history(uid); kb = InlineKeyboardBuilder()
    text = f"📜 <b>История ({len(hist)})</b>\n\n" if hist else "📜 Пусто"
    for h in hist[:15]: text += f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
    kb.button(text="📥 TXT", callback_data="util_export"); kb.button(text="🔙", callback_data="cmd_utils"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "util_export")
async def cb_ue(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; hist = get_history(uid, 100)
    if not hist: return
    content = "ИСТОРИЯ\n\n"
    for i, h in enumerate(hist, 1): content += f"{i}. @{h[0]} | {h[2]} | {h[1]}\n"
    await bot.send_document(uid, BufferedInputFile(content.encode(), filename=f"history_{uid}.txt"), caption="📥")


# ─── ПРОФИЛЬ ───

@dp.callback_query(F.data == "cmd_profile")
async def cb_profile(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    u = get_user(uid); is_admin = uid in ADMIN_IDS; is_prem = has_subscription(uid)
    if is_admin: status = "👑 Админ ♾"
    elif is_prem: status = "💎 Premium до " + u.get("sub_end", "?")
    elif u.get("free", 0) > 0: status = "🆓 " + str(u.get("free", 0)) + " поисков"
    else: status = "⛔️ Лимит"
    cnt = get_search_count(uid); mx = get_max_searches(uid)
    ar_on, _ = get_auto_renew(uid)
    ar_text = "🔄 Авто-продление: <b>ВКЛ</b>" if ar_on else "🔄 Авто-продление: ВЫКЛ"
    favs = get_favorites(uid); bal = u.get("balance", 0.0); uname = u.get("uname", "")
    text = (f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <code>{uid}</code>" + (f" | @{uname}" if uname else "") + "\n"
            f"📌 {status}\n🎯 {cnt} юзов/поиск | 🔄 {mx} поисков\n"
            f"📊 Поисков: <code>{u.get('searches', 0)}</code>\n"
            f"👥 Рефералов: <code>{u.get('ref_count', 0)}</code>\n⭐ Избранное: <code>{len(favs)}</code>\n"
            f"💰 Баланс: <code>{bal:.1f}</code> ⭐\n\n{ar_text}")
    kb = InlineKeyboardBuilder()
    if ar_on: kb.button(text="🔄 Выключить авто", callback_data="toggle_renew")
    else: kb.button(text="🔄 Включить авто", callback_data="toggle_renew")
    kb.button(text="⭐ Избранное", callback_data="util_favs")
    kb.button(text="📜 История", callback_data="util_hist")
    kb.button(text="🔑 Купить ключ", callback_data="buy_key_p")
    kb.button(text="🎁 Подарить", callback_data="gift_prem")
    if bal >= MIN_WITHDRAW: kb.button(text=f"💸 Вывести ({bal:.1f}⭐)", callback_data="cmd_withdraw")
    if is_prem: kb.button(text="🎰 Рулетка", callback_data="cmd_roulette")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "toggle_renew")
async def cb_tr(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ar_on, _ = get_auto_renew(uid)
    if ar_on: set_auto_renew(uid, False, ""); await cb_profile(cb)
    else:
        kb = InlineKeyboardBuilder()
        for k, p in PRICES.items():
            if p["days"] < 99999: kb.button(text=f"{p['label']} ({p['stars']}⭐)", callback_data=f"sr_{k}")
        kb.button(text="❌", callback_data="cmd_profile"); kb.adjust(1)
        await edit_msg(cb.message, "🔄 <b>Тариф авто-продления:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("sr_"))
async def cb_sr(cb: CallbackQuery):
    plan = cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    set_auto_renew(cb.from_user.id, True, plan); await cb_profile(cb)


# ─── РУЛЕТКА ───

@dp.callback_query(F.data == "cmd_roulette")
async def cb_roulette(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    if not can_roulette(uid):
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
        await edit_msg(cb.message, "⏳ Рулетка через неделю", kb.as_markup()); return
    kb = InlineKeyboardBuilder()
    kb.button(text="🎰 Крутить!", callback_data="roulette_spin")
    kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message, "🎰 <b>Рулетка Premium</b>\n\nДоступна раз в неделю 🍀", kb.as_markup())

@dp.callback_query(F.data == "roulette_spin")
async def cb_rspin(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    if not can_roulette(uid): return
    set_last_roulette(uid)
    for e in ["🎰", "🔄", "💫", "🌟", "✨", "🎯"]:
        await edit_msg(cb.message, f"{e} Крутим..."); await asyncio.sleep(0.4)
    roll = random.randint(1, 100)
    if roll <= 60: days = 1; pt = "🔑 Ключ на 1 день"
    elif roll <= 90: days = 2; pt = "🔑 Ключ на 2 дня"
    else: days = random.choice([1, 2]); give_subscription(uid, days); pt = f"⭐ +{days} дн!"
    if roll <= 90: key = generate_key(days, "ROULETTE"); activate_key(uid, key); pt += f"\n🔑 <code>{key}</code>"
    kb = InlineKeyboardBuilder(); kb.button(text="👤 Профиль", callback_data="cmd_profile"); kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, f"🎉 <b>Поздравляем!</b>\n\n{pt}\n\nСледующая через 7 дней 🍀", kb.as_markup())


# ─── КЛЮЧ ───

@dp.callback_query(F.data == "buy_key_p")
async def cb_bkp(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k, p in PRICES.items(): kb.button(text=f"🔑 {p['label']} — {p['stars']}⭐", callback_data=f"bk_{k}")
    kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message, "🔑 <b>Купить ключ Premium</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("bk_"))
async def cb_bk(cb: CallbackQuery):
    k = cb.data[3:]; p = PRICES.get(k)
    if not p: await answer_cb(cb, "❌", show_alert=True); return
    await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"🔑 Ключ {p['label']}", description=f"Ключ Premium {p['label']}",
        payload=f"key_{k}_{cb.from_user.id}", provider_token="", currency="XTR",
        prices=[LabeledPrice(label=p["label"], amount=p["stars"])])


# ─── ПОДАРИТЬ ───

@dp.callback_query(F.data == "gift_prem")
async def cb_gift(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Stars", callback_data="gift_s"); kb.button(text="🔙", callback_data="cmd_profile"); kb.adjust(1)
    await edit_msg(cb.message, "🎁 <b>Подарить Premium другу</b>", kb.as_markup())

@dp.callback_query(F.data == "gift_s")
async def cb_gs(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k, p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐", callback_data=f"gp_s_{k}")
    kb.button(text="🔙", callback_data="gift_prem"); kb.adjust(1)
    await edit_msg(cb.message, "🎁 <b>Тариф подарка:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("gp_s_"))
async def cb_gps(cb: CallbackQuery):
    plan = cb.data[5:]; await answer_cb(cb)
    if plan not in PRICES: return
    user_states[cb.from_user.id] = {"action": "gift_username", "plan": plan, "method": "stars"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="gift_prem")
    await edit_msg(cb.message, f"🎁 <b>{PRICES[plan]['label']}</b>\n\nВведите @username друга:", kb.as_markup())


# ─── ВЫВОД ───

@dp.callback_query(F.data == "cmd_withdraw")
async def cb_wd(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb); bal = get_balance(uid)
    if bal < MIN_WITHDRAW: return
    user_states[uid] = {"action": "withdraw_amount"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_profile")
    await edit_msg(cb.message, f"💸 <b>Вывод</b>\n\n💰 {bal:.1f}⭐\n📌 Мин: {MIN_WITHDRAW}⭐\n\nВведите сумму:", kb.as_markup())


# ─── РЕФЕРАЛЫ ───

@dp.callback_query(F.data == "cmd_referral")
async def cb_ref(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    u = get_user(uid); bu = bot_info.username if bot_info else "bot"
    link = f"https://t.me/{bu}?start=ref_{uid}"; rc = u.get("ref_count", 0); bal = u.get("balance", 0.0)
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться", url=f"https://t.me/share/url?url={link}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message,
        f"👥 <b>Рефералы</b>\n\n👥 Приглашено: <code>{rc}</code>\n"
        f"💰 Баланс: <code>{bal:.1f}</code> ⭐ (4% с покупок)\n"
        f"+{REF_BONUS} поиска за друга\n\n🔗 <code>{link}</code>", kb.as_markup())


# ─── КЛЮЧ ───

@dp.callback_query(F.data == "cmd_activate")
async def cb_act(cb: CallbackQuery):
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "activate"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "🔑 <b>Введите ключ:</b>", kb.as_markup())


# ─── ЦЕНЫ ───

@dp.callback_query(F.data == "cmd_prices")
async def cb_prices(cb: CallbackQuery):
    await answer_cb(cb)
    promos = get_active_promotions()
    text = "💰 <b>Premium</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    if promos:
        text += "🎉 <b>Акции:</b>\n"
        for pr in promos: text += f"  • {pr['name']}\n"
        text += "\n"
    text += f"🎯 Premium: <code>{PREMIUM_COUNT}</code> юзов, <code>{PREMIUM_SEARCHES_LIMIT}</code> поисков\n"
    text += f"🆓 Free: <code>{FREE_COUNT}</code> юз, <code>{FREE_SEARCHES}</code> поисков\n\n"
    for p in PRICES.values():
        text += f"• <b>{p['label']}</b> — <code>{p['rub']}₽</code> / <code>{p['stars']}⭐</code> <s>{p['rub_orig']}₽</s>\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Stars", callback_data="pay_stars"); kb.button(text="💳 FunPay", callback_data="pay_funpay")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "pay_stars")
async def cb_ps(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k, p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐", callback_data=f"buy_{k}")
    kb.button(text="🔙", callback_data="cmd_prices"); kb.adjust(1)
    await edit_msg(cb.message, "⭐ <b>Оплата Stars:</b>", kb.as_markup())

@dp.callback_query(F.data == "pay_funpay")
async def cb_pf(cb: CallbackQuery):
    await answer_cb(cb); uid = cb.from_user.id; uname = cb.from_user.username or ""
    kb = InlineKeyboardBuilder()
    for k, p in PRICES.items():
        if p.get("funpay"): kb.button(text=f"{p['label']} — {p['rub']}₽", url=p["funpay"])
    kb.button(text="🔙", callback_data="cmd_prices"); kb.adjust(1)
    ident = f"@{uname}" if uname else f"ID:{uid}"
    await edit_msg(cb.message, f"💳 <b>FunPay</b>\n🆔 Укажите: <code>{ident}</code>", kb.as_markup())

@dp.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery):
    k = cb.data[4:]; p = PRICES.get(k)
    if not p: await answer_cb(cb, "❌", show_alert=True); return
    await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"💎 {p['label']}", description=f"Premium {p['label']}",
        payload=f"sub_{k}_{cb.from_user.id}", provider_token="", currency="XTR",
        prices=[LabeledPrice(label=p["label"], amount=p["stars"])])


# ─── ПОДДЕРЖАТЬ ───

@dp.callback_query(F.data == "cmd_support")
async def cb_support(cb: CallbackQuery):
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for amt in DONATE_OPTIONS: kb.button(text=f"⭐ {amt}", callback_data=f"don_{amt}")
    kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(3, 3, 1, 1)
    await edit_msg(cb.message, "🤖 <b>Поддержать проект</b>\n\nВыберите сумму:", kb.as_markup())

@dp.callback_query(F.data.startswith("don_"))
async def cb_don(cb: CallbackQuery):
    amt = int(cb.data[4:]); await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"🤖 Донат {amt}⭐", description="Поддержка",
        payload=f"donate_{amt}_{cb.from_user.id}", provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"Донат {amt}⭐", amount=amt)])


# ─── АКЦИИ / 8 МАРТА ───

@dp.callback_query(F.data.startswith("pv_"))
async def cb_promo_view(cb: CallbackQuery):
    await answer_cb(cb)
    pid = int(cb.data[3:])
    uid = cb.from_user.id
    promos = get_active_promotions()
    promo = None
    for p in promos:
        if p["id"] == pid:
            promo = p
            break

    if not promo:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        await edit_msg(cb.message, "❌ Акция завершена.", kb.as_markup())
        return

    ptype = promo["ptype"]

    if ptype == "ref_contest":
        text = "🌸✨ <b>ПРАЗДНИЧНЫЙ КОНКУРС ОТ КОМАНДЫ SwordUser!</b> ✨🌸\n\n"
        text += "Друзья! 💖 В честь прекрасного праздника 8 Марта мы, команда SwordUser, "
        text += "подготовили для вас глобальное обновление и запускаем большой праздничный конкурс! 🎉\n\n"
        text += "🔥 <b>Условия очень простые:</b>\n"
        text += "Приглашайте как можно больше друзей по своей реферальной ссылке в бота 🤖\n"
        text += "Чем больше друзей зарегистрируется — тем выше ваш шанс занять призовое место! 🏆\n\n"
        text += "🎁 <b>Призы для ТОП-5 участников:</b>\n\n"
        text += "🥇 1 место — 1 неделя подписки ⭐\n"
        text += "🥈 2 место — 5 дней подписки ⭐\n"
        text += "🥉 3 место — 1 день подписки + 25 звёзд ⭐✨\n"
        text += "🏅 4 место — 1 день подписки + 15 звёзд ⭐\n"
        text += "🎖 5 место — 1 день подписки или 15 звёзд ⭐\n\n"
        text += "💎 Покажите свою активность, приглашайте друзей и поднимайтесь в топ участников!\n\n"
        text += "⏳ Количество мест ограничено, поэтому начинайте приглашать друзей уже сейчас! 🚀\n\n"
        text += "🌷 С праздником 8 Марта и удачи всем участникам!\n"
        text += "Ваша команда SwordUser ⚔️"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🏆 Посмотреть топ", callback_data="pt_" + str(pid))
        kb.button(text="👥 Моя ссылка", callback_data="cmd_referral")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)

        try:
            await cb.message.delete()
        except:
            pass
        
        try:
            photo = FSInputFile("march8.jpg")
            await bot.send_photo(uid, photo=photo, caption=text, reply_markup=kb.as_markup(), parse_mode="HTML")
        except:
            await bot.send_message(uid, text, reply_markup=kb.as_markup(), parse_mode="HTML")
        return

    elif ptype == "discount":
        text = "🔥 <b>" + promo["name"] + "</b>\n\n💰 Скидочная акция!"
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    elif ptype == "holiday":
        text = "🎉 <b>" + promo["name"] + "</b>\n\n🌺 Праздничная акция!"
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    else:
        text = "⭐ <b>" + promo["name"] + "</b>\n\n🎁 Специальное предложение!"
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())


# ─── ТИКТОК ───

@dp.callback_query(F.data == "cmd_tiktok")
async def cb_tt(cb: CallbackQuery):
    await answer_cb(cb)
    text = (f"🎁 <b>TikTok задание</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"1️⃣ Найди видео «словил юз тг»\n"
            f"2️⃣ Оставь {TIKTOK_SCREENSHOTS_NEEDED} комментариев:\n"
            f"💬 <code>{TIKTOK_COMMENT_TEXT}</code>\n"
            f"3️⃣ Отправь скриншоты\n4️⃣ Получи 🎁 <b>{TIKTOK_REWARD_GIFT}</b>")
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Начать задание", callback_data="tt_go")
    kb.button(text="📹 Снимай видео!", callback_data="tt_video")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "tt_video")
async def cb_ttv(cb: CallbackQuery):
    await answer_cb(cb)
    text = ("📹 <b>Снимай видео и зарабатывай!</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔥 Хотите легко зарабатывать в интернете, но не знаете как?\n\n"
            "🛡️ SwordUser — не проблема! Снимай видео про @SworuserN_bot и получай деньги!\n\n"
            "💸 1000 просмотров = 1$\n\n"
            "📲 Ознакомиться с условиями — @SwordUserTiktok\n\n"
            "⚡ <b>Быстрый поиск</b> — найди красивые юзы для видео!")
    kb = InlineKeyboardBuilder()
    kb.button(text="⚡ Быстрый поиск", callback_data="tt_fast")
    kb.button(text="📲 @SwordUserTiktok", url="https://t.me/SwordUserTiktok")
    kb.button(text="🔙 TikTok", callback_data="cmd_tiktok")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "tt_fast")
async def cb_tt_fast(cb: CallbackQuery):
    uid = cb.from_user.id
    await answer_cb(cb)

    if uid in searching_users:
        try:
            await bot.send_message(uid, "⏳ Уже идёт поиск!")
        except:
            pass
        return

    searching_users.add(uid)
    try:
        count = 5
        total_attempts = 0
        start = time.time()
        target_time = random.uniform(3, 7)
        steps = random.randint(4, 7)
        step_delay = target_time / steps

        results = []
        while len(results) < count:
            u = gen_beautiful()
            if len(u) == 5 and u.lower() not in [r["username"] for r in results]:
                results.append({"username": u.lower(), "fragment": "unavailable"})

        ps = pool.stats()

        for step in range(steps):
            total_attempts += random.randint(30, 80)
            found_so_far = min(int((step + 1) / steps * count), count)

            await edit_msg(cb.message,
                f"🔎 <b>💎 Красивые</b>\n\n"
                f"📊 Проверено: <code>{total_attempts}</code>\n"
                f"✅ Найдено: <code>{found_so_far}/{count}</code>\n"
                f"🔄 Сессій: <code>{ps['active']}/{ps['total']}</code>\n"
                f"⏱ {int(time.time() - start)}с")

            await asyncio.sleep(step_delay)

        found = results
        elapsed = int(time.time() - start)

        for item in found:
            save_history(uid, item["username"], "TT Видео", len(item["username"]))

        kb = InlineKeyboardBuilder()
        text = (f"✅ <b>Найдено {len(found)} юзернеймов:</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for i, item in enumerate(found, 1):
            ev = evaluate_username(item["username"])
            text += (f"{i}. <code>@{item['username']}</code>\n"
                     f"   {ev['rarity']} | 💰 {ev['price']}\n"
                     f"   [{ev['bar']}] {ev['score']}/200\n\n")
            kb.button(text=f"⭐ {item['username']}", callback_data=f"fav_a_{item['username']}")

        text += (f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                 f"📊 Проверено: <code>{total_attempts}</code>\n"
                 f"⏱ Время: <code>{elapsed}с</code>")

        kb.button(text="⚡ Ещё", callback_data="tt_fast")
        kb.button(text="🔙 Снимай видео", callback_data="tt_video")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())
    finally:
        searching_users.discard(uid)

@dp.callback_query(F.data == "tt_go")
async def cb_tg(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not tiktok_can_submit(uid): return
    tid = task_create(uid)
    user_states[uid] = {"action": "tiktok_proof", "task_id": tid, "photos": 0}
    kb = InlineKeyboardBuilder(); kb.button(text="❌ Отмена", callback_data="tt_cancel")
    await edit_msg(cb.message, f"📸 <b>Задание #{tid}</b>\n\n<code>0/{TIKTOK_SCREENSHOTS_NEEDED}</code>", kb.as_markup())

@dp.callback_query(F.data == "tt_cancel")
async def cb_tc(cb: CallbackQuery):
    await answer_cb(cb); user_states.pop(cb.from_user.id, None)
    t, k = build_menu(cb.from_user.id); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data.startswith("ta_"))
async def cb_ta(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await answer_cb(cb)
    tid = int(cb.data.replace("ta_", ""))
    uid = task_approve(tid, cb.from_user.id)
    if uid:
        try:
            await cb.message.edit_text(
                f"✅ TikTok #{tid} одобрено",
                parse_mode="HTML"
            )
        except:
            pass
        try:
            await bot.send_message(uid, f"🎉 TikTok задание одобрено! 🎁 {TIKTOK_REWARD_GIFT}")
        except:
            pass
    else:
        try:
            await cb.message.edit_text(f"⚠️ TikTok #{tid} уже обработано")
        except:
            pass


@dp.callback_query(F.data.startswith("tr_"))
async def cb_trj(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return
    await answer_cb(cb)
    tid = int(cb.data.replace("tr_", ""))
    uid = task_reject(tid, cb.from_user.id)
    try:
        await cb.message.edit_text(
            f"❌ TikTok #{tid} отклонено",
            parse_mode="HTML"
        )
    except:
        pass
    if uid:
        try:
            await bot.send_message(uid, "❌ TikTok задание отклонено.")
        except:
            pass


# ─── ОПЛАТА ───

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await q.answer(ok=True)

@dp.message(F.successful_payment)
async def succ_pay(msg: Message):
    payload = msg.successful_payment.invoice_payload; parts = payload.split("_")
    amount_paid = msg.successful_payment.total_amount

    if parts[0] == "sub" and len(parts) >= 3:
        k, uid = parts[1], int(parts[2]); p = PRICES.get(k)
        if p:
            end = give_subscription(uid, p["days"])
            await msg.answer(f"🎉 <b>Оплачено!</b> {p['label']} до {end}", parse_mode="HTML")
            ref_uid = get_user(uid).get("referred_by", 0)
            if ref_uid and ref_uid != uid:
                comm = round(amount_paid * REFERRAL_COMMISSION, 1)
                if comm > 0: add_balance(ref_uid, comm)
                try: await bot.send_message(ref_uid, f"💰 Комиссия <code>{comm}</code>⭐!", parse_mode="HTML")
                except: pass
            for aid in ADMIN_IDS:
                try: await bot.send_message(aid, f"💰 {uid} — {p['label']} ({p['stars']}⭐)")
                except: pass

    elif parts[0] == "gift" and len(parts) >= 4:
        k = parts[1]; tuid = int(parts[2]); buid = int(parts[3]); p = PRICES.get(k)
        if p:
            end = give_subscription(tuid, p["days"])
            await msg.answer(f"🎁 <b>Подарок!</b> {p['label']}", parse_mode="HTML")
            try: await bot.send_message(tuid, f"🎁 Вам подарили <b>{p['label']}</b>! До: {end}", parse_mode="HTML")
            except: pass

    elif parts[0] == "key" and len(parts) >= 3:
        kp = parts[1]; uid = int(parts[2]); p = PRICES.get(kp)
        if p:
            gk = generate_key(p["days"], "BOUGHT"); res = activate_key(uid, gk)
            if res: await msg.answer(f"🔑 <b>Активирован!</b> {p['label']} до {res['end']}", parse_mode="HTML")

    elif parts[0] == "donate" and len(parts) >= 3:
        amt = int(parts[1]); uid = int(parts[2])
        await msg.answer("❤️ <b>Спасибо за поддержку!</b>", parse_mode="HTML")
        for aid in ADMIN_IDS:
            try: await bot.send_message(aid, f"🤖 Донат {amt}⭐ от {uid}")
            except: pass


# ─── ВЫВОДЫ ───

@dp.callback_query(F.data.startswith("wd_ok_"))
async def cb_wdo(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wid = int(cb.data[6:]); r = process_withdrawal(wid, cb.from_user.id, True)
    if r:
        await edit_msg(cb.message, f"✅ Вывод #{wid} одобрен ({r['amount']:.1f}⭐)")
        try: await bot.send_message(r["uid"], f"✅ Вывод #{wid} {r['amount']:.1f}⭐ одобрен!")
        except: pass

@dp.callback_query(F.data.startswith("wd_no_"))
async def cb_wdn(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wid = int(cb.data[6:]); r = process_withdrawal(wid, cb.from_user.id, False)
    if r:
        await edit_msg(cb.message, f"❌ Вывод #{wid} отклонён")
        try: await bot.send_message(r["uid"], f"❌ Вывод #{wid} отклонён")
        except: pass


# ═══════════════════════ АДМИН ═══════════════════════

@dp.callback_query(F.data == "cmd_admin")
async def cb_admin(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); s = get_stats(); ps = pool.stats()
    text = (f"👑 <b>Админ-панель</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <code>{s['users']}</code> | 💎 <code>{s['subs']}</code> | 🚫 <code>{s['banned']}</code>\n"
            f"🔍 <code>{s['searches']}</code> | Сегодня: 👤<code>{s['today_users']}</code> 🔍<code>{s['today_searches']}</code>\n\n"
            f"🔄 Пул: <code>{ps['active']}/{ps['total']}</code> ({ps['checks']})\n"
            f"📱 TT: <code>{s['tasks']}</code> | 💸 <code>{s['withdrawals']}</code> | 📢 <code>{s['promos']}</code>")
    kb = InlineKeyboardBuilder()
    kb.button(text="🔑 Ключ", callback_data="a_keys"); kb.button(text="📩 Выдать", callback_data="a_give")
    kb.button(text="💎 Premium", callback_data="a_plist"); kb.button(text="🚫 Бан", callback_data="a_ban")
    kb.button(text="✅ Разбан", callback_data="a_unban"); kb.button(text=f"📱 TT ({s['tasks']})", callback_data="a_tt")
    kb.button(text=f"💸 Вывод ({s['withdrawals']})", callback_data="a_wd"); kb.button(text="📤 Рассылка", callback_data="a_bcast")
    kb.button(text="📊 Экспорт", callback_data="a_export"); kb.button(text="📢 Акции", callback_data="a_promos")
    kb.button(text="🎁 Разыграть", callback_data="a_raffle"); kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(2)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_plist")
async def cb_aplist(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); users = get_premium_users()
    text = f"💎 <b>Premium ({len(users)}):</b>\n\n"
    for u in users[:20]: text += f"• {_d(u['uid'], u['uname'])} — до {u['sub_end']}\n"
    if not users: text += "<i>Нет</i>"
    kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_ban")
async def cb_aban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "admin_ban_input"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "🚫 <b>ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_unban")
async def cb_aunban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "admin_unban_input"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "✅ <b>ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_keys")
async def cb_akeys(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    for k, p in PRICES.items(): kb.button(text=p["label"], callback_data=f"gk_{p['days']}")
    kb.button(text="✏️ Своё", callback_data="gk_custom"); kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message, "🔑 <b>Срок ключа:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("gk_"))
async def cb_gk(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); val = cb.data[3:]
    if val == "custom":
        user_states[cb.from_user.id] = {"action": "admin_key_days"}
        kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="a_keys")
        await edit_msg(cb.message, "✏️ <b>Кол-во дней:</b>", kb.as_markup()); return
    days = int(val); key = generate_key(days, f"D{days}")
    await edit_msg(cb.message, f"🔑 <code>{key}</code>\n📅 {days} дн")

@dp.callback_query(F.data == "a_give")
async def cb_agive(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "admin_give_user"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "📩 <b>ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_tt")
async def cb_att(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); tasks = get_pending_tasks()
    if not tasks:
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_admin")
        await edit_msg(cb.message, "📱 Нет заданий", kb.as_markup()); return
    kb = InlineKeyboardBuilder()
    for t in tasks:
        kb.button(text=f"✅ #{t['id']}", callback_data=f"ta_{t['id']}")
        kb.button(text=f"❌ #{t['id']}", callback_data=f"tr_{t['id']}")
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2)
    text = f"📱 <b>TikTok ({len(tasks)}):</b>\n\n"
    for t in tasks: text += f"#{t['id']} | {t['uid']} | {t['created']}\n"
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_wd")
async def cb_awd(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); wds = get_pending_withdrawals()
    if not wds:
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_admin")
        await edit_msg(cb.message, "💸 Нет выводов", kb.as_markup()); return
    text = f"💸 <b>Выводы ({len(wds)}):</b>\n\n"; kb = InlineKeyboardBuilder()
    for w in wds:
        text += f"#{w['id']} | {w['uid']} | {w['amount']:.1f}⭐\n"
        kb.button(text=f"✅ #{w['id']}", callback_data=f"wd_ok_{w['id']}")
        kb.button(text=f"❌ #{w['id']}", callback_data=f"wd_no_{w['id']}")
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_promos")
async def cb_apromos(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); promos = get_active_promotions()
    text = f"📢 <b>Акции ({len(promos)}):</b>\n\n"
    kb = InlineKeyboardBuilder()
    for pr in promos:
        text += f"• #{pr['id']} <b>{pr['name']}</b>\n  🔘 Кнопка: <code>{pr.get('button_text', pr['name'])}</code>\n  📋 {pr['ptype']}\n\n"
        kb.button(text=f"❌ #{pr['id']}", callback_data=f"a_endp_{pr['id']}")
    if not promos: text += "<i>Нет</i>\n"
    kb.button(text="➕ Создать", callback_data="a_addp"); kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_addp")
async def cb_aaddp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "admin_promo_name"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="a_promos")
    await edit_msg(cb.message, "📢 <b>Введите название акции:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("a_endp_"))
async def cb_aendp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); pid = int(cb.data[7:]); end_promotion(pid)
    await cb_apromos(cb)

@dp.callback_query(F.data == "a_raffle")
async def cb_araffle(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); kb = InlineKeyboardBuilder()
    kb.button(text="🎁 1д", callback_data="raf_1"); kb.button(text="🎁 3д", callback_data="raf_3")
    kb.button(text="🎁 7д", callback_data="raf_7"); kb.button(text="🎁 30д", callback_data="raf_30")
    kb.button(text="🔙", callback_data="cmd_admin"); kb.adjust(2, 2, 1)
    conn = sqlite3.connect(DB); c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM users WHERE banned=0").fetchone()[0]; conn.close()
    await edit_msg(cb.message, f"🎁 <b>Розыгрыш</b>\n\n👥 {total} участников", kb.as_markup())

@dp.callback_query(F.data.startswith("raf_"))
async def cb_raf(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); days = int(cb.data[4:])
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,uname FROM users WHERE banned=0"); au = c.fetchall(); conn.close()
    if not au: return
    w = random.choice(au); end = give_subscription(w[0], days)
    await edit_msg(cb.message, f"🎉 <b>Победитель:</b> {_d(w[0], w[1])}\n🎁 {days} дн до {end}")
    try: await bot.send_message(w[0], f"🎉 Вы выиграли <b>{days} дн Premium</b>! До: {end}", parse_mode="HTML")
    except: pass

@dp.callback_query(F.data == "a_bcast")
async def cb_abcast(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb); user_states[cb.from_user.id] = {"action": "admin_broadcast_text"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "📤 <b>Текст рассылки:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_export")
async def cb_aexport(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid,uname,joined,searches,sub_end,ref_count,banned,balance FROM users ORDER BY uid")
    rows = c.fetchall(); conn.close()
    content = f"USERS — {len(rows)}\n{'='*40}\n\n"
    for r in rows: content += f"{_d(r[0], r[1])} | s:{r[3]} | ref:{r[5]} | bal:{r[7] or 0:.1f}⭐\n"
    await bot.send_document(cb.from_user.id,
        BufferedInputFile(content.encode(), filename=f"users_{datetime.now().strftime('%Y%m%d')}.txt"),
        caption=f"📊 {len(rows)}")


# ═══════════════════════ НАПОМИНАНИЯ ═══════════════════════

async def reminder_loop():
    while True:
        try:
            await asyncio.sleep(REMINDER_CHECK_INTERVAL)
            today_str = datetime.now().strftime("%Y-%m-%d")
            for db in REMINDER_DAYS:
                users = get_expiring_users(db)
                for u in users:
                    rk = f"{today_str}_d{db}"
                    if rk in u.get("last_reminder", ""): continue
                    kb = InlineKeyboardBuilder(); kb.button(text="💰 Продлить", callback_data="cmd_prices"); kb.adjust(1)
                    try:
                        await bot.send_message(u["uid"], f"🔔 Подписка через {db} дн!\n⏰ {u['sub_end']}",
                                               reply_markup=kb.as_markup(), parse_mode="HTML")
                        set_last_reminder(u["uid"], rk)
                    except: pass
        except Exception as e:
            logger.error(f"Reminder: {e}")


# ═══════════════════════ ЗАПУСК ═══════════════════════

async def main():
    global http_session, bot_info
    init_db()
    bot_info = await bot.get_me()
    http_session = aiohttp.ClientSession()
    await pool.init(ACCOUNTS)
    ps = pool.stats()
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"🚀 @{bot_info.username} v21.0")
    logger.info(f"🔄 Аккаунтов: {ps['total']}")
    logger.info(f"🎯 Premium: {PREMIUM_COUNT} юзов, {PREMIUM_SEARCHES_LIMIT} поисков")
    logger.info(f"🆓 Free: {FREE_COUNT} юз, {FREE_SEARCHES} поисков")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    promos = get_active_promotions()
    if not promos:
        create_promotion("🌸 Конкурс рефералов 8 Марта", "ref_contest", button_text="🌺 8 Марта 🏆")

    asyncio.create_task(reminder_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_session.close()
        await pool.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
