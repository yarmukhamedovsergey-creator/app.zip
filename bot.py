"""
USERNAME HUNTER v23.0 — MEGA UPDATE
- 8 режимов поиска (матерный, telegram, шаблон, похожие)
- Полная панель управления юзером
- Магазин за звёзды
- Fallback без сессий
- Авто-мониторинг юзов
- Управление сервером через бота
- Rate limit + детекция ботов
- Бандлы
- Чёрный список юзов
- Лог действий
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
]

FREE_SEARCHES = 3
FREE_COUNT = 1
PREMIUM_COUNT = 3
PREMIUM_SEARCHES_LIMIT = 7
REF_BONUS = 2
REFERRAL_COMMISSION = 0.04
SEARCH_COOLDOWN = 10
MIN_WITHDRAW = 50

TIKTOK_COMMENT_TEXT = "@SworuserN_bot бесплатные звёзды, найти юз, оценить юз"
TIKTOK_REWARD_GIFT = "🧸 Мишка (15⭐)"
TIKTOK_SCREENSHOTS_NEEDED = 35
TIKTOK_DAILY_LIMIT = 2
REMINDER_DAYS = [3, 1]
REMINDER_CHECK_INTERVAL = 3600
MONITOR_CHECK_INTERVAL = 1800
MONITOR_MAX_FREE = 0
MONITOR_MAX_PREMIUM = 5

# Rate limit
RATE_SEARCH_PER_MIN = 3
RATE_CHECK_PER_HOUR = 50
TEMP_BAN_MINUTES = 30

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

BUNDLES = {
    "starter": {"label": "⚡ Starter Pack", "days": 3, "searches": 10, "stars": 80,
                "desc": "3 дня Premium + 10 поисков"},
    "pro":     {"label": "🔥 Pro Pack", "days": 7, "searches": 30, "stars": 200,
                "desc": "7 дней Premium + 30 поисков"},
    "ultra":   {"label": "💎 Ultra Pack", "days": 30, "searches": 100, "stars": 650,
                "desc": "30 дней Premium + 100 поисков"},
}

SHOP_ITEMS = {
    "s5":  {"name": "+5 поисков",  "searches": 5,  "stars": 10},
    "s15": {"name": "+15 поисков", "searches": 15, "stars": 25},
    "s50": {"name": "+50 поисков", "searches": 50, "stars": 70},
    "mon1": {"name": "👁 Мониторинг 1 юза (7д)", "monitor": 1, "stars": 20},
    "tpl1": {"name": "🎯 Шаблон (1 раз)", "template": 1, "stars": 5},
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
user_action_log = {}
_fragment_cache = {}
_fragment_cache_ttl = 600


async def edit_msg(msg, text, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    except Exception as e:
        logger.error(f"edit error: {e}")


async def answer_cb(cb, text=None, show_alert=False):
    try:
        await cb.answer(text, show_alert=show_alert)
    except:
        pass


# ═══════════════════════ RATE LIMITER ═══════════════════════

class RateLimiter:
    def __init__(self):
        self.search_times = {}
        self.check_times = {}
        self.warnings = {}
        self.temp_bans = {}

    def is_temp_banned(self, uid):
        if uid in ADMIN_IDS:
            return False
        ban_until = self.temp_bans.get(uid, 0)
        if time.time() < ban_until:
            return True
        if ban_until > 0:
            del self.temp_bans[uid]
        return False

    def temp_ban(self, uid):
        self.temp_bans[uid] = time.time() + TEMP_BAN_MINUTES * 60
        self.warnings[uid] = 0

    def check_search(self, uid):
        if uid in ADMIN_IDS:
            return True, ""
        now = time.time()
        times = self.search_times.setdefault(uid, [])
        times[:] = [t for t in times if now - t < 60]
        if len(times) >= RATE_SEARCH_PER_MIN:
            self.warnings[uid] = self.warnings.get(uid, 0) + 1
            if self.warnings.get(uid, 0) >= 3:
                self.temp_ban(uid)
                return False, "ban"
            return False, "warn"
        times.append(now)
        return True, ""

    def check_action(self, uid):
        if uid in ADMIN_IDS:
            return True, ""
        now = time.time()
        times = self.check_times.setdefault(uid, [])
        times[:] = [t for t in times if now - t < 3600]
        if len(times) >= RATE_CHECK_PER_HOUR:
            self.warnings[uid] = self.warnings.get(uid, 0) + 1
            if self.warnings.get(uid, 0) >= 3:
                self.temp_ban(uid)
                return False, "ban"
            return False, "warn"
        times.append(now)
        return True, ""

    def get_ban_remaining(self, uid):
        ban_until = self.temp_bans.get(uid, 0)
        rem = ban_until - time.time()
        return max(0, int(rem / 60))


rate_limiter = RateLimiter()


# ═══════════════════════ ACTION LOG ═══════════════════════

def log_action(uid, action, details=""):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO action_log (uid, action, details, created) VALUES (?,?,?,?)",
                  (uid, action, details, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    except:
        pass
    conn.commit()
    conn.close()

def get_action_log(limit=50):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("SELECT uid, action, details, created FROM action_log ORDER BY id DESC LIMIT ?", (limit,))
        rows = c.fetchall()
    except:
        rows = []
    conn.close()
    return [{"uid": r[0], "action": r[1], "details": r[2], "created": r[3]} for r in rows]


# ═══════════════════════ ПУЛ АККАУНТОВ v3.0 ═══════════════════════

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
        self.BASE_DELAY = 5.0
        self.MAX_DELAY = 30.0
        self.BUDGET_PER_MIN = 8
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
            logger.info("Telethon отсутствует — режим Bot API")
            return
        self.accounts_data = accounts
        # Загружаем доп. сессии
        try:
            if os.path.exists("added_sessions.json"):
                with open("added_sessions.json", "r") as f:
                    extra = json.load(f)
                    accounts = accounts + extra
        except:
            pass
        for i, acc in enumerate(accounts):
            phone = acc["phone"].replace("+", "").replace(" ", "")
            try:
                client = TelegramClient(
                    f"sessions/s_{phone}", acc["api_id"], acc["api_hash"],
                    connection_retries=5, retry_delay=3, timeout=15, request_retries=2)
                await client.connect()
                if not await client.is_user_authorized():
                    await client.start(phone=acc["phone"])
                self.clients.append(client)
                idx = len(self.clients) - 1
                self._init_session_state(idx)
                logger.info(f"✅ Session #{idx+1}: {acc['phone']}")
            except Exception as e:
                logger.error(f"❌ Session {acc['phone']}: {e}")
        logger.info(f"Пул: {len(self.clients)} сессий")
        self._health_task = asyncio.create_task(self._health_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    def _init_session_state(self, idx):
        self.status[idx] = 'warming'
        self.cooldown_until[idx] = 0
        self.last_used[idx] = 0
        self.error_streak[idx] = 0
        self.total_errors[idx] = 0
        self.req_count[idx] = 0
        self.window_start[idx] = time.time()
        self.flood_times[idx] = []
        self.adaptive_delay[idx] = self.BASE_DELAY

    def has_sessions(self):
        return any(self.status.get(i) in ('healthy', 'warming') for i in range(len(self.clients)))

    async def _health_loop(self):
        while True:
            try:
                await asyncio.sleep(60)
                now = time.time()
                for i in range(len(self.clients)):
                    st = self.status.get(i, 'dead')
                    if st == 'dead':
                        await self._try_reconnect(i)
                    elif st == 'cooldown':
                        if now >= self.cooldown_until.get(i, 0):
                            self.status[i] = 'warming'
                            self.error_streak[i] = 0
                            self.adaptive_delay[i] = self.BASE_DELAY + 2.0
                    elif st == 'warming':
                        if now - self.last_used.get(i, 0) > 30 and self.error_streak.get(i, 0) == 0:
                            self.status[i] = 'healthy'
                            self.adaptive_delay[i] = self.BASE_DELAY
                    ws = self.window_start.get(i, 0)
                    if now - ws > 60:
                        self.req_count[i] = 0
                        self.window_start[i] = now
                    if i in self.flood_times:
                        self.flood_times[i] = [t for t in self.flood_times[i] if now - t < 3600]
                alive = sum(1 for j in range(len(self.clients)) if self.status.get(j) in ('healthy', 'warming'))
                if alive > 0 and alive <= 2:
                    for i in range(len(self.clients)):
                        if self.status.get(i) in ('healthy', 'warming'):
                            self.adaptive_delay[i] = max(self.adaptive_delay[i], self.BASE_DELAY * 2)
            except Exception as e:
                logger.error(f"Health loop: {e}")

    async def _monitor_loop(self):
        while True:
            await asyncio.sleep(300)
            try:
                s = self.stats()
                logger.info(f"📊 Пул: {s['active']}/{s['total']} | checks={s['checks']}")
            except:
                pass

    async def _try_reconnect(self, idx):
        try:
            client = self.clients[idx]
            if client.is_connected():
                await client.disconnect()
            await asyncio.sleep(2)
            await client.connect()
            if await client.is_user_authorized():
                self.status[idx] = 'warming'
                self.error_streak[idx] = 0
                self.adaptive_delay[idx] = self.BASE_DELAY + 3.0
                self.reconnect_count += 1
                logger.info(f"🔄 Session #{idx+1}: reconnected!")
        except Exception as e:
            logger.error(f"Reconnect #{idx+1}: {e}")

    def _get_best_session(self, uid=None):
        now = time.time()
        candidates = []
        for i in range(len(self.clients)):
            st = self.status.get(i, 'dead')
            if st == 'dead':
                continue
            if st == 'cooldown' and now < self.cooldown_until.get(i, 0):
                continue
            ws = self.window_start.get(i, 0)
            if now - ws > 60:
                self.req_count[i] = 0
                self.window_start[i] = now
            budget = self.BUDGET_PER_MIN
            active_count = sum(1 for j in range(len(self.clients)) if self.status.get(j) in ('healthy', 'warming'))
            if active_count <= 2:
                budget = max(5, budget // 2)
            if self.req_count.get(i, 0) >= budget:
                continue
            delay = self.adaptive_delay.get(i, self.BASE_DELAY)
            if st == 'warming':
                delay += self.WARMUP_EXTRA_DELAY
            if now - self.last_used.get(i, 0) < delay:
                continue
            score = self.error_streak.get(i, 0) * 20.0 + self.req_count.get(i, 0) * 2.0
            if st == 'warming':
                score += 50.0
            if uid and uid in self.active_users.get(i, set()):
                score -= 25.0
            candidates.append((i, score))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1])
        top = candidates[:min(3, len(candidates))]
        return random.choice(top)[0]

    async def _acquire(self, uid=None, timeout=45):
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            async with self.lock:
                idx = self._get_best_session(uid)
                if idx is not None:
                    self.last_used[idx] = time.time()
                    self.req_count[idx] = self.req_count.get(idx, 0) + 1
                    self.total_checks += 1
                    return idx, self.clients[idx]
            wait = min(0.3 * (1.2 ** attempt), 3.0) + random.uniform(0, 0.5)
            await asyncio.sleep(wait)
            attempt += 1
        return None, None

    def _on_success(self, idx):
        self.error_streak[idx] = 0
        if self.status.get(idx) == 'warming' and self.req_count.get(idx, 0) >= 3:
            self.status[idx] = 'healthy'
        self.adaptive_delay[idx] = max(self.BASE_DELAY, self.adaptive_delay.get(idx, self.BASE_DELAY) * 0.95)

    def _on_error(self, idx, is_flood=False, flood_seconds=0):
        self.error_streak[idx] = self.error_streak.get(idx, 0) + 1
        self.total_errors[idx] = self.total_errors.get(idx, 0) + 1
        if is_flood:
            rest = flood_seconds + random.randint(20, 45)
            self.cooldown_until[idx] = time.time() + rest
            self.status[idx] = 'cooldown'
            self.flood_times.setdefault(idx, []).append(time.time())
            recent = [t for t in self.flood_times[idx] if time.time() - t < 3600]
            if len(recent) >= 3:
                self.cooldown_until[idx] = time.time() + self.FLOOD_REST_TIME
        elif self.error_streak[idx] >= self.MAX_ERROR_STREAK:
            self.status[idx] = 'dead'
        else:
            self.adaptive_delay[idx] = min(self.adaptive_delay.get(idx, self.BASE_DELAY) * 1.5, self.MAX_DELAY)
            self.cooldown_until[idx] = time.time() + 3
            self.status[idx] = 'cooldown'

    async def _resolve_username(self, username, uid=None):
        idx, client = await self._acquire(uid, timeout=30)
        if client is None:
            return "no_session", -1
        try:
            await client(ResolveUsernameRequest(username))
            self._on_success(idx)
            return "taken", idx
        except UsernameNotOccupiedError:
            self._on_success(idx)
            return "free", idx
        except UsernameInvalidError:
            self._on_success(idx)
            return "invalid", idx
        except FloodWaitError as e:
            self._on_error(idx, is_flood=True, flood_seconds=e.seconds)
            return "flood", idx
        except Exception as e:
            self._on_error(idx)
            return "error", idx

    async def _check_username_available(self, username, uid=None):
        idx, client = await self._acquire(uid, timeout=20)
        if client is None:
            return "no_session", -1
        try:
            ok = await client(AccountCheckUsername(username))
            self._on_success(idx)
            return ("free" if ok else "taken"), idx
        except FloodWaitError as e:
            self._on_error(idx, is_flood=True, flood_seconds=e.seconds)
            return "flood", idx
        except Exception as e:
            self._on_error(idx)
            return "error", idx

    async def _botapi_check(self, username):
        try:
            await bot.get_chat(f"@{username}")
            return "taken"
        except TelegramBadRequest as e:
            if "not found" in str(e).lower():
                return "not_found"
            return "error"
        except:
            return "not_found"

    async def check(self, username, uid=None):
        if not self.has_sessions():
            r = await self._botapi_check(username)
            return "taken" if r == "taken" else "maybe_free"
        r1, _ = await self._resolve_username(username, uid)
        if r1 in ("taken", "invalid"):
            return "taken"
        if r1 in ("flood", "no_session", "error"):
            b = await self._botapi_check(username)
            return "taken" if b == "taken" else "skip"
        b = await self._botapi_check(username)
        if b == "taken":
            self.caught_by_botapi += 1
            return "taken"
        return "maybe_free"

    async def strong_check(self, username, uid=None):
        if not self.has_sessions():
            r = await self._botapi_check(username)
            return "taken" if r == "taken" else "free"
        await asyncio.sleep(random.uniform(1.0, 2.5))
        r1, _ = await self._check_username_available(username, uid)
        if r1 == "taken":
            return "taken"
        if r1 in ("flood", "no_session", "error"):
            return "skip"
        await asyncio.sleep(random.uniform(0.8, 2.0))
        r2, _ = await self._resolve_username(username, uid)
        if r2 in ("taken", "invalid"):
            self.caught_by_recheck += 1
            return "taken"
        if r2 in ("flood", "no_session", "error"):
            return "skip"
        b = await self._botapi_check(username)
        if b == "taken":
            self.caught_by_botapi += 1
            return "taken"
        return "free"

    def add_user(self, uid):
        for i in range(len(self.clients)):
            if self.status.get(i) in ('dead',):
                continue
            if uid in self.active_users.get(i, set()):
                return i
        for i in range(len(self.clients)):
            if self.status.get(i) in ('dead',):
                continue
            users = self.active_users.setdefault(i, set())
            if len(users) < self.max_users_per_account:
                users.add(uid)
                return i
        return None

    def remove_user(self, uid):
        for idx in self.active_users:
            self.active_users[idx].discard(uid)

    def stats(self):
        active = sum(1 for i in range(len(self.clients)) if self.status.get(i) in ('healthy', 'warming'))
        warming = sum(1 for i in range(len(self.clients)) if self.status.get(i) == 'warming')
        cooldown = sum(1 for i in range(len(self.clients)) if self.status.get(i) == 'cooldown')
        dead = sum(1 for i in range(len(self.clients)) if self.status.get(i) == 'dead')
        total_errs = sum(self.total_errors.get(i, 0) for i in range(len(self.clients)))
        return {"total": len(self.clients), "active": active, "warming": warming,
                "cooldown": cooldown, "dead": dead, "checks": self.total_checks,
                "errors": total_errs, "botapi_saves": self.caught_by_botapi,
                "recheck_saves": self.caught_by_recheck, "reconnects": self.reconnect_count}

    def detailed_status(self):
        lines = []
        for i in range(len(self.clients)):
            st = self.status.get(i, 'dead')
            emoji = {'healthy': '🟢', 'warming': '🟡', 'cooldown': '🟠', 'dead': '🔴'}.get(st, '⚪')
            d = self.adaptive_delay.get(i, self.BASE_DELAY)
            e = self.error_streak.get(i, 0)
            r = self.req_count.get(i, 0)
            lines.append(f"{emoji}#{i+1} {st} d={d:.1f} e={e} r={r}")
        return "\n".join(lines)

    async def disconnect(self):
        if self._health_task: self._health_task.cancel()
        if self._monitor_task: self._monitor_task.cancel()
        for c in self.clients:
            try: await c.disconnect()
            except: pass


pool = AccountPool()


# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
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
        template_uses INTEGER DEFAULT 0
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
        status TEXT DEFAULT 'pending', created TEXT, processed_by INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_uid INTEGER,
        referred_uid INTEGER, referred_uname TEXT DEFAULT '', created TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER,
        action TEXT, details TEXT DEFAULT '', created TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        username TEXT PRIMARY KEY, added_by INTEGER, created TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS monitors (
        id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, username TEXT,
        status TEXT DEFAULT 'active', created TEXT, expires TEXT,
        last_check TEXT DEFAULT '', last_status TEXT DEFAULT 'taken'
    )""")
    # Миграции
    for col, default in [
        ("banned", "0"), ("balance", "0.0"), ("pending_ref", "0"),
        ("captcha_passed", "0"), ("last_roulette", "''"),
        ("auto_renew", "0"), ("auto_renew_plan", "''"), ("last_reminder", "''"),
        ("extra_searches", "0"), ("monitor_slots", "0"), ("template_uses", "0")
    ]:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except: pass
    try: c.execute("ALTER TABLE promotions ADD COLUMN button_text TEXT DEFAULT ''")
    except: pass
    conn.commit()
    conn.close()
# ═══════════════════════ DB ФУНКЦИИ ═══════════════════════

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
                "sub_end": "", "ref_count": 0, "referred_by": 0,
                "sub_bonus": 0, "auto_renew": 0, "auto_renew_plan": "",
                "last_reminder": "", "banned": 0, "balance": 0.0,
                "pending_ref": 0, "captcha_passed": 0, "last_roulette": "",
                "extra_searches": 0, "monitor_slots": 0, "template_uses": 0}
    d = dict(row)
    for k, v in [("auto_renew", 0), ("auto_renew_plan", ""), ("last_reminder", ""),
                 ("banned", 0), ("balance", 0.0), ("pending_ref", 0),
                 ("captcha_passed", 0), ("last_roulette", ""),
                 ("extra_searches", 0), ("monitor_slots", 0), ("template_uses", 0)]:
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
    rate_limiter.temp_bans.pop(uid, None)
    rate_limiter.warnings.pop(uid, None)

def has_subscription(uid):
    if uid in ADMIN_IDS: return True
    sub_end = get_user(uid).get("sub_end", "")
    if not sub_end: return False
    try: return datetime.strptime(sub_end, "%Y-%m-%d %H:%M") > datetime.now()
    except: return False

def can_search(uid):
    if uid in ADMIN_IDS or has_subscription(uid): return True
    u = get_user(uid)
    return u.get("free", 0) > 0 or u.get("extra_searches", 0) > 0

def use_search(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    if uid in ADMIN_IDS or has_subscription(uid):
        c.execute("UPDATE users SET searches=searches+1 WHERE uid=?", (uid,))
    else:
        u = get_user(uid)
        if u.get("extra_searches", 0) > 0:
            c.execute("UPDATE users SET extra_searches=MAX(extra_searches-1,0), searches=searches+1 WHERE uid=?", (uid,))
        else:
            c.execute("UPDATE users SET free=MAX(free-1,0), searches=searches+1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def get_search_count(uid):
    if uid in ADMIN_IDS: return 6
    return PREMIUM_COUNT if has_subscription(uid) else FREE_COUNT

def get_max_searches(uid):
    if uid in ADMIN_IDS: return 999
    if has_subscription(uid): return PREMIUM_SEARCHES_LIMIT
    u = get_user(uid)
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
    c.execute("UPDATE users SET sub_end='' WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def set_free_searches(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET free=? WHERE uid=?", (max(0, count), uid))
    conn.commit(); conn.close()

def add_extra_searches(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET extra_searches=extra_searches+? WHERE uid=?", (count, uid))
    conn.commit(); conn.close()

def set_balance(uid, amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=? WHERE uid=?", (max(0, amount), uid))
    conn.commit(); conn.close()

def add_balance(uid, amount):
    ensure_user(uid); conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=balance+? WHERE uid=?", (amount, uid))
    conn.commit(); conn.close()

def get_balance(uid): return get_user(uid).get("balance", 0.0)

def add_monitor_slots(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET monitor_slots=monitor_slots+? WHERE uid=?", (count, uid))
    conn.commit(); conn.close()

def add_template_uses(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET template_uses=template_uses+? WHERE uid=?", (count, uid))
    conn.commit(); conn.close()

def process_referral(new_uid, ref_uid):
    if new_uid == ref_uid: return False
    u = get_user(new_uid)
    if u.get("referred_by", 0) != 0: return False
    new_uname = get_user(new_uid).get("uname", "")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?", (ref_uid, new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1, free=free+? WHERE uid=?", (REF_BONUS, ref_uid))
    c.execute("INSERT INTO referrals (referrer_uid, referred_uid, referred_uname, created) VALUES (?,?,?,?)",
              (ref_uid, new_uid, new_uname, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close(); return True

def get_user_referrals(uid, limit=50):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referred_uid, referred_uname, created FROM referrals WHERE referrer_uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "uname": r[1], "created": r[2]} for r in rows]

def get_ref_top_by_period(start_date, limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referrer_uid, COUNT(*) as cnt FROM referrals WHERE created >= ? GROUP BY referrer_uid ORDER BY cnt DESC LIMIT ?", (start_date, limit))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "uname": get_user(r[0]).get("uname", ""), "ref_count": r[1]} for r in rows]

def check_referral_fraud(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT referred_uid, created FROM referrals WHERE referrer_uid=? ORDER BY created", (uid,))
    rows = c.fetchall(); conn.close()
    if len(rows) < 3: return {"fraud": False, "reason": ""}
    suspicious = 0
    for i in range(1, len(rows)):
        try:
            prev = datetime.strptime(rows[i-1][1], "%Y-%m-%d %H:%M")
            curr = datetime.strptime(rows[i][1], "%Y-%m-%d %H:%M")
            if (curr - prev).total_seconds() < 60: suspicious += 1
        except: pass
    if suspicious >= 3: return {"fraud": True, "reason": "Много рефералов за короткое время"}
    return {"fraud": False, "reason": ""}

def remove_referral(referrer_uid, referred_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM referrals WHERE referrer_uid=? AND referred_uid=?", (referrer_uid, referred_uid))
    c.execute("UPDATE users SET ref_count=MAX(ref_count-1,0), free=MAX(free-?,0) WHERE uid=?", (REF_BONUS, referrer_uid))
    c.execute("UPDATE users SET referred_by=0 WHERE uid=?", (referred_uid,))
    conn.commit(); conn.close()

async def check_referral_subscription(referrer_uid):
    refs = get_user_referrals(referrer_uid, 100); removed = 0
    for ref in refs:
        ns = await check_subscribed(ref["uid"])
        if ns: remove_referral(referrer_uid, ref["uid"]); removed += 1
    return removed

def set_pending_ref(uid, ref_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET pending_ref=? WHERE uid=?", (ref_uid, uid)); conn.commit(); conn.close()

def get_pending_ref(uid): return get_user(uid).get("pending_ref", 0)

def set_captcha_passed(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET captcha_passed=1 WHERE uid=?", (uid,)); conn.commit(); conn.close()

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
    c.execute("UPDATE users SET last_reminder=? WHERE uid=?", (ds, uid)); conn.commit(); conn.close()

def get_expiring_users(days_before):
    conn = sqlite3.connect(DB); c = conn.cursor()
    t = datetime.now() + timedelta(days=days_before)
    c.execute("SELECT uid,sub_end,auto_renew,auto_renew_plan,last_reminder FROM users WHERE sub_end BETWEEN ? AND ? AND sub_end!=''",
              (t.strftime("%Y-%m-%d 00:00"), t.strftime("%Y-%m-%d 23:59")))
    rows = c.fetchall(); conn.close()
    return [{"uid": r[0], "sub_end": r[1], "auto_renew": r[2], "auto_renew_plan": r[3], "last_reminder": r[4] or ""} for r in rows]

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
    return [{"id": r[0], "name": r[1], "ptype": r[2], "data": json.loads(r[3] or "{}"),
             "created": r[4], "button_text": r[5] if r[5] else r[1]} for r in rows]

def end_promotion(pid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE promotions SET active=0,ended=? WHERE id=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"), pid)); conn.commit(); conn.close()

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

# ─── Чёрный список ───

def add_blacklist(username, admin_uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("INSERT INTO blacklist (username, added_by, created) VALUES (?,?,?)",
                  (username.lower(), admin_uid, datetime.now().strftime("%Y-%m-%d %H:%M")))
    except: pass
    conn.commit(); conn.close()

def remove_blacklist(username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM blacklist WHERE username=?", (username.lower(),))
    conn.commit(); conn.close()

def is_blacklisted(username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username FROM blacklist WHERE username=?", (username.lower(),))
    r = c.fetchone(); conn.close(); return r is not None

def get_blacklist():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username, added_by, created FROM blacklist ORDER BY created DESC")
    rows = c.fetchall(); conn.close()
    return [{"username": r[0], "added_by": r[1], "created": r[2]} for r in rows]

# ─── Мониторинг ───

def add_monitor(uid, username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    expires = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M")
    c.execute("INSERT INTO monitors (uid, username, status, created, expires) VALUES (?,?,?,?,?)",
              (uid, username.lower(), 'active', datetime.now().strftime("%Y-%m-%d %H:%M"), expires))
    mid = c.lastrowid; conn.commit(); conn.close(); return mid

def get_user_monitors(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id, username, status, created, expires, last_status FROM monitors WHERE uid=? AND status='active'", (uid,))
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "username": r[1], "status": r[2], "created": r[3], "expires": r[4], "last_status": r[5]} for r in rows]

def remove_monitor(mid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET status='removed' WHERE id=? AND uid=?", (mid, uid))
    conn.commit(); conn.close()

def get_active_monitors():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    c.execute("SELECT id, uid, username, expires FROM monitors WHERE status='active' AND expires>?", (now_s,))
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "uid": r[1], "username": r[2], "expires": r[3]} for r in rows]

def update_monitor_status(mid, status):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET last_check=?, last_status=? WHERE id=?",
              (datetime.now().strftime("%Y-%m-%d %H:%M"), status, mid))
    conn.commit(); conn.close()

def expire_monitors():
    conn = sqlite3.connect(DB); c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    c.execute("UPDATE monitors SET status='expired' WHERE status='active' AND expires<=?", (now_s,))
    conn.commit(); conn.close()

def get_monitor_count(uid):
    return len(get_user_monitors(uid))

def get_monitor_limit(uid):
    if uid in ADMIN_IDS: return 99
    u = get_user(uid)
    base = MONITOR_MAX_PREMIUM if has_subscription(uid) else MONITOR_MAX_FREE
    return base + u.get("monitor_slots", 0)

# ─── Статистика ───

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
        "monitors": c.execute("SELECT COUNT(*) FROM monitors WHERE status='active'").fetchone()[0],
        "blacklist": c.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0],
    }
    conn.close(); return r

def find_user(inp):
    """Поиск юзера по ID или username"""
    inp = inp.strip().replace("@", "")
    if inp.isdigit():
        return int(inp)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT uid FROM users WHERE uname=?", (inp,))
    r = c.fetchone(); conn.close()
    return r[0] if r else None


# ═══════════════════════ ГЕНЕРАТОРЫ v3 ═══════════════════════

_V = "aeiou"
_C = "bcdfghjklmnprstvwxyz"

def _pronounceable(length):
    w = []; sc = random.choice([True, False])
    for i in range(length):
        w.append(random.choice(_C) if (i % 2 == 0) == sc else random.choice(_V))
    return "".join(w)

def gen_default():
    style = random.randint(1, 4)
    if style == 1: return _pronounceable(5)
    elif style == 2:
        return random.choice(_C) + random.choice(_V) + random.choice(_C) + random.choice(_V) + random.choice(_C)
    elif style == 3:
        s1 = random.choice(_C) + random.choice(_V)
        s2 = random.choice(_C) + random.choice(_V)
        return s1 + s2 + random.choice(_C)
    else:
        return random.choice(_C) + ''.join(random.choice(_C + _V) for _ in range(4))

def gen_beautiful():
    v = "aeiou"; c = "bcdfghjklmnprstvwxyz"
    patterns = ["cvcvc","cvccv","ccvcv","vcvcv","cvccv","vccvc","ccvcc","cvvcv","vcvcc","cvcvc"]
    pat = random.choice(patterns); word = []; used_c = set()
    for ch in pat:
        if ch == "c":
            pool_c = [x for x in c if x not in used_c]
            if not pool_c: pool_c = list(c)
            letter = random.choice(pool_c); word.append(letter); used_c.add(letter)
        else: word.append(random.choice(v))
    return "".join(word)

def gen_meaningful():
    pre = ["my","go","hi","ok","no","up","on","in","mr","dj","pro","top","hot","big",
           "old","new","red","max","neo","zen","ice","sun","sky","air","sea","own",
           "try","run","fly","win","get","set","fix","mix","pop","raw","now","day","one",
           "la","el","to","so","we","do","be","ex","re","co","mc","de","al","an"]
    suf = ["bot","dev","pro","man","boy","cat","dog","fox","owl","god","war","run",
           "fly","win","fan","art","lab","hub","app","web","net","box","job","pay",
           "buy","car","map","log","key","pin","tag","tip","spy","doc","gem","ink",
           "zen","ace","mod","pal","ion","orb","neo","era","vox","hex","pix","bit"]
    mid = ["cool","fast","best","good","real","true","dark","wild","bold","epic",
           "mega","gold","blue","easy","mini","deep","kind","wise","calm","warm",
           "solo","auto","meta","flex","peak","core","base","nova","zero","alfa"]
    style = random.choice(["ps","pm","us","um","sm","pn","two","three"])
    if style == "ps": r = random.choice(pre) + random.choice(suf)
    elif style == "pm": r = random.choice(pre) + random.choice(mid)
    elif style == "us": r = random.choice(pre) + "_" + random.choice(suf)
    elif style == "um": r = random.choice(mid) + "_" + random.choice(suf)
    elif style == "sm": r = random.choice(suf) + random.choice(mid)
    elif style == "pn": r = random.choice(pre) + str(random.randint(1, 99))
    elif style == "two": r = random.choice(pre) + random.choice(pre) + random.choice(suf)
    else: r = random.choice(suf) + random.choice(suf)
    if len(r) < 5: r += random.choice(suf)
    if len(r) > 15: r = r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r): return _pronounceable(5)
    return r

def gen_anyword():
    return _pronounceable(random.randint(5, 7))

# ─── НОВЫЕ РЕЖИМЫ ───

def gen_mat():
    """🔞 Матерный генератор"""
    roots = ["blyad","suka","nahui","pizdec","huy","pizd","ebat","dris","mraz",
             "gand","shlyuh","pidor","mudak","chmo","loh","xyilo","padla","ueban",
             "tvar","chert","blya","fck","wtf","dmn","btch","azz","dck","cnt",
             "fuk","shyt","hll","pzdc","nah","suk","ebl","pid","mdk","lox"]
    suf = ["_pro","_god","_king","_boss","_man","_boy","_top","_go","_gg","_xx",
           "_69","_228","_666","_13","_1","_x","ka","on","ik","er","ok","ec",
           "ych","an","off","iz","ov","in","chik"]
    pre = ["mr_","el_","big_","top_","my_","x_","da_","not_","im_","ya_","the_",""]
    style = random.randint(1, 5)
    if style == 1: r = random.choice(pre) + random.choice(roots)
    elif style == 2: r = random.choice(roots) + random.choice(suf)
    elif style == 3: r = random.choice(pre) + random.choice(roots) + random.choice(suf)
    elif style == 4: r = random.choice(roots) + random.choice(roots[:10])
    else: r = random.choice(roots) + str(random.randint(1, 999))
    r = r.replace("__", "_").strip("_")
    if len(r) < 5: r += random.choice(suf)
    if len(r) > 15: r = r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r): return gen_default()
    return r

def gen_telegram():
    """📱 Telegram-тематика"""
    tg_words = ["tg","telegram","telega","durov","channel","chat","group","sticker",
                "bot","premium","stars","msg","dm","pm","call","voice","video",
                "emoji","gif","media","admin","mod","owner","reply","forward"]
    roles = ["admin","mod","owner","god","king","boss","pro","master","guru","ninja",
             "dev","hacker","geek","nerd","fan","lover","addict","maniac"]
    actions = ["send","post","share","like","join","spam","flood","ban","kick",
               "mute","pin","edit","delete","read","type","call"]
    suf = ["_pro","_god","_king","_x","_gg","_1","_top","er","ist","ik","ka","off","on"]
    pre = ["my_","the_","im_","mr_","x_","ya_",""]
    style = random.randint(1, 6)
    if style == 1: r = random.choice(pre) + random.choice(tg_words) + random.choice(suf)
    elif style == 2: r = random.choice(tg_words) + "_" + random.choice(roles)
    elif style == 3: r = random.choice(actions) + "_" + random.choice(tg_words)
    elif style == 4: r = random.choice(tg_words) + random.choice(roles)
    elif style == 5: r = random.choice(pre) + random.choice(tg_words) + str(random.randint(1, 99))
    else: r = random.choice(tg_words) + "_" + random.choice(tg_words)
    r = r.replace("__", "_").strip("_")
    if len(r) < 5: r += random.choice(suf)
    if len(r) > 15: r = r[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', r): return gen_default()
    return r

def gen_from_template(template):
    """🎯 Генерация по шаблону: max_* → max_xyz, *_dev → abc_dev, a???b → axxxb"""
    result = ""
    for ch in template:
        if ch == "*":
            result += ''.join(random.choice(_C + _V) for _ in range(random.randint(2, 4)))
        elif ch == "?":
            result += random.choice(_C + _V)
        else:
            result += ch
    if len(result) < 5: result += ''.join(random.choice(_C + _V) for _ in range(5 - len(result)))
    if len(result) > 15: result = result[:15]
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', result): return None
    return result

def gen_similar(base):
    """🔄 Генератор похожих юзернеймов"""
    base = base.lower().replace("@", "")
    mutations = []
    # Удаление буквы
    for i in range(len(base)):
        m = base[:i] + base[i+1:]
        if len(m) >= 5: mutations.append(m)
    # Замена буквы
    for i in range(len(base)):
        rep = random.choice(_C + _V)
        m = base[:i] + rep + base[i+1:]
        mutations.append(m)
    # Добавление буквы
    for i in range(len(base) + 1):
        ins = random.choice(_C + _V)
        m = base[:i] + ins + base[i:]
        if len(m) <= 15: mutations.append(m)
    # Удвоение буквы
    for i in range(len(base)):
        m = base[:i] + base[i] + base[i:]
        if len(m) <= 15: mutations.append(m)
    # С подчёркиванием
    for i in range(1, len(base)):
        m = base[:i] + "_" + base[i:]
        if len(m) <= 15: mutations.append(m)
    # Суффиксы
    for suf in ["_x","_1","_go","_pro","x","1","o","i"]:
        m = base + suf
        if len(m) <= 15: mutations.append(m)
    # Фильтрация
    valid = []
    seen = set()
    for m in mutations:
        if m not in seen and m != base and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', m) and len(m) >= 5:
            valid.append(m)
            seen.add(m)
    random.shuffle(valid)
    return valid

SEARCH_MODES = {
    "default":    {"name": "Дефолт",      "emoji": "🎲", "desc": "Произносимые (5 букв)",        "premium": False, "func": gen_default},
    "beautiful":  {"name": "Красивые",    "emoji": "💎", "desc": "Паттерны без повторов (5 букв)","premium": True,  "func": gen_beautiful},
    "meaningful": {"name": "Со смыслом",  "emoji": "📖", "desc": "Комбинации слов",              "premium": True,  "func": gen_meaningful},
    "anyword":    {"name": "Любое слово", "emoji": "🔤", "desc": "Произносимые (5-7 букв)",      "premium": True,  "func": gen_anyword},
    "mat":        {"name": "Матерные",    "emoji": "🔞", "desc": "18+ юзернеймы",                "premium": True,  "func": gen_mat},
    "telegram":   {"name": "Telegram",    "emoji": "📱", "desc": "TG-тематика",                  "premium": True,  "func": gen_telegram},
}

INVALID_WORDS = ["admin","support","help","test","telegram","bot","official",
                 "service","security","account","login","password","verify",
                 "moderator","system","null","undefined","root","user"]

def is_valid_username(username):
    u = username.lower().replace("_", "")
    for word in INVALID_WORDS:
        if word in u: return False
    if "__" in username or username.startswith("_") or username.endswith("_"): return False
    if is_blacklisted(username): return False
    return True


# ═══════════════════════ ЧЕКЕРЫ ═══════════════════════

async def check_username(username):
    return await pool.strong_check(username)

async def check_fragment(username):
    now = time.time()
    cached = _fragment_cache.get(username)
    if cached and now - cached[1] < _fragment_cache_ttl: return cached[0]
    url = f"https://fragment.com/username/{username.lower()}"
    try:
        async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=8),
            headers={"User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            ])}) as resp:
            if resp.status != 200: return "unavailable"
            text = await resp.text()
            if "Sold" in text or "sold" in text: result = "sold"
            elif any(x in text for x in ["Available","Make an offer","Bid","auction"]): result = "fragment"
            else: result = "unavailable"
            _fragment_cache[username] = (result, now)
            return result
    except: return "unavailable"

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


# ═══════════════════════ ПОИСК v4 ═══════════════════════

async def do_search(count, gen_func, msg, mode_name, uid):
    found = []; attempts = 0; start = time.time()
    last_update = 0; checked_cache = set(); skips_in_row = 0
    fallback = not pool.has_sessions()

    acc_idx = pool.add_user(uid) if not fallback else None
    if acc_idx is None and pool.clients and not fallback:
        await edit_msg(msg, f"🔄 <b>{mode_name}</b>\n\n⏳ Ждём сессию...")
        for _ in range(60):
            await asyncio.sleep(1)
            acc_idx = pool.add_user(uid)
            if acc_idx is not None: break
        if acc_idx is None: fallback = True

    if fallback:
        await edit_msg(msg, f"⚠️ <b>{mode_name}</b> — упрощённый режим\n\n⏳ Ищу через Bot API...")

    try:
        while len(found) < count and attempts < 2000:
            u = None
            for _ in range(50):
                candidate = gen_func()
                if (len(candidate) >= 5 and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', candidate)
                    and candidate.lower() not in checked_cache and is_valid_username(candidate)):
                    u = candidate.lower(); break
            if u is None: continue
            checked_cache.add(u); attempts += 1

            if fallback:
                # Bot API only — без Fragment
                b = await pool._botapi_check(u)
                if b == "taken":
                    await asyncio.sleep(0.3)
                    continue
                found.append({"username": u, "fragment": "unavailable"})
                save_history(uid, u, mode_name, len(u))
                await asyncio.sleep(0.5)
            else:
                ps = pool.stats(); alive = ps['active']
                pause = 4.0 if alive <= 1 else (2.0 if alive <= 3 else (1.0 if alive <= 5 else 0.5))

                quick = await pool.check(u, uid)
                if quick == "taken":
                    skips_in_row = 0; await asyncio.sleep(pause * 0.3); continue
                if quick == "skip":
                    skips_in_row += 1
                    if skips_in_row >= 5:
                        await edit_msg(msg, f"🔎 <b>{mode_name}</b>\n\n⚠️ Пауза 30с...\n📊 {attempts} проверено\n✅ {len(found)}/{count}")
                        await asyncio.sleep(30); skips_in_row = 0
                    continue
                skips_in_row = 0
                if quick == "maybe_free":
                    deep = await pool.strong_check(u, uid)
                    if deep in ("taken", "skip"):
                        await asyncio.sleep(pause); continue
                    if deep == "free":
                        fr = await check_fragment(u)
                        if fr == "fragment": await asyncio.sleep(pause); continue
                        found.append({"username": u, "fragment": fr})
                        save_history(uid, u, mode_name, len(u))
                await asyncio.sleep(pause + random.uniform(0, 0.5))

            now = time.time()
            if now - last_update > 4.0:
                last_update = now; el = int(now - start)
                if fallback:
                    sl = "⚠️ Bot API only"
                else:
                    ps = pool.stats()
                    sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
                await edit_msg(msg, f"🔎 <b>{mode_name}</b>\n\n📊 Проверено: <code>{attempts}</code>\n✅ Найдено: <code>{len(found)}/{count}</code>\n🔄 {sl}\n⏱ {el}с")

            if len(found) >= count: break
        return found, {"attempts": attempts, "elapsed": int(time.time() - start)}
    finally:
        if not fallback: pool.remove_user(uid)

async def do_template_search(template, count, msg, uid):
    """Поиск по шаблону"""
    found = []; attempts = 0; start = time.time()
    last_update = 0; checked = set(); fallback = not pool.has_sessions()

    try:
        while len(found) < count and attempts < 500:
            u = gen_from_template(template)
            if u is None or u.lower() in checked or not is_valid_username(u):
                attempts += 1; continue
            checked.add(u.lower()); attempts += 1

            if fallback:
                b = await pool._botapi_check(u.lower())
                if b != "taken":
                    found.append({"username": u.lower(), "fragment": "unavailable"})
                    save_history(uid, u.lower(), "Шаблон", len(u))
                await asyncio.sleep(0.5)
            else:
                quick = await pool.check(u.lower(), uid)
                if quick == "maybe_free":
                    deep = await pool.strong_check(u.lower(), uid)
                    if deep == "free":
                        fr = await check_fragment(u.lower())
                        if fr != "fragment":
                            found.append({"username": u.lower(), "fragment": fr})
                            save_history(uid, u.lower(), "Шаблон", len(u))
                await asyncio.sleep(1.5)

            now = time.time()
            if now - last_update > 3.0:
                last_update = now
                await edit_msg(msg, f"🎯 <b>Шаблон: {template}</b>\n\n📊 <code>{attempts}</code> проверено\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")

        return found, {"attempts": attempts, "elapsed": int(time.time() - start)}
    finally:
        pass

async def do_similar_search(base, count, msg, uid):
    """Поиск похожих юзернеймов"""
    mutations = gen_similar(base)
    found = []; attempts = 0; start = time.time()
    last_update = 0; fallback = not pool.has_sessions()

    try:
        for u in mutations:
            if len(found) >= count or attempts >= 200: break
            attempts += 1

            if fallback:
                b = await pool._botapi_check(u)
                if b != "taken":
                    found.append({"username": u, "fragment": "unavailable"})
                    save_history(uid, u, "Похожие", len(u))
                await asyncio.sleep(0.5)
            else:
                quick = await pool.check(u, uid)
                if quick == "maybe_free":
                    deep = await pool.strong_check(u, uid)
                    if deep == "free":
                        fr = await check_fragment(u)
                        if fr != "fragment":
                            found.append({"username": u, "fragment": fr})
                            save_history(uid, u, "Похожие", len(u))
                await asyncio.sleep(1.5)

            now = time.time()
            if now - last_update > 3.0:
                last_update = now
                await edit_msg(msg, f"🔄 <b>Похожие на @{base}</b>\n\n📊 <code>{attempts}</code> проверено\n✅ <code>{len(found)}/{count}</code>\n⏱ {int(now-start)}с")

        return found, {"attempts": attempts, "elapsed": int(time.time() - start)}
    finally:
        pass

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
    elif u.get("free", 0) + u.get("extra_searches", 0) > 0:
        si, st = "🆓", "FREE"; sub_info = str(u.get("free", 0) + u.get("extra_searches", 0)) + " поисков"
    else: si, st, sub_info = "⛔️", "ЛИМИТ", "закончились"
    
    cnt = get_search_count(uid); mx = get_max_searches(uid); bal = u.get("balance", 0.0)
    promos = get_active_promotions()
    
    session_line = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
    
    text = (f"🔍 <b>USERNAME HUNTER</b> {si}\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>{st}</b> | 🎯 <code>{cnt}</code> юзов/поиск\n"
            f"📊 {sub_info}\n🔢 Поисков: <code>{u.get('searches', 0)}</code>\n"
            f"🔄 Сессии: {session_line}\n"
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
    kb.button(text="🏪 Магазин", callback_data="cmd_shop")
    kb.button(text="👥 Рефералы", callback_data="cmd_referral")
    kb.button(text="🎁 TikTok", callback_data="cmd_tiktok")
    for pr in promos:
        kb.button(text=pr.get("button_text") or pr["name"], callback_data=f"pv_{pr['id']}")
    kb.button(text="🤖 Поддержать", callback_data="cmd_support")
    if is_admin: kb.button(text="👑 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    return text, kb.as_markup()


# ═══════════════════════ КОМАНДЫ ═══════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, command: CommandObject):
    uid = msg.from_user.id; uname = msg.from_user.username or ""
    is_new = get_user(uid).get("searches", 0) == 0
    ensure_user(uid, uname)
    log_action(uid, "start", command.args or "")
    
    if is_banned(uid):
        rem = rate_limiter.get_ban_remaining(uid)
        if rem > 0:
            await msg.answer(f"🚫 Временная блокировка. Подождите {rem} мин.")
        else:
            await msg.answer("🚫 Вы заблокированы.")
        return
    
    if command.args and command.args.startswith("ref_") and is_new:
        try:
            ref_id = int(command.args.replace("ref_", ""))
            if ref_id != uid:
                set_pending_ref(uid, ref_id)
                kb = InlineKeyboardBuilder()
                kb.button(text="Не робот 🌟", callback_data="captcha_ok")
                await msg.answer("🤖 <b>Подтвердите что вы не робот:</b>\n\nНажмите кнопку 👇",
                                 reply_markup=kb.as_markup(), parse_mode="HTML")
                return
        except: pass
    
    ns = await check_subscribed(uid)
    if ns: t, k = build_sub_kb(ns)
    else: t, k = build_menu(uid)
    await msg.answer(t, reply_markup=k, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    if is_banned(msg.from_user.id): return
    kb = InlineKeyboardBuilder(); kb.button(text="🔙 Меню", callback_data="cmd_menu")
    await msg.answer(
        f"📖 <b>Справка USERNAME HUNTER v23.0</b>\n\n"
        f"🔍 <b>Команды:</b>\n"
        f"/start — Главное меню\n"
        f"/check username — Быстрая проверка\n"
        f"/similar username — Похожие юзернеймы\n"
        f"/balance — Баланс звёзд\n"
        f"/id — Ваш ID\n\n"
        f"📩 Поддержка: @{ADMIN_CONTACT}",
        reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.message(Command("id"))
async def cmd_id(msg: Message):
    await msg.answer(f"🆔 <code>{msg.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("check"))
async def cmd_check(msg: Message, command: CommandObject):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    
    ok, reason = rate_limiter.check_action(uid)
    if not ok:
        if reason == "ban":
            await msg.answer(f"🚫 Слишком много запросов. Блокировка {TEMP_BAN_MINUTES} мин.")
        else:
            await msg.answer("⚠️ Слишком быстро! Подождите немного.")
        return
    
    un = (command.args or "").strip().replace("@", "").lower()
    if not validate_username(un):
        await msg.answer("❌ Использование: <code>/check username</code>", parse_mode="HTML")
        return
    
    log_action(uid, "check", un)
    wm = await msg.answer("⏳ Проверка...")
    tg = await check_username(un)
    st = {"free": "✅ Свободен!", "taken": "❌ Занят", "error": "⚠️ Ошибка", "skip": "⚠️ Не удалось"}.get(tg, "❓")
    
    try: await wm.delete()
    except: pass
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Оценка", callback_data=f"eval_{un}")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(2)
    await msg.answer(f"🔍 <b>@{un}</b> — {st}", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.message(Command("similar"))
async def cmd_similar(msg: Message, command: CommandObject):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await msg.answer("🔒 <b>Нужен Premium</b>\n\nПоиск похожих доступен для Premium!", reply_markup=kb.as_markup(), parse_mode="HTML")
        return
    
    un = (command.args or "").strip().replace("@", "").lower()
    if not validate_username(un):
        await msg.answer("❌ Использование: <code>/similar username</code>", parse_mode="HTML")
        return
    
    log_action(uid, "similar", un)
    wm = await msg.answer(f"🔄 Ищу похожие на @{un}...")
    
    found, stats = await do_similar_search(un, 5, wm, uid)
    
    kb = InlineKeyboardBuilder()
    if found:
        text = f"🔄 <b>Похожие на @{un}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, item in enumerate(found, 1):
            ev = evaluate_username(item["username"])
            text += f"{i}. <code>@{item['username']}</code> — {ev['rarity']}\n"
        text += f"\n📊 {stats['attempts']} проверено ⏱ {stats['elapsed']}с"
    else:
        text = f"😔 Не найдено похожих на @{un}\n\n📊 {stats['attempts']} проверено"
    
    kb.button(text="🔍 Поиск", callback_data="cmd_search")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
    await edit_msg(wm, text, kb.as_markup())

@dp.message(Command("balance"))
async def cmd_balance(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)
    u = get_user(uid); bal = u.get("balance", 0.0)
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 +50⭐", callback_data="topup_50")
    kb.button(text="💰 +100⭐", callback_data="topup_100")
    kb.button(text="💰 +200⭐", callback_data="topup_200")
    if bal >= MIN_WITHDRAW:
        kb.button(text=f"💸 Вывести ({bal:.1f}⭐)", callback_data="cmd_withdraw")
    kb.button(text="🏪 Магазин", callback_data="cmd_shop")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(3, 1, 1, 1)
    await msg.answer(
        f"💰 <b>Баланс:</b> <code>{bal:.1f}</code> ⭐\n\n"
        f"Звёзды можно тратить на:\n• 🔍 Дополнительные поиски\n• 👁 Мониторинг юзов\n• 🎯 Поиск по шаблону",
        reply_markup=kb.as_markup(), parse_mode="HTML")


# ═══════════════════════ ТЕКСТ ═══════════════════════

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: Message):
    uid = msg.from_user.id
    if is_banned(uid): return
    ensure_user(uid, msg.from_user.username)
    state = user_states.get(uid, {})
    action = state.get("action")

    # ─── Активация ключа ───
    if action == "activate":
        user_states.pop(uid, None)
        r = activate_key(uid, msg.text.strip())
        if r:
            log_action(uid, "key_activate", msg.text.strip())
            await msg.answer(f"🎉 <b>Активировано!</b> {r['days']} дн до {r['end']}", parse_mode="HTML")
        else:
            await msg.answer("❌ Неверный ключ")
        t, k = build_menu(uid); await msg.answer(t, reply_markup=k, parse_mode="HTML"); return

    # ─── Оценка ───
    if action == "evaluate":
        user_states.pop(uid, None)
        un = msg.text.strip().replace("@", "").lower()
        if not validate_username(un):
            await msg.answer("❌ Некорректный (мин 5 символов)"); return
        wm = await msg.answer("⏳ Проверка...")
        tg = await check_username(un); fr = await check_fragment(un)
        tgs = {"free": "✅ Свободен", "taken": "❌ Занят", "error": "⚠️ Ошибка"}.get(tg, "❓")
        frs = {"fragment": "💎 Fragment", "sold": "✅ Продан", "unavailable": "—"}.get(fr, "❓")
        ev = evaluate_username(un)
        fac = "\n".join("  " + f for f in ev["factors"]) or "  —"
        kb = InlineKeyboardBuilder()
        if tg == "free":
            kb.button(text="👁 Мониторинг", callback_data=f"mon_add_{un}")
        kb.button(text="📊 Ещё", callback_data="cmd_evaluate")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(
            f"📊 <b>Оценка @{un}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📱 Telegram: {tgs}\n💎 Fragment: {frs}\n\n"
            f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
            f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}",
            reply_markup=kb.as_markup(), parse_mode="HTML"); return

    # ─── Быстрая проверка ───
    if action == "quick_check":
        user_states.pop(uid, None)
        un = msg.text.strip().replace("@", "").lower()
        if not validate_username(un):
            await msg.answer("❌ Некорректный"); return
        wm = await msg.answer("⏳ Проверка...")
        tg = await check_username(un)
        st = {"free": "✅ Свободен!", "taken": "❌ Занят", "error": "⚠️ Ошибка"}.get(tg, "❓")
        kb = InlineKeyboardBuilder()
        if tg == "free":
            kb.button(text="👁 Мониторинг", callback_data=f"mon_add_{un}")
        kb.button(text="🔍 Ещё", callback_data="util_check")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(f"🔍 <b>@{un}</b> — {st}", reply_markup=kb.as_markup(), parse_mode="HTML"); return

    # ─── Массовая проверка ───
    if action == "mass_check":
        user_states.pop(uid, None)
        names = [n.strip().replace("@", "").lower() for n in msg.text.split("\n")
                 if validate_username(n.strip().replace("@", "").lower())][:20]
        if not names:
            await msg.answer("❌ Нет валидных"); return
        wm = await msg.answer(f"⏳ Проверяю {len(names)}...")
        results = []
        for n in names:
            r = await pool.check(n, uid)
            results.append(r)
            await asyncio.sleep(0.3)
        fc = sum(1 for r in results if r in ("free", "maybe_free"))
        tc = sum(1 for r in results if r == "taken")
        text = f"📋 <b>Массовая ({len(names)})</b> ✅{fc} ❌{tc}\n\n"
        for i, r in enumerate(results):
            icon = {"free": "✅", "maybe_free": "✅", "taken": "❌", "error": "⚠️"}.get(r, "❓")
            text += f"{icon} @{names[i]}\n"
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Ещё", callback_data="util_mass")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML"); return

    # ─── Поиск по шаблону ───
    if action == "template_search":
        user_states.pop(uid, None)
        template = msg.text.strip().lower()
        if len(template) < 3 or ("*" not in template and "?" not in template):
            await msg.answer("❌ Шаблон должен содержать * или ?\n\nПримеры: <code>max_*</code>, <code>*_dev</code>, <code>a???b</code>", parse_mode="HTML")
            return
        
        # Проверка лимита
        u = get_user(uid)
        if uid not in ADMIN_IDS and not has_subscription(uid):
            if u.get("template_uses", 0) <= 0:
                kb = InlineKeyboardBuilder()
                kb.button(text="🏪 Магазин", callback_data="cmd_shop")
                kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
                await msg.answer("❌ Нет использований шаблона\n\nКупите в магазине!", reply_markup=kb.as_markup(), parse_mode="HTML")
                return
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("UPDATE users SET template_uses=template_uses-1 WHERE uid=?", (uid,))
            conn.commit(); conn.close()
        
        log_action(uid, "template_search", template)
        wm = await msg.answer(f"🎯 Поиск по шаблону: <code>{template}</code>...", parse_mode="HTML")
        
        found, stats = await do_template_search(template, 3, wm, uid)
        
        kb = InlineKeyboardBuilder()
        if found:
            text = f"🎯 <b>Шаблон: {template}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, item in enumerate(found, 1):
                ev = evaluate_username(item["username"])
                text += f"{i}. <code>@{item['username']}</code> — {ev['rarity']}\n"
            text += f"\n📊 {stats['attempts']} проверено ⏱ {stats['elapsed']}с"
        else:
            text = f"😔 Не найдено по шаблону {template}\n\n📊 {stats['attempts']} проверено"
        
        kb.button(text="🎯 Ещё шаблон", callback_data="cmd_template")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(wm, text, kb.as_markup()); return

    # ─── Похожие ───
    if action == "similar_search":
        user_states.pop(uid, None)
        base = msg.text.strip().replace("@", "").lower()
        if not validate_username(base):
            await msg.answer("❌ Некорректный юзернейм"); return
        
        log_action(uid, "similar_search", base)
        wm = await msg.answer(f"🔄 Ищу похожие на @{base}...")
        
        found, stats = await do_similar_search(base, 5, wm, uid)
        
        kb = InlineKeyboardBuilder()
        if found:
            text = f"🔄 <b>Похожие на @{base}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, item in enumerate(found, 1):
                ev = evaluate_username(item["username"])
                text += f"{i}. <code>@{item['username']}</code> — {ev['rarity']}\n"
            text += f"\n📊 {stats['attempts']} проверено ⏱ {stats['elapsed']}с"
        else:
            text = f"😔 Не найдено похожих на @{base}"
        
        kb.button(text="🔄 Ещё похожие", callback_data="cmd_similar")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(wm, text, kb.as_markup()); return

    # ─── Вывод ───
    if action == "withdraw_amount":
        user_states.pop(uid, None)
        try:
            amount = float(msg.text.strip())
            assert amount >= MIN_WITHDRAW
        except:
            await msg.answer(f"❌ Минимум {MIN_WITHDRAW}⭐"); return
        bal = get_balance(uid)
        if amount > bal:
            await msg.answer(f"❌ Недостаточно ({bal:.1f}⭐)"); return
        wid = create_withdrawal(uid, amount)
        log_action(uid, "withdraw", str(amount))
        await msg.answer(f"✅ Заявка #{wid} на {amount:.1f}⭐")
        for aid in ADMIN_IDS:
            try:
                akb = InlineKeyboardBuilder()
                akb.button(text="✅", callback_data=f"wd_ok_{wid}")
                akb.button(text="❌", callback_data=f"wd_no_{wid}"); akb.adjust(2)
                await bot.send_message(aid, f"💰 Вывод #{wid}\n{amount:.1f}⭐ от {uid}", reply_markup=akb.as_markup())
            except: pass
        return

    # ─── Подарок ───
    if action == "gift_username":
        user_states.pop(uid, None)
        tu = msg.text.strip().replace("@", "")
        if not tu:
            await msg.answer("❌ Введите @username"); return
        tuid = find_user(tu)
        if not tuid:
            await msg.answer("❌ Не найден в боте"); return
        plan = state.get("plan"); p = PRICES.get(plan)
        if not p: return
        await bot.send_invoice(uid, title=f"🎁 Подарок {p['label']} для @{tu}",
            description=f"Premium {p['label']} для @{tu}",
            payload=f"gift_{plan}_{tuid}_{uid}",
            provider_token="", currency="XTR",
            prices=[LabeledPrice(label=p["label"], amount=p["stars"])])
        return

    # ─── АДМИНКА: рассылка ───
    if action == "admin_broadcast_text":
        if uid not in ADMIN_IDS: user_states.pop(uid, None); return
        user_states.pop(uid, None)
        bt = msg.text.strip()
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("SELECT uid FROM users WHERE banned=0")
        aus = [r[0] for r in c.fetchall()]; conn.close()
        s, f = 0, 0
        sm = await msg.answer(f"📤 0/{len(aus)}")
        for i, tu in enumerate(aus):
            try: await bot.send_message(tu, bt, parse_mode="HTML"); s += 1
            except: f += 1
            if (i + 1) % 50 == 0:
                try: await sm.edit_text(f"📤 {i+1}/{len(aus)} ✅{s} ❌{f}")
                except: pass
            await asyncio.sleep(0.05)
        log_action(uid, "broadcast", f"s={s} f={f}")
        try: await sm.edit_text(f"✅ Готово! ✅{s} ❌{f}")
        except: pass
        return

    # ─── АДМИНКА: выдать подписку ───
    if action == "admin_give_user":
        inp = msg.text.strip()
        target = find_user(inp)
        if not target:
            await msg.answer("❌ Не найден"); return
        user_states[uid] = {"action": "admin_give_days", "target": target}
        await msg.answer(f"📅 Дней для <code>{target}</code>?", parse_mode="HTML"); return

    if action == "admin_give_days":
        try:
            days = int(msg.text.strip())
            assert days > 0
        except:
            await msg.answer("❌ Число!"); return
        target = state["target"]; user_states.pop(uid, None)
        end = give_subscription(target, days)
        log_action(uid, "admin_give", f"{target} +{days}d")
        await msg.answer(f"✅ {days}дн для <code>{target}</code> до {end}", parse_mode="HTML")
        try: await bot.send_message(target, f"🎉 Подписка <b>{days}дн</b> до <b>{end}</b>!", parse_mode="HTML")
        except: pass
        return

    # ─── АДМИНКА: ключ ───
    if action == "admin_key_days":
        try:
            days = int(msg.text.strip())
            assert days > 0
        except:
            await msg.answer("❌ Число!"); return
        user_states.pop(uid, None)
        key = generate_key(days, f"D{days}")
        log_action(uid, "admin_key", key)
        await msg.answer(f"🔑 <code>{key}</code> — {days} дн", parse_mode="HTML"); return

    # ─── АДМИНКА: бан ───
    if action == "admin_ban_input":
        user_states.pop(uid, None)
        target = find_user(msg.text.strip())
        if not target:
            await msg.answer("❌ Не найден"); return
        ban_user(target)
        log_action(uid, "admin_ban", str(target))
        await msg.answer("🚫 Заблокирован"); return

    # ─── АДМИНКА: разбан ───
    if action == "admin_unban_input":
        user_states.pop(uid, None)
        target = find_user(msg.text.strip())
        if not target:
            await msg.answer("❌ Не найден"); return
        unban_user(target)
        log_action(uid, "admin_unban", str(target))
        await msg.answer("✅ Разблокирован"); return

    # ─── АДМИНКА: чёрный список ───
    if action == "admin_blacklist_add":
        user_states.pop(uid, None)
        un = msg.text.strip().replace("@", "").lower()
        if not validate_username(un):
            await msg.answer("❌ Некорректный"); return
        add_blacklist(un, uid)
        log_action(uid, "blacklist_add", un)
        await msg.answer(f"⛔ @{un} в чёрном списке"); return

    # ─── АДМИНКА: акция ───
    if action == "admin_promo_name":
        user_states[uid] = {"action": "admin_promo_btn", "name": msg.text.strip()}
        await msg.answer("🔘 Текст кнопки в меню:"); return

    if action == "admin_promo_btn":
        user_states[uid] = {"action": "admin_promo_type", "name": state.get("name"), "btn": msg.text.strip()}
        await msg.answer("📋 Тип: discount / holiday / ref_contest / custom"); return

    if action == "admin_promo_type":
        ptype = msg.text.strip().lower()
        name = state.get("name"); btn = state.get("btn", name)
        user_states.pop(uid, None)
        pid = create_promotion(name, ptype, button_text=btn)
        log_action(uid, "promo_create", f"{name} #{pid}")
        await msg.answer(f"✅ Акция <b>{name}</b> #{pid}", parse_mode="HTML"); return

    # ─── АДМИНКА: управление юзером ───
    if action == "admin_user_search":
        user_states.pop(uid, None)
        target = find_user(msg.text.strip())
        if not target:
            await msg.answer("❌ Не найден"); return
        # Показываем панель юзера
        await show_user_panel(msg, target); return

    if action == "admin_user_set_free":
        target = state.get("target")
        user_states.pop(uid, None)
        try:
            count = int(msg.text.strip())
        except:
            await msg.answer("❌ Число!"); return
        set_free_searches(target, count)
        log_action(uid, "admin_set_free", f"{target} = {count}")
        await msg.answer(f"✅ Установлено {count} поисков"); return

    if action == "admin_user_add_searches":
        target = state.get("target")
        user_states.pop(uid, None)
        try:
            count = int(msg.text.strip())
        except:
            await msg.answer("❌ Число!"); return
        add_extra_searches(target, count)
        log_action(uid, "admin_add_searches", f"{target} +{count}")
        await msg.answer(f"✅ Добавлено {count} поисков"); return

    if action == "admin_user_set_balance":
        target = state.get("target")
        user_states.pop(uid, None)
        try:
            amount = float(msg.text.strip())
        except:
            await msg.answer("❌ Число!"); return
        set_balance(target, amount)
        log_action(uid, "admin_set_balance", f"{target} = {amount}")
        await msg.answer(f"✅ Баланс: {amount:.1f}⭐"); return

    if action == "admin_user_add_days":
        target = state.get("target")
        user_states.pop(uid, None)
        try:
            days = int(msg.text.strip())
        except:
            await msg.answer("❌ Число!"); return
        end = give_subscription(target, days)
        log_action(uid, "admin_add_days", f"{target} +{days}d")
        await msg.answer(f"✅ +{days}дн, до {end}"); return

    if action == "admin_user_msg":
        target = state.get("target")
        user_states.pop(uid, None)
        try:
            await bot.send_message(target, msg.text.strip(), parse_mode="HTML")
            await msg.answer("✅ Отправлено")
        except Exception as e:
            await msg.answer(f"❌ Ошибка: {e}")
        return

    # ─── АДМИНКА: добавить сессию ───
    if action == "admin_add_session_api_id":
        try:
            api_id = int(msg.text.strip())
        except:
            await msg.answer("❌ api_id должен быть числом"); return
        user_states[uid] = {"action": "admin_add_session_api_hash", "api_id": api_id}
        await msg.answer("Введите <b>api_hash</b>:", parse_mode="HTML"); return

    if action == "admin_add_session_api_hash":
        api_hash = msg.text.strip()
        if len(api_hash) < 10:
            await msg.answer("❌ Неверный api_hash"); return
        user_states[uid] = {
            "action": "admin_add_session_phone",
            "api_id": state["api_id"],
            "api_hash": api_hash
        }
        await msg.answer("Введите <b>номер телефона</b> (с +):", parse_mode="HTML"); return

    if action == "admin_add_session_phone":
        phone = msg.text.strip()
        if not phone.startswith("+"):
            await msg.answer("❌ Номер должен начинаться с +"); return
        user_states.pop(uid, None)
        api_id = state["api_id"]; api_hash = state["api_hash"]
        wm = await msg.answer("🔄 Подключаю сессию...")
        try:
            clean_phone = phone.replace("+", "").replace(" ", "")
            client = TelegramClient(
                f"sessions/s_{clean_phone}", api_id, api_hash,
                connection_retries=5, retry_delay=3, timeout=15, request_retries=2)
            await client.connect()
            if not await client.is_user_authorized():
                await client.start(phone=phone)
            idx = len(pool.clients)
            pool.clients.append(client)
            pool._init_session_state(idx)
            pool.accounts_data.append({"api_id": api_id, "api_hash": api_hash, "phone": phone})
            # Сохраняем
            new_acc = {"api_id": api_id, "api_hash": api_hash, "phone": phone}
            try:
                existing = []
                if os.path.exists("added_sessions.json"):
                    with open("added_sessions.json", "r") as f:
                        existing = json.load(f)
                existing.append(new_acc)
                with open("added_sessions.json", "w") as f:
                    json.dump(existing, f, indent=2)
            except: pass
            ps = pool.stats()
            log_action(uid, "add_session", phone)
            try: await wm.delete()
            except: pass
            await msg.answer(f"✅ <b>Сессия #{idx+1} добавлена!</b>\n📱 {phone}\n🔄 Всего: {ps['active']}/{ps['total']}", parse_mode="HTML")
        except Exception as e:
            try: await wm.delete()
            except: pass
            await msg.answer(f"❌ Ошибка: <code>{str(e)[:200]}</code>", parse_mode="HTML")
        return

    # ─── По умолчанию — меню ───
    ns = await check_subscribed(uid)
    if ns: t, k = build_sub_kb(ns)
    else: t, k = build_menu(uid)
    await msg.answer(t, reply_markup=k, parse_mode="HTML")


# ═══════════════════════ ПАНЕЛЬ ЮЗЕРА ═══════════════════════

async def show_user_panel(msg_or_cb, target_uid):
    """Полная панель управления юзером"""
    u = get_user(target_uid)
    is_prem = has_subscription(target_uid)
    is_ban = u.get("banned", 0) == 1
    
    status = "👑 ADMIN" if target_uid in ADMIN_IDS else ("💎 PREMIUM" if is_prem else "🆓 FREE")
    if is_ban: status = "🚫 BANNED"
    
    text = (
        f"👤 <b>Панель юзера</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <code>{target_uid}</code>\n"
        f"👤 @{u.get('uname', '-') or '-'}\n"
        f"📌 {status}\n\n"
        f"🔍 Поиски (free): <code>{u.get('free', 0)}</code>\n"
        f"🔍 Поиски (extra): <code>{u.get('extra_searches', 0)}</code>\n"
        f"📊 Всего поисков: <code>{u.get('searches', 0)}</code>\n"
        f"💰 Баланс: <code>{u.get('balance', 0):.1f}</code> ⭐\n"
        f"👥 Рефералов: <code>{u.get('ref_count', 0)}</code>\n"
        f"💎 Подписка до: <code>{u.get('sub_end', '-') or '-'}</code>\n"
        f"📅 Регистрация: <code>{u.get('joined', '-')}</code>\n"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 +поиски", callback_data=f"au_adds_{target_uid}")
    kb.button(text="🔍 =поиски", callback_data=f"au_sets_{target_uid}")
    kb.button(text="💰 +баланс", callback_data=f"au_addb_{target_uid}")
    kb.button(text="💰 =баланс", callback_data=f"au_setb_{target_uid}")
    kb.button(text="💎 +подписка", callback_data=f"au_addd_{target_uid}")
    kb.button(text="💎 Убрать подписку", callback_data=f"au_remd_{target_uid}")
    
    if is_ban:
        kb.button(text="✅ Разбанить", callback_data=f"au_unban_{target_uid}")
    else:
        kb.button(text="🚫 Забанить", callback_data=f"au_ban_{target_uid}")
    
    kb.button(text="📜 История", callback_data=f"au_hist_{target_uid}")
    kb.button(text="👥 Рефералы", callback_data=f"au_refs_{target_uid}")
    kb.button(text="📤 Написать", callback_data=f"au_msg_{target_uid}")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2, 2, 2, 1, 2, 1, 1)
    
    if hasattr(msg_or_cb, 'message'):
        await edit_msg(msg_or_cb.message, text, kb.as_markup())
    else:
        await msg_or_cb.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")


# ═══════════════════════ ФОТО (TikTok) ═══════════════════════

@dp.message(F.photo)
async def handle_photo(msg: Message):
    uid = msg.from_user.id
    state = user_states.get(uid, {})
    if state.get("action") != "tiktok_proof": return
    
    tid = state.get("task_id")
    photos = state.get("photos", 0) + 1
    user_states[uid]["photos"] = photos
    if "file_ids" not in user_states[uid]:
        user_states[uid]["file_ids"] = []
    user_states[uid]["file_ids"].append(msg.photo[-1].file_id)
    
    if photos < TIKTOK_SCREENSHOTS_NEEDED:
        await msg.answer(f"📸 {photos}/{TIKTOK_SCREENSHOTS_NEEDED}")
        return
    
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
            akb.button(text="❌ Отклонить", callback_data=f"tr_{tid}"); akb.adjust(2)
            await bot.send_message(aid,
                f"📱 <b>TikTok #{tid}</b>\n👤 {display} (<code>{uid}</code>)\n📸 {photos}",
                reply_markup=akb.as_markup(), parse_mode="HTML")
            for i in range(0, len(file_ids), 10):
                batch = file_ids[i:i+10]
                media = [InputMediaPhoto(media=fid) for fid in batch]
                if i == 0: media[0].caption = f"TikTok #{tid} | {display}"
                await bot.send_media_group(aid, media)
        except Exception as e:
            logger.error(f"TikTok admin {aid}: {e}")


# ═══════════════════════ CALLBACKS: Меню ═══════════════════════

@dp.callback_query(F.data == "captcha_ok")
async def cb_captcha(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ensure_user(uid, cb.from_user.username)
    ref_uid = get_pending_ref(uid)
    if ref_uid and ref_uid != uid:
        ok = process_referral(uid, ref_uid)
        set_pending_ref(uid, 0); set_captcha_passed(uid)
        if ok:
            log_action(uid, "referral_join", str(ref_uid))
            try: await bot.send_message(ref_uid, f"🎉 Новый реферал! <b>+{REF_BONUS}</b> поисков", parse_mode="HTML")
            except: pass
    else:
        set_captcha_passed(uid)
    t, k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "check_sub")
async def cb_cs(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ns = await check_subscribed(uid)
    if ns:
        t, k = build_sub_kb(ns); await edit_msg(cb.message, t, k); return
    u = get_user(uid)
    if u.get("sub_bonus", 0) == 0:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET free=free+2,sub_bonus=1 WHERE uid=?", (uid,))
        conn.commit(); conn.close()
    t, k = build_menu(uid); await edit_msg(cb.message, t, k)

@dp.callback_query(F.data == "cmd_menu")
async def cb_menu(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    user_states.pop(uid, None)
    t, k = build_menu(uid); await edit_msg(cb.message, t, k)


# ═══════════════════════ CALLBACKS: Поиск ═══════════════════════

@dp.callback_query(F.data == "cmd_search")
async def cb_search(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    
    ok, reason = rate_limiter.check_search(uid)
    if not ok:
        if reason == "ban":
            await edit_msg(cb.message, f"🚫 Слишком много запросов!\n\nБлокировка на {TEMP_BAN_MINUTES} мин.")
        else:
            await edit_msg(cb.message, "⚠️ Слишком быстро! Подождите немного.")
        return
    
    if not can_search(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🏪 Магазин", callback_data="cmd_shop")
        kb.button(text="👥 Рефералы", callback_data="cmd_referral")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>\n\n💰 Купите Premium или поиски в магазине", kb.as_markup())
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
    
    # Доп. режимы
    if is_prem:
        kb.button(text="🎯 По шаблону", callback_data="cmd_template")
        kb.button(text="🔄 Похожие", callback_data="cmd_similar")
    else:
        kb.button(text="🔒 🎯 По шаблону", callback_data="need_prem")
        kb.button(text="🔒 🔄 Похожие", callback_data="need_prem")
    
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(2, 2, 2, 2, 1)

    mt = ""
    for key, m in SEARCH_MODES.items():
        lk = "🔒" if m["premium"] and not is_prem else "✅"
        mt += f"{lk} <b>{m['emoji']} {m['name']}</b> — {m['desc']}\n"
    mt += f"\n{'✅' if is_prem else '🔒'} <b>🎯 По шаблону</b> — max_*, *_dev\n"
    mt += f"{'✅' if is_prem else '🔒'} <b>🔄 Похожие</b> — вариации юза\n"

    await edit_msg(cb.message,
        f"🔍 <b>Выберите режим:</b>\n\n{mt}\n🎯 <code>{cnt}</code> юзов/поиск | Осталось: <b>{fl}</b>",
        kb.as_markup())

@dp.callback_query(F.data == "need_prem")
async def cb_np(cb: CallbackQuery):
    await answer_cb(cb, "🔒 Нужен Premium!", show_alert=True)

@dp.callback_query(F.data.startswith("go_"))
async def cb_go(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    
    ok, reason = rate_limiter.check_search(uid)
    if not ok:
        if reason == "ban":
            await edit_msg(cb.message, f"🚫 Блокировка на {TEMP_BAN_MINUTES} мин.")
        else:
            await edit_msg(cb.message, "⚠️ Подождите немного.")
        return
    
    if not can_search(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>", kb.as_markup())
        return

    mode = cb.data[3:]; mi = SEARCH_MODES.get(mode)
    if not mi: return
    is_prem = uid in ADMIN_IDS or has_subscription(uid)
    if mi["premium"] and not is_prem: return

    if uid not in ADMIN_IDS:
        if uid in searching_users:
            try: await bot.send_message(uid, "⏳ Уже идёт поиск!")
            except: pass
            return
        cd = user_search_cooldown.get(uid, 0)
        rem = SEARCH_COOLDOWN - (time.time() - cd)
        if rem > 0:
            try: await bot.send_message(uid, f"⏳ Подождите {int(rem)} сек.")
            except: pass
            return

    searching_users.add(uid)
    try:
        ps = pool.stats()
        sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
        await edit_msg(cb.message, f"🚀 <b>{mi['emoji']} {mi['name']}</b>\n\n🔄 Сессии: {sl}\n⏳ Ищу...")

        use_search(uid)
        log_action(uid, "search", mode)
        count = get_search_count(uid)
        found, stats = await do_search(count, mi["func"], cb.message, mi["name"], uid)

        kb = InlineKeyboardBuilder()
        if found:
            text = f"✅ <b>Найдено {len(found)}:</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for i, item in enumerate(found, 1):
                ev = evaluate_username(item["username"])
                fri = " 💎" if item["fragment"] == "fragment" else (" 🏷" if item["fragment"] == "sold" else "")
                text += f"{i}. <code>@{item['username']}</code> — {ev['rarity']}{fri}\n"
            text += f"\n📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"
        else:
            text = f"😔 <b>Не найдено</b>\n\n📊 <code>{stats['attempts']}</code> проверок ⏱ <code>{stats['elapsed']}с</code>"

        if can_search(uid):
            kb.button(text="🔄 Ещё", callback_data=cb.data)
        kb.button(text="🔍 Режимы", callback_data="cmd_search")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())
    finally:
        searching_users.discard(uid)
        if uid not in ADMIN_IDS:
            user_search_cooldown[uid] = time.time()

@dp.callback_query(F.data == "cmd_template")
async def cb_template(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        u = get_user(uid)
        if u.get("template_uses", 0) <= 0:
            kb = InlineKeyboardBuilder()
            kb.button(text="🏪 Магазин", callback_data="cmd_shop")
            kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
            await edit_msg(cb.message, "🔒 <b>Поиск по шаблону</b>\n\nНужен Premium или купите в магазине!", kb.as_markup())
            return
    
    user_states[uid] = {"action": "template_search"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="cmd_search")
    await edit_msg(cb.message,
        "🎯 <b>Поиск по шаблону</b>\n\n"
        "Введите шаблон:\n"
        "• <code>*</code> — любые 2-4 символа\n"
        "• <code>?</code> — любой 1 символ\n\n"
        "Примеры:\n"
        "• <code>max_*</code> → max_abc, max_xyz\n"
        "• <code>*_dev</code> → pro_dev, new_dev\n"
        "• <code>a???b</code> → axxxb, ayyyb",
        kb.as_markup())

@dp.callback_query(F.data == "cmd_similar")
async def cb_similar(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, "🔒 <b>Нужен Premium</b>", kb.as_markup())
        return
    
    user_states[uid] = {"action": "similar_search"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="cmd_search")
    await edit_msg(cb.message,
        "🔄 <b>Поиск похожих</b>\n\n"
        "Введите юзернейм, для которого найти похожие:\n\n"
        "Например: <code>maxim</code>\n"
        "→ mxim, maxxim, maxim_x, maxim1...",
        kb.as_markup())


# ═══════════════════════ CALLBACKS: Оценка / Утилиты ═══════════════════════

@dp.callback_query(F.data == "cmd_evaluate")
async def cb_eval(cb: CallbackQuery):
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "evaluate"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "📊 <b>Введите юзернейм для оценки:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("eval_"))
async def cb_eval_direct(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    un = cb.data[5:]
    wm_text = "⏳ Оцениваю..."
    await edit_msg(cb.message, wm_text)
    
    tg = await check_username(un); fr = await check_fragment(un)
    tgs = {"free": "✅ Свободен", "taken": "❌ Занят", "error": "⚠️ Ошибка"}.get(tg, "❓")
    frs = {"fragment": "💎 Fragment", "sold": "✅ Продан", "unavailable": "—"}.get(fr, "❓")
    ev = evaluate_username(un)
    fac = "\n".join("  " + f for f in ev["factors"]) or "  —"
    
    kb = InlineKeyboardBuilder()
    if tg == "free":
        kb.button(text="👁 Мониторинг", callback_data=f"mon_add_{un}")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
    
    await edit_msg(cb.message,
        f"📊 <b>Оценка @{un}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📱 Telegram: {tgs}\n💎 Fragment: {frs}\n\n"
        f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
        f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}",
        kb.as_markup())

@dp.callback_query(F.data == "cmd_utils")
async def cb_utils(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверка", callback_data="util_check")
    kb.button(text="📋 Массовая", callback_data="util_mass")
    kb.button(text="📜 История", callback_data="util_hist")
    kb.button(text="👁 Мониторинг", callback_data="cmd_monitors")
    kb.button(text="📥 Экспорт", callback_data="util_export")
    kb.button(text="🔙", callback_data="cmd_menu")
    kb.adjust(2, 2, 1, 1)
    await edit_msg(cb.message, "🔧 <b>Утилиты</b>", kb.as_markup())

@dp.callback_query(F.data == "util_check")
async def cb_uc(cb: CallbackQuery):
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "quick_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "🔍 <b>Введите юзернейм:</b>", kb.as_markup())

@dp.callback_query(F.data == "util_mass")
async def cb_um(cb: CallbackQuery):
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "mass_check"}
    kb = InlineKeyboardBuilder(); kb.button(text="❌", callback_data="cmd_utils")
    await edit_msg(cb.message, "📋 <b>Юзернеймы по строке (макс 20):</b>", kb.as_markup())

@dp.callback_query(F.data == "util_hist")
async def cb_uh(cb: CallbackQuery):
    await answer_cb(cb)
    uid = cb.from_user.id
    hist = get_history(uid)
    kb = InlineKeyboardBuilder()
    text = f"📜 <b>История ({len(hist)})</b>\n\n" if hist else "📜 Пусто"
    for h in hist[:15]:
        text += f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
    kb.button(text="📥 TXT", callback_data="util_export")
    kb.button(text="🔙", callback_data="cmd_utils"); kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "util_export")
async def cb_ue(cb: CallbackQuery):
    await answer_cb(cb)
    uid = cb.from_user.id
    hist = get_history(uid, 100)
    if not hist: return
    content = "ИСТОРИЯ ПОИСКОВ\n\n"
    for i, h in enumerate(hist, 1):
        content += f"{i}. @{h[0]} | {h[2]} | {h[1]}\n"
    await bot.send_document(uid, BufferedInputFile(content.encode(), filename=f"history_{uid}.txt"), caption="📥 История")


# ═══════════════════════ CALLBACKS: Мониторинг ═══════════════════════

@dp.callback_query(F.data == "cmd_monitors")
async def cb_monitors(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    mons = get_user_monitors(uid)
    limit = get_monitor_limit(uid)
    
    kb = InlineKeyboardBuilder()
    text = f"👁 <b>Мониторинг</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📊 Слотов: <code>{len(mons)}/{limit}</code>\n\n"
    
    if mons:
        for m in mons:
            status_icon = "✅" if m["last_status"] == "free" else "❌"
            text += f"{status_icon} <code>@{m['username']}</code> — до {m['expires'][:10]}\n"
            kb.button(text=f"❌ {m['username']}", callback_data=f"mon_del_{m['id']}")
    else:
        text += "<i>Нет юзов на мониторинге</i>\n"
    
    text += "\n💡 Бот проверяет юзы каждые 30 мин и уведомит когда освободится"
    
    kb.button(text="➕ Добавить", callback_data="mon_add_new")
    kb.button(text="🔙 Утилиты", callback_data="cmd_utils"); kb.adjust(2, 1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "mon_add_new")
async def cb_mon_add_new(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    limit = get_monitor_limit(uid)
    current = get_monitor_count(uid)
    
    if current >= limit:
        kb = InlineKeyboardBuilder()
        kb.button(text="🏪 Магазин", callback_data="cmd_shop")
        kb.button(text="🔙", callback_data="cmd_monitors"); kb.adjust(1)
        await edit_msg(cb.message, f"❌ Лимит слотов ({current}/{limit})\n\nКупите ещё в магазине!", kb.as_markup())
        return
    
    user_states[uid] = {"action": "monitor_add"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="cmd_monitors")
    await edit_msg(cb.message, "👁 <b>Добавить мониторинг</b>\n\nВведите занятый юзернейм:", kb.as_markup())

@dp.callback_query(F.data.startswith("mon_add_"))
async def cb_mon_add(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    un = cb.data[8:]
    
    if un == "new":
        return
    
    limit = get_monitor_limit(uid)
    current = get_monitor_count(uid)
    
    if current >= limit:
        await answer_cb(cb, f"❌ Лимит слотов ({current}/{limit})", show_alert=True)
        return
    
    # Проверяем что юз занят
    status = await pool.check(un, uid)
    if status != "taken":
        await answer_cb(cb, "⚠️ Юз уже свободен!", show_alert=True)
        return
    
    mid = add_monitor(uid, un)
    log_action(uid, "monitor_add", un)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="👁 Мои мониторы", callback_data="cmd_monitors")
    kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
    
    await edit_msg(cb.message,
        f"✅ <b>@{un}</b> добавлен на мониторинг!\n\n"
        f"Бот проверяет каждые 30 мин и уведомит когда юз освободится.",
        kb.as_markup())

@dp.callback_query(F.data.startswith("mon_del_"))
async def cb_mon_del(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    mid = int(cb.data[8:])
    remove_monitor(mid, uid)
    await cb_monitors(cb)

# ═══════════════════════ CALLBACKS: Магазин ═══════════════════════

@dp.callback_query(F.data == "cmd_shop")
async def cb_shop(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    u = get_user(uid)
    bal = u.get("balance", 0.0)
    
    text = (
        f"🏪 <b>Магазин</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Ваш баланс: <code>{bal:.1f}</code> ⭐\n\n"
        f"<b>🔍 Поиски:</b>\n"
    )
    for k, item in SHOP_ITEMS.items():
        if "searches" in item:
            text += f"• {item['name']} — <code>{item['stars']}</code>⭐\n"
    
    text += f"\n<b>🛠 Инструменты:</b>\n"
    for k, item in SHOP_ITEMS.items():
        if "monitor" in item or "template" in item:
            text += f"• {item['name']} — <code>{item['stars']}</code>⭐\n"
    
    text += f"\n<b>📦 Бандлы (выгодно!):</b>\n"
    for k, b in BUNDLES.items():
        text += f"• {b['label']} — <code>{b['stars']}</code>⭐\n  <i>{b['desc']}</i>\n"
    
    kb = InlineKeyboardBuilder()
    # Поиски
    kb.button(text="🔍 +5 (10⭐)", callback_data="buy_s5")
    kb.button(text="🔍 +15 (25⭐)", callback_data="buy_s15")
    kb.button(text="🔍 +50 (70⭐)", callback_data="buy_s50")
    # Инструменты
    kb.button(text="👁 Монитор (20⭐)", callback_data="buy_mon1")
    kb.button(text="🎯 Шаблон (5⭐)", callback_data="buy_tpl1")
    # Бандлы
    kb.button(text="⚡ Starter (80⭐)", callback_data="buy_b_starter")
    kb.button(text="🔥 Pro (200⭐)", callback_data="buy_b_pro")
    kb.button(text="💎 Ultra (650⭐)", callback_data="buy_b_ultra")
    # Пополнение
    kb.button(text="💰 Пополнить баланс", callback_data="cmd_topup")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(3, 2, 3, 1, 1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "cmd_topup")
async def cb_topup_menu(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 +50⭐", callback_data="topup_50")
    kb.button(text="💰 +100⭐", callback_data="topup_100")
    kb.button(text="💰 +200⭐", callback_data="topup_200")
    kb.button(text="💰 +500⭐", callback_data="topup_500")
    kb.button(text="🔙 Магазин", callback_data="cmd_shop")
    kb.adjust(2, 2, 1)
    await edit_msg(cb.message, "💰 <b>Пополнить баланс</b>\n\nВыберите сумму:", kb.as_markup())

@dp.callback_query(F.data.startswith("topup_"))
async def cb_topup(cb: CallbackQuery):
    await answer_cb(cb)
    amount = int(cb.data.replace("topup_", ""))
    uid = cb.from_user.id
    await bot.send_invoice(uid, title=f"💰 Пополнение {amount}⭐",
        description=f"+{amount} звёзд на баланс",
        payload=f"topup_{amount}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"{amount}⭐", amount=amount)])

@dp.callback_query(F.data.startswith("buy_s"))
async def cb_buy_searches(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    item_key = cb.data[4:]  # s5, s15, s50
    item = SHOP_ITEMS.get(item_key)
    if not item: return
    
    await bot.send_invoice(uid, title=f"🔍 {item['name']}",
        description=f"Дополнительные поиски",
        payload=f"shop_{item_key}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=item['name'], amount=item['stars'])])

@dp.callback_query(F.data.startswith("buy_mon"))
async def cb_buy_monitor(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    item = SHOP_ITEMS.get("mon1")
    if not item: return
    
    await bot.send_invoice(uid, title=f"👁 {item['name']}",
        description=f"Слот мониторинга юзернейма",
        payload=f"shop_mon1_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=item['name'], amount=item['stars'])])

@dp.callback_query(F.data.startswith("buy_tpl"))
async def cb_buy_template(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    item = SHOP_ITEMS.get("tpl1")
    if not item: return
    
    await bot.send_invoice(uid, title=f"🎯 {item['name']}",
        description=f"Одно использование поиска по шаблону",
        payload=f"shop_tpl1_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=item['name'], amount=item['stars'])])

@dp.callback_query(F.data.startswith("buy_b_"))
async def cb_buy_bundle(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bundle_key = cb.data[6:]  # starter, pro, ultra
    bundle = BUNDLES.get(bundle_key)
    if not bundle: return
    
    await bot.send_invoice(uid, title=bundle['label'],
        description=bundle['desc'],
        payload=f"bundle_{bundle_key}_{uid}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=bundle['label'], amount=bundle['stars'])])


# ═══════════════════════ CALLBACKS: Профиль ═══════════════════════

@dp.callback_query(F.data == "cmd_profile")
async def cb_profile(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if is_banned(uid): return
    
    u = get_user(uid)
    is_admin = uid in ADMIN_IDS
    is_prem = has_subscription(uid)
    
    if is_admin:
        status = "👑 Админ ♾"
    elif is_prem:
        status = "💎 Premium до " + u.get("sub_end", "?")
    elif u.get("free", 0) + u.get("extra_searches", 0) > 0:
        status = f"🆓 {u.get('free', 0) + u.get('extra_searches', 0)} поисков"
    else:
        status = "⛔️ Лимит"
    
    cnt = get_search_count(uid)
    mx = get_max_searches(uid)
    ar_on, ar_plan = get_auto_renew(uid)
    ar_text = f"🔄 Авто-продление: <b>ВКЛ</b> ({ar_plan})" if ar_on else "🔄 Авто-продление: ВЫКЛ"
    bal = u.get("balance", 0.0)
    uname = u.get("uname", "")
    monitors = get_monitor_count(uid)
    mon_limit = get_monitor_limit(uid)
    
    text = (
        f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <code>{uid}</code>" + (f" | @{uname}" if uname else "") + "\n"
        f"📌 {status}\n"
        f"🎯 {cnt} юзов/поиск | 🔄 {mx} осталось\n"
        f"📊 Всего поисков: <code>{u.get('searches', 0)}</code>\n"
        f"👥 Рефералов: <code>{u.get('ref_count', 0)}</code>\n"
        f"👁 Мониторинг: <code>{monitors}/{mon_limit}</code>\n"
        f"💰 Баланс: <code>{bal:.1f}</code> ⭐\n\n"
        f"{ar_text}"
    )
    
    kb = InlineKeyboardBuilder()
    if ar_on:
        kb.button(text="🔄 Выключить авто", callback_data="toggle_renew")
    else:
        kb.button(text="🔄 Включить авто", callback_data="toggle_renew")
    kb.button(text="📜 История", callback_data="util_hist")
    kb.button(text="👁 Мониторинг", callback_data="cmd_monitors")
    kb.button(text="🔑 Ввести ключ", callback_data="cmd_activate")
    kb.button(text="🎁 Подарить", callback_data="gift_prem")
    if bal >= MIN_WITHDRAW:
        kb.button(text=f"💸 Вывести ({bal:.1f}⭐)", callback_data="cmd_withdraw")
    if is_prem:
        kb.button(text="🎰 Рулетка", callback_data="cmd_roulette")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "toggle_renew")
async def cb_tr(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    ar_on, _ = get_auto_renew(uid)
    if ar_on:
        set_auto_renew(uid, False, "")
        await cb_profile(cb)
    else:
        kb = InlineKeyboardBuilder()
        for k, p in PRICES.items():
            if p["days"] < 99999:
                kb.button(text=f"{p['label']} ({p['stars']}⭐)", callback_data=f"sr_{k}")
        kb.button(text="❌", callback_data="cmd_profile")
        kb.adjust(1)
        await edit_msg(cb.message, "🔄 <b>Выберите тариф авто-продления:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("sr_"))
async def cb_sr(cb: CallbackQuery):
    plan = cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    set_auto_renew(cb.from_user.id, True, plan)
    await cb_profile(cb)


# ═══════════════════════ CALLBACKS: Рулетка ═══════════════════════

@dp.callback_query(F.data == "cmd_roulette")
async def cb_roulette(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    
    if not can_roulette(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙", callback_data="cmd_profile")
        await edit_msg(cb.message, "⏳ Рулетка доступна раз в неделю", kb.as_markup())
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🎰 Крутить!", callback_data="roulette_spin")
    kb.button(text="🔙", callback_data="cmd_profile")
    kb.adjust(1)
    await edit_msg(cb.message,
        "🎰 <b>Рулетка Premium</b>\n\n"
        "Доступна раз в неделю!\n\n"
        "🎁 Призы:\n"
        "• 1-3 дня подписки\n"
        "• +1-5 поисков\n"
        "• Звёзды на баланс",
        kb.as_markup())

@dp.callback_query(F.data == "roulette_spin")
async def cb_rspin(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not has_subscription(uid) and uid not in ADMIN_IDS: return
    if not can_roulette(uid): return
    
    set_last_roulette(uid)
    log_action(uid, "roulette", "spin")
    
    for e in ["🎰", "🔄", "💫", "🌟", "✨", "🎯"]:
        await edit_msg(cb.message, f"{e} Крутим...")
        await asyncio.sleep(0.4)
    
    roll = random.randint(1, 100)
    
    if roll <= 40:
        # 40% — 1-2 дня подписки
        days = random.choice([1, 2])
        give_subscription(uid, days)
        prize = f"💎 +{days} дн. подписки!"
    elif roll <= 70:
        # 30% — поиски
        searches = random.choice([1, 2, 3, 5])
        add_extra_searches(uid, searches)
        prize = f"🔍 +{searches} поисков!"
    elif roll <= 90:
        # 20% — звёзды
        stars = random.choice([5, 10, 15])
        add_balance(uid, stars)
        prize = f"⭐ +{stars} звёзд!"
    else:
        # 10% — джекпот
        days = random.choice([3, 5, 7])
        give_subscription(uid, days)
        prize = f"🎉 ДЖЕКПОТ! +{days} дн. подписки!"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Профиль", callback_data="cmd_profile")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    
    await edit_msg(cb.message,
        f"🎉 <b>Поздравляем!</b>\n\n{prize}\n\nСледующий спин через 7 дней 🍀",
        kb.as_markup())


# ═══════════════════════ CALLBACKS: Ключ ═══════════════════════

@dp.callback_query(F.data == "cmd_activate")
async def cb_act(cb: CallbackQuery):
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "activate"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_menu")
    await edit_msg(cb.message, "🔑 <b>Введите ключ:</b>", kb.as_markup())


# ═══════════════════════ CALLBACKS: Вывод ═══════════════════════

@dp.callback_query(F.data == "cmd_withdraw")
async def cb_wd(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    bal = get_balance(uid)
    if bal < MIN_WITHDRAW: return
    
    user_states[uid] = {"action": "withdraw_amount"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_profile")
    await edit_msg(cb.message,
        f"💸 <b>Вывод</b>\n\n"
        f"💰 Баланс: {bal:.1f}⭐\n"
        f"📌 Минимум: {MIN_WITHDRAW}⭐\n\n"
        f"Введите сумму:",
        kb.as_markup())


# ═══════════════════════ CALLBACKS: Рефералы ═══════════════════════

@dp.callback_query(F.data == "cmd_referral")
async def cb_ref(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    u = get_user(uid)
    bu = bot_info.username if bot_info else "bot"
    link = f"https://t.me/{bu}?start=ref_{uid}"
    rc = u.get("ref_count", 0)
    bal = u.get("balance", 0.0)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться", url=f"https://t.me/share/url?url={link}&text=🔍 Найди свободный юзернейм в Telegram!")
    kb.button(text="👥 Мои рефералы", callback_data="my_refs")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    
    await edit_msg(cb.message,
        f"👥 <b>Реферальная программа</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Приглашено: <code>{rc}</code>\n"
        f"💰 Баланс: <code>{bal:.1f}</code> ⭐\n\n"
        f"🎁 <b>Бонусы:</b>\n"
        f"• +{REF_BONUS} поиска за каждого друга\n"
        f"• +4% с покупок рефералов\n\n"
        f"🔗 Ваша ссылка:\n<code>{link}</code>",
        kb.as_markup())

@dp.callback_query(F.data == "my_refs")
async def cb_my_refs(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    refs = get_user_referrals(uid, 20)
    
    text = f"👥 <b>Ваши рефералы ({len(refs)})</b>\n\n"
    if refs:
        for r in refs:
            name = f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"
            text += f"• {name} — {r['created']}\n"
    else:
        text += "<i>Пока никого не пригласили</i>"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔙 Рефералы", callback_data="cmd_referral")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())


# ═══════════════════════ CALLBACKS: Premium ═══════════════════════

@dp.callback_query(F.data == "cmd_prices")
async def cb_prices(cb: CallbackQuery):
    await answer_cb(cb)
    promos = get_active_promotions()
    
    text = "💰 <b>Premium</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if promos:
        text += "🎉 <b>Акции:</b>\n"
        for pr in promos:
            text += f"  • {pr['name']}\n"
        text += "\n"
    
    text += f"🎯 <b>Premium даёт:</b>\n"
    text += f"• {PREMIUM_COUNT} юзов за поиск (вместо {FREE_COUNT})\n"
    text += f"• {PREMIUM_SEARCHES_LIMIT} поисков/день\n"
    text += f"• Все режимы поиска\n"
    text += f"• Поиск по шаблону\n"
    text += f"• Похожие юзернеймы\n"
    text += f"• Мониторинг {MONITOR_MAX_PREMIUM} юзов\n"
    text += f"• Рулетка с призами\n\n"
    
    text += "<b>Тарифы:</b>\n"
    for p in PRICES.values():
        text += f"• <b>{p['label']}</b> — <code>{p['stars']}⭐</code> <s>{p['stars_orig']}⭐</s>\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Оплата Stars", callback_data="pay_stars")
    kb.button(text="💳 FunPay", callback_data="pay_funpay")
    kb.button(text="📦 Бандлы", callback_data="cmd_shop")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "pay_stars")
async def cb_ps(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    for k, p in PRICES.items():
        kb.button(text=f"{p['label']} — {p['stars']}⭐", callback_data=f"buy_{k}")
    kb.button(text="🔙", callback_data="cmd_prices")
    kb.adjust(1)
    await edit_msg(cb.message, "⭐ <b>Оплата Telegram Stars:</b>", kb.as_markup())

@dp.callback_query(F.data == "pay_funpay")
async def cb_pf(cb: CallbackQuery):
    await answer_cb(cb)
    uid = cb.from_user.id
    uname = cb.from_user.username or ""
    
    kb = InlineKeyboardBuilder()
    for k, p in PRICES.items():
        if p.get("funpay"):
            kb.button(text=f"{p['label']} — {p['rub']}₽", url=p["funpay"])
    kb.button(text="🔙", callback_data="cmd_prices")
    kb.adjust(1)
    
    ident = f"@{uname}" if uname else f"ID:{uid}"
    await edit_msg(cb.message,
        f"💳 <b>FunPay</b>\n\n"
        f"При покупке укажите:\n🆔 <code>{ident}</code>",
        kb.as_markup())

@dp.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery):
    k = cb.data[4:]
    p = PRICES.get(k)
    if not p:
        await answer_cb(cb, "❌", show_alert=True)
        return
    await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"💎 {p['label']}",
        description=f"Premium {p['label']} — {p['desc']}",
        payload=f"sub_{k}_{cb.from_user.id}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=p["label"], amount=p["stars"])])


# ═══════════════════════ CALLBACKS: Подарить ═══════════════════════

@dp.callback_query(F.data == "gift_prem")
async def cb_gift(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    for k, p in PRICES.items():
        kb.button(text=f"{p['label']} — {p['stars']}⭐", callback_data=f"gp_{k}")
    kb.button(text="🔙", callback_data="cmd_profile")
    kb.adjust(1)
    await edit_msg(cb.message, "🎁 <b>Подарить Premium</b>\n\nВыберите тариф:", kb.as_markup())

@dp.callback_query(F.data.startswith("gp_"))
async def cb_gp(cb: CallbackQuery):
    plan = cb.data[3:]; await answer_cb(cb)
    if plan not in PRICES: return
    user_states[cb.from_user.id] = {"action": "gift_username", "plan": plan}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="gift_prem")
    await edit_msg(cb.message,
        f"🎁 <b>{PRICES[plan]['label']}</b>\n\nВведите @username получателя:",
        kb.as_markup())


# ═══════════════════════ CALLBACKS: Поддержать ═══════════════════════

@dp.callback_query(F.data == "cmd_support")
async def cb_support(cb: CallbackQuery):
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    for amt in DONATE_OPTIONS:
        kb.button(text=f"⭐ {amt}", callback_data=f"don_{amt}")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(3, 3, 1, 1)
    await edit_msg(cb.message, "🤖 <b>Поддержать проект</b>\n\nВыберите сумму:", kb.as_markup())

@dp.callback_query(F.data.startswith("don_"))
async def cb_don(cb: CallbackQuery):
    amt = int(cb.data[4:]); await answer_cb(cb)
    await bot.send_invoice(cb.from_user.id, title=f"🤖 Донат {amt}⭐",
        description="Поддержка проекта",
        payload=f"donate_{amt}_{cb.from_user.id}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"Донат {amt}⭐", amount=amt)])


# ═══════════════════════ CALLBACKS: Акции ═══════════════════════

@dp.callback_query(F.data.startswith("pv_"))
async def cb_promo_view(cb: CallbackQuery):
    await answer_cb(cb)
    pid = int(cb.data[3:])
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
        text = (
            "🌸✨ <b>ПРАЗДНИЧНЫЙ КОНКУРС!</b> ✨🌸\n\n"
            "🔥 <b>Условия:</b>\n"
            "Приглашайте друзей по реферальной ссылке!\n\n"
            "🎁 <b>Призы ТОП-5:</b>\n"
            "🥇 1 место — 1 неделя подписки\n"
            "🥈 2 место — 5 дней подписки\n"
            "🥉 3 место — 3 дня подписки\n"
            "🏅 4-5 место — 1 день подписки\n"
        )
        kb = InlineKeyboardBuilder()
        kb.button(text="🏆 Топ участников", callback_data=f"pt_{pid}")
        kb.button(text="👥 Моя ссылка", callback_data="cmd_referral")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
    else:
        text = f"🎉 <b>{promo['name']}</b>\n\n{promo.get('data', {}).get('desc', 'Специальное предложение!')}"
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Premium", callback_data="cmd_prices")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("pt_"))
async def cb_promo_top(cb: CallbackQuery):
    await answer_cb(cb)
    pid = int(cb.data[3:])
    uid = cb.from_user.id
    
    promos = get_active_promotions()
    start_date = "2024-01-01 00:00"
    for p in promos:
        if p["id"] == pid and p["ptype"] == "ref_contest":
            start_date = p["created"]
            break
    
    top = get_ref_top_by_period(start_date, 20)
    
    text = "🏆 <b>Топ рефералов</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if top:
        for i, t in enumerate(top, 1):
            medals = {1: "🥇", 2: "🥈", 3: "🥉", 4: "🏅", 5: "🏅"}
            medal = medals.get(i, f"{i}.")
            name = f"@{t['uname']}" if t["uname"] else f"ID:{t['uid']}"
            you = " ← <b>ты</b>" if t["uid"] == uid else ""
            text += f"{medal} {name} — <code>{t['ref_count']}</code>{you}\n"
    else:
        text += "<i>Пока никто не участвует</i>\n"
    
    my_place, my_refs = get_my_ref_place(uid)
    text += f"\n━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📍 Твоё место: <b>#{my_place}</b>\n"
    text += f"👥 Твоих рефералов: <code>{my_refs}</code>\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Моя ссылка", callback_data="cmd_referral")
    kb.button(text="🔙 К акции", callback_data=f"pv_{pid}")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ═══════════════════════ CALLBACKS: TikTok ═══════════════════════

@dp.callback_query(F.data == "cmd_tiktok")
async def cb_tt(cb: CallbackQuery):
    await answer_cb(cb)
    text = (
        f"🎁 <b>TikTok задание</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ Найди видео «словил юз тг»\n"
        f"2️⃣ Оставь {TIKTOK_SCREENSHOTS_NEEDED} комментариев:\n"
        f"💬 <code>{TIKTOK_COMMENT_TEXT}</code>\n"
        f"3️⃣ Отправь скриншоты\n"
        f"4️⃣ Получи 🎁 <b>{TIKTOK_REWARD_GIFT}</b>"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Начать задание", callback_data="tt_go")
    kb.button(text="📹 Снимай видео!", callback_data="tt_video")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "tt_video")
async def cb_ttv(cb: CallbackQuery):
    await answer_cb(cb)
    text = (
        "📹 <b>Снимай видео и зарабатывай!</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔥 Снимай видео про бота и получай деньги!\n\n"
        "💸 1000 просмотров = 1$\n\n"
        "📲 Условия — @SwordUserTiktok"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 @SwordUserTiktok", url="https://t.me/SwordUserTiktok")
    kb.button(text="🔙 TikTok", callback_data="cmd_tiktok")
    kb.adjust(1)
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "tt_go")
async def cb_tg(cb: CallbackQuery):
    uid = cb.from_user.id; await answer_cb(cb)
    if not tiktok_can_submit(uid):
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙", callback_data="cmd_tiktok")
        await edit_msg(cb.message, f"❌ Лимит заданий на сегодня ({TIKTOK_DAILY_LIMIT})", kb.as_markup())
        return
    
    tid = task_create(uid)
    user_states[uid] = {"action": "tiktok_proof", "task_id": tid, "photos": 0}
    
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="tt_cancel")
    await edit_msg(cb.message,
        f"📸 <b>Задание #{tid}</b>\n\n"
        f"Отправьте {TIKTOK_SCREENSHOTS_NEEDED} скриншотов комментариев\n\n"
        f"<code>0/{TIKTOK_SCREENSHOTS_NEEDED}</code>",
        kb.as_markup())

@dp.callback_query(F.data == "tt_cancel")
async def cb_tc(cb: CallbackQuery):
    await answer_cb(cb)
    user_states.pop(cb.from_user.id, None)
    t, k = build_menu(cb.from_user.id)
    await edit_msg(cb.message, t, k)

@dp.callback_query(F.data.startswith("ta_"))
async def cb_ta(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    tid = int(cb.data[3:])
    uid = task_approve(tid, cb.from_user.id)
    if uid:
        log_action(cb.from_user.id, "task_approve", str(tid))
        try:
            await cb.message.edit_text(f"✅ TikTok #{tid} одобрено", parse_mode="HTML")
        except: pass
        try:
            await bot.send_message(uid, f"🎉 TikTok задание одобрено!\n🎁 {TIKTOK_REWARD_GIFT}")
        except: pass

@dp.callback_query(F.data.startswith("tr_"))
async def cb_trj(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    tid = int(cb.data[3:])
    uid = task_reject(tid, cb.from_user.id)
    log_action(cb.from_user.id, "task_reject", str(tid))
    try:
        await cb.message.edit_text(f"❌ TikTok #{tid} отклонено", parse_mode="HTML")
    except: pass
    if uid:
        try:
            await bot.send_message(uid, "❌ TikTok задание отклонено. Попробуйте ещё раз.")
        except: pass


# ═══════════════════════ ОПЛАТА ═══════════════════════

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await q.answer(ok=True)

@dp.message(F.successful_payment)
async def succ_pay(msg: Message):
    payload = msg.successful_payment.invoice_payload
    parts = payload.split("_")
    amount_paid = msg.successful_payment.total_amount
    uid = msg.from_user.id

    # Подписка
    if parts[0] == "sub" and len(parts) >= 3:
        k = parts[1]
        target_uid = int(parts[2])
        p = PRICES.get(k)
        if p:
            end = give_subscription(target_uid, p["days"])
            log_action(target_uid, "purchase_sub", f"{k} = {p['stars']}⭐")
            await msg.answer(f"🎉 <b>Оплачено!</b> {p['label']} до {end}", parse_mode="HTML")
            
            ref_uid = get_user(target_uid).get("referred_by", 0)
            if ref_uid and ref_uid != target_uid:
                comm = round(amount_paid * REFERRAL_COMMISSION, 1)
                if comm > 0:
                    add_balance(ref_uid, comm)
                    try:
                        await bot.send_message(ref_uid, f"💰 Комиссия <code>{comm}</code>⭐ от реферала!", parse_mode="HTML")
                    except: pass
            
            for aid in ADMIN_IDS:
                try:
                    await bot.send_message(aid, f"💰 Покупка: {target_uid} — {p['label']} ({p['stars']}⭐)")
                except: pass

    # Подарок
    elif parts[0] == "gift" and len(parts) >= 4:
        k = parts[1]
        target_uid = int(parts[2])
        from_uid = int(parts[3])
        p = PRICES.get(k)
        if p:
            end = give_subscription(target_uid, p["days"])
            log_action(from_uid, "gift", f"{target_uid} {k}")
            await msg.answer(f"🎁 <b>Подарок отправлен!</b> {p['label']}", parse_mode="HTML")
            try:
                await bot.send_message(target_uid,
                    f"🎁 Вам подарили <b>{p['label']}</b> Premium!\nДо: {end}",
                    parse_mode="HTML")
            except: pass

    # Пополнение баланса
    elif parts[0] == "topup" and len(parts) >= 3:
        amount = int(parts[1])
        target_uid = int(parts[2])
        add_balance(target_uid, amount)
        log_action(target_uid, "topup", str(amount))
        await msg.answer(f"✅ <b>+{amount}⭐</b> на баланс!", parse_mode="HTML")

    # Магазин
    elif parts[0] == "shop" and len(parts) >= 3:
        item_key = parts[1]
        target_uid = int(parts[2])
        item = SHOP_ITEMS.get(item_key)
        if item:
            log_action(target_uid, "shop_buy", item_key)
            if "searches" in item:
                add_extra_searches(target_uid, item["searches"])
                await msg.answer(f"✅ <b>+{item['searches']} поисков</b> добавлено!", parse_mode="HTML")
            elif "monitor" in item:
                add_monitor_slots(target_uid, item["monitor"])
                await msg.answer(f"✅ <b>+{item['monitor']} слот мониторинга</b> добавлен!", parse_mode="HTML")
            elif "template" in item:
                add_template_uses(target_uid, item["template"])
                await msg.answer(f"✅ <b>+{item['template']} использование шаблона</b> добавлено!", parse_mode="HTML")

    # Бандл
    elif parts[0] == "bundle" and len(parts) >= 3:
        bundle_key = parts[1]
        target_uid = int(parts[2])
        bundle = BUNDLES.get(bundle_key)
        if bundle:
            give_subscription(target_uid, bundle["days"])
            add_extra_searches(target_uid, bundle["searches"])
            log_action(target_uid, "bundle_buy", bundle_key)
            await msg.answer(
                f"🎉 <b>{bundle['label']} активирован!</b>\n\n"
                f"💎 +{bundle['days']} дней Premium\n"
                f"🔍 +{bundle['searches']} поисков",
                parse_mode="HTML")

    # Донат
    elif parts[0] == "donate" and len(parts) >= 3:
        amt = int(parts[1])
        target_uid = int(parts[2])
        log_action(target_uid, "donate", str(amt))
        await msg.answer("❤️ <b>Спасибо за поддержку!</b>", parse_mode="HTML")
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(aid, f"🤖 Донат {amt}⭐ от {target_uid}")
            except: pass


# ═══════════════════════ ВЫВОДЫ ═══════════════════════

@dp.callback_query(F.data.startswith("wd_ok_"))
async def cb_wdo(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    wid = int(cb.data[6:])
    r = process_withdrawal(wid, cb.from_user.id, True)
    if r:
        log_action(cb.from_user.id, "wd_approve", str(wid))
        await edit_msg(cb.message, f"✅ Вывод #{wid} одобрен ({r['amount']:.1f}⭐)")
        try:
            await bot.send_message(r["uid"], f"✅ Вывод #{wid} на {r['amount']:.1f}⭐ одобрен!")
        except: pass

@dp.callback_query(F.data.startswith("wd_no_"))
async def cb_wdn(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    wid = int(cb.data[6:])
    r = process_withdrawal(wid, cb.from_user.id, False)
    if r:
        log_action(cb.from_user.id, "wd_reject", str(wid))
        await edit_msg(cb.message, f"❌ Вывод #{wid} отклонён")
        try:
            await bot.send_message(r["uid"], f"❌ Вывод #{wid} отклонён")
        except: pass

# ═══════════════════════ АДМИНКА ═══════════════════════

@dp.callback_query(F.data == "cmd_admin")
async def cb_admin(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    s = get_stats()
    ps = pool.stats()
    
    session_line = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"
    
    text = (
        f"👑 <b>Админ-панель v23.0</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Юзеров: <code>{s['users']}</code> | 💎 Premium: <code>{s['subs']}</code>\n"
        f"🚫 Бан: <code>{s['banned']}</code> | 🔍 Поисков: <code>{s['searches']}</code>\n"
        f"📅 Сегодня: 👤<code>{s['today_users']}</code> 🔍<code>{s['today_searches']}</code>\n\n"
        f"🔄 Сессии: {session_line}\n"
        f"📊 Проверок: <code>{ps['checks']}</code> | 🛡 Спасено: <code>{ps['botapi_saves']+ps.get('recheck_saves',0)}</code>\n\n"
        f"👁 Мониторов: <code>{s['monitors']}</code> | ⛔ Чёрный список: <code>{s['blacklist']}</code>\n"
        f"📱 TikTok: <code>{s['tasks']}</code> | 💸 Выводы: <code>{s['withdrawals']}</code>"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"🔄 Сессии ({ps['active']}/{ps['total']})", callback_data="a_sessions")
    kb.button(text="👤 Юзер", callback_data="a_user")
    kb.button(text="🔑 Ключ", callback_data="a_keys")
    kb.button(text="📩 Выдать", callback_data="a_give")
    kb.button(text="🚫 Бан", callback_data="a_ban")
    kb.button(text="✅ Разбан", callback_data="a_unban")
    kb.button(text=f"📱 TikTok ({s['tasks']})", callback_data="a_tt")
    kb.button(text=f"💸 Выводы ({s['withdrawals']})", callback_data="a_wd")
    kb.button(text="📤 Рассылка", callback_data="a_bcast")
    kb.button(text="📢 Акции", callback_data="a_promos")
    kb.button(text="⛔ Чёрный список", callback_data="a_blacklist")
    kb.button(text="📋 Лог действий", callback_data="a_log")
    kb.button(text="📥 Обновить код", callback_data="a_update")
    kb.button(text="🔄 Перезапуск", callback_data="a_restart")
    kb.button(text="📋 Логи сервера", callback_data="a_logs")
    kb.button(text="💻 Сервер", callback_data="a_server")
    kb.button(text="🔙 Меню", callback_data="cmd_menu")
    kb.adjust(1, 2, 2, 2, 2, 2, 2, 2, 1)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ─── Управление юзером ───

@dp.callback_query(F.data == "a_user")
async def cb_a_user(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_user_search"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "👤 <b>Введите ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("au_"))
async def cb_au(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    parts = cb.data.split("_")
    action = parts[1]
    target_uid = int(parts[2])
    
    if action == "adds":
        user_states[cb.from_user.id] = {"action": "admin_user_add_searches", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "🔍 <b>Сколько поисков добавить?</b>", kb.as_markup())
    
    elif action == "sets":
        user_states[cb.from_user.id] = {"action": "admin_user_set_free", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "🔍 <b>Установить кол-во поисков (free):</b>", kb.as_markup())
    
    elif action == "addb":
        user_states[cb.from_user.id] = {"action": "admin_user_add_balance", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "💰 <b>Сколько звёзд добавить?</b>", kb.as_markup())
    
    elif action == "setb":
        user_states[cb.from_user.id] = {"action": "admin_user_set_balance", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "💰 <b>Установить баланс:</b>", kb.as_markup())
    
    elif action == "addd":
        user_states[cb.from_user.id] = {"action": "admin_user_add_days", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "💎 <b>Сколько дней добавить?</b>", kb.as_markup())
    
    elif action == "remd":
        remove_subscription(target_uid)
        log_action(cb.from_user.id, "admin_remove_sub", str(target_uid))
        await show_user_panel(cb, target_uid)
    
    elif action == "ban":
        ban_user(target_uid)
        log_action(cb.from_user.id, "admin_ban", str(target_uid))
        await show_user_panel(cb, target_uid)
    
    elif action == "unban":
        unban_user(target_uid)
        log_action(cb.from_user.id, "admin_unban", str(target_uid))
        await show_user_panel(cb, target_uid)
    
    elif action == "hist":
        hist = get_history(target_uid, 15)
        text = f"📜 <b>История юзера {target_uid}:</b>\n\n"
        if hist:
            for h in hist:
                text += f"• <code>@{h[0]}</code> {h[2]} {h[1]}\n"
        else:
            text += "<i>Пусто</i>"
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 К юзеру", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, text, kb.as_markup())
    
    elif action == "refs":
        refs = get_user_referrals(target_uid, 15)
        text = f"👥 <b>Рефералы юзера {target_uid}:</b>\n\n"
        if refs:
            for r in refs:
                name = f"@{r['uname']}" if r['uname'] else f"ID:{r['uid']}"
                text += f"• {name} — {r['created']}\n"
        else:
            text += "<i>Нет рефералов</i>"
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 К юзеру", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, text, kb.as_markup())
    
    elif action == "msg":
        user_states[cb.from_user.id] = {"action": "admin_user_msg", "target": target_uid}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data=f"au_back_{target_uid}")
        await edit_msg(cb.message, "📤 <b>Введите сообщение для юзера:</b>", kb.as_markup())
    
    elif action == "back":
        await show_user_panel(cb, target_uid)


# ─── Сессии ───

@dp.callback_query(F.data == "a_sessions")
async def cb_asessions(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    ps = pool.stats()
    detail = pool.detailed_status()
    
    text = (
        f"🔄 <b>Сессии</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🟢 Healthy: <code>{ps['active']-ps.get('warming',0)}</code>\n"
        f"🟡 Warming: <code>{ps.get('warming',0)}</code>\n"
        f"🟠 Cooldown: <code>{ps.get('cooldown',0)}</code>\n"
        f"🔴 Dead: <code>{ps.get('dead',0)}</code>\n\n"
        f"📊 Проверок: <code>{ps['checks']}</code>\n"
        f"❌ Ошибок: <code>{ps.get('errors',0)}</code>\n"
        f"🛡 Спасено: <code>{ps['botapi_saves']+ps.get('recheck_saves',0)}</code>\n"
        f"🔄 Реконнектов: <code>{ps.get('reconnects',0)}</code>\n\n"
        f"<pre>{detail}</pre>"
    )
    
    kb = InlineKeyboardBuilder()
    for i in range(len(pool.clients)):
        st = pool.status.get(i, 'dead')
        if st == 'dead':
            kb.button(text=f"🔄 #{i+1} Воскресить", callback_data=f"a_revive_{i}")
        else:
            kb.button(text=f"💀 #{i+1} Отключить", callback_data=f"a_kill_{i}")
    kb.button(text="➕ Добавить сессию", callback_data="a_add_session")
    kb.button(text="⚡ Reconnect all", callback_data="a_reconnect_all")
    kb.button(text="🔄 Обновить", callback_data="a_sessions")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data.startswith("a_kill_"))
async def cb_kill_session(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    idx = int(cb.data[7:])
    if idx < len(pool.clients):
        pool.status[idx] = 'dead'
        pool.cooldown_until[idx] = time.time() + 9999
        try: await pool.clients[idx].disconnect()
        except: pass
        log_action(cb.from_user.id, "session_kill", str(idx))
    await cb_asessions(cb)

@dp.callback_query(F.data.startswith("a_revive_"))
async def cb_revive_session(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    idx = int(cb.data[9:])
    if idx < len(pool.clients):
        await pool._try_reconnect(idx)
        log_action(cb.from_user.id, "session_revive", str(idx))
    await cb_asessions(cb)

@dp.callback_query(F.data == "a_reconnect_all")
async def cb_areconnect(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    await edit_msg(cb.message, "🔄 Реконнект мёртвых сессий...")
    
    reconnected = 0
    for i in range(len(pool.clients)):
        if pool.status.get(i) == 'dead':
            await pool._try_reconnect(i)
            if pool.status.get(i) != 'dead':
                reconnected += 1
            await asyncio.sleep(2)
    
    log_action(cb.from_user.id, "reconnect_all", str(reconnected))
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Статус", callback_data="a_sessions")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message, f"✅ Реконнект: <b>{reconnected}</b> восстановлено", kb.as_markup())

@dp.callback_query(F.data == "a_add_session")
async def cb_add_session(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_add_session_api_id"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="a_sessions")
    await edit_msg(cb.message, "➕ <b>Новая сессия</b>\n\nВведите <b>api_id</b>:", kb.as_markup())


# ─── Ключи ───

@dp.callback_query(F.data == "a_keys")
async def cb_akeys(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    kb = InlineKeyboardBuilder()
    for k, p in PRICES.items():
        kb.button(text=p["label"], callback_data=f"gk_{p['days']}")
    kb.button(text="✏️ Своё кол-во", callback_data="gk_custom")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    await edit_msg(cb.message, "🔑 <b>Генерация ключа</b>\n\nВыберите срок:", kb.as_markup())

@dp.callback_query(F.data.startswith("gk_"))
async def cb_gk(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    val = cb.data[3:]
    if val == "custom":
        user_states[cb.from_user.id] = {"action": "admin_key_days"}
        kb = InlineKeyboardBuilder()
        kb.button(text="❌", callback_data="a_keys")
        await edit_msg(cb.message, "✏️ <b>Введите кол-во дней:</b>", kb.as_markup())
        return
    days = int(val)
    key = generate_key(days, f"D{days}")
    log_action(cb.from_user.id, "key_generate", key)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔑 Ещё", callback_data="a_keys")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    await edit_msg(cb.message, f"🔑 <code>{key}</code>\n📅 {days} дней", kb.as_markup())


# ─── Выдать подписку ───

@dp.callback_query(F.data == "a_give")
async def cb_agive(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_give_user"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "📩 <b>Введите ID или @username:</b>", kb.as_markup())


# ─── Бан / Разбан ───

@dp.callback_query(F.data == "a_ban")
async def cb_aban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_ban_input"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "🚫 <b>Введите ID или @username:</b>", kb.as_markup())

@dp.callback_query(F.data == "a_unban")
async def cb_aunban(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_unban_input"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "✅ <b>Введите ID или @username:</b>", kb.as_markup())


# ─── TikTok модерация ───

@dp.callback_query(F.data == "a_tt")
async def cb_att(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    tasks = get_pending_tasks()
    
    if not tasks:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        await edit_msg(cb.message, "📱 Нет ожидающих заданий", kb.as_markup())
        return
    
    text = f"📱 <b>TikTok задания ({len(tasks)}):</b>\n\n"
    kb = InlineKeyboardBuilder()
    for t in tasks:
        text += f"#{t['id']} | {t['uid']} | {t['created']}\n"
        kb.button(text=f"✅ #{t['id']}", callback_data=f"ta_{t['id']}")
        kb.button(text=f"❌ #{t['id']}", callback_data=f"tr_{t['id']}")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ─── Выводы ───

@dp.callback_query(F.data == "a_wd")
async def cb_awd(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    wds = get_pending_withdrawals()
    
    if not wds:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        await edit_msg(cb.message, "💸 Нет ожидающих выводов", kb.as_markup())
        return
    
    text = f"💸 <b>Выводы ({len(wds)}):</b>\n\n"
    kb = InlineKeyboardBuilder()
    for w in wds:
        text += f"#{w['id']} | {w['uid']} | {w['amount']:.1f}⭐\n"
        kb.button(text=f"✅ #{w['id']}", callback_data=f"wd_ok_{w['id']}")
        kb.button(text=f"❌ #{w['id']}", callback_data=f"wd_no_{w['id']}")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ─── Рассылка ───

@dp.callback_query(F.data == "a_bcast")
async def cb_abcast(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_broadcast_text"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="cmd_admin")
    await edit_msg(cb.message, "📤 <b>Введите текст рассылки (HTML):</b>", kb.as_markup())


# ─── Акции ───

@dp.callback_query(F.data == "a_promos")
async def cb_apromos(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    promos = get_active_promotions()
    
    text = f"📢 <b>Акции ({len(promos)}):</b>\n\n"
    kb = InlineKeyboardBuilder()
    for pr in promos:
        text += f"• #{pr['id']} <b>{pr['name']}</b> ({pr['ptype']})\n"
        kb.button(text=f"❌ #{pr['id']}", callback_data=f"a_endp_{pr['id']}")
    if not promos:
        text += "<i>Нет активных</i>\n"
    
    kb.button(text="➕ Создать", callback_data="a_addp")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "a_addp")
async def cb_aaddp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_promo_name"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="a_promos")
    await edit_msg(cb.message, "📢 <b>Введите название акции:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("a_endp_"))
async def cb_aendp(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    pid = int(cb.data[7:])
    end_promotion(pid)
    log_action(cb.from_user.id, "promo_end", str(pid))
    await cb_apromos(cb)


# ─── Чёрный список ───

@dp.callback_query(F.data == "a_blacklist")
async def cb_ablacklist(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    bl = get_blacklist()
    
    text = f"⛔ <b>Чёрный список ({len(bl)}):</b>\n\n"
    text += "<i>Юзернеймы которые НЕ выдаются при поиске</i>\n\n"
    
    kb = InlineKeyboardBuilder()
    for item in bl[:20]:
        text += f"• <code>@{item['username']}</code>\n"
        kb.button(text=f"❌ {item['username']}", callback_data=f"bl_del_{item['username']}")
    if not bl:
        text += "<i>Пусто</i>\n"
    
    kb.button(text="➕ Добавить", callback_data="bl_add")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(2, 1)
    
    await edit_msg(cb.message, text, kb.as_markup())

@dp.callback_query(F.data == "bl_add")
async def cb_bl_add(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    user_states[cb.from_user.id] = {"action": "admin_blacklist_add"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌", callback_data="a_blacklist")
    await edit_msg(cb.message, "⛔ <b>Введите юзернейм для блокировки:</b>", kb.as_markup())

@dp.callback_query(F.data.startswith("bl_del_"))
async def cb_bl_del(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    un = cb.data[7:]
    remove_blacklist(un)
    log_action(cb.from_user.id, "blacklist_remove", un)
    await cb_ablacklist(cb)


# ─── Лог действий ───

@dp.callback_query(F.data == "a_log")
async def cb_alog(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    logs = get_action_log(30)
    
    text = f"📋 <b>Лог действий (последние 30):</b>\n\n"
    for log in logs:
        text += f"<code>{log['created'][5:]}</code> | {log['uid']} | {log['action']}"
        if log['details']:
            text += f" | {log['details'][:20]}"
        text += "\n"
    
    if not logs:
        text += "<i>Пусто</i>"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="a_log")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ─── Управление сервером ───

@dp.callback_query(F.data == "a_update")
async def cb_update(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    await edit_msg(cb.message, "📥 Скачиваю обновление с GitHub...")
    
    try:
        result = subprocess.run(
            ["git", "pull", "origin", "main"],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        output = (result.stdout + "\n" + result.stderr).strip()
    except Exception as e:
        output = f"Ошибка: {e}"
    
    if "Already up to date" in output:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        await edit_msg(cb.message, "✅ Уже последняя версия", kb.as_markup())
        return
    
    log_action(cb.from_user.id, "update", "git pull")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Применить (перезапуск)", callback_data="a_restart")
    kb.button(text="🔙 Не сейчас", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message,
        f"📥 <b>Код обновлён!</b>\n\n<pre>{output[:800]}</pre>\n\n⚠️ Нажмите перезапуск",
        kb.as_markup())

@dp.callback_query(F.data == "a_restart")
async def cb_restart(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    ps = pool.stats()
    log_action(cb.from_user.id, "restart", "")
    
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid,
                f"🔄 <b>Перезапуск бота...</b>\n\n"
                f"Сессий было: {ps['active']}/{ps['total']}\n"
                f"Вернусь через 3-5 сек",
                parse_mode="HTML")
        except: pass
    
    await asyncio.sleep(1)
    
    try: await pool.disconnect()
    except: pass
    try: await http_session.close()
    except: pass
    
    os.execv(sys.executable, [sys.executable] + sys.argv)

@dp.callback_query(F.data == "a_logs")
async def cb_logs(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    try:
        result = subprocess.run(
            ["journalctl", "-u", "hunter", "-n", "40", "--no-pager"],
            capture_output=True, text=True, timeout=10
        )
        logs = result.stdout[-3500:] if result.stdout else "Логи пусты или сервис не настроен"
    except Exception as e:
        logs = f"Ошибка: {e}\n\nВозможно systemd сервис не настроен"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="a_logs")
    kb.button(text="📥 Скачать полные", callback_data="a_logs_full")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message, f"📋 <b>Логи сервера:</b>\n\n<pre>{logs}</pre>", kb.as_markup())

@dp.callback_query(F.data == "a_logs_full")
async def cb_logs_full(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    try:
        result = subprocess.run(
            ["journalctl", "-u", "hunter", "-n", "500", "--no-pager"],
            capture_output=True, text=True, timeout=15
        )
        content = result.stdout or "Пусто"
    except Exception as e:
        content = f"Ошибка: {e}"
    
    await bot.send_document(
        cb.from_user.id,
        BufferedInputFile(content.encode(), filename=f"logs_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"),
        caption="📋 Полные логи сервера")

@dp.callback_query(F.data == "a_server")
async def cb_server(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    await answer_cb(cb)
    
    try:
        uptime = subprocess.run(["uptime", "-p"], capture_output=True, text=True, timeout=5).stdout.strip()
    except:
        uptime = "N/A"
    
    try:
        mem = subprocess.run(["free", "-h"], capture_output=True, text=True, timeout=5).stdout
    except:
        mem = "N/A"
    
    try:
        disk = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=5).stdout
    except:
        disk = "N/A"
    
    try:
        load = subprocess.run(["cat", "/proc/loadavg"], capture_output=True, text=True, timeout=5).stdout.strip()
    except:
        load = "N/A"
    
    text = (
        f"💻 <b>Сервер</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ <b>Аптайм:</b> {uptime}\n"
        f"⚡ <b>Нагрузка:</b> {load}\n\n"
        f"💾 <b>RAM:</b>\n<pre>{mem}</pre>\n"
        f"💿 <b>Диск:</b>\n<pre>{disk}</pre>"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="a_server")
    kb.button(text="🔙 Админ", callback_data="cmd_admin")
    kb.adjust(1)
    
    await edit_msg(cb.message, text, kb.as_markup())


# ═══════════════════════ НАПОМИНАНИЯ ═══════════════════════

async def reminder_loop():
    while True:
        try:
            await asyncio.sleep(REMINDER_CHECK_INTERVAL)
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            for days_before in REMINDER_DAYS:
                users = get_expiring_users(days_before)
                for u in users:
                    rk = f"{today_str}_d{days_before}"
                    if rk in u.get("last_reminder", ""):
                        continue
                    
                    kb = InlineKeyboardBuilder()
                    kb.button(text="💰 Продлить", callback_data="cmd_prices")
                    
                    try:
                        await bot.send_message(u["uid"],
                            f"🔔 <b>Подписка заканчивается!</b>\n\n"
                            f"Осталось {days_before} дн.\n"
                            f"⏰ До: {u['sub_end']}",
                            reply_markup=kb.as_markup(), parse_mode="HTML")
                        set_last_reminder(u["uid"], rk)
                    except: pass
        except Exception as e:
            logger.error(f"Reminder: {e}")


# ═══════════════════════ МОНИТОРИНГ ЮЗОВ ═══════════════════════

async def monitor_loop():
    """Проверяет юзы на мониторинге каждые 30 мин"""
    await asyncio.sleep(60)
    
    while True:
        try:
            expire_monitors()
            monitors = get_active_monitors()
            
            for mon in monitors:
                try:
                    status = await pool.check(mon["username"])
                    
                    if status in ("free", "maybe_free"):
                        # Юз освободился!
                        update_monitor_status(mon["id"], "free")
                        
                        try:
                            kb = InlineKeyboardBuilder()
                            kb.button(text="📊 Оценить", callback_data=f"eval_{mon['username']}")
                            kb.button(text="👁 Мониторинг", callback_data="cmd_monitors")
                            
                            await bot.send_message(mon["uid"],
                                f"🎉 <b>Юзернейм освободился!</b>\n\n"
                                f"<code>@{mon['username']}</code>\n\n"
                                f"Скорее занимай!",
                                reply_markup=kb.as_markup(), parse_mode="HTML")
                        except: pass
                    else:
                        update_monitor_status(mon["id"], "taken")
                    
                    await asyncio.sleep(3)
                except: pass
            
        except Exception as e:
            logger.error(f"Monitor loop: {e}")
        
        await asyncio.sleep(MONITOR_CHECK_INTERVAL)


# ═══════════════════════ SESSION WATCHDOG ═══════════════════════

async def session_watchdog():
    """Тихий watchdog — проверяет каждые 15 мин"""
    await asyncio.sleep(60)
    last_alert = 0
    
    while True:
        try:
            ps = pool.stats()
            total = ps['total']
            dead = ps.get('dead', 0)
            
            # Реконнект мёртвых
            if dead > 0:
                for i in range(len(pool.clients)):
                    if pool.status.get(i) == 'dead':
                        await pool._try_reconnect(i)
                        await asyncio.sleep(10)
            
            # Алерт только раз в час
            ps2 = pool.stats()
            now = time.time()
            if ps2['active'] <= max(total // 2, 1) and total > 0 and now - last_alert > 3600:
                last_alert = now
                await notify_admins(
                    f"⚠️ <b>Мало сессий!</b>\n\n"
                    f"Живых: {ps2['active']}/{total}\n"
                    f"Dead: {ps2.get('dead', 0)}\n"
                    f"Cooldown: {ps2.get('cooldown', 0)}")
        except Exception as e:
            logger.error(f"Watchdog: {e}")
        
        await asyncio.sleep(900)


# ═══════════════════════ SYSTEMD AUTO-SETUP ═══════════════════════

def setup_systemd():
    """Автонастройка systemd при первом запуске"""
    service_path = "/etc/systemd/system/hunter.service"
    
    if os.path.exists(service_path):
        return
    
    if os.geteuid() != 0:
        logger.warning("Не root — systemd не настроен. Запустите от root для автонастройки.")
        return
    
    bot_path = os.path.abspath(__file__)
    bot_dir = os.path.dirname(bot_path)
    python_path = sys.executable
    
    service = f"""[Unit]
Description=Username Hunter Bot v23.0
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={bot_dir}
ExecStart={python_path} {bot_path}
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
"""
    
    try:
        with open(service_path, "w") as f:
            f.write(service)
        
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "enable", "hunter"], check=True)
        
        logger.info("✅ Systemd сервис создан автоматически!")
        logger.info("Бот будет автоматически перезапускаться при падении")
    except Exception as e:
        logger.error(f"Systemd setup failed: {e}")


# ═══════════════════════ MAIN ═══════════════════════

async def main():
    global http_session, bot_info
    
    init_db()
    setup_systemd()
    
    bot_info = await bot.get_me()
    http_session = aiohttp.ClientSession()
    
    await pool.init(ACCOUNTS)
    ps = pool.stats()
    
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"🚀 @{bot_info.username} v23.0 — MEGA UPDATE")
    logger.info(f"🔄 Сессий: {ps['total']} (🟢{ps['active']} 🟡{ps.get('warming',0)} 🔴{ps.get('dead',0)})")
    logger.info(f"🎯 Режимов поиска: {len(SEARCH_MODES) + 2}")
    logger.info(f"💎 Premium: {PREMIUM_COUNT} юзов, {PREMIUM_SEARCHES_LIMIT} поисков")
    logger.info(f"🆓 Free: {FREE_COUNT} юз, {FREE_SEARCHES} поисков")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Фоновые задачи
    asyncio.create_task(reminder_loop())
    asyncio.create_task(session_watchdog())
    asyncio.create_task(monitor_loop())
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_session.close()
        await pool.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
