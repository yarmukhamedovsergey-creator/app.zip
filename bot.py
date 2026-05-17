"""
USERNAME HUNTER v25.0 — VIP + ТЕМАТИЧЕСКИЙ ПОИСК + ССЫЛКИ
FIXED: working fast search v7-no-silent-hang
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
import html
import os
import sys
import subprocess
from datetime import datetime, timedelta
from aiogram.types import FSInputFile
from pathlib import Path

CACHE_FILE = "free_usernames.json"
cache_lock = asyncio.Lock()

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    LabeledPrice, PreCheckoutQuery, InputMediaPhoto
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Telethon полностью отключён: бот работает без user-сессий.
HAS_TELETHON = False

pool = None
http_session = None
bot_info = None

# ═══════════════════════ НАСТРОЙКИ ═══════════════════════

MAIN_TOKEN = "8325751391:AAFGB3KV34DiOp_Z0HXg5nErgxwOk6XR3C4"
ADMIN_IDS = [5969266721, 7894051808]
ADMIN_CONTACT = "emeuw"

ACCOUNTS = []  # режим без user-сессий


FREE_SEARCHES = 2
FREE_COUNT = 2
PREMIUM_COUNT = 3
VIP_COUNT = 5  # ═ НОВОЕ ═
FREE_SEARCHES_LIMIT = 2
PREMIUM_SEARCHES_LIMIT = 7
VIP_SEARCHES_LIMIT = 15  # ═ НОВОЕ ═
REF_BONUS = 2
REFERRAL_PREMIUM_BONUSES = {5: 1, 10: 3, 15: 8, 30: 14}
REFERRAL_COMMISSION = 0.04
SEARCH_COOLDOWN = 10
MIN_WITHDRAW = 50
SEARCH_PRICE_STARS = 5
PAY_CONTACT = "Soveqk"
REQUIRED_CHANNELS = ["SwordUsers"]
MONITOR_CHECK_INTERVAL = 1800
MONITOR_MAX_FREE = 0
MONITOR_MAX_PREMIUM = 5
MONITOR_MAX_VIP = 10
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
    "1d":   {"label": "1 день",    "days": 1,     "stars": 40,   "rub": 45},
    "3d":   {"label": "3 дня",     "days": 3,     "stars": 100,  "rub": 120},
    "7d":   {"label": "7 дней",    "days": 7,     "stars": 180,  "rub": 250},
    "30d":  {"label": "1 месяц",   "days": 30,    "stars": 600,  "rub": 800},
    "90d":  {"label": "3 месяца",  "days": 90,    "stars": 1800, "rub": 2200},
    "365d": {"label": "1 год",     "days": 365,   "stars": 6000, "rub": 8000},
    "life": {"label": "Навсегда",  "days": 99999, "stars": 9000, "rub": 11999},
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
user_states = {}
http_session = None
bot_info = None
DB = "hunter.db"
# os.makedirs("sessions", exist_ok=True)  # user-сессии отключены
searching_users = set()
user_search_cooldown = {}
_fragment_cache = {}
_fragment_cache_ttl = 600
BOT_CONFIG_FILE = "bot_config.json"
MENU_IMAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "image.png")

async def send_menu_photo(chat_id, text, kb=None):
    try:
        photo = FSInputFile(MENU_IMAGE)
        await bot.send_photo(chat_id, photo=photo, caption=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)

async def edit_to_photo(msg, text, kb=None):
    try:
        photo = FSInputFile(MENU_IMAGE)
        media = InputMediaPhoto(media=photo, caption=text, parse_mode="HTML")
        await msg.edit_media(media=media, reply_markup=kb)
        return
    except Exception:
        pass
    chat_id = msg.chat.id
    try: await msg.delete()
    except: pass
    await send_menu_photo(chat_id, text, kb)

async def edit_msg(msg, text, kb=None):
    try:
        if msg.photo:
            chat_id = msg.chat.id
            try: await msg.delete()
            except: pass
            await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        else:
            await msg.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except: pass

async def answer_cb(cb, text=None, show_alert=False):
    try: await cb.answer(text, show_alert=show_alert)
    except: pass

def with_main_branding(text):
    return text or ""

def format_results(found, stats, title):
    attempts = int((stats or {}).get("attempts", 0) or 0)
    elapsed = int((stats or {}).get("elapsed", 0) or 0)
    if not found:
        return (
            f"🔍 <b>{title}</b>\n\n"
            "❌ Ничего не найдено\n\n"
            f"📊 Проверено: <code>{attempts}</code>\n"
            f"⏱ Время: <code>{elapsed}с</code>"
        )

    lines = [f"🔍 <b>{title}</b>", "", f"✅ Найдено: <code>{len(found)}</code>", ""]
    for i, item in enumerate(found, 1):
        username = item.get("username", "")
        fragment = item.get("fragment", "unavailable")
    
    # Красивый статус
        if fragment == "available":
            frag_text = "💎 Доступен"
        elif fragment == "sold":
            frag_text = "❌ Продан"
        elif fragment == "in auction":
            frag_text = "🔨 Аукцион"
        elif fragment == "reserved":
            frag_text = "⏳ Резерв"
        else:
            frag_text = "—"
    
        lines.extend([
            f"{i}. <code>@{username}</code>",
            f"📱 <a href='https://t.me/{username}'>Telegram</a> · 💎 <a href='https://fragment.com/username/{username}'>{frag_text}</a>",
            "",
        ])

    lines.extend([
        f"📊 Проверено: <code>{attempts}</code>",
        f"⏱ Время: <code>{elapsed}с</code>",
    ])
    return "\n".join(lines)

# ═══════════════════════ КОНФИГ ═══════════════════════

DEFAULT_CONFIG = {
    "free_searches": 3, "free_count": 2, "premium_count": 3, "vip_count": 5,
    "free_searches_limit": 2, "premium_searches_limit": 7, "vip_searches_limit": 15,
    "ref_bonus": 2, "search_cooldown": 10,
    "search_price_stars": 5, "min_withdraw": 50, "pay_contact": "SoveqK",
    "required_channels": ["SwordUsers"],
    "text_welcome": "", "text_found": "", "text_empty": "",
    "text_profile_header": "", "text_shop_header": "",
    "btn_tiktok": True, "btn_roulette": False, "btn_monitor": True,
    "btn_shop": True, "btn_support": True, "btn_referral": True,
    "mode_default": True, "mode_beautiful": True,
    "mode_default_premium": False, "mode_beautiful_premium": True,
    "prices": {}, "daily_report": True, "daily_report_hour": 23,
    "notify_purchases": True, "notify_milestones": True,
    "checker_mode": "public_strict", "cache_limit": 5000,
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
    global FREE_SEARCHES_LIMIT, PREMIUM_SEARCHES_LIMIT, VIP_SEARCHES_LIMIT
    global REF_BONUS, SEARCH_COOLDOWN, SEARCH_PRICE_STARS, MIN_WITHDRAW
    global PAY_CONTACT, REQUIRED_CHANNELS, CHECKER_MODE

    FREE_SEARCHES = config.get("free_searches", 2)
    FREE_COUNT = config.get("free_count", 2)
    PREMIUM_COUNT = config.get("premium_count", 3)
    VIP_COUNT = config.get("vip_count", 5)
    FREE_SEARCHES_LIMIT = config.get("free_searches_limit", 2)
    PREMIUM_SEARCHES_LIMIT = config.get("premium_searches_limit", 7)
    VIP_SEARCHES_LIMIT = config.get("vip_searches_limit", 15)
    REF_BONUS = config.get("ref_bonus", 2)
    SEARCH_COOLDOWN = config.get("search_cooldown", 10)
    SEARCH_PRICE_STARS = config.get("search_price_stars", 5)
    MIN_WITHDRAW = config.get("min_withdraw", 50)
    PAY_CONTACT = config.get("pay_contact", "SoveqK")
    REQUIRED_CHANNELS = config.get("required_channels", ["SwordUsers"])
    CHECKER_MODE = config.get("checker_mode", "botapi")

    for key in SEARCH_MODES:
        SEARCH_MODES[key]["premium"] = config.get(
            f"mode_{key}_premium",
            SEARCH_MODES[key].get("_default_premium", False)
        )
        SEARCH_MODES[key]["disabled"] = not config.get(f"mode_{key}", True)

    if "prices" in config:
        for k, v in config["prices"].items():
            if k in PRICES:
                PRICES[k]["stars"] = v

def load_saved_sessions():
    """User-сессии удалены. Возвращаем пустой список для совместимости админ-панели."""
    return []

def is_button_enabled(name):
    return load_bot_config().get(f"btn_{name}", True)

def get_checker_mode():
    return "public_strict"

def is_sessions_checker():
    return False

# ═══════════════════════ RATE LIMITER (ОТКЛЮЧЁН) ═══════════════════════

class RateLimiter:
    def __init__(self):
        self.search_times = {}
        self.check_times = {}
        self.warnings = {}
        self.temp_bans = {}
        self.status = {}
        self.cooldown_until = {}
        self.last_used = {}
        self.error_streak = {}
        self.total_errors = {}
        self.req_count = {}
        self.window_start = {}
        self.flood_times = {}
        self.adaptive_delay = {}
        self.active_users = {}
        self.total_checks = 0
        self.caught_by_botapi = 0
        self.caught_by_recheck = 0
        self.reconnect_count = 0
        self.max_users_per_account = 3
        self.BASE_DELAY = 2.0
        self.MAX_DELAY = 20.0
        self.BUDGET_PER_MIN = 12
        self.MAX_ERROR_STREAK = 3
        self.FLOOD_REST_TIME = 600
        self.WARMUP_EXTRA_DELAY = 10.0
    # ================
        self._health_task = None
        self._monitor_task = None
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
    """Заглушка вместо Telethon-пула. В проекте нет user-сессий вообще."""

    def __init__(self):
        self.clients = []
        self.status = {}
        self.cooldown_until = {}
        self.total_checks = 0
        self.caught_by_botapi = 0
        self.caught_by_recheck = 0
        self.reconnect_count = 0

    async def init(self, accounts=None):
        logger.info("Public strict mode: Telethon sessions are disabled")

    def has_sessions(self):
        return False

    async def check(self, u, uid=None):
        self.total_checks += 1
        try:
            return "free" if await is_username_free(u, uid) else "taken"
        except Exception as e:
            logger.debug(f"[public-check] @{u}: {e}")
            return "skip"

    async def strong_check(self, u, uid=None):
        return await self.check(u, uid)

    def add_user(self, uid):
        return None

    def remove_user(self, uid):
        return None

    def stats(self):
        return {
            "total": 0,
            "active": 0,
            "warming": 0,
            "cooldown": 0,
            "dead": 0,
            "checks": self.total_checks,
            "errors": 0,
            "botapi_saves": self.caught_by_botapi,
            "recheck_saves": self.caught_by_recheck,
            "reconnects": self.reconnect_count,
        }

    def detailed_status(self):
        return "User-сессии отключены. Проверка работает только публичными методами: t.me + Bot API + Fragment."

    async def _try_reconnect(self, i):
        return False

    async def _acquire(self, uid=None, timeout=0):
        return None, None

    def _ok(self, i):
        return None

    def _err(self, i, flood=False, secs=0):
        return None

    async def disconnect(self):
        return None


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

def gen_default():
    """6-буквенные, только буквы"""
    return _pronounceable(6)

def gen_dictionary():
    """5-буквенные слова из словаря"""
    words = [
        "apple","beach","chair","dance","eagle","flame","grape","heart","ivory","jolly",
        "lemon","music","night","ocean","pearl","queen","river","stone","tiger",
        "unity","voice","water","youth","zebra","brick","candy","dream","earth","frost",
        "ghost","honey","image","jewel","knife","light","mouse","noble","olive","peace",
        "smile","table","under","whale","yacht","blade","cloud","flute","grace","horse",
        "juice","mango","opera","piano","raven","sugar","value","angel","crown","daisy",
        "globe","karma","tulip","urban",
    ]
    return random.choice(words).lower()

def gen_beautiful():
    """Красивые паттерны, 5 букв"""
    patterns = [
        lambda: random.choice(_V)+random.choice(_C)+random.choice(_V)+random.choice(_C)+random.choice(_V),
        lambda: random.choice(_C)+random.choice(_V)+random.choice(_C)+random.choice(_C)+random.choice(_V),
        lambda: random.choice(_C)+random.choice(_V)+random.choice(_V)+random.choice(_C)+random.choice(_C),
        lambda: random.choice(_V)+random.choice(_C)+random.choice(_V)+random.choice(_V)+random.choice(_C),
    ]
    return random.choice(patterns)()

_pattern_template = ""
def gen_pattern():
    """Генерация по шаблону (a__le -> a + random + random + l + e)"""
    if not _pattern_template or len(_pattern_template) != 5:
        return gen_default()
    result = []
    for ch in _pattern_template.lower():
        if ch == "_":
            result.append(random.choice(_V if random.random() > 0.5 else _C))
        else:
            result.append(ch)
    return "".join(result)

def gen_word_combinations(word):
    """Комбинирует слово с реальными словами → осмысленные юзернеймы"""
    word = word.lower().strip().replace("@", "")

    w3 = [
        "car","box","lab","hub","man","boy","god","pro","dev","art","web","net",
        "app","bot","fan","run","fly","win","pay","buy","dog","cat","fox","owl",
        "key","pin","tag","tip","spy","doc","gem","ink","map","log","job","war",
        "cup","bar","van","jet","ace","bag","bat","bed","bee","bug","bus","cab",
        "cam","cap","dew","dim","elf","era","eve","fig","fin","fit","fog","fur",
        "gig","gin","gun","gym","ham","hat","hen","hop","hut","ivy","jam","jar",
        "joy","jug","kit","lap","law","leg","lid","lip","lot","mad","mob","mod",
        "mop","mud","mug","nap","nod","nut","oak","oil","orb","ore","pad","pal",
        "pan","pat","paw","pea","peg","pen","pet","pie","pig","pit","pod","pot",
        "pub","pug","rag","ram","rap","rat","ray","rib","rig","rim","rip","rod",
        "row","rub","rug","rum","saw","sax","sir","ski","sob","son","spa","sub",
        "sum","tab","tan","tap","tar","tax","tie","tin","toe","ton","toy","tub",
        "tug","urn","vat","vet","vim","vow","wad","wig","wit","woe","wok","yak",
        "yam","yen","yew","zip","zoo","max","mix","fix","pop","raw","day","one",
        "bit","dot","top","hot","big","old","new","red","ice","zen","sun","sky",
        "air","sea","try","set","get","way","eye","tea",
    ]

    w4 = [
        "cook","shop","club","gang","crew","team","band","camp","city","town",
        "land","park","road","path","lake","moon","rain","wind","snow","gold",
        "iron","rock","wood","dust","sand","wave","coin","cash","bank","card",
        "gift","show","play","game","song","film","book","wall","door","roof",
        "lamp","bell","ring","ball","bowl","drum","fish","bird","bear","wolf",
        "deer","lion","bull","duck","frog","crab","worm","seed","leaf","root",
        "vine","rose","lily","tree","food","meal","cake","milk","rice","meat",
        "soup","wine","beer","salt","mint","sage","chef","king","duke","earl",
        "work","task","plan","goal","idea","mind","soul","life","love","hope",
        "fear","rage","calm","cool","warm","cold","dark","deep","fast","slow",
        "hard","soft","wild","bold","wise","kind","pure","true","real","good",
        "best","mega","boss","lord","star","fire","face","hand","head","bone",
        "fist","tail","wing","claw","horn","fury","glow","haze","mist","void",
        "flux","grid","mesh","core","code","node","link","base","site","zone",
        "mode","byte","chip","data","hack","loop","scan","sync","tech","volt",
        "beam","gear","tool","fuel","pump","tank","helm","cape","mask","robe",
        "vest","boot","belt","whip","pill","dose","dope","hemp","kush","haze",
        "acid","weed","hash","flex","drip","swag","trap","vibe","mood","flow",
        "grim","nuke","bomb","doom","fury","riot","punk","goth","rave","bass",
    ]

    w5 = [
        "craft","world","storm","flame","frost","steel","blade","night","light",
        "dream","ghost","devil","angel","power","force","magic","chaos","order",
        "peace","death","blood","heart","brain","nerve","pulse","flash","spark",
        "blaze","smoke","steam","stone","earth","ocean","river","cloud","shade",
        "ninja","titan","demon","beast","snake","eagle","raven","shark","tiger",
        "viper","cobra","money","price","trade","store","tower","house","guard",
        "watch","quest","point","score","level","arena","field","track","rider",
        "racer","pilot","chief","cyber","pixel","sonic","turbo","ultra","super",
        "hyper","alpha","omega","delta","sigma","prime","elite","royal","mafia",
        "cartel","crack","speed",
    ]

    pre_short = ["my","mr","el","la","de","go","no","hi","ok","up","on","in","dj","dr","mc"]
    pre_long = ["big","top","hot","old","new","red","ice","pro","neo","zen","raw",
                "the","its","not","sir","don","von","max","all","any","bad","mad","sad"]

    results = []
    all_suf = w3 + w4 + w5
    all_pre = pre_short + pre_long + w3 + w4

    # word + суффикс  (mef + cook = mefcook)
    for s in all_suf:
        u = word + s
        if 5 <= len(u) <= 15: results.append(u)

    # префикс + word  (big + mef = bigmef)
    for p in all_pre:
        u = p + word
        if 5 <= len(u) <= 15: results.append(u)

    # суффикс + word  (cook + mef = cookmef)
    for s in w3 + w4:
        u = s + word
        if 5 <= len(u) <= 15: results.append(u)

    # word_суффикс
    for s in w3 + w4[:60]:
        u = word + "_" + s
        if 5 <= len(u) <= 15: results.append(u)

    # префикс_word
    for p in pre_short + pre_long:
        u = p + "_" + word
        if 5 <= len(u) <= 15: results.append(u)

    # word + цифра
    for n in [1,2,3,5,7,9,11,13,23,42,69,77,99,228]:
        u = word + str(n)
        if 5 <= len(u) <= 15: results.append(u)

    # Фильтр
    valid = []; seen = set()
    for u in results:
        ul = u.lower()
        if (ul not in seen and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', ul)
                and 5 <= len(ul) <= 15 and "__" not in ul
                and not ul.startswith("_") and not ul.endswith("_")):
            valid.append(ul); seen.add(ul)

    random.shuffle(valid)
    return valid

async def do_word_search(word, count, msg, uid):
    combinations = gen_word_combinations(word)
    found = []
    attempts = 0
    start = time.time()
    last_update = 0
    checked = set()

    try:
        for u in combinations:
            if len(found) >= count or attempts >= 500:
                break

            if u in checked:
                continue
            checked.add(u)
            if "__" in u or u.startswith("_") or u.endswith("_"):
                continue

            if not is_valid_username_word(u):
                attempts += 1
                continue

            attempts += 1

            if await is_username_free(u, uid):
                found.append({"username": u, "fragment": "unavailable"})
                save_history(uid, u, f"По слову: {word}", len(u))

            await asyncio.sleep(0.3)

            now = time.time()
            if now - last_update > 2.5:
                last_update = now
                try:
                    await msg.edit_text(
                        f"🎯 <b>По слову: {word}</b>\n\n"
                        f"🔍 Проверено: <code>{attempts}</code>\n"
                        f"✅ Найдено: <code>{len(found)}</code>",
                        parse_mode="HTML"
                    )
                except: pass

        elapsed = time.time() - start
        stats = {"attempts": attempts, "elapsed": elapsed}
        return found, stats

    except Exception as e:
        logger.error(f"Word search error: {e}")
        raise

def is_valid_username_default(u):
    """Валидация для дефолт режима (6 букв)"""
    if len(u) != 6 or not u.isalpha():
        return False
    ul = u.lower()
    for w in INVALID_WORDS:
        if w in ul:
            return False
    if is_blacklisted(u):
        return False
    return True

def is_valid_username_beautiful(u):
    """Валидация для красивых (5 букв)"""
    if len(u) != 5 or not u.isalpha():
        return False
    ul = u.lower()
    for w in INVALID_WORDS:
        if w in ul:
            return False
    if is_blacklisted(u):
        return False
    return True

def is_valid_username(u):
    """Общая валидация (5 или 6 букв)"""
    if len(u) not in [5, 6] or not u.isalpha():
        return False
    ul = u.lower()
    for w in INVALID_WORDS:
        if w in ul:
            return False
    if is_blacklisted(u):
        return False
    return True

def is_valid_username_word(u):
    """Оставлено для совместимости: теперь допускаются только 5/6 латинских букв, без цифр и подчёркиваний."""
    return is_valid_username(u)

SEARCH_MODES = {
    "default": {
        "name": "Дефолт",
        "emoji": "🎲",
        "desc": "6 букв, доступен всем",
        "_default_premium": False,
        "premium": False,
        "func": gen_default,
        "validate": is_valid_username_default,
        "disabled": False
    },
    "beautiful": {
        "name": "Красивые",
        "emoji": "💎",
        "desc": "5 букв, стильные паттерны",
        "_default_premium": True,
        "premium": True,
        "func": gen_beautiful,
        "validate": is_valid_username_beautiful,
        "disabled": False
    },
}

SEARCH_MODE_ALIASES = {
    "go_default": "default",
    "default": "default",
    "дефолт": "default",
    "дeфолт": "default",  # на случай латинской e в старом callback_data
    "🎲 дефолт": "default",
    "go_beautiful": "beautiful",
    "beautiful": "beautiful",
    "красивые": "beautiful",
    "красивая": "beautiful",
    "💎 красивые": "beautiful",
}

def normalize_search_mode_callback(data):
    """Приводит старые и новые callback_data кнопок поиска к ключам SEARCH_MODES."""
    raw = (data or "").strip()
    if not raw:
        return ""

    lowered = raw.lower()
    if lowered in SEARCH_MODE_ALIASES:
        return SEARCH_MODE_ALIASES[lowered]

    if lowered.startswith("go_"):
        mode = lowered[3:]
        return mode if mode in SEARCH_MODES else ""

    return lowered if lowered in SEARCH_MODES else ""


# ═══════════════════════ HARD CALLBACK FIX v5 ═══════════════════════
# Маркер ниже должен появиться в консоли после запуска. Если его нет — запущен старый файл.
SEARCH_CALLBACK_FIX_VERSION = "v7-working-fast-search"

def _cb_data(cb):
    return (getattr(cb, "data", "") or "").strip()

def _is_cmd_search_callback(cb):
    return _cb_data(cb) == "cmd_search"

def _is_search_mode_callback(cb):
    return normalize_search_mode_callback(_cb_data(cb)) in SEARCH_MODES

def _print_callback_event(name, cb, extra=""):
    try:
        uid = getattr(getattr(cb, "from_user", None), "id", "unknown")
        data = _cb_data(cb)
        print(f"[{SEARCH_CALLBACK_FIX_VERSION}] {name} uid={uid} data={data!r} {extra}", flush=True)
    except Exception as e:
        print(f"[{SEARCH_CALLBACK_FIX_VERSION}] callback-log-error: {e}", flush=True)

INVALID_WORDS = ["admin","support","help","test","telegram","bot","official",
                 "service","security","account","login","password","verify",
                 "moderator","system","null","undefined","root","user","about","contact",
                 "info","news","updates","support","status","api","dev","beta","alpha"]
    
# ═══════════════════════ ЧЕКЕРЫ ═══════════════════════

async def fast_check_username(u: str) -> str:
    """Быстрая проверка больше не отдаёт free по одному t.me-запросу."""
    return await check_username(u)

def is_valid_telegram_profile_username(u: str) -> bool:
    """Разрешены только буквенные username из рабочих режимов: 5 или 6 латинских букв."""
    if not u:
        return False
    u = u.strip().replace("@", "")
    if len(u) not in (5, 6):
        return False
    if not re.match(r"^[a-zA-Z]+$", u):
        return False
    return True

async def check_username_botapi(u: str) -> str:
    """
    Дополнительная публичная проверка через Bot API getChat.
    taken    — Bot API нашёл публичный username;
    free     — Bot API явно ответил, что чат не найден;
    unknown  — ошибка сети/лимит/неоднозначный ответ.

    Важно: Bot API не является официальной проверкой установки username,
    поэтому free здесь используется только вместе с t.me и Fragment.
    """
    if not bot:
        return "unknown"
    u = (u or "").strip().replace("@", "").lower()
    if not u:
        return "unknown"
    try:
        await bot.get_chat(f"@{u}")
        return "taken"
    except TelegramBadRequest as e:
        text = str(e).lower()
        if "chat not found" in text or "not found" in text:
            return "free"
        logger.debug(f"[botapi] @{u}: {e}")
        return "unknown"
    except Exception as e:
        logger.debug(f"[botapi] @{u}: {e}")
        return "unknown"

async def public_username_status(u: str, uid=None) -> str:
    """
    Строгий публичный статус без user-сессий.
    Возвращает free только если три независимых публичных проверки сошлись.
    Любая неопределённость превращается в unknown/taken и не попадает в выдачу.
    """
    u = (u or "").strip().replace("@", "").lower()

    if not is_valid_telegram_profile_username(u):
        return "taken"
    if is_blacklisted(u):
        return "taken"
    for w in INVALID_WORDS:
        if w == u or w in u:
            return "taken"

    tme_results = []
    for attempt in range(3):
        r = await check_username_tme(u)
        tme_results.append(r)
        if r == "taken":
            return "taken"
        if r == "unknown":
            await asyncio.sleep(0.6 + attempt * 0.4)
            continue
        await asyncio.sleep(0.25)

    if tme_results.count("free") < 2:
        logger.info(f"[public] @{u} t.me unstable={tme_results} → skip")
        return "unknown"

    botapi = await check_username_botapi(u)
    if botapi == "taken":
        logger.info(f"[public] @{u} BotAPI=taken → skip")
        return "taken"
    if botapi == "unknown":
        logger.info(f"[public] @{u} BotAPI=unknown → skip")
        return "unknown"

    fr = await check_fragment(u)
    if fr == "unknown":
        logger.info(f"[public] @{u} Fragment=unknown → skip")
        return "unknown"
    if fr != "unavailable":
        logger.info(f"[public] @{u} Fragment={fr} → skip")
        return "taken"

    return "free"

async def check_username(u: str) -> str:
    """Публичный строгий статус username без Telethon/user-сессий."""
    u = (u or "").strip().replace("@", "").lower()
    try:
        return await public_username_status(u)
    except Exception as e:
        logger.debug(f"[check_username] @{u}: {e}")
        return "unknown"
        
async def check_fragment(u):
    u = (u or "").strip().replace("@", "").lower()
    now = time.time()
    cached = _fragment_cache.get(u)
    if cached and now - cached[1] < _fragment_cache_ttl:
        return cached[0]

    try:
        async with http_session.get(
            f"https://fragment.com/username/{u}",
            timeout=aiohttp.ClientTimeout(total=10),
            headers={
                "User-Agent": random.choice(_TME_USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            allow_redirects=True,
        ) as resp:
            if resp.status != 200:
                logger.info(f"[fragment] @{u} HTTP {resp.status} → unknown")
                return "unknown"

            text = await resp.text()
            low = text.lower()

            status_match = re.search(r'class="tm-section-header-status[^\"]*"[^>]*>(.*?)<', text, re.I | re.S)
            status = re.sub(r"\s+", " ", status_match.group(1)).strip().lower() if status_match else ""

            if any(x in low or x in status for x in ("sold", "owner", "owned")):
                r = "sold"
            elif any(x in low or x in status for x in ("auction", "bidding")):
                r = "in auction"
            elif any(x in low or x in status for x in ("buy now", "for sale", "available")):
                r = "available"
            elif "reserved" in low or "reserved" in status:
                r = "reserved"
            elif "tm-section-header-status" in low:
                # Есть карточка Fragment, но статус не распознан — не рискуем.
                r = "unknown"
            else:
                # Карточки username на Fragment нет — публично не числится как NFT/аукцион.
                r = "unavailable"

            logger.info(f"[fragment] @{u} → {r}")
            _fragment_cache[u] = (r, now)
            return r

    except Exception as e:
        logger.info(f"[fragment] @{u} error: {e} → unknown")
        return "unknown"

_TME_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",
]

async def check_username_tme(u: str) -> str:
    """
    Проверка через публичную страницу t.me.
    Возвращает: free / taken / unknown.
    Никакой unknown не считается свободным.
    """
    u = (u or "").strip().replace("@", "").lower()
    url = f"https://t.me/{u}"

    for attempt in range(3):
        headers = {
            "User-Agent": random.choice(_TME_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Cache-Control": "no-cache",
        }
        try:
            async with http_session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
                headers=headers,
                allow_redirects=True,
            ) as resp:
                if resp.status == 429:
                    wait = int(resp.headers.get("Retry-After", 3))
                    logger.warning(f"[tme] 429 @{u}, wait {wait}s")
                    await asyncio.sleep(wait + random.uniform(0.5, 2.0))
                    continue

                if resp.status in (401, 403):
                    return "taken"
                if resp.status != 200:
                    logger.debug(f"[tme] @{u} HTTP {resp.status}")
                    return "unknown"

                text = await resp.text()
                low = text.lower()

                taken_markers = (
                    'class="tgme_page_title"',
                    "tgme_page_extra",
                    "tgme_page_photo",
                    "tgme_action_button_new",
                    "tgme_page_context_link",
                    "tgme_widget_message",
                )
                if any(m in low for m in taken_markers):
                    return "taken"

                free_by_noindex = 'content="noindex' in low and "tgme_page_title" not in low
                free_by_icon = "tgme_page_icon" in low and "tgme_page_title" not in low
                not_found_text = any(x in low for x in (
                    "this username is not occupied",
                    "username not found",
                    "page not found",
                    "not found",
                ))

                if free_by_noindex or free_by_icon or not_found_text:
                    return "free"

                return "unknown"

        except asyncio.TimeoutError:
            logger.debug(f"[tme] timeout @{u} #{attempt+1}")
            await asyncio.sleep(0.7 + attempt * 0.5)
        except Exception as e:
            logger.debug(f"[tme] error @{u}: {e}")
            await asyncio.sleep(0.5 + attempt * 0.5)

    return "unknown"

    
async def is_username_free(u: str, uid=None) -> bool:
    """Финальный фильтр выдачи username без user-сессий."""
    return await public_username_status(u, uid) == "free"

    
async def get_rechecked_cached_free(mode, count):
    cached = await get_cached_free(mode, max(count * 3, count))
    validate_func = SEARCH_MODES.get(mode, {}).get("validate", is_valid_username)
    found = []
    rejected = []
    for u in cached:
        u = (u or "").strip().replace("@", "").lower()
        if validate_func(u) and await is_username_free(u):
            found.append({"username": u, "fragment": "unavailable"})
            if len(found) >= count:
                break
        else:
            rejected.append(u)
    if rejected:
        logger.info(f"Rejected stale cache for mode={mode}: {len(rejected)}")
    return found

async def check_subscribed(uid):
    if uid in ADMIN_IDS or not REQUIRED_CHANNELS: return []
    bad=[]
    for ch in REQUIRED_CHANNELS:
        try:
            m=await bot.get_chat_member(f"@{ch}",uid)
            if m.status in ("left","kicked"): bad.append(ch)
        except Exception as e:
            logger.warning(f"check_subscribed @{ch} uid={uid}: {e}")
            bad.append(ch)  # при ошибке считаем не подписан
    return bad

def validate_username(u):
    return bool(u) and len(u) in [5, 6] and re.match(r'^[a-zA-Z]{5,6}$', u)

def evaluate_username(u):
    score=0; factors=[]; ul=u.lower(); ln=len(ul)
    if ln!=5: score+=0
    if len(set(ul))==1: score+=90; factors.append("🔥 Моно")
    if ul==ul[::-1]: score+=40; factors.append("🪞 Палиндром")
    if ul.isalpha(): score+=15; factors.append("🔤 Чистые буквы")
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

SEARCH_LIVE_BATCH_SIZE = 80
SEARCH_LIVE_MAX_ATTEMPTS = 5000
SEARCH_LIVE_TIMEOUT = 75
CACHE_WARMER_TARGETS = {
    "default": 300,
    "beautiful": 180,
}
CACHE_WARMER_BATCH_SIZE = 120

async def fast_available_for_search(u: str, uid=None) -> bool:
    """
    Рабочий быстрый фильтр для поиска.
    Он не зависает на Fragment/BotAPI по 10-30 секунд на каждый юз.
    Основной критерий: публичная страница t.me явно показывает, что username не занят.
    BotAPI используется только как быстрый стоп-фильтр: если он сразу видит чат — юз занят.
    """
    u = (u or "").strip().replace("@", "").lower()

    if not is_valid_telegram_profile_username(u):
        return False
    if is_blacklisted(u):
        return False
    for w in INVALID_WORDS:
        if w == u or w in u:
            return False

    try:
        tme = await asyncio.wait_for(check_username_tme(u), timeout=7)
    except Exception:
        return False

    if tme != "free":
        return False

    # Быстрый стоп-фильтр. Если Bot API тормозит или отвечает unknown — не держим поиск.
    try:
        botapi = await asyncio.wait_for(check_username_botapi(u), timeout=2.5)
        if botapi == "taken":
            return False
    except Exception:
        pass

    return True


def make_unique_candidates(gen_func, validate_func, checked, amount):
    candidates = []
    for _ in range(amount * 40):
        if len(candidates) >= amount:
            break
        try:
            c = gen_func()
        except Exception:
            continue
        u = (c or "").strip().replace("@", "").lower()
        if not u or u in checked:
            continue
        if not u.isalpha():
            continue
        if not validate_func(u):
            continue
        checked.add(u)
        candidates.append(u)
    return candidates


async def check_candidates_fast(candidates, uid=None, stop_after=999999):
    """Параллельно проверяет пачку кандидатов и возвращает найденные свободные username."""
    found = []

    async def one(u):
        try:
            ok = await fast_available_for_search(u, uid)
            return u if ok else None
        except Exception:
            return None

    tasks = [asyncio.create_task(one(u)) for u in candidates]
    try:
        for fut in asyncio.as_completed(tasks):
            res = await fut
            if res:
                found.append(res)
                if len(found) >= stop_after:
                    break
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()

    return found


async def do_search(count, gen_func, msg, mode_name, uid, mode_key="default"):
    found = []
    start = time.time()
    last_update = 0
    attempts = 0

    validate_func = SEARCH_MODES.get(mode_key, {}).get("validate", is_valid_username)

    # 1) Сначала отдаём JSON-кэш мгновенно. Никакой повторной долгой проверки тут нет.
    cached = await get_cached_free(mode_key, count * 5)
    for u in cached:
        if len(found) >= count:
            break
        u = (u or "").strip().replace("@", "").lower()
        if not validate_func(u):
            continue
        found.append({"username": u, "fragment": "unavailable"})
        save_history(uid, u, mode_name, len(u))

    if len(found) >= count:
        return found[:count], {"attempts": 0, "elapsed": int(time.time() - start)}

    # 2) Если кэша не хватило — ищем прямо сейчас, параллельно, пачками.
    checked = {item["username"] for item in found}

    if msg:
        try:
            await edit_msg(
                msg,
                f"🔍 <b>{mode_name}</b>\n\n"
                f"✅ Из кэша: <code>{len(found)}/{count}</code>\n"
                f"⚙️ Кэша не хватило — запускаю быстрый поиск пачками.\n"
                f"⏱ <code>0с</code>",
            )
        except Exception:
            pass

    print(
        f"[{SEARCH_CALLBACK_FIX_VERSION}] WORKING-SEARCH uid={uid} mode={mode_key} cache={len(found)} need={count}",
        flush=True,
    )

    while len(found) < count and attempts < SEARCH_LIVE_MAX_ATTEMPTS:
        if time.time() - start > SEARCH_LIVE_TIMEOUT:
            break

        need = count - len(found)
        batch_size = max(SEARCH_LIVE_BATCH_SIZE, need * 25)
        candidates = make_unique_candidates(gen_func, validate_func, checked, batch_size)
        if not candidates:
            attempts += batch_size
            continue

        attempts += len(candidates)

        now = time.time()
        if msg and now - last_update > 1.5:
            last_update = now
            try:
                pct = min(len(found) / max(count, 1), 1.0)
                filled = int(pct * 10)
                bar = "█" * filled + "░" * (10 - filled)
                await edit_msg(
                    msg,
                    f"🔍 <b>{mode_name}</b>\n\n"
                    f"[{bar}] {int(pct * 100)}%\n\n"
                    f"✅ Найдено: <code>{len(found)}/{count}</code>\n"
                    f"📊 Проверено кандидатов: <code>{attempts}</code>\n"
                    f"⏱ <code>{int(now - start)}с</code>",
                )
            except Exception:
                pass

        batch_found = await check_candidates_fast(candidates, uid, stop_after=need)

        for u in batch_found:
            if len(found) >= count:
                break
            if u in {item["username"] for item in found}:
                continue
            found.append({"username": u, "fragment": "unavailable"})
            save_history(uid, u, mode_name, len(u))

        if batch_found:
            print(
                f"[{SEARCH_CALLBACK_FIX_VERSION}] FOUND uid={uid} mode={mode_key} batch={batch_found} total={len(found)}/{count}",
                flush=True,
            )

        await asyncio.sleep(0.05)

    elapsed = int(time.time() - start)
    print(
        f"[{SEARCH_CALLBACK_FIX_VERSION}] WORKING-SEARCH-END uid={uid} mode={mode_key} found={len(found)} attempts={attempts} elapsed={elapsed}s",
        flush=True,
    )

    return found[:count], {"attempts": attempts, "elapsed": elapsed}

# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS free_cache (
        username TEXT PRIMARY KEY, checked_at TEXT, mode TEXT, length INTEGER DEFAULT 5
    )""")
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
        reviewed_by INTEGER DEFAULT 0, photo_count INTEGER DEFAULT 0,
        proof_url TEXT DEFAULT '', sbp TEXT DEFAULT '', amount_rub REAL DEFAULT 0)""")
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
        ("last_reminder","''"),("extra_searches","0"),("monitor_slots","0"),("template_uses","0"),
        ("daily_searches_used","0"),("daily_searches_date",""),
        ("vip_end","")]:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except: pass
    for col, col_type, default in [
        ("proof_url", "TEXT", "''"),
        ("sbp", "TEXT", "''"),
        ("amount_rub", "REAL", "0")
    ]:
        try: c.execute(f"ALTER TABLE tasks ADD COLUMN {col} {col_type} DEFAULT {default}")
        except: pass
    try: c.execute("ALTER TABLE promotions ADD COLUMN button_text TEXT DEFAULT ''")
    except: pass
    # Миграция: добавить длину если нет
    try:
        c.execute("ALTER TABLE free_cache ADD COLUMN length INTEGER DEFAULT 5")
    except:
        pass
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

    c.execute("""CREATE TABLE IF NOT EXISTS favorites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, username TEXT,
        added_at TEXT DEFAULT '',
        UNIQUE(uid, username)
    )""")

    for col, default in [("lang","'ru'"),("last_active","''")]:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col} DEFAULT {default}")
        except: pass

    # ALTER для market (старые БД)
    for col, default in [
        ("promoted","0"),("promoted_until","''"),("is_nft","0"),
        ("fragment_url","''"),("fast_mod","0"),("listing_paid","0"),
        ("charge_id","''")
    ]:
        try: c.execute(f"ALTER TABLE market ADD COLUMN {col} DEFAULT {default}")
        except: pass
    conn.commit(); conn.close()

async def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


async def save_cache(data):
    async with cache_lock:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


async def get_cached_free(mode, count):
    data = await load_cache()

    if mode not in data:
        return []

    usernames = data[mode][:count]
    data[mode] = data[mode][count:]

    await save_cache(data)

    return usernames


async def add_free_cache(usernames, mode):
    if not usernames:
        return

    data = await load_cache()

    if mode not in data:
        data[mode] = []

    existing = set(data[mode])

    for u in usernames:
        u = u.lower()

        if u not in existing:
            data[mode].append(u)
            existing.add(u)

    random.shuffle(data[mode])

    data[mode] = data[mode][:10000]

    await save_cache(data)

async def get_free_cache_count(mode):
    data = await load_cache()
    return len(data.get(mode, []))

def is_blacklisted(username):
    if not username:
        return False
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT 1 FROM blacklist WHERE username=? LIMIT 1", (username.lower(),))
    row = c.fetchone()
    conn.close()
    return bool(row)

def add_blacklist(username, added_by=0):
    if not username:
        return
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO blacklist (username, added_by, created) VALUES (?,?,?)",
        (username.lower(), added_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit(); conn.close()

def remove_blacklist(username):
    if not username:
        return
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM blacklist WHERE username=?", (username.lower(),))
    conn.commit(); conn.close()

def get_blacklist():
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT username, added_by, created FROM blacklist ORDER BY created DESC")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def save_history(uid, username, mode, length=5):
    if not uid or not username:
        return
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute(
        "INSERT INTO history (uid, username, found_at, mode, length) VALUES (?,?,?,?,?)",
        (uid, username.lower(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), mode, length)
    )
    conn.commit(); conn.close()

def is_banned(uid):
    user = get_user(uid)
    if int(user.get("banned", 0) or 0) == 1:
        return True
    return rate_limiter.is_temp_banned(uid)

def ban_user(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def unban_user(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET banned=0 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def find_user(value):
    raw = (value or "").strip().replace("@", "")
    if not raw:
        return 0
    conn = sqlite3.connect(DB); c = conn.cursor()
    uid = 0
    if raw.isdigit():
        c.execute("SELECT uid FROM users WHERE uid=? LIMIT 1", (int(raw),))
        row = c.fetchone()
        uid = row[0] if row else 0
    else:
        c.execute("SELECT uid FROM users WHERE LOWER(uname)=? LIMIT 1", (raw.lower(),))
        row = c.fetchone()
        uid = row[0] if row else 0
    conn.close()
    return uid

def get_monitor_limit(uid):
    if uid in ADMIN_IDS or has_vip(uid):
        return MONITOR_MAX_VIP
    if has_subscription(uid):
        return MONITOR_MAX_PREMIUM
    user = get_user(uid)
    return MONITOR_MAX_FREE + int(user.get("monitor_slots", 0) or 0)

def get_monitor_count(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM monitors WHERE uid=? AND status='active'", (uid,))
    count = c.fetchone()[0]
    conn.close()
    return count

def add_monitor(uid, username):
    now = datetime.now()
    expires = (now + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute(
        "INSERT INTO monitors (uid, username, status, created, expires, last_check, last_status) VALUES (?,?,?,?,?,?,?)",
        (
            uid,
            username.lower(),
            "active",
            now.strftime("%Y-%m-%d %H:%M:%S"),
            expires,
            "",
            "taken",
        ),
    )
    mid = c.lastrowid
    conn.commit(); conn.close()
    return mid

def remove_monitor(mid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM monitors WHERE id=? AND uid=?", (mid, uid))
    conn.commit(); conn.close()

def get_user_monitors(uid):
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute(
        "SELECT id, username, status, created, expires, last_check, last_status FROM monitors WHERE uid=? ORDER BY id DESC",
        (uid,)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def get_active_monitors():
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
    c.execute("SELECT id, uid, username, status, created, expires, last_check, last_status FROM monitors WHERE status='active'")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def update_monitor_status(mid, status):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute(
        "UPDATE monitors SET last_status=?, last_check=? WHERE id=?",
        (status, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), mid)
    )
    conn.commit(); conn.close()

def expire_monitors():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE monitors SET status='expired' WHERE status='active' AND expires<>'' AND expires<?", (now,))
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

def build_sub_kb(missing_channels):
    kb = InlineKeyboardBuilder()
    for ch in missing_channels[:8]:
        kb.button(text=f"📢 @{ch}", url=f"https://t.me/{ch}")
    kb.button(text="✅ Проверить", callback_data="check_sub")
    kb.adjust(1)
    text = (
        "🔒 <b>Для использования бота нужно подписаться на каналы</b>\n\n" +
        "\n".join(f"• @{ch}" for ch in missing_channels)
    )
    return text, kb.as_markup()

# ═══════════════════════ НЕДОСТАЮЩИЕ HELPER-ФУНКЦИИ ═══════════════════════

def estimate_username_stars(username):
    ln = len(username)
    if ln <= 4: base = 80
    elif ln <= 5: base = 60
    elif ln <= 6: base = 45
    elif ln <= 7: base = 30
    elif ln <= 8: base = 20
    else: base = 12
    if username.isalpha(): base += 10
    if "_" not in username: base += 5
    base += random.randint(-5, 8)
    return max(5, base)

def add_monitor_slots(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET monitor_slots=monitor_slots+? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

def add_template_uses(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET template_uses=template_uses+? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

def process_referral(new_uid, ref_uid):
    if new_uid == ref_uid:
        return False
    u = get_user(new_uid)
    if u.get("referred_by", 0) != 0:
        return False

    ensure_user(ref_uid)
    ref_user = get_user(ref_uid)
    old_count = int(ref_user.get("ref_count", 0) or 0)
    new_count = old_count + 1
    premium_days = int(REFERRAL_PREMIUM_BONUSES.get(new_count, 0) or 0)
    uname = get_user(new_uid).get("uname", "")

    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?", (ref_uid, new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1 WHERE uid=?", (ref_uid,))
    c.execute("INSERT INTO referrals (referrer_uid,referred_uid,referred_uname,created) VALUES (?,?,?,?)",
              (ref_uid, new_uid, uname, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit(); conn.close()

    sub_end = ""
    if premium_days > 0:
        sub_end = give_subscription(ref_uid, premium_days)
        log_action(ref_uid, "ref_premium_bonus", f"{new_count} refs +{premium_days}d")
    else:
        log_action(ref_uid, "ref_join", f"{new_count} refs from {new_uid}")

    return {"ref_count": new_count, "premium_days": premium_days, "sub_end": sub_end}

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

def get_history(uid, limit=20):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username,found_at,mode FROM history WHERE uid=? ORDER BY id DESC LIMIT ?", (uid,limit))
    rows = c.fetchall(); conn.close(); return rows

def delete_history_pattern(uid, pattern):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM history WHERE uid=? AND username LIKE ?", (uid, f"%{pattern}%"))
    deleted = c.rowcount
    conn.commit(); conn.close()
    return deleted

def set_free_searches(uid, count):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET free=? WHERE uid=?", (count, uid)); conn.commit(); conn.close()

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

def get_referrals_until_next_bonus(ref_count):
    ref_count = int(ref_count or 0)
    for threshold in sorted(REFERRAL_PREMIUM_BONUSES):
        if ref_count < threshold:
            return threshold - ref_count
    return 0

def get_tiktok_approved_count(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM tasks WHERE uid=? AND status='approved'", (uid,))
        count = c.fetchone()[0]
    except Exception:
        count = 0
    conn.close()
    return int(count or 0)

def add_favorite(uid, username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("INSERT INTO favorites (uid, username, added_at) VALUES (?,?,?)",
                  (uid, username, datetime.now().strftime("%Y-%m-%d %H:%M")))
        conn.commit()
    except sqlite3.IntegrityError: pass
    conn.close()

def get_favorites(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT username, added_at FROM favorites WHERE uid=? ORDER BY added_at DESC", (uid,))
    rows = c.fetchall(); conn.close()
    return [{"username":r[0], "added_at":r[1]} for r in rows]

def update_last_active(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET last_active=? WHERE uid=?", (datetime.now().strftime("%Y-%m-%d %H:%M"), uid))
    conn.commit(); conn.close()

def remove_favorite(uid, username):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("DELETE FROM favorites WHERE uid=? AND username=?", (uid, username))
    conn.commit(); conn.close()

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

def task_set_proof(tid, proof_url):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE tasks SET proof_url=? WHERE id=?", (proof_url, tid))
    conn.commit(); conn.close()

def task_set_sbp(tid, sbp):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE tasks SET sbp=? WHERE id=?", (sbp, tid))
    conn.commit(); conn.close()

def task_set_amount(tid, amount_rub):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE tasks SET amount_rub=? WHERE id=?", (float(amount_rub), tid))
    conn.commit(); conn.close()

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
    c.execute("SELECT id,uid,created,photo_count,proof_url,sbp,amount_rub FROM tasks WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"uid":r[1],"created":r[2],"photos":r[3],"proof_url":r[4] or "","sbp":r[5] or "","amount_rub":r[6] or 0} for r in rows]

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
        "today_purchases": c.execute("SELECT COUNT(*) FROM users WHERE sub_end >= ? AND sub_end LIKE ?", (now_s, today+"%")).fetchone()[0],
        "cache_default": c.execute("SELECT COUNT(*) FROM free_cache WHERE mode='default'").fetchone()[0],
        "cache_beautiful": c.execute("SELECT COUNT(*) FROM free_cache WHERE mode='beautiful'").fetchone()[0],
        "total_found": c.execute("SELECT COUNT(*) FROM history").fetchone()[0],
    }
    conn.close(); return r

# ═══════════════════════ МАРКЕТПЛЕЙС ФУНКЦИИ ═══════════════════════

def market_create_lot(seller_uid, mtype, title, description, price, is_nft=0, fragment_url=""):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
    INSERT INTO market (
        seller_uid, mtype, title, description, price,
        status, created, listing_paid, is_nft, fragment_url
    )
    VALUES (?,?,?,?,?, 'pending', ?, 1, ?, ?)
    """, (
        seller_uid, mtype, title, description, price,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        is_nft, fragment_url
    ))
    lot_id = c.lastrowid
    conn.commit()
    conn.close()
    return lot_id

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
    try:
        c.execute("SELECT extra_slots FROM market_slots WHERE uid=?", (uid,))
        row = c.fetchone(); extra = row[0] if row else 0
    except: extra = 0
    conn.close()
    return base + extra

def market_add_slot(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("CREATE TABLE IF NOT EXISTS market_slots (uid INTEGER PRIMARY KEY, extra_slots INTEGER DEFAULT 0)")
        c.execute("INSERT OR REPLACE INTO market_slots (uid, extra_slots) VALUES (?, COALESCE((SELECT extra_slots FROM market_slots WHERE uid=?),0)+1)", (uid, uid))
        conn.commit()
    except: pass
    conn.close()

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
    try:
        c.execute("SELECT created FROM lootbox_history WHERE uid=? ORDER BY id DESC LIMIT 1", (uid,))
        row = c.fetchone(); conn.close()
        if not row: return True
        try:
            last = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
            return (datetime.now() - last).total_seconds() >= LOOTBOX_COOLDOWN
        except: return True
    except: conn.close(); return True

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
    try:
        c.execute("CREATE TABLE IF NOT EXISTS lootbox_history (id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, prize TEXT, prize_type TEXT, created TEXT)")
        c.execute("INSERT INTO lootbox_history (uid,prize,prize_type,created) VALUES (?,?,?,?)",
                  (uid, prize, prize_type, datetime.now().strftime("%Y-%m-%d %H:%M")))
        conn.commit()
    except: pass
    conn.close()
    return prize, prize_type

# ═══ КОЛЕСО ФОРТУНЫ ═══

def wheel_free_spins_today(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM wheel_spins WHERE uid=? AND created LIKE ?", (uid, today+"%"))
        cnt = c.fetchone()[0]; conn.close()
    except: conn.close(); cnt = 0
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
            try:
                c.execute("CREATE TABLE IF NOT EXISTS wheel_spins (id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, prize TEXT, created TEXT)")
                c.execute("INSERT INTO wheel_spins (uid,prize,created) VALUES (?,?,?)",
                          (uid, text, datetime.now().strftime("%Y-%m-%d %H:%M")))
                conn.commit()
            except: pass
            conn.close()
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
    try:
        c.execute("SELECT AVG(rating),COUNT(*) FROM reviews WHERE to_uid=?", (uid,))
        row = c.fetchone(); conn.close()
        return {"avg":round(row[0],1) if row[0] else 0, "count":row[1] or 0}
    except: conn.close(); return {"avg":0,"count":0}

def get_user_reviews(uid, limit=10):
    conn = sqlite3.connect(DB); c = conn.cursor()
    try:
        c.execute("SELECT from_uid,rating,text,created FROM reviews WHERE to_uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
        rows = c.fetchall(); conn.close()
        return [{"from":r[0],"rating":r[1],"text":r[2],"created":r[3]} for r in rows]
    except: conn.close(); return []

# ═══ ОБМЕННИК ═══

def exchange_create(uid, offer):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO exchanges (initiator_uid,initiator_offer,status,created) VALUES (?,?,'open',?)",
              (uid, offer, datetime.now().strftime("%Y-%m-%d %H:%M")))
    eid = c.lastrowid; conn.commit(); conn.close(); return eid

def exchange_accept(eid, partner_uid, partner_offer):
    conn = sqlite3.connect(DB); c = conn.cursor()
    deadline = (datetime.now() + timedelta(hours=MARKET_ESCROW_HOURS)).strftime("%Y-%m-%d %H:%M")
    c.execute("""UPDATE exchanges SET partner_uid=?, partner_offer=?, status='escrow', escrow_deadline=?
                 WHERE id=? AND status='open' AND initiator_uid!=?""",
             (partner_uid, partner_offer, deadline, eid, partner_uid))
    changed = c.rowcount; conn.commit()
    if changed > 0:
        c.execute("SELECT initiator_uid, initiator_offer FROM exchanges WHERE id=?", (eid,))
        row = c.fetchone(); conn.close()
        if row:
            return True, {"initiator_uid": row[0], "initiator_offer": row[1], "partner_offer": partner_offer}
    conn.close()
    return False, None

def exchange_confirm(eid, uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("""SELECT initiator_uid, partner_uid, status, initiator_confirmed, partner_confirmed,
                 initiator_offer, partner_offer FROM exchanges WHERE id=?""", (eid,))
    row = c.fetchone()
    if not row: conn.close(); return False, None
    init_uid, part_uid, status, init_conf, part_conf, init_offer, part_offer = row
    if status not in ('escrow', 'open'): conn.close(); return False, None
    if uid == init_uid:
        c.execute("UPDATE exchanges SET initiator_confirmed=1 WHERE id=?", (eid,))
    elif uid == part_uid:
        c.execute("UPDATE exchanges SET partner_confirmed=1 WHERE id=?", (eid,))
    else: conn.close(); return False, None
    conn.commit()
    c.execute("""SELECT initiator_confirmed, partner_confirmed, initiator_uid, partner_uid,
                 initiator_offer, partner_offer FROM exchanges WHERE id=?""", (eid,))
    r = c.fetchone()
    if r and r[0] and r[1]:
        c.execute("UPDATE exchanges SET status='completed', completed_at=? WHERE id=?",
                 (datetime.now().strftime("%Y-%m-%d %H:%M"), eid))
        conn.commit(); conn.close()
        return True, {"initiator_uid": r[2], "partner_uid": r[3], "initiator_offer": r[4], "partner_offer": r[5]}
    conn.close()
    return False, None

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
            sm = re.search(r'class="tm-section-header-status[^"]*"[^>]*>([^<]+)<', text)
            if sm:
                st = sm.group(1).strip().lower()
                if "sold" in st: return {"is_nft": True, "status": "sold"}
                elif "available" in st or "auction" in st: return {"is_nft": True, "status": "on_sale"}
            return {"is_nft": False, "status": "not_found"}
    except:
        return {"is_nft": False, "status": "error"}

async def verify_nft_owner(uid, username):
    # Без user-сессий проверяем только username самого пользователя, доступный Bot API.
    try:
        chat = await bot.get_chat(uid)
        if chat.username and chat.username.lower() == username.lower():
            return True
    except:
        pass
    return False

def _d(uid_val, uname_val):
    return "@"+uname_val if uname_val else "ID:"+str(uid_val)

async def notify_admins(text, exclude=None, kb=None):
    for aid in ADMIN_IDS:
        if exclude and aid==exclude: continue
        try: await bot.send_message(aid, text, reply_markup=kb, parse_mode="HTML")
        except: pass

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

def build_menu(uid):
    ensure_user(uid)
    u = get_user(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Поиск", callback_data="cmd_search")
    kb.button(text="👤 Профиль", callback_data="cmd_profile")
    kb.button(text="🏪 Магазин", callback_data="cmd_shop")
    if is_button_enabled("referral"):
        kb.button(text="👥 Рефералы", callback_data="cmd_referral")
    kb.button(text="📋 Документы бота", callback_data="cmd_documents")
    if is_button_enabled("support"):
        kb.button(text="💬 Поддержка", url=f"https://t.me/{ADMIN_CONTACT}")
    if uid in ADMIN_IDS:
        kb.button(text="👑 Админ", callback_data="cmd_admin")
    kb.adjust(2)

    if uid in ADMIN_IDS:
        status = "👑 Админ"
    elif has_vip(uid):
        status = "🌟 VIP"
    elif has_subscription(uid):
        status = "💎 Premium"
    else:
        status = "🆓 Free"

    text = (
        "👋 <b>Главное меню</b>\n\n"
        f"🪪 Статус: <b>{status}</b>\n"
        f"🔍 За поиск: <code>{get_search_count(uid)}</code> юза\n"
        f"📆 Лимит в день: <code>{get_max_searches(uid)}</code>\n"
        f"💰 Баланс: <code>{get_balance(uid):.1f}⭐</code>"
    )
    return text, kb.as_markup()

def get_balance(uid):
    return float(get_user(uid).get("balance", 0.0) or 0.0)

def set_balance(uid, amount):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET balance=? WHERE uid=?", (float(amount), uid))
    conn.commit(); conn.close()

def add_balance(uid, amount):
    set_balance(uid, get_balance(uid) + float(amount))

def has_subscription(uid):
    if uid in ADMIN_IDS:
        return True
    sub_end = (get_user(uid).get("sub_end", "") or "").strip()
    if not sub_end:
        return False
    try:
        return datetime.strptime(sub_end, "%Y-%m-%d %H:%M:%S") > datetime.now()
    except:
        try:
            return datetime.strptime(sub_end, "%Y-%m-%d %H:%M") > datetime.now()
        except:
            return False

def has_vip(uid):
    if uid in ADMIN_IDS:
        return True
    vip_end = (get_user(uid).get("vip_end", "") or "").strip()
    if not vip_end:
        return False
    try:
        return datetime.strptime(vip_end, "%Y-%m-%d %H:%M:%S") > datetime.now()
    except:
        try:
            return datetime.strptime(vip_end, "%Y-%m-%d %H:%M") > datetime.now()
        except:
            return False

def give_subscription(uid, days):
    user = get_user(uid)
    now = datetime.now()
    current = now
    sub_end = (user.get("sub_end", "") or "").strip()
    if sub_end:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(sub_end, fmt)
                if parsed > now:
                    current = parsed
                break
            except:
                pass
    new_end = current + timedelta(days=int(days))
    end_str = new_end.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET sub_end=? WHERE uid=?", (end_str, uid))
    conn.commit(); conn.close()
    return end_str

def remove_subscription(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET sub_end='' WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def give_vip(uid, days):
    user = get_user(uid)
    now = datetime.now()
    current = now
    vip_end = (user.get("vip_end", "") or "").strip()
    if vip_end:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(vip_end, fmt)
                if parsed > now:
                    current = parsed
                break
            except:
                pass
    new_end = current + timedelta(days=int(days))
    end_str = new_end.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET vip_end=? WHERE uid=?", (end_str, uid))
    conn.commit(); conn.close()
    return end_str

def remove_vip(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET vip_end='' WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def get_max_searches(uid):
    if uid in ADMIN_IDS:
        return 999999
    user = get_user(uid)
    if has_vip(uid):
        return VIP_SEARCHES_LIMIT
    if has_subscription(uid):
        return PREMIUM_SEARCHES_LIMIT
    return FREE_SEARCHES_LIMIT + int(user.get("extra_searches", 0) or 0)

def get_search_count(uid):
    if uid in ADMIN_IDS:
        return VIP_COUNT
    if has_vip(uid):
        return VIP_COUNT
    if has_subscription(uid):
        return PREMIUM_COUNT
    return FREE_COUNT

def can_search(uid):
    if uid in ADMIN_IDS:
        return True
    _reset_daily_if_needed(uid)
    user = get_user(uid)
    used = int(user.get("daily_searches_used", 0) or 0)
    return used < get_max_searches(uid)

def _reset_daily_if_needed(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    user = get_user(uid)
    if (user.get("daily_searches_date", "") or "") != today:
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute(
            "UPDATE users SET daily_searches_used=0, daily_searches_date=? WHERE uid=?",
            (today, uid)
        )
        conn.commit(); conn.close()

def use_search(uid):
    _reset_daily_if_needed(uid)
    conn = sqlite3.connect(DB); c = conn.cursor()
    if uid in ADMIN_IDS:
        c.execute("UPDATE users SET searches=searches+1 WHERE uid=?", (uid,))
    elif has_subscription(uid) or has_vip(uid):
        c.execute("UPDATE users SET searches=searches+1, daily_searches_used=daily_searches_used+1 WHERE uid=?", (uid,))
    else:
        u = get_user(uid)
        if u.get("extra_searches", 0) > 0:
            c.execute("UPDATE users SET extra_searches=MAX(extra_searches-1,0), searches=searches+1, daily_searches_used=daily_searches_used+1 WHERE uid=?", (uid,))
        else:
            c.execute("UPDATE users SET free=MAX(free-1,0), searches=searches+1, daily_searches_used=daily_searches_used+1 WHERE uid=?", (uid,))
    conn.commit(); conn.close()

def add_extra_searches(uid, count):
    user = get_user(uid)
    new_value = int(user.get("extra_searches", 0) or 0) + int(count)
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("UPDATE users SET extra_searches=? WHERE uid=?", (new_value, uid))
    conn.commit(); conn.close()

def get_auto_renew(uid):
    user = get_user(uid)
    return bool(int(user.get("auto_renew", 0) or 0)), user.get("auto_renew_plan", "") or ""

def can_roulette(uid):
    last = (get_user(uid).get("last_roulette", "") or "").strip()
    if not last:
        return True
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(last, fmt)
            return datetime.now() - dt >= timedelta(days=7)
        except:
            pass
    return True

def set_last_roulette(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute(
        "UPDATE users SET last_roulette=? WHERE uid=?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), uid)
    )
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
                 ("last_roulette",""),("extra_searches",0),("monitor_slots",0),("template_uses",0),
                 ("daily_searches_used",0),("daily_searches_date",""),
                 ("vip_end","")]:
        d.setdefault(k, v)
    return d

async def redirect_payment(uid, title, stars, rub=None, back_cb="cmd_shop"):
    if rub is None:
        rub = int(stars * STAR_TO_RUB)
    kb = InlineKeyboardBuilder()
    kb.button(text=f"💳 Написать @{PAY_CONTACT}", url=f"https://t.me/{PAY_CONTACT}")
    kb.button(text="🔙 Назад", callback_data=back_cb)
    kb.adjust(1)
    await bot.send_message(uid,
        f"💰 <b>{title}</b>\n\n"
        f"💵 Цена: <code>{stars}⭐</code> / <code>{rub}₽</code>\n\n"
        f"📩 Для оплаты напишите @{PAY_CONTACT}\n"
        f"После оплаты админ активирует покупку ✅",
        reply_markup=kb.as_markup(), parse_mode="HTML")

# ═══════════════════════ КОМАНДЫ ═══════════════════════

async def register_handlers(dp: Dispatcher):
    """Регистрация всех хендлеров"""
    
    @dp.message(Command("start"))
    async def cmd_start(msg: Message, command: CommandObject):
        uid = msg.from_user.id; uname = msg.from_user.username or ""
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("SELECT uid FROM users WHERE uid=?", (uid,))
        is_new = c.fetchone() is None; conn.close()
        ensure_user(uid, uname); log_action(uid, "start", command.args or "")
        update_last_active(uid)
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
        else: t,k = build_menu(uid); t = with_main_branding(t)
        await send_menu_photo(uid, t, k)
    
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

    @dp.message(Command("fav"))
    async def cmd_fav(msg: Message, command: CommandObject):
        uid = msg.from_user.id
        if is_banned(uid): return
        un = (command.args or "").strip().replace("@","").lower()
        if not un or len(un)<3:
            await msg.answer("❌ <code>/fav username</code>", parse_mode="HTML"); return
        add_favorite(uid, un)
        await msg.answer(f"⭐ <code>@{un}</code> в избранном", parse_mode="HTML")

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

    @dp.message(Command("set_cache"))
    async def set_cache(msg: Message, command: CommandObject):
        if msg.from_user.id not in ADMIN_IDS:
            return

        try:
            value = int(command.args)
        except:
            await msg.answer("Используй: /set_cache 1000")
            return

        config = load_bot_config()
        config["cache_limit"] = value
        save_bot_config(config)

        await msg.answer(f"✅ Лимит кэша: {value}")
    
    # ═══ CALLBACKS: Базовые ═══
    
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
                try:
                    if isinstance(ok, dict):
                        bonus_days = int(ok.get("premium_days", 0) or 0)
                        ref_count = int(ok.get("ref_count", 0) or 0)
                        text = f"🎉 Новый реферал!\n👥 Всего приглашено: <code>{ref_count}</code>"
                        if bonus_days > 0:
                            text += f"\n💎 Бонус: <b>+{bonus_days} дн. Premium</b>\nДо: <code>{ok.get('sub_end','')}</code>"
                    else:
                        text = "🎉 Новый реферал!"
                    await bot.send_message(ref_uid, text, parse_mode="HTML")
                except: pass
        else: set_captcha_passed(uid)
        t,k = build_menu(uid); t = with_main_branding(t); await edit_to_photo(cb.message, t, k)

    @dp.callback_query(F.data == "setlang_ru")
    async def cb_setlang_ru(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET lang='ru' WHERE uid=?", (uid,))
        conn.commit(); conn.close()
        fname = cb.from_user.first_name or "друг"
        welcome = (
            f"👋 <b>Привет, {fname}!</b>\n\n"
            f"С помощью нашего бота ты можешь искать красивые, "
            f"а самое главное свободные 5-буквенные юзернеймы "
            f"для себя или продажи ⚡\n\n"
            f"🔍 <b>Поиск</b> — найди свободный юзернейм\n"
            f"🎁 У тебя <b>{FREE_SEARCHES}</b> бесплатных поиска!")
        try:
            photo = FSInputFile(MENU_IMAGE)
            await bot.send_photo(uid, photo=photo, caption=welcome, parse_mode="HTML")
        except:
            await cb.message.answer(welcome, parse_mode="HTML")
        ns = await check_subscribed(uid)
        if ns: t,k = build_sub_kb(ns)
        else: t,k = build_menu(uid); t = with_main_branding(t)
        await send_menu_photo(uid, t, k)

    @dp.callback_query(F.data == "setlang_en")
    async def cb_setlang_en(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        conn = sqlite3.connect(DB); c = conn.cursor()
        c.execute("UPDATE users SET lang='en' WHERE uid=?", (uid,))
        conn.commit(); conn.close()
        fname = cb.from_user.first_name or "friend"
        welcome = (
            f"👋 <b>Hello, {fname}!</b>\n\n"
            f"With our bot you can search for beautiful, "
            f"and most importantly free 5-letter usernames "
            f"for yourself or for sale ⚡\n\n"
            f"🔍 <b>Search</b> — find free username\n"
            f"🎁 You have <b>{FREE_SEARCHES}</b> free searches!")
        try:
            photo = FSInputFile(MENU_IMAGE)
            await bot.send_photo(uid, photo=photo, caption=welcome, parse_mode="HTML")
        except:
            await cb.message.answer(welcome, parse_mode="HTML")
        ns = await check_subscribed(uid)
        if ns: t,k = build_sub_kb(ns)
        else: t,k = build_menu(uid); t = with_main_branding(t)
        await send_menu_photo(uid, t, k)

    @dp.callback_query(F.data == "check_sub")
    async def cb_cs(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        ns = await check_subscribed(uid)
        if ns: t,k = build_sub_kb(ns)
        else: t,k = build_menu(uid); t = with_main_branding(t)
        await edit_to_photo(cb.message, t, k); return

    @dp.callback_query(F.data == "cmd_menu")
    async def cb_menu(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        if is_banned(uid): return
        user_states.pop(uid, None)
        ns = await check_subscribed(uid)
        if ns: t,k = build_sub_kb(ns)
        else: t,k = build_menu(uid); t = with_main_branding(t)
        await edit_to_photo(cb.message, t, k)

    @dp.callback_query(F.data == "cmd_documents")
    async def cb_documents(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        if is_banned(uid): return
        kb = InlineKeyboardBuilder()
        kb.button(
            text="🔐 Политика конфиденциальности",
            url="https://telegra.ph/Politika-konfidencialnosti-04-01-26"
        )
        kb.button(
            text="📄 Пользовательское соглашение",
            url="https://telegra.ph/Polzovatelskoe-soglashenie-04-01-19"
        )
        kb.button(text="🔙 Главное меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(
            cb.message,
            "📋 <b>Документы бота</b>\n\nВыберите нужный документ:",
            kb.as_markup()
        )

    
    # ═══ CALLBACKS: Поиск ═══
    
    @dp.callback_query(_is_cmd_search_callback)
    async def cb_search(cb: CallbackQuery):
        _print_callback_event("CALLBACK-CMD-SEARCH", cb)
        uid = cb.from_user.id; await answer_cb(cb)
        if is_banned(uid): return
        config = load_bot_config()
        ok,reason = rate_limiter.check_search(uid)
        if not ok:
            await edit_msg(cb.message, f"🚫 Блокировка {TEMP_BAN_MINUTES} мин." if reason=="ban" else "⚠️ Подождите.")
            return
        if not can_search(uid):
            kb = InlineKeyboardBuilder()
            kb.button(text="🏪 Магазин", callback_data="cmd_shop")
            kb.button(text="👥 Рефералы", callback_data="cmd_referral")
            kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
            await edit_msg(cb.message, "⛔️ <b>Поиски закончились!</b>", kb.as_markup())
            return
        is_prem = uid in ADMIN_IDS or has_subscription(uid)
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

        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(2)
        await edit_msg(cb.message,
            f"🔍 <b>Режим:</b>\n\n{mt}\n🎯 <code>{cnt}</code> юзов | Осталось: <b>{fl}</b>",
            kb.as_markup())

    @dp.callback_query(F.data == "need_prem")
    async def cb_np(cb: CallbackQuery): 
        await answer_cb(cb, "🔒 Нужен Premium!", show_alert=True)
    
    @dp.callback_query(F.data == "need_vip")
    async def cb_nv(cb: CallbackQuery): 
        await answer_cb(cb, "🌟 Нужен VIP! Купи в магазине.", show_alert=True)

    @dp.callback_query(_is_search_mode_callback)
    async def cb_go(cb: CallbackQuery):
        _print_callback_event("CALLBACK-SEARCH-MODE", cb)
        uid = cb.from_user.id
        await answer_cb(cb)
        if is_banned(uid):
            return

        ok,reason = rate_limiter.check_search(uid)
        if not ok:
            await edit_msg(cb.message, "⚠️ Подождите.")
            return

        if not can_search(uid):
            kb = InlineKeyboardBuilder()
            kb.button(text="🏪", callback_data="cmd_shop")
            kb.button(text="🔙", callback_data="cmd_menu")
            kb.adjust(1)
            await edit_msg(cb.message, "⛔️ <b>Закончились!</b>", kb.as_markup())
            return

        mode = normalize_search_mode_callback(cb.data)
        mi = SEARCH_MODES.get(mode)
        if not mi or mi.get("disabled"):
            return

        is_prem = uid in ADMIN_IDS or has_subscription(uid)
        if mi["premium"] and not is_prem:
            return

        if uid not in ADMIN_IDS:
            if uid in searching_users:
                try:
                    await bot.send_message(uid, "⏳ Уже идёт поиск!")
                except:
                    pass
                return

        cd = user_search_cooldown.get(uid,0)
        rem = SEARCH_COOLDOWN-(time.time()-cd)
        if rem>0:
            try:
                await bot.send_message(uid, f"⏳ {int(rem)} сек.")
            except:
                pass
            return

        searching_users.add(uid)
        if uid not in ADMIN_IDS:
            user_search_cooldown[uid]=time.time()
        try:
            count = get_search_count(uid)
            print(f"[{SEARCH_CALLBACK_FIX_VERSION}] SEARCH-START uid={uid} mode={mode} count={count}", flush=True)
            found, stats = await do_search(count, mi["func"], cb.message, mi["name"], uid, mode)
            print(f"[{SEARCH_CALLBACK_FIX_VERSION}] SEARCH-FINISH uid={uid} mode={mode} found={len(found)} attempts={stats.get('attempts')}", flush=True)
            text = format_results(found, stats, mi["name"])
            kb = InlineKeyboardBuilder()
            kb.button(text="🔙 Меню", callback_data="cmd_menu")
            kb.button(text="🔄 Повторить", callback_data=f"go_{mode}")
            kb.adjust(2)
            try: await cb.message.delete()
            except: pass
            await send_menu_photo(uid, text, kb.as_markup())
        except Exception as e:
            logger.error(f"Search error {uid}: {e}")
            try:
                await edit_msg(cb.message, "❌ Ошибка поиска!")
            except:
                pass
        finally:
            searching_users.discard(uid)
        use_search(uid)
        log_action(uid,"search",mode)

    # ═══ CALLBACKS: Оценка ═══
    
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
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(1)
        text = (f"📊 <b>@{un}</b>\n\n📱 {tgs}\n💎 {frs}\n\n"
                f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
                f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}\n\n"
                f"📱 <a href='https://t.me/{un}'>Telegram</a> · "
                f"💎 <a href='https://fragment.com/username/{un}'>Fragment</a>")
        await edit_msg(cb.message, text, kb.as_markup())

    # ═══ МАРКЕТПЛЕЙС ОТКЛЮЧЁН ═══

    @dp.callback_query(F.data == "cmd_market")
    async def cb_market_disabled(cb: CallbackQuery):
        await answer_cb(cb)
        t, k = build_menu(cb.from_user.id)
        await edit_to_photo(cb.message, t, k)

    @dp.callback_query(F.data.startswith("market_"))
    async def cb_market_callbacks_disabled(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)


    # ═══════════════════════ CALLBACKS: Магазин ═══════════════════════

    @dp.callback_query(F.data == "cmd_shop")
    async def cb_shop(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        bal = get_balance(uid)

        text = (f"🏪 <b>Магазин</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 Баланс: <code>{bal:.1f}⭐</code>\n\n"
                f"Выберите раздел:\n"
                f"• 💎 Premium\n"
                f"• 🌟 VIP\n"
                f"• 📦 Бандл Premium+VIP")

        kb = InlineKeyboardBuilder()
        kb.button(text="💎 Premium", callback_data="shop_premium")
        kb.button(text="🌟 VIP", callback_data="shop_vip")
        kb.button(text="📦 Бандл Premium+VIP", callback_data="shop_bundle")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    @dp.callback_query(F.data == "shop_buy_searches")
    async def cb_shop_buy(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_shop(cb)

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

    @dp.callback_query(F.data == "shop_promo")
    async def cb_shop_promo(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_shop(cb)

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
        count = int(cb.data[18:]); total = count * SEARCH_PRICE_STARS
        await redirect_payment(uid, f"🔍 +{count} поисков", total, None, "cmd_shop")

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
        rub = int(p['stars'] * STAR_TO_RUB)
        await redirect_payment(uid, f"💎 Premium {p['label']}", p["stars"], rub, "shop_premium")

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
        rub = int(vp['stars'] * STAR_TO_RUB)
        await redirect_payment(uid, f"🌟 VIP {vp['label']}", vp["stars"], rub, "shop_vip")

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
        rub = int(bp['stars'] * STAR_TO_RUB)
        await redirect_payment(uid, f"📦 Бандл {bp['label']}", bp["stars"], rub, "shop_bundle")


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
        bal = u.get("balance",0.0); uname = u.get("uname","")
        custom_header = config.get("text_profile_header","")

        text = f"👤 <b>Профиль</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if custom_header: text += f"{custom_header}\n\n"
        text += (f"🆔 <code>{uid}</code>" + (f" | @{uname}" if uname else "") + "\n"
                 f"📌 {status}\n"
                 f"🎯 {cnt} юзов/поиск | 🔄 {mx} осталось\n"
                 f"📊 Всего: <code>{u.get('searches',0)}</code>\n"
                 f"👥 Рефералов: <code>{u.get('ref_count',0)}</code>\n"
                 f"💰 Баланс: <code>{bal:.1f}</code> ⭐ (<code>{bal*STAR_TO_RUB:.0f}₽</code>)")

        kb = InlineKeyboardBuilder()
        kb.button(text="🔑 Ключ", callback_data="cmd_activate")
        if bal>=MIN_WITHDRAW: kb.button(text=f"💸 Вывести ({bal:.1f}⭐)", callback_data="cmd_withdraw")
        kb.button(text="🔙 Меню", callback_data="cmd_menu"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    @dp.callback_query(F.data == "cmd_favorites")
    async def cb_favorites(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_profile(cb)

    @dp.callback_query(F.data.startswith("remfav_"))
    async def cb_rem_fav(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)

    @dp.callback_query(F.data.startswith("addfav_"))
    async def cb_add_fav(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)

    @dp.callback_query(F.data == "toggle_renew")
    async def cb_tr(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_profile(cb)

    @dp.callback_query(F.data.startswith("sr_"))
    async def cb_sr(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)

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


    # ═══════════════════════ CALLBACKS: Рулетка отключена ═══════════════════════

    @dp.callback_query(F.data == "cmd_roulette")
    async def cb_roulette(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_profile(cb)

    @dp.callback_query(F.data == "roulette_spin")
    async def cb_rspin(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)

    # ═══════════════════════ CALLBACKS: Рефералы / Подарить / Донат ═══════════════════════

    @dp.callback_query(F.data == "cmd_referral")
    async def cb_ref(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        u = get_user(uid)
        invited = int(u.get("ref_count", 0) or 0)
        approved = get_tiktok_approved_count(uid)
        kb = InlineKeyboardBuilder()
        kb.button(text="🔗 Реферальная программа", callback_data="ref_link_program")
        kb.button(text="🎵 TikTok-программа", callback_data="ref_tiktok")
        kb.button(text="Назад", callback_data="cmd_menu")
        kb.adjust(1)
        await edit_msg(cb.message,
            "Выберите раздел:\n\n"
            "🔗 <b>Реферальная программа</b>\n"
            "Приглашайте друзей и получайте бесплатные дни Premium\n"
            f"Приглашено друзей: <code>{invited}</code>\n\n"
            "🎵 <b>TikTok-программа</b>\n"
            "Снимайте видео с нашим ботом и получайте 50 ₽ за каждые 1000 просмотров\n"
            f"Одобрено видео: <code>{approved}</code>",
            kb.as_markup())

    @dp.callback_query(F.data == "ref_link_program")
    async def cb_ref_link_program(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        u = get_user(uid)
        invited = int(u.get("ref_count", 0) or 0)
        to_next = get_referrals_until_next_bonus(invited)
        bu = bot_info.username if bot_info else "bot"
        link = f"https://t.me/{bu}?start=ref_{uid}"
        kb = InlineKeyboardBuilder()
        kb.button(text="📤 Поделиться", url=f"https://t.me/share/url?url={link}&text=🔍 Найди юзернейм!")
        kb.button(text="Назад", callback_data="cmd_referral")
        kb.adjust(1)
        await edit_msg(cb.message,
            "🔗 <b>Рефералы по ссылке</b>\n\n\n"
            f"🔘 Приглашено: <code>{invited}</code>\n"
            f"📈 До следующего бонуса: <code>{to_next}</code> рефералов\n\n"
            "Бонусы:\n"
            "• 5 друзей → +1 день Premium\n"
            "• 10 друзей → +3 дня\n"
            "• 15 друзей → +8 дней\n"
            "• 30 друзей → +14 дней\n\n"
            f"Ваша ссылка:\n<code>{link}</code>",
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
        kb = InlineKeyboardBuilder(); kb.button(text="Назад", callback_data="ref_link_program"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    @dp.callback_query(F.data == "ref_top")
    async def cb_ref_top(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        top = get_ref_top(15)
        text = "🏆 <b>Топ рефералов</b>\n\n"
        medals = {1:"🥇",2:"🥈",3:"🥉"}
        for i, t in enumerate(top, 1):
            m = medals.get(i, f"<b>{i}.</b>")
            name = f"@{t['uname']}" if t["uname"] else f"ID:{t['uid']}"
            you = " ← <b>ты</b>" if t["uid"] == uid else ""
            text += f"{m} {name} — <code>{t['ref_count']}</code> 👥{you}\n"
        if not top: text += "<i>Пока пусто</i>"
        place, my = get_my_ref_place(uid)
        text += f"\n\n📍 Ты: <b>#{place}</b> | 👥 <code>{my}</code>"
        kb = InlineKeyboardBuilder(); kb.button(text="Назад", callback_data="ref_link_program"); kb.adjust(1)
        await edit_msg(cb.message, text, kb.as_markup())

    @dp.callback_query(F.data == "gift_prem")
    async def cb_gift(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)
        await cb_profile(cb)

    @dp.callback_query(F.data.startswith("gp_"))
    async def cb_gp(cb: CallbackQuery):
        await answer_cb(cb, "Раздел отключён", show_alert=True)

    @dp.callback_query(F.data == "cmd_support")
    async def cb_support(cb: CallbackQuery):
        await answer_cb(cb); kb = InlineKeyboardBuilder()
        for amt in DONATE_OPTIONS: kb.button(text=f"⭐ {amt}", callback_data=f"don_{amt}")
        kb.button(text="🔙", callback_data="cmd_menu"); kb.adjust(3,3,1,1)
        await edit_msg(cb.message, "🤖 <b>Поддержать</b>", kb.as_markup())

    @dp.callback_query(F.data.startswith("don_"))
    async def cb_don(cb: CallbackQuery):
        amt = int(cb.data[4:]); await answer_cb(cb)
        await redirect_payment(cb.from_user.id, f"🤖 Донат {amt}⭐", amt, None, "cmd_support")

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


    # ═══════════════════════ CALLBACKS: TikTok / Рефералы ═══════════════════════

    def tiktok_referral_text(uid):
        approved = get_tiktok_approved_count(uid)
        return (
            "🎵 <b>TikTok рефералы</b>\n\n"
            "📱 <b>Как заработать:</b>\n"
            "1. Сними видео с нашим ботом (покажи поиск ников)\n"
            "2. Выложи в TikTok\n"
            "3. Нажми «Отправить на проверку» — пришли ссылку\n"
            "4. После одобрения введи реквизиты СБП для выплаты\n\n"
            "💰 <b>Вознаграждение:</b> 50₽ за каждые 1000 просмотров\n"
            "Пример: 4 000 просмотров = 200₽\n\n"
            "⚠️ Накрутка = бан навсегда\n\n"
            "🎬 Примеры видео: @SwordUserVideo\n\n"
            "💡 Нужны попытки для съёмки? Напишите в @Soveqk\n\n"
            f"✅ Одобрено видео: <code>{approved}</code>"
        )

    @dp.callback_query(F.data == "ref_tiktok")
    async def cb_ref_tiktok(cb: CallbackQuery):
        await answer_cb(cb)
        kb = InlineKeyboardBuilder()
        kb.button(text="Отправить на проверку🗳", callback_data="tt_go")
        kb.button(text="Назад", callback_data="cmd_referral")
        kb.adjust(1)
        await edit_msg(cb.message, tiktok_referral_text(cb.from_user.id), kb.as_markup())

    @dp.callback_query(F.data == "cmd_tiktok")
    async def cb_tt(cb: CallbackQuery):
        await cb_ref_tiktok(cb)

    @dp.callback_query(F.data == "tt_video")
    async def cb_ttv(cb: CallbackQuery):
        await answer_cb(cb)
        kb = InlineKeyboardBuilder()
        kb.button(text="🎬 @SwordUserVideo", url="https://t.me/SwordUserVideo")
        kb.button(text="🔙", callback_data="ref_tiktok")
        kb.adjust(1)
        await edit_msg(cb.message, "🎬 <b>Примеры видео:</b> @SwordUserVideo", kb.as_markup())

    @dp.callback_query(F.data == "tt_go")
    async def cb_tg_tt(cb: CallbackQuery):
        uid = cb.from_user.id; await answer_cb(cb)
        if not tiktok_can_submit(uid):
            kb = InlineKeyboardBuilder(); kb.button(text="Назад", callback_data="ref_tiktok")
            await edit_msg(cb.message, "❌ Лимит заявок на сегодня исчерпан", kb.as_markup()); return
        tid = task_create(uid)
        user_states[uid] = {"action":"tiktok_link_proof","task_id":tid}
        kb = InlineKeyboardBuilder(); kb.button(text="Назад", callback_data="tt_cancel")
        await edit_msg(cb.message, f"📤 <b>Заявка #{tid}</b>\n\nПришлите ссылку на TikTok-видео одним сообщением.", kb.as_markup())

    @dp.callback_query(F.data == "tt_cancel")
    async def cb_tc(cb: CallbackQuery):
        await answer_cb(cb); user_states.pop(cb.from_user.id, None)
        kb = InlineKeyboardBuilder(); kb.button(text="Назад", callback_data="ref_tiktok"); kb.adjust(1)
        await edit_msg(cb.message, "❌ Заявка отменена", kb.as_markup())

    @dp.callback_query(F.data.startswith("ta_"))
    async def cb_ta(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb); tid = int(cb.data[3:])
        user_states[cb.from_user.id] = {"action":"tiktok_amount","task_id":tid}
        kb = InlineKeyboardBuilder(); kb.button(text="Не одобрить", callback_data=f"tr_{tid}"); kb.adjust(1)
        try:
            await edit_msg(cb.message, f"✅ Заявка #{tid}\n\nУкажите сумму выплаты в рублях одним сообщением.", kb.as_markup())
        except:
            await cb.message.answer(f"✅ Заявка #{tid}\n\nУкажите сумму выплаты в рублях одним сообщением.")

    @dp.callback_query(F.data.startswith("tr_"))
    async def cb_tr(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb); tid = int(cb.data[3:])
        user_states.pop(cb.from_user.id, None)
        uid = task_reject(tid, cb.from_user.id)
        if uid:
            log_action(cb.from_user.id, "task_reject", str(tid))
            try: await cb.message.edit_text(f"❌ #{tid} отклонено")
            except: pass
            try: await bot.send_message(uid, "❌ Заявка отклонена. Проверьте правила и попробуйте снова.")
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
                akb.button(text="Одобрить", callback_data=f"ta_{tid}"); akb.button(text="Не одобрить", callback_data=f"tr_{tid}"); akb.adjust(2)
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

    async def start_search(message):
        uid = message.from_user.id
        if not can_search(uid):
            await message.answer("❌ Лимит поисков исчерпан. Купите подписку или дождитесь обновления.")
            return
        if is_banned(uid):
            await message.answer("❌ Доступ заблокирован.")
            return
        bad = await check_subscribed(uid)
        if bad:
            await message.answer(f"❌ Подпишитесь на каналы: {', '.join(['@'+ch for ch in bad])}")
            return
        count = get_search_count(uid)
        msg = await message.answer("🚀 Запускаю поиск...")
        found, stats = await do_search(count, gen_default, msg, "~Дефолт~", uid, "default")
        if found:
            txt = "\n".join(f"@{x['username']}" for x in found)
            await message.answer(f"✅ Найдено:\n{txt}")
        else:
            await message.answer("❌ Ничего не найдено.")
        use_search(uid)

    @dp.message(Command("search"))
    async def cmd_search(msg: Message):
        uid = msg.from_user.id
        if is_banned(uid): return
        if not can_search(uid):
            await msg.answer("❌ Поиски закончились. Купите подписку или дождитесь обновления.")
            return
        await start_search(msg)

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
            t,k=build_menu(uid); t = with_main_branding(t); await send_menu_photo(uid, t, k); return

        if action=="tiktok_amount":
            if uid not in ADMIN_IDS:
                user_states.pop(uid, None)
                return
            raw_amount = msg.text.strip().replace(",", ".")
            try:
                amount_rub = float(raw_amount)
                if amount_rub <= 0:
                    raise ValueError
            except:
                await msg.answer("❌ Укажите сумму числом, например: 200")
                return
            tid = state.get("task_id")
            task_set_amount(tid, amount_rub)
            approved_uid = task_approve(tid, uid)
            user_states.pop(uid, None)
            if approved_uid:
                log_action(uid, "task_approve", f"{tid} amount={amount_rub}")
                user_states[approved_uid] = {"action":"tiktok_sbp","task_id":tid}
                amount_text = int(amount_rub) if amount_rub.is_integer() else amount_rub
                await msg.answer(f"✅ Заявка #{tid} одобрена. Сумма: {amount_text}₽. Пользователю отправлен запрос реквизитов.")
                try:
                    await bot.send_message(
                        approved_uid,
                        f"✅ Видео одобрено.\n💰 Сумма выплаты: <b>{amount_text}₽</b>\n\nПришлите реквизиты СБП для выплаты одним сообщением.",
                        parse_mode="HTML"
                    )
                except: pass
            else:
                await msg.answer("❌ Заявка не найдена или уже обработана.")
            return

        if action=="tiktok_link_proof":
            proof_url = msg.text.strip()
            if not re.match(r"^https?://", proof_url):
                await msg.answer("❌ Пришлите корректную ссылку на TikTok-видео."); return
            tid = state.get("task_id")
            task_set_proof(tid, proof_url)
            user_states.pop(uid, None)
            safe_url = html.escape(proof_url)
            display = f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"
            kb = InlineKeyboardBuilder(); kb.button(text="Одобрить", callback_data=f"ta_{tid}"); kb.button(text="Не одобрить", callback_data=f"tr_{tid}"); kb.adjust(2)
            for aid in ADMIN_IDS:
                try:
                    await bot.send_message(aid,
                        f"🎵 <b>TikTok заявка #{tid}</b>\n"
                        f"👤 {display} (<code>{uid}</code>)\n"
                        f"🔗 {safe_url}",
                        reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True)
                except Exception as e: logger.error(f"TikTok link notify {aid}: {e}")
            await msg.answer("✅ Ссылка отправлена на проверку. После одобрения бот запросит реквизиты СБП.")
            return

        if action=="tiktok_sbp":
            sbp = msg.text.strip()
            tid = state.get("task_id")
            task_set_sbp(tid, sbp)
            user_states.pop(uid, None)
            safe_sbp = html.escape(sbp)
            display = f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"
            for aid in ADMIN_IDS:
                try:
                    await bot.send_message(aid,
                        f"💳 <b>СБП по TikTok заявке #{tid}</b>\n"
                        f"👤 {display} (<code>{uid}</code>)\n"
                        f"🏦 <code>{safe_sbp}</code>", parse_mode="HTML")
                except Exception as e: logger.error(f"TikTok SBP notify {aid}: {e}")
            await msg.answer("✅ Реквизиты СБП отправлены. Выплата будет обработана после финальной проверки просмотров.")
            return

        if action=="evaluate":
            user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
            if not validate_username(un): await msg.answer("❌ Мин 5 символов"); return

        if action=="input_pattern":
            user_states.pop(uid,None)
            await msg.answer("❌ Режим шаблона недоступен"); return

        if action=="evaluate":
            user_states.pop(uid,None); un=msg.text.strip().replace("@","").lower()
            if not validate_username(un): await msg.answer("❌ Мин 5 символов"); return
            wm=await msg.answer("⏳..."); tg=await check_username(un); fr=await check_fragment(un)
            tgs={"free":"✅ Свободен","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
            frs={"fragment":"💎","sold":"✅ Продан","unavailable":"—"}.get(fr,"❓")
            ev=evaluate_username(un); fac="\n".join("  "+f for f in ev["factors"]) or "  —"
            kb=InlineKeyboardBuilder()
            kb.button(text="📊 Ещё",callback_data="cmd_evaluate"); kb.button(text="🔙",callback_data="cmd_menu"); kb.adjust(1)
            try: await wm.delete()
            except: pass
            text = (f"📊 <b>@{un}</b>\n\n📱 {tgs}\n💎 {frs}\n\n"
                    f"🏷 <b>{ev['rarity']}</b> | 💰 <b>{ev['price']}</b>\n"
                    f"[{ev['bar']}] <code>{ev['score']}/200</code>\n\n{fac}\n\n"
                    f"📱 <a href='https://t.me/{un}'>Telegram</a> · "
                    f"💎 <a href='https://fragment.com/username/{un}'>Fragment</a>")
            await msg.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True); return

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
            lot_id = market_create_lot(uid, state["mtype"], state["title"], state["desc"], price)
            commission = int(price * MARKET_COMMISSION)
            log_action(uid, "market_create", str(lot_id))
            bal = get_balance(uid)
            rub = int(MARKET_LISTING_FEE * STAR_TO_RUB)
            kb = InlineKeyboardBuilder()
            if bal >= MARKET_LISTING_FEE:
                kb.button(
                    text=f"💰 С баланса ({MARKET_LISTING_FEE}⭐)",
                    callback_data=f"paybal_listing_{lot_id}"
                )
            kb.button(
                text=f"✍️ Написать @{PAY_CONTACT}",
                url=f"https://t.me/{PAY_CONTACT}"
            )
            kb.button(text="❌ Отмена", callback_data="market_sell")
            kb.adjust(1)
            await msg.answer(
                f"📦 <b>Лот #{lot_id} создан!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🏷 {state['title']}\n"
                f"💰 Цена: <code>{price}⭐</code>\n"
                f"📊 Вы получите: <code>{price - commission}⭐</code>\n\n"
                f"<b>Оплата размещения: {MARKET_LISTING_FEE}⭐ ({rub}₽)</b>\n\n"
                f"Выберите способ оплаты:",
                reply_markup=kb.as_markup(),
                parse_mode="HTML"
            )
            return

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

        if action == "exchange_counter":
            user_states.pop(uid, None)
            eid = state["eid"]

            offer = msg.text.strip().replace("@", "").lower()

            # Валидация юзернейма
            if not validate_username(offer):
                await msg.answer("❌ Некорректный юзернейм (5-32 символа, латиница)")
                return

            ok, data = exchange_accept(eid, uid, offer)

            if not ok:
                await msg.answer("❌ Обмен уже закрыт или недоступен")
                return

            ex = exchange_get(eid)
            init_uid = ex["initiator_uid"]

        # Кнопки подтверждения для ОБОИХ
            confirm_kb = InlineKeyboardBuilder()
            confirm_kb.button(
                text="✅ Я готов отдать юз", 
                callback_data=f"exconfirm_{eid}"
            )
            confirm_kb.adjust(1)

            # Уведомляем инициатора
            try:
                await bot.send_message(init_uid,
                    f"🤝 <b>Обмен #{eid} принят!</b>\n\n"
                    f"📤 Ты отдаёшь: <code>@{ex['initiator_offer']}</code>\n"
                    f"📥 Ты получишь: <code>@{offer}</code>\n\n"
                    f"⏰ Дедлайн: {MARKET_ESCROW_HOURS} часов\n\n"
                    f"Нажми кнопку когда будешь готов передать юзернейм:",
                    reply_markup=confirm_kb.as_markup(),
                    parse_mode="HTML")
            except: pass

            # Сообщение партнёру
            await msg.answer(
                f"✅ <b>Обмен #{eid} создан!</b>\n\n"
                f"📥 Ты получишь: <code>@{ex['initiator_offer']}</code>\n"
                f"📤 Ты отдаёшь: <code>@{offer}</code>\n\n"
                f"⏰ Дедлайн: {MARKET_ESCROW_HOURS} часов\n\n"
                f"Нажми кнопку когда будешь готов передать юзернейм:",
                reply_markup=confirm_kb.as_markup(),
                parse_mode="HTML"
            )
            return

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
            user_states.pop(uid,None)
            await msg.answer("❌ Раздел отключён")
            return

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

        if action=="shop_activate_promo":
            user_states.pop(uid,None)
            code = msg.text.strip().upper()
            result = check_promocode(code, uid, 0, "shop")
            if not result["valid"]:
                await msg.answer(f"❌ {result.get('reason','Промокод недействителен')}"); return
            discount = result["discount"]
            use_promocode(code, uid, discount)
            if discount > 0:
                add_balance(uid, discount)
                log_action(uid, "promo_activate", f"{code} +{discount}⭐")
                await msg.answer(
                    f"🎉 <b>Промокод активирован!</b>\n\n"
                    f"🏷 <code>{code}</code>\n"
                    f"💰 +{discount}⭐ на баланс\n"
                    f"💰 Баланс: <code>{get_balance(uid):.1f}⭐</code>",
                    parse_mode="HTML")
            else:
                log_action(uid, "promo_activate", code)
                await msg.answer(f"✅ Промокод <code>{code}</code> активирован!", parse_mode="HTML")
            return

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
            await msg.answer(f"✅ +{days}дн до {end}")
            try:
                await bot.send_message(state["target"],
                    f"🎉 <b>Вам выдан Premium!</b>\n\n"
                    f"💎 +{days} дней\n📅 До: {end}\n\n"
                    f"Приятного использования!", parse_mode="HTML")
            except: pass
            return

        # ═══ НОВОЕ: Админ VIP ═══
        if action=="admin_user_add_vip_days":
            user_states.pop(uid,None)
            try: days=int(msg.text.strip())
            except: await msg.answer("❌"); return
            end=give_vip(state["target"],days); log_action(uid,"add_vip",f"{state['target']}+{days}")
            await msg.answer(f"✅ 🌟 +{days}дн VIP до {end}")
            try:
                await bot.send_message(state["target"],
                    f"🎉 <b>Вам выдан VIP!</b>\n\n"
                    f"🌟 +{days} дней\n📅 До: {end}\n\n"
                    f"Приятного использования!", parse_mode="HTML")
            except: pass
            return

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

        if action in ("admin_add_session_api_id", "admin_add_session_api_hash", "admin_add_session_phone"):
            user_states.pop(uid, None)
            await msg.answer("⚡ User-сессии удалены. Бот работает только в публичном строгом режиме.")
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
        else: t,k=build_menu(uid); t = with_main_branding(t)
        await send_menu_photo(uid, t, k)


    # ═══════════════════════ АДМИНКА ═══════════════════════
    
    @dp.callback_query(F.data == "cmd_admin")
    async def cb_admin(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        s = get_stats(); ps = pool.stats()

        sl = f"🟢{ps['active']-ps.get('warming',0)} 🟡{ps.get('warming',0)} 🟠{ps.get('cooldown',0)} 🔴{ps.get('dead',0)}"

        text = (
            f"👑 <b>Админ-панель v26.0</b>\n{'='*20}\n\n"
            f"👥 Всего юзеров: <code>{s['users']}</code> | � Бан: <code>{s['banned']}</code>\n"
            f"💎 Premium: <code>{s['subs']}</code>\n\n"
            f"� <b>Сегодня:</b>\n"
            f"  🆕 Новых: <code>{s['today_users']}</code>\n"
            f"  🔍 Поисков: <code>{s['today_searches']}</code>\n"
            f"  💳 Покупок: <code>{s['today_purchases']}</code>\n\n"
            f"📊 <b>Всего:</b>\n"
            f"  🔢 Поисков: <code>{s['searches']}</code>\n"
            f"  🏆 Найдено юзов: <code>{s['total_found']}</code>\n\n"
            f"💾 <b>Кэш:</b>\n"
            f"  🎲 Дефолт: <code>{s['cache_default']}</code>\n"
            f"  💎 Красивые: <code>{s['cache_beautiful']}</code>\n\n"
            f"🔑 Сессии: {sl}"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="👤 Юзеры", callback_data="adm_users")
        kb.button(text="💎 Подписки", callback_data="adm_subs")
        kb.button(text="🔑 Сессии", callback_data="adm_sessions")
        kb.button(text="🗑 Очистить кэш", callback_data="clear_cache")
        kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
        kb.button(text="⚙️ Настройки", callback_data="a_control")
        kb.button(text="🖥 Сервер", callback_data="adm_server")
        kb.button(text="🔙 Меню", callback_data="cmd_menu")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === ЮЗЕРЫ ===
    @dp.callback_query(F.data == "adm_users")
    async def cb_adm_users(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        s = get_stats()
        text = f"👤 <b>Юзеры</b>\n\nВсего: <code>{s['users']}</code>\nБан: <code>{s['banned']}</code>"
        kb = InlineKeyboardBuilder()
        kb.button(text="🔍 Найти", callback_data="a_user")
        kb.button(text="🚫 Бан", callback_data="a_ban")
        kb.button(text="✅ Разбан", callback_data="a_unban")
        kb.button(text="📤 Экспорт", callback_data="a_export")
        kb.button(text="👥 Рефералы", callback_data="a_refs")
        kb.button(text="⚫ Чёрный список", callback_data="a_blacklist")
        kb.button(text="📋 Лог", callback_data="a_log")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === ПОДПИСКИ ===
    @dp.callback_query(F.data == "adm_subs")
    async def cb_adm_subs(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        s = get_stats()
        text = f"💎 <b>Подписки</b>\n\nАктивных: <code>{s['subs']}</code>"
        kb = InlineKeyboardBuilder()
        kb.button(text="💎 Список Premium", callback_data="a_plist")
        kb.button(text="🎁 Выдать", callback_data="a_give")
        kb.button(text="🔑 Ключи", callback_data="a_keys")
        kb.button(text="🎰 Розыгрыш", callback_data="a_raffle")
        kb.button(text="🎪 Акции", callback_data="a_promos")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === МАРКЕТ ===
    @dp.callback_query(F.data == "adm_market")
    async def cb_adm_market(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        pending_m = len(market_get_pending())
        disputes_m = len(market_get_disputes())
        s = get_stats()
        text = (f"🏪 <b>Маркет</b>\n\n"
                f"📋 Модерация: <code>{pending_m}</code>\n"
                f"⚠️ Споры: <code>{disputes_m}</code>\n"
                f"💸 Выводы: <code>{s['withdrawals']}</code>")
        kb = InlineKeyboardBuilder()
        kb.button(text=f"📋 Модерация ({pending_m})", callback_data="a_mmod")
        kb.button(text=f"⚠️ Споры ({disputes_m})", callback_data="a_mdisputes")
        kb.button(text="🏷️ Промокоды", callback_data="a_promocodes")
        kb.button(text=f"💸 Выводы ({s['withdrawals']})", callback_data="a_wd")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === СЕССИИ ===
    @dp.callback_query(F.data == "adm_sessions")
    async def cb_adm_sessions(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return
        await answer_cb(cb)

        ps = pool.stats()
        detail = pool.detailed_status()

        text = (
            "⚡ <b>Публичная строгая проверка</b>\n\n"
            "User-сессии и Telethon-пул отключены полностью.\n"
            "Поиск использует только t.me, Bot API getChat и Fragment.\n\n"
            f"🔢 Проверок: <code>{ps['checks']}</code>\n\n"
            f"<pre>{detail}</pre>"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data="adm_sessions")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(1)

        await edit_msg(cb.message, text, kb.as_markup())

    @dp.callback_query(F.data == "a_toggle_checker")
    async def cb_toggle_checker(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return
        await answer_cb(cb)

        log_action(cb.from_user.id, "checker_mode", "public_strict")
        await answer_cb(cb, "User-сессии отключены. Доступен только публичный строгий режим.", show_alert=True)
        await cb_adm_sessions(cb)

    # === ВСЕ АККАУНТЫ ===
    @dp.callback_query(F.data == "adm_all_accounts")
    async def cb_all_accounts(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="adm_sessions")
        await edit_msg(cb.message, "💾 <b>User-сессии удалены. Сохранённых аккаунтов нет.</b>", kb.as_markup())

    @dp.callback_query(F.data == "adm_export_sessions")
    async def cb_export_sessions(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        await answer_cb(cb, "User-сессии удалены, экспорт пустой", show_alert=True)

    @dp.callback_query(F.data == "clear_cache")
    async def cb_clear_cache(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return
        await answer_cb(cb)
    
        conn = sqlite3.connect(DB)
        c = conn.cursor()
    
    # Удаляем все юзы неправильной длины
        c.execute("DELETE FROM free_cache WHERE mode='default' AND length != 6")
        deleted_default = c.rowcount
    
        c.execute("DELETE FROM free_cache WHERE mode='beautiful' AND length != 5")
        deleted_beautiful = c.rowcount
    
        conn.commit()
        conn.close()
        
        await cb.message.answer(
            f"🗑 Очищено:\n"
            f"default: {deleted_default}\n"
            f"beautiful: {deleted_beautiful}"
        )
    
    # === РАССЫЛКА ===
    @dp.callback_query(F.data == "adm_broadcast")
    async def cb_adm_broadcast(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        s = get_stats()
        text = f"📢 <b>Рассылка и экспорт</b>"
        kb = InlineKeyboardBuilder()
        kb.button(text="📢 Рассылка", callback_data="a_bcast")
        kb.button(text="📊 Экспорт юзеров", callback_data="a_export")
        kb.button(text=f"🎬 TikTok ({s['tasks']})", callback_data="a_tt")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === СЕРВЕР ===
    @dp.callback_query(F.data == "adm_server")
    async def cb_adm_server_menu(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        text = f"🖥 <b>Сервер и код</b>"
        kb = InlineKeyboardBuilder()
        kb.button(text="🖥 Состояние", callback_data="a_server")
        kb.button(text="📋 Логи", callback_data="a_logs")
        kb.button(text="🔄 Обновить код", callback_data="a_update")
        kb.button(text="🔄 Перезапуск", callback_data="a_restart")
        kb.button(text="📥 Скачать bot.py", callback_data="adm_download_code")
        kb.button(text="📥 Скачать БД", callback_data="adm_download_db")
        kb.button(text="🔙 Админ", callback_data="cmd_admin")
        kb.adjust(2)
        await edit_msg(cb.message, text, kb.as_markup())

    # === СКАЧАТЬ КОД И БД ===
    @dp.callback_query(F.data == "adm_download_code")
    async def cb_download_code(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        try:
            fn = "bot.py"
            if not os.path.exists(fn):
                # Пробуем найти текущий файл
                fn = os.path.abspath(__file__)
            with open(fn, "rb") as f:
                await bot.send_document(cb.from_user.id,
                    BufferedInputFile(f.read(), filename=f"bot_{datetime.now().strftime('%Y%m%d_%H%M')}.py"),
                    caption="📥 Текущий код бота")
        except Exception as e:
            await cb.message.answer(f"❌ {e}")

    @dp.callback_query(F.data == "adm_download_db")
    async def cb_download_db(cb: CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return
        await answer_cb(cb)
        try:
            with open(DB, "rb") as f:
                await bot.send_document(cb.from_user.id,
                    BufferedInputFile(f.read(), filename=f"hunter_{datetime.now().strftime('%Y%m%d_%H%M')}.db"),
                    caption="📥 База данных")
        except Exception as e:
            await cb.message.answer(f"❌ {e}")

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
            "⚡ <b>Без user-сессий</b>\n\n"
            f"📊 Проверок: <code>{ps['checks']}</code>\n\n"
            f"<pre>{detail}</pre>"
        )
        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data="a_sessions")
        kb.button(text="🔙", callback_data="cmd_admin")
        kb.adjust(1)
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
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="a_sessions")
        await edit_msg(cb.message, "⚡ <b>User-сессии удалены. Добавление аккаунтов отключено.</b>", kb.as_markup())

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
            kb.button(text=f"Одобрить #{t['id']}",callback_data=f"ta_{t['id']}"); kb.button(text=f"Не одобрить #{t['id']}",callback_data=f"tr_{t['id']}")
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
        btns={"tiktok":"🎁 TikTok","roulette":"🎰 Рулетка","shop":"🏪 Магазин","support":"🤖 Донат","referral":"👥 Рефералы"}
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
        await answer_cb(cb)
        kb = InlineKeyboardBuilder(); kb.button(text="🔙", callback_data="cmd_admin")
        await edit_msg(cb.message, "❌ <b>Автообновление отключено.</b>\nЗаливайте файл вручную.", kb.as_markup())

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


    @dp.callback_query()
    async def cb_unhandled_callback_debug(cb: CallbackQuery):
        _print_callback_event("CALLBACK-UNHANDLED", cb)
        try:
            await cb.answer("Кнопка не обработана. Смотри data в консоли.", show_alert=False)
        except Exception:
            pass

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

async def auto_renew_loop():
    await asyncio.sleep(300)
    while True:
        try:
            now = datetime.now()
            conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row; c = conn.cursor()
            c.execute("SELECT uid,sub_end,auto_renew,auto_renew_plan,balance FROM users WHERE auto_renew=1 AND auto_renew_plan!='' AND sub_end!=''")
            users = [dict(r) for r in c.fetchall()]; conn.close()
            for u in users:
                try:
                    sub_end = datetime.strptime(u["sub_end"], "%Y-%m-%d %H:%M:%S")
                except:
                    try: sub_end = datetime.strptime(u["sub_end"], "%Y-%m-%d %H:%M")
                    except: continue
                if sub_end > now: continue
                plan = u["auto_renew_plan"]
                price_info = PRICES.get(plan)
                if not price_info: continue
                bal = float(u.get("balance", 0) or 0)
                if bal >= price_info["stars"]:
                    set_balance(u["uid"], bal - price_info["stars"])
                    end = give_subscription(u["uid"], price_info["days"])
                    log_action(u["uid"], "auto_renew", f"{plan} -{price_info['stars']}⭐")
                    try:
                        kb = InlineKeyboardBuilder(); kb.button(text="👤 Профиль", callback_data="cmd_profile")
                        await bot.send_message(u["uid"],
                            f"🔄 <b>Авто-продление!</b>\n\n"
                            f"💎 {price_info['label']} продлено\n"
                            f"💰 Списано: <code>{price_info['stars']}⭐</code>\n"
                            f"📅 До: <code>{end}</code>",
                            reply_markup=kb.as_markup(), parse_mode="HTML")
                    except: pass
                else:
                    try:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="💰 Пополнить", callback_data="cmd_shop")
                        await bot.send_message(u["uid"],
                            f"⚠️ <b>Не удалось продлить подписку!</b>\n\n"
                            f"Нужно: <code>{price_info['stars']}⭐</code>\n"
                            f"Баланс: <code>{bal:.1f}⭐</code>\n\n"
                            f"Пополните баланс для авто-продления.",
                            reply_markup=kb.as_markup(), parse_mode="HTML")
                    except: pass
        except Exception as e: logger.error(f"AutoRenew: {e}")
        await asyncio.sleep(3600)

async def return_user_push_loop():
    await asyncio.sleep(600)
    while True:
        try:
            days_threshold = 7
            threshold_time = (datetime.now() - timedelta(days=days_threshold)).strftime("%Y-%m-%d %H:%M")
            conn = sqlite3.connect(DB); c = conn.cursor()
            c.execute("SELECT uid,uname FROM users WHERE last_active < ? AND last_active != ''", (threshold_time,))
            users = c.fetchall(); conn.close()
            for uid, uname in users:
                try:
                    kb = InlineKeyboardBuilder()
                    kb.button(text="🔍 Поиск", callback_data="cmd_search")
                    kb.button(text="👤 Профиль", callback_data="cmd_profile")
                    kb.adjust(1)
                    name = f"@{uname}" if uname else f"ID:{uid}"
                    await bot.send_message(uid,
                        f"👋 <b>Привет, {name}!</b>\n\n"
                        f"Мы скучали! Заходи, проверь новые юзернеймы ⚡",
                        reply_markup=kb.as_markup(), parse_mode="HTML")
                    update_last_active(uid)
                    await asyncio.sleep(60)
                except: pass
        except Exception as e: logger.error(f"Return push: {e}")
        await asyncio.sleep(86400)

async def monitor_loop():
    await asyncio.sleep(60)
    while True:
        try:
            expire_monitors()
            monitors = get_active_monitors()
            for mon in monitors:
                try:
                    tg = await check_username(mon["username"])
                    if tg == "free":
                        fr = await check_fragment(mon["username"])
                        if fr == "unavailable":
                            update_monitor_status(mon["id"], "free")
                            kb = InlineKeyboardBuilder()
                            kb.button(text="📊 Оценить", callback_data=f"eval_{mon['username']}")
                            kb.adjust(1)
                            try:
                                await bot.send_message(mon["uid"],
                                    f"🎉 <b>@{mon['username']} свободен!</b>\n\n"
                                    f"📱 <a href='https://t.me/{mon['username']}'>Telegram</a> · "
                                    f"💎 <a href='https://fragment.com/username/{mon['username']}'>Fragment</a>\n\n"
                                    f"⚡ Скорее забирайте!",
                                    reply_markup=kb.as_markup(), parse_mode="HTML", disable_web_page_preview=True)
                            except: pass
                            logger.info(f"[monitor] @{mon['username']} FREE → notified uid={mon['uid']}")
                        else:
                            update_monitor_status(mon["id"], "taken")
                    else:
                        update_monitor_status(mon["id"], "taken")
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.error(f"[monitor] @{mon['username']}: {e}")
        except Exception as e: logger.error(f"Monitor: {e}")
        await asyncio.sleep(MONITOR_CHECK_INTERVAL)

async def session_watchdog():
    # User-сессии удалены; watchdog оставлен как пустой фоновой таск для совместимости.
    while True:
        await asyncio.sleep(3600)

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

async def cache_warmer_check_username(u: str) -> bool:
    """Кэш наполняется тем же быстрым рабочим фильтром, что и ручной поиск."""
    return await fast_available_for_search(u)


async def free_cache_warmer_loop():
    """
    Фоновое наполнение JSON-кэша после запуска бота.
    Не блокирует polling и не спамит консоль каждой проверкой.
    """
    await asyncio.sleep(1)
    print(f"[{SEARCH_CALLBACK_FIX_VERSION}] CACHE-WARMER-START targets={CACHE_WARMER_TARGETS}", flush=True)
    logger.info(f"[{SEARCH_CALLBACK_FIX_VERSION}] CACHE-WARMER-START targets={CACHE_WARMER_TARGETS}")

    while True:
        try:
            for mode_key, target in CACHE_WARMER_TARGETS.items():
                mode = SEARCH_MODES.get(mode_key)
                if not mode or mode.get("disabled"):
                    continue

                current = await get_free_cache_count(mode_key)
                if current >= target:
                    continue

                validate_func = mode.get("validate", is_valid_username)
                gen_func = mode["func"]
                checked = set()
                missing = target - current
                candidates = make_unique_candidates(
                    gen_func,
                    validate_func,
                    checked,
                    min(CACHE_WARMER_BATCH_SIZE, max(40, missing * 3)),
                )

                if not candidates:
                    continue

                found = await check_candidates_fast(candidates, None, stop_after=min(missing, 20))
                if found:
                    await add_free_cache(found, mode_key)
                    total_now = await get_free_cache_count(mode_key)
                    print(
                        f"[{SEARCH_CALLBACK_FIX_VERSION}] CACHE-WARMER-ADD mode={mode_key} added={len(found)} total={total_now}",
                        flush=True,
                    )
                    logger.info(f"[cache-warmer] {mode_key}: added={len(found)} total={total_now}")
                else:
                    print(
                        f"[{SEARCH_CALLBACK_FIX_VERSION}] CACHE-WARMER-PASS mode={mode_key} checked={len(candidates)} added=0 current={current}",
                        flush=True,
                    )

                await asyncio.sleep(0.3)

        except Exception as e:
            print(f"[{SEARCH_CALLBACK_FIX_VERSION}] CACHE-WARMER-ERROR {e}", flush=True)
            logger.error(f"Cache warmer error: {e}")

        await asyncio.sleep(3)

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
    global http_session, bot_info, pool
    dp = Dispatcher()
    pool = AccountPool()
    init_db(); setup_systemd()
    config=load_bot_config(); apply_config(config)
    if "prices" in config:
        for k,v in config["prices"].items():
            if k in PRICES: PRICES[k]["stars"]=v
    bot_info=await bot.get_me(); http_session=aiohttp.ClientSession()
    # User-сессии не подключаются: проверка работает через t.me + Bot API getChat + Fragment.
    ps=pool.stats()
    await register_handlers(dp)
    logger.info("━"*30)
    logger.info(f"🚀 @{bot_info.username} v25.0")
    print("SEARCH_CALLBACK_FIX_ACTIVE v7-working-fast-search", flush=True)
    logger.info("SEARCH_CALLBACK_FIX_ACTIVE v7-working-fast-search")
    logger.info("🔄 Режим проверки: public_strict без user-сессий")
    logger.info("━"*30)
    asyncio.create_task(reminder_loop())
    asyncio.create_task(auto_renew_loop())
    asyncio.create_task(return_user_push_loop())
    asyncio.create_task(session_watchdog())
    asyncio.create_task(daily_report_loop())
    asyncio.create_task(free_cache_warmer_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_session.close(); await pool.disconnect()

if __name__=="__main__":
    asyncio.run(main())
