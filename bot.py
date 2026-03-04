"""
🤖 USERNAME HUNTER BOT v16.0
Сессионный поиск, акция, напоминания, авто-продление
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
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    LabeledPrice, PreCheckoutQuery
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Telethon — максимально точная проверка
try:
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from telethon.errors import (
        FloodWaitError, UsernameNotOccupiedError,
        UsernameInvalidError
    )
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

# ═══════════════════════ НАСТРОЙКИ ═══════════════════════

MAIN_TOKEN = "8325751391:AAHZo7xC6TO4ldKEEqM--Ik9EXaszULnwWc"
ADMIN_IDS = [5969266721, 7894051808]
ADMIN_CONTACT = "onuse"
REQUIRED_CHANNELS = ["SwordUsers"]

# ── Telethon (самый точный чекер) ──
USE_SESSION_SEARCH = True  # ВКЛЮЧИ ОБЯЗАТЕЛЬНО!
API_ID = 30858734          # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
API_HASH = "765935f2e41055e0563210290c8e17da"  # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
SESSION_NAME = "hunter_session"
SEARCH_DELAY = 1.5         # задержка только для Telethon
SEARCH_BATCH = 5

# ── Лимиты ──
FREE_SEARCHES = 5
FREE_COUNT = 1
PREMIUM_COUNT = 3
REF_BONUS = 2
HTTP_BATCH_SIZE = 30       # теперь можно больше — проверка параллельная

# ── TikTok ──
TIKTOK_COMMENT_TEXT = "@SworuserN_bot бесплатные звёзды, найти юз, оценить юз"
TIKTOK_REWARD_GIFT = "🧸 Мишка (15⭐)"
TIKTOK_SCREENSHOTS_NEEDED = 35
TIKTOK_DAILY_LIMIT = 2

# ── Напоминания ──
REMINDER_DAYS = [3, 1]  # За сколько дней напоминать
REMINDER_CHECK_INTERVAL = 3600  # Проверка каждый час (сек)

# ── Тарифы с АКЦИЕЙ ──
PRICES = {
    "1d": {
        "days": 1, "rub": 45, "stars": 40,
        "label": "1 день", "desc": "Попробуй Premium на 24 часа",
        "funpay": "https://funpay.com/lots/offer?id=65182705"
    },
    "3d": {
        "days": 3, "rub": 120, "stars": 100,
        "label": "3 дня", "desc": "Идеально для быстрого поиска",
        "funpay": "https://funpay.com/lots/offer?id=65182951"
    },
    "7d": {
        "days": 7, "rub": 250, "stars": 200,
        "label": "7 дней", "desc": "Неделя полного доступа",
        "funpay": "https://funpay.com/lots/offer?id=65182991"
    },
    "1m": {
        "days": 30, "rub": 800, "stars": 650,
        "label": "1 месяц", "desc": "Лучшее соотношение цена/качество",
        "funpay": "https://funpay.com/lots/offer?id=65183001"
    },
    "3m": {
        "days": 90, "rub": 2200, "stars": 2000,
        "label": "3 месяца", "desc": "Для серьёзных охотников",
        "funpay": "https://funpay.com/lots/offer?id=65183010"
    },
    "1y": {
        "days": 365, "rub": 8000, "stars": 6500,
        "label": "1 год", "desc": "Целый год без ограничений",
        "funpay": "https://funpay.com/lots/offer?id=65183025"
    },
    "forever": {
        "days": 99999, "rub": 1999, "stars": 1599,
        "label": "Навсегда 🔥 АКЦИЯ",
        "desc": "Было 12999₽ → 1999₽ (скидка 85%!)",
        "funpay": "https://funpay.com/lots/offer?id=65183050"
    },
}

# ── Словари ──

COMMON_WORDS = [
    "admin","trade","money","super","power","elite","prime","royal",
    "alpha","omega","delta","sigma","cyber","titan","storm","flame",
    "frost","night","light","dream","magic","ghost","angel","stone",
    "steel","swift","eagle","tiger","ninja","space","world","earth",
    "ocean","solar","blade","sword","crown","queen","noble","brain",
    "smart","crack","shark","snipe","flash","blaze","spark","pixel",
    "cloud","logic","boost","turbo","ultra","hyper","gamer","coder",
    "chess","piano","dance","music","photo","video","crypto","stock",
    "poker","lucky","happy","crazy","black","white","green","saint",
    "trust","faith","grace","peace","dark","fire","ice","wolf",
    "bear","lion","king","star","moon","sun","gold","ruby","jade",
    "onyx","zen","pro","ace","max","neo","vex","hex","fox","sky",
    "art","dev","bot","net","vip","top","win","run","fly","war",
    "aim","orb","gem","ray","glow","flux","wave","haze","myth",
    "echo","void","nova","atom","bolt","claw","dawn","dusk","edge",
    "fate","fuse","grit","halo","iris","jinx","kite","lynx","maze",
    "neon","opal","peak","rage","sage","veil","zeal","apex","aura",
    "bane","core","dome","fang","grip","icon","jolt","lens","mist",
]

CRYPTO_WORDS = {
    3: ["btc","eth","sol","bnb","nft","dao","ape","dex","gas","ton","xrp","ltc"],
    4: ["defi","hodl","pump","moon","mint","swap","farm","coin","doge","pepe","shib"],
    5: ["whale","token","yield","stake","block","chain","miner","trade","vault"],
    6: ["crypto","mining","wallet","ledger","staker","trader"],
    7: ["bitcoin","polygon","cardano","staking","farming","trading"],
}
GAMING_WORDS = {
    3: ["pro","ace","mvp","fps","rpg","pvp","dmg","exp","lol","cod","wow"],
    4: ["frag","noob","boss","raid","loot","tank","mage","buff","nerf","nuke","rush","meta","rank","kill"],
    5: ["snipe","spawn","dodge","combo","arena","clash","quest","grind","carry"],
    6: ["gaming","ranked","player","legend","master","sniper","clutch"],
    7: ["warrior","paladin","diamond","skilled","fighter","shooter"],
}
TECH_WORDS = {
    3: ["dev","api","git","web","sql","cpu","ram","app","bot","css","npm"],
    4: ["code","hack","root","ping","byte","data","node","rust","java","json","html"],
    5: ["linux","swift","react","coder","debug","cloud","stack","nginx","redis","cyber"],
    6: ["python","docker","github","devops","kotlin","server","nodejs","script"],
    7: ["hacking","android","backend","webhook","cluster","mongodb"],
}

# ═══════════════════════ ИНИЦИАЛИЗАЦИЯ ═══════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
bot = Bot(token=MAIN_TOKEN)
dp = Dispatcher()
http_session = None
telethon_client = None
session_ready = False
DB = "hunter.db"


# ═══════════════════════ БАЗА ДАННЫХ ═══════════════════════

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '', joined TEXT DEFAULT '',
        free INTEGER DEFAULT 5, searches INTEGER DEFAULT 0, sub_end TEXT DEFAULT '',
        referred_by INTEGER DEFAULT 0, ref_count INTEGER DEFAULT 0,
        sub_bonus INTEGER DEFAULT 0, favorites TEXT DEFAULT '[]',
        auto_renew INTEGER DEFAULT 0, auto_renew_plan TEXT DEFAULT '',
        last_reminder TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY, days INTEGER, ktype TEXT, created TEXT,
        used INTEGER DEFAULT 0, used_by INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS market (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, price TEXT,
        seller_uid INTEGER, approved INTEGER DEFAULT 0, created TEXT,
        sold INTEGER DEFAULT 0, description TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, market_id INTEGER,
        buyer_uid INTEGER, seller_uid INTEGER, username TEXT, price TEXT,
        status TEXT DEFAULT 'pending', created TEXT,
        admin_confirmed INTEGER DEFAULT 0, confirmed_by INTEGER DEFAULT 0,
        notes TEXT DEFAULT ''
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS appeals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER, uid INTEGER,
        reason TEXT, status TEXT DEFAULT 'pending', created TEXT,
        resolved_by INTEGER DEFAULT 0
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
    # Миграция
    try: c.execute("ALTER TABLE users ADD COLUMN auto_renew INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN auto_renew_plan TEXT DEFAULT ''")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN last_reminder TEXT DEFAULT ''")
    except: pass
    try: c.execute("ALTER TABLE history ADD COLUMN length INTEGER DEFAULT 5")
    except: pass
    conn.commit()
    conn.close()
    logger.info("✅ БД инициализирована")


def ensure_user(uid, uname=""):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT uid FROM users WHERE uid=?", (uid,))
    if not c.fetchone():
        c.execute("INSERT INTO users (uid, uname, joined, free) VALUES (?, ?, ?, ?)",
                  (uid, uname or "", datetime.now().strftime("%Y-%m-%d %H:%M"), FREE_SEARCHES))
    elif uname:
        c.execute("UPDATE users SET uname=? WHERE uid=?", (uname, uid))
    conn.commit()
    conn.close()


def get_user(uid):
    ensure_user(uid)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE uid=?", (uid,))
    row = c.fetchone()
    conn.close()
    if not row:
        return {"uid": uid, "uname": "", "free": FREE_SEARCHES, "searches": 0,
                "sub_end": "", "ref_count": 0, "favorites": "[]",
                "referred_by": 0, "sub_bonus": 0, "auto_renew": 0,
                "auto_renew_plan": "", "last_reminder": ""}
    d = dict(row)
    d.setdefault("auto_renew", 0)
    d.setdefault("auto_renew_plan", "")
    d.setdefault("last_reminder", "")
    return d


def has_subscription(uid):
    if uid in ADMIN_IDS:
        return True
    u = get_user(uid)
    sub_end = u.get("sub_end", "")
    if not sub_end:
        return False
    try:
        return datetime.strptime(sub_end, "%Y-%m-%d %H:%M") > datetime.now()
    except (ValueError, TypeError):
        return False


def can_search(uid):
    if uid in ADMIN_IDS or has_subscription(uid):
        return True
    return get_user(uid).get("free", 0) > 0


def use_search(uid):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    if uid in ADMIN_IDS or has_subscription(uid):
        c.execute("UPDATE users SET searches=searches+1 WHERE uid=?", (uid,))
    else:
        c.execute("UPDATE users SET free=MAX(free-1,0), searches=searches+1 WHERE uid=?", (uid,))
    conn.commit()
    conn.close()


def get_search_count(uid):
    return PREMIUM_COUNT if (uid in ADMIN_IDS or has_subscription(uid)) else FREE_COUNT


def give_subscription(uid, days):
    ensure_user(uid)
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    now = datetime.now()
    u = get_user(uid)
    sub_end = u.get("sub_end", "")
    base = now
    if sub_end:
        try:
            cur = datetime.strptime(sub_end, "%Y-%m-%d %H:%M")
            if cur > now:
                base = cur
        except (ValueError, TypeError):
            pass
    new_end = base + timedelta(days=days)
    c.execute("UPDATE users SET sub_end=? WHERE uid=?", (new_end.strftime("%Y-%m-%d %H:%M"), uid))
    conn.commit()
    conn.close()
    return new_end.strftime("%d.%m.%Y %H:%M")


def process_referral(new_uid, ref_uid):
    if new_uid == ref_uid:
        return False
    u = get_user(new_uid)
    if u.get("referred_by", 0) != 0:
        return False
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET referred_by=? WHERE uid=?", (ref_uid, new_uid))
    c.execute("UPDATE users SET ref_count=ref_count+1, free=free+? WHERE uid=?", (REF_BONUS, ref_uid))
    conn.commit()
    conn.close()
    return True


def activate_key(uid, key_text):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT days, ktype FROM keys WHERE key=? AND used=0", (key_text.strip(),))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    days, ktype = row
    c.execute("UPDATE keys SET used=1, used_by=? WHERE key=?", (uid, key_text.strip()))
    conn.commit()
    conn.close()
    end = give_subscription(uid, days)
    return {"days": days, "end": end}


def generate_key(days, ktype="MANUAL"):
    key = f"HUNT-{ktype}-{secrets.token_hex(4).upper()}"
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("INSERT INTO keys (key, days, ktype, created) VALUES (?, ?, ?, ?)",
              (key, days, ktype, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit()
    conn.close()
    return key


# ── Авто-продление ──

def set_auto_renew(uid, enabled, plan=""):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET auto_renew=?, auto_renew_plan=? WHERE uid=?",
              (1 if enabled else 0, plan, uid))
    conn.commit()
    conn.close()


def get_auto_renew(uid):
    u = get_user(uid)
    return bool(u.get("auto_renew", 0)), u.get("auto_renew_plan", "")


# ── Напоминания ──

def set_last_reminder(uid, date_str):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET last_reminder=? WHERE uid=?", (date_str, uid))
    conn.commit()
    conn.close()


def get_expiring_users(days_before):
    """Юзеры чья подписка истекает через days_before дней"""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    now = datetime.now()
    target = now + timedelta(days=days_before)
    target_start = target.strftime("%Y-%m-%d 00:00")
    target_end = target.strftime("%Y-%m-%d 23:59")
    c.execute(
        "SELECT uid, sub_end, auto_renew, auto_renew_plan, last_reminder "
        "FROM users WHERE sub_end BETWEEN ? AND ? AND sub_end != ''",
        (target_start, target_end)
    )
    rows = c.fetchall()
    conn.close()
    return [{"uid": r[0], "sub_end": r[1], "auto_renew": r[2],
             "auto_renew_plan": r[3], "last_reminder": r[4] or ""} for r in rows]


# ── Избранное/История/Статистика ──

def add_favorite(uid, username):
    u = get_user(uid)
    try:
        favs = json.loads(u.get("favorites", "[]"))
    except:
        favs = []
    username = username.lower().strip()
    if username not in favs:
        favs.append(username)
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute("UPDATE users SET favorites=? WHERE uid=?", (json.dumps(favs), uid))
        conn.commit()
        conn.close()
    return favs

def get_favorites(uid):
    u = get_user(uid)
    try:
        return json.loads(u.get("favorites", "[]"))
    except:
        return []

def remove_favorite(uid, username):
    favs = get_favorites(uid)
    if username.lower().strip() in favs:
        favs.remove(username.lower().strip())
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute("UPDATE users SET favorites=? WHERE uid=?", (json.dumps(favs), uid))
        conn.commit()
        conn.close()
    return favs

def save_history(uid, username, mode, length=5):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("INSERT INTO history (uid, username, found_at, mode, length) VALUES (?, ?, ?, ?, ?)",
              (uid, username, datetime.now().strftime("%Y-%m-%d %H:%M"), mode, length))
    conn.commit()
    conn.close()

def get_history(uid, limit=20):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT username, found_at, mode FROM history WHERE uid=? ORDER BY id DESC LIMIT ?", (uid, limit))
    rows = c.fetchall()
    conn.close()
    return rows

def get_stats():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    today = datetime.now().strftime("%Y-%m-%d")
    r = {
        "users": c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "subs": c.execute("SELECT COUNT(*) FROM users WHERE sub_end>?", (now_s,)).fetchone()[0],
        "searches": c.execute("SELECT COALESCE(SUM(searches),0) FROM users").fetchone()[0],
        "market": c.execute("SELECT COUNT(*) FROM market WHERE approved=1 AND sold=0").fetchone()[0],
        "pending": c.execute("SELECT COUNT(*) FROM market WHERE approved=0").fetchone()[0],
        "deals": c.execute("SELECT COUNT(*) FROM deals WHERE status='active'").fetchone()[0],
        "appeals": c.execute("SELECT COUNT(*) FROM appeals WHERE status='pending'").fetchone()[0],
        "pending_deals": c.execute("SELECT COUNT(*) FROM deals WHERE status='pending'").fetchone()[0],
        "tasks": c.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0],
        "today_users": c.execute("SELECT COUNT(*) FROM users WHERE joined LIKE ?", (today+"%",)).fetchone()[0],
        "today_searches": c.execute("SELECT COUNT(*) FROM history WHERE found_at LIKE ?", (today+"%",)).fetchone()[0],
        "auto_renew": c.execute("SELECT COUNT(*) FROM users WHERE auto_renew=1").fetchone()[0],
    }
    conn.close()
    return r


# ═══════════════════════ МАРКЕТ/СДЕЛКИ/АПЕЛЛЯЦИИ/ТИКТОК БД ═══════════════════════

def market_add(username, price, seller_uid, approved=0):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("INSERT INTO market (username, price, seller_uid, approved, created) VALUES (?, ?, ?, ?, ?)",
              (username, price, seller_uid, approved, datetime.now().strftime("%Y-%m-%d %H:%M")))
    lid = c.lastrowid; conn.commit(); conn.close(); return lid

def market_list(approved=1, limit=15):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,username,price,seller_uid,approved,created,sold FROM market WHERE approved=? AND sold=0 ORDER BY id DESC LIMIT ?", (approved, limit))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"username":r[1],"price":r[2],"seller_uid":r[3],"approved":r[4],"created":r[5],"sold":r[6]} for r in rows]

def market_get(item_id):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,username,price,seller_uid,approved,created,sold FROM market WHERE id=?", (item_id,))
    r = c.fetchone(); conn.close()
    if not r: return None
    return {"id":r[0],"username":r[1],"price":r[2],"seller_uid":r[3],"approved":r[4],"created":r[5],"sold":r[6]}

def market_count(approved=1):
    conn = sqlite3.connect(DB); c = conn.cursor()
    cnt = c.execute("SELECT COUNT(*) FROM market WHERE approved=? AND sold=0", (approved,)).fetchone()[0]
    conn.close(); return cnt

def market_approve(i): conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE market SET approved=1 WHERE id=?",(i,));conn.commit();conn.close()
def market_reject(i): conn=sqlite3.connect(DB);c=conn.cursor();c.execute("DELETE FROM market WHERE id=?",(i,));conn.commit();conn.close()
def market_remove(i): market_reject(i)

def deal_create(market_id, buyer_uid, seller_uid, username, price):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("INSERT INTO deals (market_id,buyer_uid,seller_uid,username,price,status,created) VALUES (?,?,?,?,?,'pending',?)",
              (market_id, buyer_uid, seller_uid, username, price, datetime.now().strftime("%Y-%m-%d %H:%M")))
    did = c.lastrowid; conn.commit(); conn.close(); return did

def deal_get(did):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,market_id,buyer_uid,seller_uid,username,price,status,created,admin_confirmed,confirmed_by,notes FROM deals WHERE id=?", (did,))
    r = c.fetchone(); conn.close()
    if not r: return None
    return {"id":r[0],"market_id":r[1],"buyer_uid":r[2],"seller_uid":r[3],"username":r[4],"price":r[5],"status":r[6],"created":r[7],"admin_confirmed":r[8],"confirmed_by":r[9],"notes":r[10] or ""}

def deal_activate(did, aid):
    conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE deals SET status='active',admin_confirmed=1,confirmed_by=? WHERE id=?",(aid,did));conn.commit();conn.close()

def deal_complete(did):
    conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE deals SET status='completed' WHERE id=?",(did,))
    d=deal_get(did)
    if d: c.execute("UPDATE market SET sold=1 WHERE id=?",(d["market_id"],))
    conn.commit();conn.close()

def deal_cancel(did):
    conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE deals SET status='cancelled' WHERE id=?",(did,));conn.commit();conn.close()

def deal_reject(did):
    conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE deals SET status='rejected' WHERE id=?",(did,));conn.commit();conn.close()

def deal_get_active(uid):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,market_id,buyer_uid,seller_uid,username,price,status,created,admin_confirmed FROM deals WHERE (buyer_uid=? OR seller_uid=?) AND status IN ('pending','active')", (uid, uid))
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"market_id":r[1],"buyer_uid":r[2],"seller_uid":r[3],"username":r[4],"price":r[5],"status":r[6],"created":r[7],"admin_confirmed":r[8]} for r in rows]

def deal_list_pending():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute("SELECT id,market_id,buyer_uid,seller_uid,username,price,status,created FROM deals WHERE status='pending'")
    rows = c.fetchall(); conn.close()
    return [{"id":r[0],"market_id":r[1],"buyer_uid":r[2],"seller_uid":r[3],"username":r[4],"price":r[5],"status":r[6],"created":r[7]} for r in rows]

def generate_deal_txt(deal):
    buyer = get_user(deal["buyer_uid"]); seller = get_user(deal["seller_uid"])
    bid, sid = deal["buyer_uid"], deal["seller_uid"]
    bn, sn = buyer.get("uname","—"), seller.get("uname","—")
    return (f"СДЕЛКА #{deal['id']}\n\n@{deal['username']} — {deal['price']}\n"
            f"Статус: {deal['status']}\nСоздана: {deal['created']}\n\n"
            f"Покупатель: {bid} @{bn}\nПродавец: {sid} @{sn}\n"
            f"Админ: {'Да' if deal.get('admin_confirmed') else 'Нет'}\n")

def appeal_create(did, uid, reason):
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("INSERT INTO appeals (deal_id,uid,reason,status,created) VALUES (?,?,?,'pending',?)",(did,uid,reason,datetime.now().strftime("%Y-%m-%d %H:%M")))
    aid=c.lastrowid;conn.commit();conn.close();return aid

def appeal_get(aid):
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT id,deal_id,uid,reason,status,created,resolved_by FROM appeals WHERE id=?",(aid,))
    r=c.fetchone();conn.close()
    if not r: return None
    return {"id":r[0],"deal_id":r[1],"uid":r[2],"reason":r[3],"status":r[4],"created":r[5],"resolved_by":r[6]}

def appeal_list_pending():
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT id,deal_id,uid,reason,status,created FROM appeals WHERE status='pending'")
    rows=c.fetchall();conn.close()
    return [{"id":r[0],"deal_id":r[1],"uid":r[2],"reason":r[3],"status":r[4],"created":r[5]} for r in rows]

def appeal_resolve(aid, resolution, admin_uid=0):
    conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE appeals SET status=?,resolved_by=? WHERE id=?",(resolution,admin_uid,aid));conn.commit();conn.close()

def tiktok_can_submit(uid):
    today = datetime.now().strftime("%Y-%m-%d")
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT COUNT(*) FROM tasks WHERE uid=? AND created LIKE ?",(uid,today+"%"))
    cnt=c.fetchone()[0];conn.close();return cnt < TIKTOK_DAILY_LIMIT

def task_create(uid):
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT id FROM tasks WHERE uid=? AND status='pending'",(uid,))
    ex=c.fetchone()
    if ex: conn.close();return ex[0]
    c.execute("INSERT INTO tasks (uid,status,created) VALUES (?,'pending',?)",(uid,datetime.now().strftime("%Y-%m-%d %H:%M")))
    tid=c.lastrowid;conn.commit();conn.close();return tid

def task_approve(tid, admin_uid):
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'",(tid,))
    r=c.fetchone()
    if not r: conn.close();return None
    uid=r[0];c.execute("UPDATE tasks SET status='approved',reviewed_by=? WHERE id=?",(admin_uid,tid));conn.commit();conn.close();return uid

def task_reject(tid, admin_uid):
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT uid FROM tasks WHERE id=? AND status='pending'",(tid,))
    r=c.fetchone()
    if not r: conn.close();return None
    uid=r[0];c.execute("UPDATE tasks SET status='rejected',reviewed_by=? WHERE id=?",(admin_uid,tid));conn.commit();conn.close();return uid


# ═══════════════════════ ПРОВЕРКА ЮЗЕРНЕЙМОВ ═══════════════════════

async def check_subscribed(uid):
    if uid in ADMIN_IDS or not REQUIRED_CHANNELS:
        return []
    bad = []
    for ch in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(f"@{ch}", uid)
            if member.status in ("left", "kicked"):
                bad.append(ch)
        except Exception:
            pass
    return bad


async def check_username_http(username: str) -> str:
    """Сверхбыстрый и точный HTTP-чекер через t.me"""
    url = f"https://t.me/{username}"
    try:
        async with http_session.get(url, timeout=12) as resp:
            if resp.status != 200:
                return "taken"
            text = await resp.text()
            # Точные фразы с текущей версии Telegram Web (2025)
            if any(phrase in text for phrase in [
                "is available for claiming",
                "public username is free",
                "you can claim it in Telegram",
                "If you have <strong>Telegram</strong>, you can view and join"
            ]):
                return "free"
            return "taken"
    except:
        return "taken"

async def check_username_telethon(username: str) -> str:
    """Самый точный чекер через пользовательскую сессию"""
    if not telethon_client or not session_ready:
        return "taken"
    try:
        await telethon_client(ResolveUsernameRequest(username))
        return "taken"
    except UsernameNotOccupiedError:
        return "free"
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 5)
        return await check_username_telethon(username)
    except:
        return "taken"

async def verify_username_bot(username: str) -> bool:
    """Финальная 100% проверка через Bot API (только для найденных)"""
    try:
        await bot.get_chat(f"@{username}")
        return False
    except:
        return True  # free

async def check_fragment(username: str) -> str:
    """Точная проверка через официальный API Fragment"""
    url = f"https://fragment.com/api/v1/username/{username.lower()}"
    try:
        async with http_session.get(url, timeout=10) as resp:
            if resp.status == 404:
                return "unavailable"
            if resp.status != 200:
                return "unavailable"
            data = await resp.json()
            status = data.get("status", "")
            if status == "sold":
                return "sold"
            if status in ("auction", "available", "bidding"):
                return "fragment"
            return "unavailable"
    except:
        return "unavailable"

async def check_username(username: str) -> str:
    """Главный универсальный чекер"""
    if session_ready and USE_SESSION_SEARCH:
        return await check_username_telethon(username)
    else:
        return await check_username_http(username)


# ═══════════════════════ ГЕНЕРАТОРЫ ═══════════════════════

def gen_ordinary(length=5):
    return "".join(random.choices(string.ascii_lowercase, k=length))

def gen_beautiful(length=5):
    a = random.choice(string.ascii_lowercase)
    b = random.choice(string.ascii_lowercase)
    c = random.choice(string.ascii_lowercase)
    while b == a: b = random.choice(string.ascii_lowercase)
    while c in (a, b): c = random.choice(string.ascii_lowercase)
    
    if length == 5:
        pats = [f"{a}{a}{a}{b}{b}", f"{a}{b}{a}{b}{a}", f"{a}{a}{b}{b}{a}", 
                f"{a}{b}{c}{b}{a}", f"{a}{b}{b}{b}{a}", f"{a}{a}{b}{a}{a}"]
    elif length == 6:
        pats = [f"{a}{a}{a}{b}{b}{b}", f"{a}{b}{a}{b}{a}{b}", f"{a}{a}{b}{b}{a}{a}",
                f"{a}{b}{c}{c}{b}{a}", f"{a}{a}{a}{a}{b}{b}"]
    else:
        pats = [f"{a}{a}{a}{b}{b}{b}{b}", f"{a}{b}{a}{b}{a}{b}{a}", 
                f"{a}{b}{c}{a}{c}{b}{a}", f"{a}{a}{a}{b}{a}{a}{a}"]
    return random.choice(pats)[:length]

def gen_premium(length=5):
    v = "aeiou"
    cn = "bcdfghjklmnprstvwxyz"
    return "".join([random.choice(cn) if i % 2 == 0 else random.choice(v) for i in range(length)])

def gen_word(length=5):
    exact = [w for w in COMMON_WORDS if len(w) == length and w.isalpha()]
    if exact:
        return random.choice(exact)
    return gen_premium(length)

def _gen_themed(wd, length):
    suitable = []
    for words in wd.values():
        for w in words:
            if len(w) <= length and w.isalpha():
                suitable.append(w)
    if not suitable:
        return gen_premium(length)
    base = random.choice(suitable)
    rem = length - len(base)
    if rem == 0:
        return base
    suffix = "".join(random.choices(string.ascii_lowercase, k=rem))
    return base + suffix

def gen_crypto(l=5): return _gen_themed(CRYPTO_WORDS, l)
def gen_gaming(l=5): return _gen_themed(GAMING_WORDS, l)
def gen_tech(l=5): return _gen_themed(TECH_WORDS, l)

def gen_og(length=5):
    v = "aeiou"
    cn = "bcdfghjklmnprstvwxyz"
    p5 = ["CVCVC", "CVCCV", "CCVCV", "VCVCV", "CVVCV"]
    p6 = ["CVCCVC", "CVCVCV", "CCVCCV", "CVCCVV", "CCVCVC"]
    p7 = ["CVCVCVC", "CVCCVCV", "CCVCCVC", "CVCVCVV"]
    if length == 5: pat = random.choice(p5)
    elif length == 6: pat = random.choice(p6)
    else: pat = random.choice(p7)
    pat = pat[:length]
    while len(pat) < length:
        pat += "V" if pat[-1] == "C" else "C"
    return "".join(random.choice(cn) if ch == "C" else random.choice(v) for ch in pat)

GENERATORS = {
    "ordinary":{"func":gen_ordinary,"name":"🎲 Обычные","desc":"Простые комбинации","premium":False},
    "beautiful":{"func":gen_beautiful,"name":"💎 Красивые","desc":"Палиндромы, паттерны","premium":False},
    "premium":{"func":gen_premium,"name":"📖 Словарные","desc":"Произносимые слова","premium":False},
}


# ═══════════════════════ ОЦЕНКА ═══════════════════════

def evaluate_username(username):
    score=0;factors=[];u=username.lower().replace("_","");ln=len(u)
    if ln<=3: score+=95;factors.append("🔥 Ультракороткий")
    elif ln<=4: score+=80;factors.append("💎 Очень короткий")
    elif ln<=5: score+=60;factors.append("✨ Короткий")
    elif ln<=6: score+=30;factors.append("📏 Средний")
    else: score+=10;factors.append("📦 Длинный")
    if len(set(u))==1 and ln>=3: score+=90;factors.append("🔥 Моно")
    if u==u[::-1] and ln>=3: score+=40;factors.append("🪞 Палиндром")
    if u in COMMON_WORDS: score+=80;factors.append("📖 Слово")
    if u.isalpha() and "_" not in username: score+=15;factors.append("🔤 Буквы")
    if not any(ch.isdigit() for ch in u): score+=10;factors.append("🅰️ Без цифр")
    score=min(score,200)
    if score>=150: pr,ra="$3k+","🔥🔥🔥 ЛЕГЕНДА"
    elif score>=100: pr,ra="$500-$3k","💎💎 РЕДКИЙ"
    elif score>=70: pr,ra="$100-$500","💎 ХОРОШИЙ"
    elif score>=40: pr,ra="$20-$100","✨ СТАНДАРТ"
    else: pr,ra="$1-$20","📦 ОБЫЧНЫЙ"
    filled=min(score//20,10)
    return {"score":score,"bar":"▓"*filled+"░"*(10-filled),"factors":factors,"price":pr,"rarity":ra}


# ═══════════════════════ ПОИСК ═══════════════════════
async def verify_username_bot(username):
    """Финальная проверка через Bot API - самая точная"""
    try:
        chat = await bot.get_chat(f"@{username}")
        return False  # Существует = занят
    except Exception as e:
        err = str(e).lower()
        if "not found" in err or "chat not found" in err:
            return True  # Не найден = свободен!
        if "deactivated" in err:
            return False  # Деактивирован но занят
        return False  # Любая другая ошибка = считаем занятым

async def do_search(count, gen_func, msg, mode_name, uid, length):
    found = []
    attempts = 0
    start_time = time.time()
    max_attempts = 1500

    while len(found) < count and attempts < max_attempts:
        # Генерируем батч
        batch_size = SEARCH_BATCH if session_ready else HTTP_BATCH_SIZE
        batch = []
        for _ in range(batch_size):
            username = gen_func(length) if 'length' in str(gen_func) else gen_func()
            attempts += 1
            username = username.lower()
            if len(username) == length and re.match(r'^[a-z0-9_]+$', username) and username.isalnum() or '_' in username:
                batch.append(username)

        if not batch:
            await asyncio.sleep(0.1)
            continue

        # Проверяем батч
        if session_ready:
            results = []
            for u in batch:
                res = await check_username(u)
                results.append(res)
                await asyncio.sleep(0.3)  # защита от флуда
        else:
            results = await asyncio.gather(*[check_username(u) for u in batch], return_exceptions=False)

        # Обрабатываем результаты
        for username, result in zip(batch, results):
            if result == "free":
                # Финальная проверка Bot API — 100% точность
                if await verify_username_bot(username):
                    found.append(username)
                    save_history(uid, username, mode_name, length)
                    if len(found) >= count:
                        break

        # Обновляем сообщение
        elapsed = int(time.time() - start_time)
        try:
            await msg.edit_text(
                f"🔎 **{mode_name}** ({length} букв)\n\n"
                f"📊 Проверено: `{attempts}`\n"
                f"✅ Найдено: `{len(found)}/{count}`\n"
                f"⏱ {elapsed}сек",
                parse_mode="Markdown"
            )
        except:
            pass

        if len(found) < count:
            await asyncio.sleep(0 if not session_ready else SEARCH_DELAY)

    elapsed = int(time.time() - start_time)
    stats = {
        "attempts": attempts,
        "elapsed": elapsed,
        "taken": attempts - len(found),
        "errors": 0
    }
    return found, stats

# ═══════════════════════ НАПОМИНАНИЯ (фоновая задача) ═══════════════════════

async def reminder_loop():
    """Фоновая задача: проверяет истекающие подписки и шлёт напоминания"""
    while True:
        try:
            await asyncio.sleep(REMINDER_CHECK_INTERVAL)
            today_str = datetime.now().strftime("%Y-%m-%d")

            for days_before in REMINDER_DAYS:
                users = get_expiring_users(days_before)
                for u in users:
                    uid = u["uid"]
                    reminder_key = f"{today_str}_d{days_before}"
                    # Не отправлять повторно
                    if reminder_key in u.get("last_reminder", ""):
                        continue

                    auto_on, plan = get_auto_renew(uid)
                    kb = InlineKeyboardBuilder()

                    if auto_on and plan:
                        p = PRICES.get(plan)
                        if p:
                            text = (
                                f"🔔 **Подписка истекает через {days_before} дн!**\n\n"
                                f"⏰ До: {u['sub_end']}\n"
                                f"🔄 Авто-продление: **ВКЛ** ({p['label']})\n\n"
                                f"Подписка продлится автоматически через Stars.\n"
                                f"Чтобы отключить — зайдите в профиль."
                            )
                            # Отправляем инвойс для авто-продления
                            kb.button(text="💰 Продлить сейчас", callback_data=f"buy_{plan}")
                            kb.button(text="⚙️ Выключить авто-продление", callback_data="toggle_renew")
                        else:
                            text = f"🔔 Подписка истекает через {days_before} дн!\n⏰ До: {u['sub_end']}"
                            kb.button(text="💰 Продлить", callback_data="prices")
                    else:
                        text = (
                            f"🔔 **Подписка истекает через {days_before} дн!**\n\n"
                            f"⏰ До: {u['sub_end']}\n\n"
                            f"🔥 **АКЦИЯ:** Навсегда за 1999₽ / 1599⭐!\n\n"
                            f"💡 Включите авто-продление в профиле,\n"
                            f"чтобы не потерять доступ!"
                        )
                        kb.button(text="💰 Продлить", callback_data="prices")
                        kb.button(text="🔄 Вкл. авто-продление", callback_data="toggle_renew")

                    kb.adjust(1)
                    try:
                        await bot.send_message(uid, text, reply_markup=kb.as_markup(), parse_mode="Markdown")
                        set_last_reminder(uid, reminder_key)
                        logger.info(f"🔔 Напоминание: {uid} ({days_before} дн)")
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"Reminder error: {e}")


# ═══════════════════════ МЕНЮ ═══════════════════════

def _disp(uid_val, uname_val):
    return "@"+uname_val if uname_val else "ID:"+str(uid_val)

def build_sub_kb(channels):
    text="📢 **Подпишитесь для использования бота:**\n\n"
    kb=InlineKeyboardBuilder()
    for ch in channels: text+=f"  ❌ @{ch}\n";kb.button(text=f"📢 @{ch}",url=f"https://t.me/{ch}")
    text+="\n✅ За подписку **+2 бесплатных поиска!**"
    kb.button(text="✅ Проверить",callback_data="check_sub");kb.adjust(1)
    return text,kb.as_markup()

def build_menu(uid):
    u=get_user(uid);is_admin=uid in ADMIN_IDS
    if is_admin: si,st,sub_info,cnt="👑","ADMIN","♾",PREMIUM_COUNT
    elif has_subscription(uid): si,st="💎","PREMIUM";sub_info="до "+u.get("sub_end","?");cnt=PREMIUM_COUNT
    elif u.get("free",0)>0: si,st="🆓","FREE";sub_info=str(u.get("free",0))+" поисков";cnt=FREE_COUNT
    else: si,st,sub_info,cnt="⛔️","ЛИМИТ","закончились",0

    ar_on,_=get_auto_renew(uid)
    ar_text = "🔄 Авто: ВКЛ" if ar_on else ""
    method = "🔗 Сессия" if (USE_SESSION_SEARCH and session_ready) else "🌐 HTTP"

    text=(f"🔍 **USERNAME HUNTER** {si}\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"📌 **{st}** | 🎯 `{cnt}` юзов/поиск\n📊 {sub_info} {ar_text}\n"
          f"🔢 Поисков: `{u.get('searches',0)}` | {method}\n\n"
          f"🔥 **АКЦИЯ: Навсегда за 1999₽ / 1599⭐!**\n"
          f"━━━━━━━━━━━━━━━━━━━━━━━")
    kb=InlineKeyboardBuilder()
    kb.button(text="🔍 Поиск",callback_data="search")
    kb.button(text="📊 Оценка",callback_data="evaluate")
    kb.button(text="🛒 Маркет",callback_data="market")
    kb.button(text="🔧 Утилиты",callback_data="utils")
    kb.button(text="👤 Профиль",callback_data="profile")
    kb.button(text="💰 Premium",callback_data="prices")
    kb.button(text="👥 Рефералы",callback_data="referral")
    kb.button(text="🎁 TikTok",callback_data="tiktok")
    kb.button(text="🔑 Ключ",callback_data="activate")
    if is_admin: kb.button(text="👑 Админ",callback_data="admin")
    kb.adjust(2,2,2,2,1,1)
    return text,kb.as_markup()


# ═══════════════════════ ХЭНДЛЕРЫ ═══════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, command: CommandObject):
    uid=msg.from_user.id;uname=msg.from_user.username or ""
    is_new=get_user(uid).get("searches",0)==0;ensure_user(uid,uname)
    if command.args and command.args.startswith("ref_") and is_new:
        try:
            ref_id=int(command.args.replace("ref_",""))
            if process_referral(uid,ref_id):
                await msg.answer(f"🎉 **+{REF_BONUS} поиска** по приглашению!",parse_mode="Markdown")
                try: await bot.send_message(ref_id,f"🎉 Новый реферал! **+{REF_BONUS}**",parse_mode="Markdown")
                except: pass
        except: pass
    ns=await check_subscribed(uid)
    text,kb=(build_sub_kb(ns) if ns else build_menu(uid))
    await msg.answer(text,reply_markup=kb,parse_mode="Markdown")

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    kb=InlineKeyboardBuilder();kb.button(text="🔙 Меню",callback_data="menu")
    await msg.answer(f"📖 **Справка**\n\n🔍 Поиск\n📊 Оценка\n🛒 Маркет\n🔧 Утилиты\n👤 Профиль\n💰 Premium\n👥 Рефералы\n🎁 TikTok\n🔑 Ключ\n\n📩 @{ADMIN_CONTACT}",
                     reply_markup=kb.as_markup(),parse_mode="Markdown")

@dp.message(Command("id"))
async def cmd_id(msg: Message):
    await msg.answer(f"🆔 `{msg.from_user.id}`",parse_mode="Markdown")

@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: Message):
    uid=msg.from_user.id;ensure_user(uid,msg.from_user.username)
    state=user_states.get(uid,{});action=state.get("action")

    if action=="admin_broadcast_text":
        if uid not in ADMIN_IDS: user_states.pop(uid,None);return
        user_states.pop(uid,None);bt=msg.text.strip()
        conn=sqlite3.connect(DB);c=conn.cursor();c.execute("SELECT uid FROM users");aus=[r[0] for r in c.fetchall()];conn.close()
        s,f=0,0;sm=await msg.answer(f"📤 0/{len(aus)}")
        for i,tu in enumerate(aus):
            try: await bot.send_message(tu,bt,parse_mode="Markdown");s+=1
            except: f+=1
            if (i+1)%50==0:
                try: await sm.edit_text(f"📤 {i+1}/{len(aus)} ✅{s} ❌{f}")
                except: pass
            await asyncio.sleep(0.05)
        try: await sm.edit_text(f"✅ Готово! ✅{s} ❌{f}")
        except: pass
        return

    if action=="activate":
        user_states.pop(uid,None);r=activate_key(uid,msg.text.strip())
        if r: await msg.answer(f"🎉 **Активировано!** {r['days']} дн до {r['end']}",parse_mode="Markdown")
        else: await msg.answer("❌ Неверный ключ")
        t,k=build_menu(uid);await msg.answer(t,reply_markup=k,parse_mode="Markdown");return

    if action=="evaluate":
        user_states.pop(uid,None);un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌ Некорректный (мин 5 символов)");return
        ev=evaluate_username(un);tg=await check_username(un);fr=await check_fragment(un)
        tgs={"free":"✅ Свободен","taken":"❌ Занят","error":"⚠️ Ошибка"}.get(tg,"❓")
        frs={"fragment":"💎 Fragment","sold":"✅ Продан","unavailable":"—"}.get(fr,"❓")
        fac="\n".join(f"  {f}" for f in ev["factors"]) or "  —"
        kb=InlineKeyboardBuilder()
        if tg=="free": kb.button(text="⭐ Избранное",callback_data=f"fav_add_{un}")
        kb.button(text="📊 Ещё",callback_data="evaluate");kb.button(text="🔙",callback_data="menu");kb.adjust(2,1)
        await msg.answer(f"📊 **@{un}**\n\n📱 {tgs}\n💎 {frs}\n\n🏷 **{ev['rarity']}** | 💰 **{ev['price']}**\n[{ev['bar']}] `{ev['score']}/200`\n\n{fac}",
                         reply_markup=kb.as_markup(),parse_mode="Markdown");return

    if action=="quick_check":
        user_states.pop(uid,None);un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌ Некорректный");return
        tg=await check_username(un);st={"free":"✅ Свободен!","taken":"❌ Занят","error":"⚠️"}.get(tg,"❓")
        kb=InlineKeyboardBuilder()
        if tg=="free": kb.button(text="⭐",callback_data=f"fav_add_{un}")
        kb.button(text="🔍 Ещё",callback_data="util_check");kb.button(text="🔙",callback_data="menu");kb.adjust(2,1)
        await msg.answer(f"🔍 **@{un}** — {st}",reply_markup=kb.as_markup(),parse_mode="Markdown");return

    if action=="mass_check":
        user_states.pop(uid,None);names=[n.strip().replace("@","").lower() for n in msg.text.split("\n") if validate_username(n.strip().replace("@","").lower())][:20]
        if not names: await msg.answer("❌ Нет валидных");return
        wm=await msg.answer(f"⏳ Проверяю {len(names)}...");results=await asyncio.gather(*[check_username(n) for n in names])
        fc=sum(1 for r in results if r=="free")
        text=f"📋 **({len(names)})** ✅{fc} ❌{len(names)-fc}\n\n"
        for i,r in enumerate(results): text+=("✅" if r=="free" else "❌")+f" @{names[i]}\n"
        kb=InlineKeyboardBuilder();kb.button(text="📋 Ещё",callback_data="util_mass");kb.button(text="🔙",callback_data="menu");kb.adjust(1)
        try: await wm.delete()
        except: pass
        await msg.answer(text,reply_markup=kb.as_markup(),parse_mode="Markdown");return

    if action=="market_username":
        un=msg.text.strip().replace("@","").lower()
        if not validate_username(un): await msg.answer("❌ Некорректный");return
        user_states[uid]={"action":"market_price","username":un,"currency":state["currency"]}
        await msg.answer(f"💰 Цена для @{un} ({state['currency']}):");return

    if action=="market_price":
        un=state["username"];cur=state["currency"];pr=msg.text.strip().replace("₽","").replace("$","").replace(" ","")
        if not pr.isdigit(): await msg.answer("❌ Число!");return
        price=pr+cur;user_states.pop(uid,None);app=1 if uid in ADMIN_IDS else 0;iid=market_add(un,price,uid,app)
        if app: await msg.answer(f"✅ @{un} — {price} опубликован!")
        else:
            await msg.answer(f"⏳ @{un} — {price} на модерации")
            for aid in ADMIN_IDS:
                try:
                    akb=InlineKeyboardBuilder();akb.button(text="✅",callback_data=f"mka_{iid}");akb.button(text="❌",callback_data=f"mkr_{iid}");akb.adjust(2)
                    sd=_disp(uid,get_user(uid).get("uname",""));await bot.send_message(aid,f"📩 #{iid} @{un} — {price} от {sd}",reply_markup=akb.as_markup())
                except: pass
        t,k=build_menu(uid);await msg.answer(t,reply_markup=k,parse_mode="Markdown");return

    if action=="appeal_reason":
        did=state["deal_id"];user_states.pop(uid,None);aid=appeal_create(did,uid,msg.text.strip()[:500])
        await msg.answer(f"🚨 Апелляция #{aid} создана!")
        d=deal_get(did);un=d["username"] if d else "?"
        for admin in ADMIN_IDS:
            try:
                akb=InlineKeyboardBuilder();akb.button(text="✅ Покупателю",callback_data=f"appeal_buyer_{aid}");akb.button(text="✅ Продавцу",callback_data=f"appeal_seller_{aid}");akb.adjust(1)
                await bot.send_message(admin,f"🚨 #{aid} сделка#{did} @{un}\n{msg.text[:200]}",reply_markup=akb.as_markup())
            except: pass
        return

    if action=="admin_give_user":
        inp=msg.text.strip();target=None
        if inp.isdigit(): target=int(inp)
        else:
            conn=sqlite3.connect(DB);c=conn.cursor();c.execute("SELECT uid FROM users WHERE uname=?",(inp.replace("@",""),));r=c.fetchone();conn.close()
            target=r[0] if r else None
        if not target: await msg.answer("❌ Не найден");return
        user_states[uid]={"action":"admin_give_days","target":target};await msg.answer(f"📅 Дней для `{target}`?",parse_mode="Markdown");return

    if action=="admin_give_days":
        try: days=int(msg.text.strip());assert days>0
        except: await msg.answer("❌ Число!");return
        target=state["target"];user_states.pop(uid,None);end=give_subscription(target,days)
        await msg.answer(f"✅ {days}дн для `{target}` до {end}",parse_mode="Markdown")
        try: await bot.send_message(target,f"🎉 Подписка **{days}дн** до **{end}**!",parse_mode="Markdown")
        except: pass
        t,k=build_menu(uid);await msg.answer(t,reply_markup=k,parse_mode="Markdown");return

    if action=="admin_key_days":
        try: days=int(msg.text.strip());assert days>0
        except: await msg.answer("❌ Число!");return
        user_states.pop(uid,None);key=generate_key(days,f"D{days}")
        tariff=f"{days}дн"
        for p in PRICES.values():
            if p["days"]==days: tariff=p["label"];break
        await msg.answer(f"🔑 `{key}` — {tariff}",parse_mode="Markdown")
        bu=bot_info.username if bot_info else "bot"
        await bot.send_message(uid,f"🎁 **КЛЮЧ** {tariff}\n🔑 `{key}`\nАктивируй → @{bu}",parse_mode="Markdown");return

    if action=="auto_renew_plan":
        user_states.pop(uid,None)
        plan=msg.text.strip().lower()
        if plan not in PRICES:
            await msg.answer("❌ Неверный тариф");return
        set_auto_renew(uid,True,plan)
        p=PRICES[plan]
        await msg.answer(f"🔄 **Авто-продление включено!**\n\n📌 Тариф: **{p['label']}**\n⭐ {p['stars']} Stars\n\nЗа 3 и 1 день до окончания подписки\nбот напомнит и предложит оплату.\n\nВыключить: Профиль → ⚙️",parse_mode="Markdown")
        t,k=build_menu(uid);await msg.answer(t,reply_markup=k,parse_mode="Markdown");return

    ns=await check_subscribed(uid)
    text,kb=(build_sub_kb(ns) if ns else build_menu(uid))
    await msg.answer(text,reply_markup=kb,parse_mode="Markdown")

@dp.message(F.photo)
async def handle_photo(msg: Message):
    uid=msg.from_user.id;state=user_states.get(uid,{})
    if state.get("action")!="tiktok_proof": return
    tid=state.get("task_id");photos=state.get("photos",0)+1;user_states[uid]["photos"]=photos
    for aid in ADMIN_IDS:
        try: await msg.forward(aid)
        except: pass
    if photos<TIKTOK_SCREENSHOTS_NEEDED:
        await msg.answer(f"📸 {photos}/{TIKTOK_SCREENSHOTS_NEEDED}");return
    user_states.pop(uid,None);await msg.answer(f"✅ Все {TIKTOK_SCREENSHOTS_NEEDED} скринов! Ожидайте.")
    for aid in ADMIN_IDS:
        try:
            akb=InlineKeyboardBuilder();akb.button(text="✅",callback_data=f"ta_{tid}");akb.button(text="❌",callback_data=f"tr_{tid}");akb.adjust(2)
            await bot.send_message(aid,f"📱 TikTok #{tid} от {uid} ({photos} скринов)",reply_markup=akb.as_markup())
        except: pass


# ═══════════════════════ CALLBACKS ═══════════════════════

@dp.callback_query(F.data=="check_sub")
async def cb_cs(cb: CallbackQuery):
    uid=cb.from_user.id;ns=await check_subscribed(uid)
    if ns:
        t,k=build_sub_kb(ns);await cb.answer("❌",show_alert=True)
        try: await cb.message.edit_text(t,reply_markup=k,parse_mode="Markdown")
        except: pass
        return
    u=get_user(uid)
    if u.get("sub_bonus",0)==0:
        conn=sqlite3.connect(DB);c=conn.cursor();c.execute("UPDATE users SET free=free+2,sub_bonus=1 WHERE uid=?",(uid,));conn.commit();conn.close()
        await cb.answer("✅ +2 поиска!",show_alert=True)
    else: await cb.answer("✅")
    t,k=build_menu(uid)
    try: await cb.message.edit_text(t,reply_markup=k,parse_mode="Markdown")
    except: pass

@dp.callback_query(F.data=="menu")
async def cb_menu(cb: CallbackQuery):
    user_states.pop(cb.from_user.id,None);t,k=build_menu(cb.from_user.id)
    try: await cb.message.edit_text(t,reply_markup=k,parse_mode="Markdown")
    except: pass
    await cb.answer()

@dp.callback_query(F.data=="search")
async def cb_search(cb: CallbackQuery):
    uid=cb.from_user.id
    if not can_search(uid):
        kb=InlineKeyboardBuilder();kb.button(text="💰 Premium",callback_data="prices");kb.button(text="👥 Рефералы",callback_data="referral");kb.button(text="🔙",callback_data="menu");kb.adjust(1)
        await cb.message.edit_text("⛔️ **Поиски закончились!**\n\n🔥 Навсегда за 1999₽!",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer();return
    kb=InlineKeyboardBuilder();kb.button(text="5️⃣",callback_data="len_5");kb.button(text="6️⃣",callback_data="len_6");kb.button(text="7️⃣",callback_data="len_7");kb.button(text="🔙",callback_data="menu");kb.adjust(3,1)
    is_prem=uid in ADMIN_IDS or has_subscription(uid);cnt=PREMIUM_COUNT if is_prem else FREE_COUNT
    fl="♾" if is_prem else str(get_user(uid).get("free",0))
    method = "🔗 Сессия" if (USE_SESSION_SEARCH and session_ready) else "🌐 HTTP"
    await cb.message.edit_text(f"🔍 **Длина:**\n🎯 `{cnt}` юзов | Осталось: **{fl}** | {method}",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data.startswith("len_"))
async def cb_len(cb: CallbackQuery):
    uid=cb.from_user.id;length=int(cb.data.replace("len_",""));is_prem=uid in ADMIN_IDS or has_subscription(uid)
    kb=InlineKeyboardBuilder()
    for key,gen in GENERATORS.items():
        if gen.get("premium") and not is_prem: kb.button(text=f"🔒 {gen['name']}",callback_data="need_prem")
        else: kb.button(text=gen["name"],callback_data=f"go_{key}_{length}")
    kb.button(text="🔙",callback_data="search");kb.button(text="🔙 Меню",callback_data="menu");kb.adjust(2)
    mt=""
    for key,gen in GENERATORS.items():
        lk="🔒" if gen.get("premium") and not is_prem else "✅"
        mt+=f"{lk} **{gen['name']}** — {gen.get('desc','')}\n"
    await cb.message.edit_text(f"🔍 **{length} букв:**\n\n{mt}",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="need_prem")
async def cb_np(cb: CallbackQuery): await cb.answer("🔒 Нужен Premium!\n🔥 Навсегда за 1999₽!",show_alert=True)

@dp.callback_query(F.data.regexp(r"^go_\w+_\d+$"))
async def cb_go(cb: CallbackQuery):
    uid=cb.from_user.id
    if not can_search(uid): await cb.answer("⛔️",show_alert=True);return
    parts=cb.data.split("_");mode=parts[1];length=int(parts[2])
    gi=GENERATORS.get(mode)
    if not gi: await cb.answer("❌",show_alert=True);return
    if gi.get("premium") and uid not in ADMIN_IDS and not has_subscription(uid): await cb.answer("🔒",show_alert=True);return
    await cb.answer();method="🔗" if (USE_SESSION_SEARCH and session_ready) else "🌐"
    await cb.message.edit_text(f"🚀 **{gi['name']}** ({length} букв) {method}...")
    use_search(uid);gf=gi.get("func",gen_ordinary);gen_func=lambda:gf(length)
    count=get_search_count(uid);found,stats=await do_search(count,gen_func,cb.message,gi["name"],uid,length)
    kb=InlineKeyboardBuilder()
    if found:
        lines=""
        for i,u in enumerate(found,1): ev=evaluate_username(u);lines+=f"  {i}. `@{u}` — {ev['rarity']}\n"
        text=f"✅ **{len(found)}**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n{lines}\n📊 `{stats['attempts']}` ❌`{stats['taken']}` ⏱`{stats['elapsed']}с`"
        for u in found: kb.button(text=f"⭐ @{u}",callback_data=f"fav_add_{u}")
    else: text=f"😔 Не найдено | `{stats['attempts']}` | ⏱`{stats['elapsed']}с`"
    if can_search(uid): kb.button(text="🔄 Ещё",callback_data=cb.data)
    kb.button(text="🔍 Режим",callback_data=f"len_{length}");kb.button(text="🔙",callback_data="menu");kb.adjust(2,1,1)
    try: await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown")
    except: pass

@dp.callback_query(F.data=="evaluate")
async def cb_eval(cb: CallbackQuery):
    user_states[cb.from_user.id]={"action":"evaluate"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="menu")
    await cb.message.edit_text("📊 Юзернейм:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data=="utils")
async def cb_utils(cb: CallbackQuery):
    kb=InlineKeyboardBuilder();kb.button(text="🔍 Проверка",callback_data="util_check");kb.button(text="📋 Массовая",callback_data="util_mass")
    kb.button(text="⭐ Избранное",callback_data="util_favs");kb.button(text="📜 История",callback_data="util_history")
    kb.button(text="📥 Экспорт",callback_data="util_export");kb.button(text="🔙",callback_data="menu");kb.adjust(2,2,1,1)
    await cb.message.edit_text("🔧 **Утилиты**",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="util_check")
async def cb_uc(cb: CallbackQuery):
    user_states[cb.from_user.id]={"action":"quick_check"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="utils")
    await cb.message.edit_text("🔍 Юзернейм:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data=="util_mass")
async def cb_um(cb: CallbackQuery):
    user_states[cb.from_user.id]={"action":"mass_check"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="utils")
    await cb.message.edit_text("📋 По строке (макс 20):",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data=="util_favs")
async def cb_uf(cb: CallbackQuery):
    uid=cb.from_user.id;favs=get_favorites(uid);kb=InlineKeyboardBuilder()
    if favs:
        text=f"⭐ **({len(favs)})**\n\n"
        for f in favs: text+=f"• `@{f}`\n";kb.button(text=f"❌ {f}",callback_data=f"fav_rm_{f}")
    else: text="⭐ Пусто"
    kb.button(text="🔙",callback_data="utils");kb.adjust(2,1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data.startswith("fav_add_"))
async def cb_fa(cb: CallbackQuery): add_favorite(cb.from_user.id,cb.data.replace("fav_add_",""));await cb.answer("⭐ Добавлено!",show_alert=True)
@dp.callback_query(F.data.startswith("fav_rm_"))
async def cb_fr(cb: CallbackQuery): remove_favorite(cb.from_user.id,cb.data.replace("fav_rm_",""));await cb.answer("❌");await cb_uf(cb)

@dp.callback_query(F.data=="util_history")
async def cb_uh(cb: CallbackQuery):
    uid=cb.from_user.id;hist=get_history(uid);kb=InlineKeyboardBuilder()
    text=f"📜 **({len(hist)})**\n\n" if hist else "📜 Пусто"
    for h in hist[:15]: text+=f"• `@{h[0]}` {h[2]} {h[1]}\n"
    kb.button(text="📥 TXT",callback_data="util_export");kb.button(text="🔙",callback_data="utils");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="util_export")
async def cb_ue(cb: CallbackQuery):
    uid=cb.from_user.id;hist=get_history(uid,100)
    if not hist: await cb.answer("Пусто!",show_alert=True);return
    content="ИСТОРИЯ\n\n"
    for i,h in enumerate(hist,1): content+=f"{i}. @{h[0]} | {h[2]} | {h[1]}\n"
    await cb.answer();await bot.send_document(uid,BufferedInputFile(content.encode(),filename=f"history_{uid}.txt"),caption="📥")

# ── Профиль с авто-продлением ──

@dp.callback_query(F.data=="profile")
async def cb_profile(cb: CallbackQuery):
    uid=cb.from_user.id;u=get_user(uid)
    if uid in ADMIN_IDS: status="👑 Админ ♾"
    elif has_subscription(uid): status="💎 Premium до "+u.get("sub_end","?")
    elif u.get("free",0)>0: status="🆓 "+str(u.get("free",0))+" поисков"
    else: status="⛔️"
    ar_on,ar_plan=get_auto_renew(uid)
    ar_text="🔄 Авто-продление: **ВКЛ**" if ar_on else "🔄 Авто-продление: ВЫКЛ"
    if ar_on and ar_plan and ar_plan in PRICES:
        ar_text+=f" ({PRICES[ar_plan]['label']})"
    deals=deal_get_active(uid);favs=get_favorites(uid)
    text=(f"👤 **Профиль**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🆔 `{uid}`\n📌 {status}\n📊 Поисков: `{u.get('searches',0)}`\n"
          f"👥 Рефералов: `{u.get('ref_count',0)}`\n⭐ Избранное: `{len(favs)}`\n\n"
          f"{ar_text}\n\n"
          f"💡 _Авто-продление через Stars:\nбот напомнит за 3 и 1 день\nи предложит оплату. Можно выключить._")
    kb=InlineKeyboardBuilder()
    if ar_on: kb.button(text="🔄 Выключить авто-продление",callback_data="toggle_renew")
    else: kb.button(text="🔄 Включить авто-продление",callback_data="toggle_renew")
    kb.button(text="⭐ Избранное",callback_data="util_favs");kb.button(text="📜 История",callback_data="util_history")
    if deals: kb.button(text=f"🤝 Сделки ({len(deals)})",callback_data="my_deals")
    kb.button(text="🔙 Меню",callback_data="menu");kb.adjust(1,2,1,1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="toggle_renew")
async def cb_toggle_renew(cb: CallbackQuery):
    uid=cb.from_user.id;ar_on,_=get_auto_renew(uid)
    if ar_on:
        set_auto_renew(uid,False,"")
        await cb.answer("🔄 Авто-продление ВЫКЛЮЧЕНО",show_alert=True)
        await cb_profile(cb)
    else:
        # Показываем выбор тарифа
        kb=InlineKeyboardBuilder()
        for k,p in PRICES.items():
            if p["days"]<99999:  # Не "навсегда"
                kb.button(text=f"{p['label']} ({p['stars']}⭐)",callback_data=f"set_renew_{k}")
        kb.button(text="❌ Отмена",callback_data="profile");kb.adjust(1)
        await cb.message.edit_text(
            "🔄 **Авто-продление через Stars**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Выберите тариф для автоматического\nпродления подписки.\n\n"
            "📌 Как работает:\n"
            "• За 3 и 1 день до окончания\n  бот пришлёт напоминание\n"
            "• Предложит оплатить через Stars\n"
            "• Можно выключить в любой момент\n\n"
            "Выберите тариф:",
            reply_markup=kb.as_markup(),parse_mode="Markdown")
        await cb.answer()

@dp.callback_query(F.data.startswith("set_renew_"))
async def cb_set_renew(cb: CallbackQuery):
    plan=cb.data.replace("set_renew_","")
    if plan not in PRICES: await cb.answer("❌",show_alert=True);return
    set_auto_renew(cb.from_user.id,True,plan)
    p=PRICES[plan]
    await cb.answer(f"🔄 Авто-продление: {p['label']}",show_alert=True)
    await cb_profile(cb)

@dp.callback_query(F.data=="my_deals")
async def cb_md(cb: CallbackQuery):
    uid=cb.from_user.id;deals=deal_get_active(uid);kb=InlineKeyboardBuilder()
    if deals:
        text=f"🤝 **({len(deals)})**\n\n"
        for d in deals:
            role="👤" if d["buyer_uid"]==uid else "📤";si="⏳" if d["status"]=="pending" else "🟢"
            text+=f"{si} #{d['id']} `@{d['username']}` {role}\n";kb.button(text=f"📋 #{d['id']}",callback_data=f"deal_{d['id']}")
    else: text="🤝 Нет"
    kb.button(text="🔙",callback_data="profile");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="referral")
async def cb_ref(cb: CallbackQuery):
    uid=cb.from_user.id;u=get_user(uid);bu=bot_info.username if bot_info else "bot"
    link=f"https://t.me/{bu}?start=ref_{uid}";rc=u.get("ref_count",0)
    kb=InlineKeyboardBuilder();kb.button(text="📤 Поделиться",url=f"https://t.me/share/url?url={link}");kb.button(text="🔙",callback_data="menu");kb.adjust(1)
    await cb.message.edit_text(f"👥 **Рефералы**\n\n+{REF_BONUS} за друга | 👥 `{rc}`\n\n🔗 `{link}`",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="activate")
async def cb_act(cb: CallbackQuery):
    user_states[cb.from_user.id]={"action":"activate"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="menu")
    await cb.message.edit_text("🔑 Ключ:",reply_markup=kb.as_markup());await cb.answer()

# ── Оплата с акцией ──

@dp.callback_query(F.data=="prices")
async def cb_prices(cb: CallbackQuery):
    text=(f"💰 **Premium**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🔥🔥🔥 **АКЦИЯ!** 🔥🔥🔥\n"
          f"Подписка **НАВСЕГДА** — `1999₽` / `1599⭐`\n"
          f"~~Было: 12999₽~~ → **Скидка 85%!**\n\n"
          f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"🔓 {PREMIUM_COUNT} юзов/поиск, все режимы\n"
          f"🔄 Авто-продление через Stars\n\n📋 Тарифы:\n\n")
    for p in PRICES.values():
        text+=f"• **{p['label']}** — `{p['rub']}₽` / `{p['stars']}⭐`\n  _{p.get('desc','')}_\n"
    kb=InlineKeyboardBuilder()
    kb.button(text="⭐ Stars (моментально)",callback_data="pay_stars")
    kb.button(text="💳 FunPay (₽)",callback_data="pay_funpay")
    kb.button(text="🔙",callback_data="menu");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="pay_stars")
async def cb_ps(cb: CallbackQuery):
    kb=InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=f"{p['label']} — {p['stars']}⭐",callback_data=f"buy_{k}")
    kb.button(text=f"📩 @{ADMIN_CONTACT}",url=f"https://t.me/{ADMIN_CONTACT}");kb.button(text="🔙",callback_data="prices");kb.adjust(1)
    await cb.message.edit_text("⭐ **Stars**\n\n🔄 После оплаты можно включить\nавто-продление в профиле!\n\nВыберите:",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="pay_funpay")
async def cb_pf(cb: CallbackQuery):
    uid_val=cb.from_user.id;kb=InlineKeyboardBuilder()
    for k,p in PRICES.items():
        if p.get("funpay"): kb.button(text=f"{p['label']} — {p['rub']}₽",url=p["funpay"])
        else: kb.button(text=f"{p['label']} ({p['rub']}₽)",url=f"https://t.me/{ADMIN_CONTACT}")
    kb.button(text="🔙",callback_data="prices");kb.adjust(1)
    await cb.message.edit_text(
        f"💳 **FunPay**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔥 Навсегда за **1999₽** — скидка 85%!\n\n"
        f"После оплаты напишите админу ID.\n🆔 Ваш: `{uid_val}`\n\n"
        f"⚠️ Авто-продление через FunPay недоступно.\n"
        f"Используйте ⭐ Stars для авто-продления!",
        reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def cb_buy(cb: CallbackQuery):
    key=cb.data.replace("buy_","");p=PRICES.get(key)
    if not p: await cb.answer("❌",show_alert=True);return
    await cb.answer()
    await bot.send_invoice(cb.from_user.id,title=f"💎 {p['label']}",
        description=f"Premium {p['label']}. {PREMIUM_COUNT} юзов/поиск. Все режимы.",
        payload=f"sub_{key}_{cb.from_user.id}",provider_token="",currency="XTR",
        prices=[LabeledPrice(label=p["label"],amount=p["stars"])])

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery): await q.answer(ok=True)

@dp.message(F.successful_payment)
async def succ_pay(msg: Message):
    payload=msg.successful_payment.invoice_payload.split("_")
    if len(payload)>=3 and payload[0]=="sub":
        key,uid=payload[1],int(payload[2]);p=PRICES.get(key)
        if p:
            end=give_subscription(uid,p["days"])
            await msg.answer(
                f"🎉 **Оплачено!** {p['label']} до {end}\n\n"
                f"💡 Включите авто-продление в профиле,\n"
                f"чтобы подписка продлевалась автоматически!",
                parse_mode="Markdown")
            for aid in ADMIN_IDS:
                try: await bot.send_message(aid,f"💰 {uid} — {p['label']} ({p['stars']}⭐)")
                except: pass

# ── Маркет/Сделки/Апелляции/TikTok callbacks ──
# (аналогично предыдущей версии, с _disp() вместо вложенных f-string)

@dp.callback_query(F.data=="market")
async def cb_market(cb: CallbackQuery):
    uid=cb.from_user.id;items=market_list();total=market_count()
    text=f"🛒 **Маркет** ({total})\n⚠️ Сделки через админа\n\n"
    kb=InlineKeyboardBuilder()
    for it in items:
        sd=_disp(it["seller_uid"],get_user(it["seller_uid"]).get("uname",""))
        text+=f"🔹 `@{it['username']}` — **{it['price']}** ({sd})\n"
        if it["seller_uid"]!=uid: kb.button(text=f"🛒 @{it['username']}",callback_data=f"deal_start_{it['id']}")
        if uid in ADMIN_IDS or it["seller_uid"]==uid: kb.button(text=f"🗑 @{it['username']}",callback_data=f"mkd_{it['id']}")
    if not items: text+="_Пусто_\n"
    kb.button(text="📤 Продать",callback_data="market_sell");kb.button(text="🔙",callback_data="menu");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="market_sell")
async def cb_ms(cb: CallbackQuery):
    kb=InlineKeyboardBuilder();kb.button(text="🇷🇺 ₽",callback_data="curr_rub");kb.button(text="🇺🇸 $",callback_data="curr_usd");kb.button(text="❌",callback_data="market");kb.adjust(2,1)
    await cb.message.edit_text("📤 Валюта:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data.startswith("curr_"))
async def cb_cur(cb: CallbackQuery):
    cur="₽" if cb.data=="curr_rub" else "$";user_states[cb.from_user.id]={"action":"market_username","currency":cur}
    kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="market")
    await cb.message.edit_text(f"📤 Юзернейм ({cur}):",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data.startswith("mkd_"))
async def cb_mkd(cb: CallbackQuery):
    uid=cb.from_user.id;iid=int(cb.data.replace("mkd_",""));item=market_get(iid)
    if not item or (uid not in ADMIN_IDS and item["seller_uid"]!=uid): await cb.answer("⛔️",show_alert=True);return
    market_remove(iid);await cb.answer("🗑");await cb_market(cb)

@dp.callback_query(F.data.startswith("mka_"))
async def cb_mka(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    iid=int(cb.data.replace("mka_",""));item=market_get(iid);market_approve(iid);await cb.answer("✅")
    un=item["username"] if item else "?"
    await cb.message.edit_text(f"✅ @{un}")
    if item:
        try: await bot.send_message(item["seller_uid"],f"✅ @{un} опубликован!")
        except: pass

@dp.callback_query(F.data.startswith("mkr_"))
async def cb_mkr(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    iid=int(cb.data.replace("mkr_",""));item=market_get(iid);market_reject(iid);await cb.answer("❌")
    await cb.message.edit_text("❌ Отклонён")
    if item:
        try: await bot.send_message(item["seller_uid"],f"❌ @{item['username']} отклонён")
        except: pass

@dp.callback_query(F.data.startswith("deal_start_"))
async def cb_ds(cb: CallbackQuery):
    uid=cb.from_user.id;iid=int(cb.data.replace("deal_start_",""));item=market_get(iid)
    if not item or item.get("sold"): await cb.answer("❌ Продан!",show_alert=True);return
    if item["seller_uid"]==uid: await cb.answer("❌ Свой!",show_alert=True);return
    sid=item["seller_uid"];did=deal_create(iid,uid,sid,item["username"],item["price"])
    seller=get_user(sid);buyer=get_user(uid);sn=seller.get("uname","");bn=buyer.get("uname","")
    bd=_disp(uid,bn);sd=_disp(sid,sn)
    dd=deal_get(did);txt=generate_deal_txt(dd)
    kb=InlineKeyboardBuilder();kb.button(text=f"📋 #{did}",callback_data=f"deal_{did}");kb.button(text="🔙",callback_data="market");kb.adjust(1)
    await cb.message.edit_text(f"🤝 #{did}\n`@{item['username']}` — {item['price']}\n⏳ Ожидает админа",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()
    await bot.send_document(uid,BufferedInputFile(txt.encode(),filename=f"deal_{did}.txt"),caption=f"📄 #{did}")
    try:
        skb=InlineKeyboardBuilder();skb.button(text=f"📋 #{did}",callback_data=f"deal_{did}");skb.adjust(1)
        await bot.send_message(sid,f"💰 Покупка! #{did}\n@{item['username']} — {item['price']}\n👤 {bd}",reply_markup=skb.as_markup())
    except: pass
    for aid in ADMIN_IDS:
        try:
            akb=InlineKeyboardBuilder();akb.button(text="✅",callback_data=f"deal_confirm_{did}");akb.button(text="❌",callback_data=f"deal_reject_{did}");akb.adjust(1)
            await bot.send_document(aid,BufferedInputFile(txt.encode(),filename=f"deal_{did}.txt"),
                caption=f"🤝 #{did}\n@{item['username']} — {item['price']}\n👤 {bd} → 📤 {sd}",reply_markup=akb.as_markup())
        except: pass

@dp.callback_query(F.data.startswith("deal_confirm_"))
async def cb_dc(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    did=int(cb.data.replace("deal_confirm_",""));d=deal_get(did)
    if not d or d["status"]!="pending": await cb.answer("❌",show_alert=True);return
    deal_activate(did,cb.from_user.id);await cb.answer("✅");await cb.message.edit_text(f"✅ #{did}")
    buyer=get_user(d["buyer_uid"]);seller=get_user(d["seller_uid"]);bn=buyer.get("uname","");sn=seller.get("uname","")
    for target,name in [(d["buyer_uid"],sn),(d["seller_uid"],bn)]:
        try:
            tkb=InlineKeyboardBuilder();tkb.button(text=f"📋 #{did}",callback_data=f"deal_{did}")
            if name: tkb.button(text=f"💬 @{name}",url=f"https://t.me/{name}")
            tkb.adjust(1);await bot.send_message(target,f"✅ #{did} подтверждена!\n@{d['username']} — {d['price']}",reply_markup=tkb.as_markup())
        except: pass

@dp.callback_query(F.data.startswith("deal_reject_"))
async def cb_dr(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    did=int(cb.data.replace("deal_reject_",""));d=deal_get(did)
    if not d: return
    deal_reject(did);await cb.answer("❌");await cb.message.edit_text(f"❌ #{did}")
    for t in [d["buyer_uid"],d["seller_uid"]]:
        try: await bot.send_message(t,f"❌ #{did} отклонена. @{ADMIN_CONTACT}")
        except: pass

@dp.callback_query(F.data.regexp(r"^deal_\d+$"))
async def cb_dv(cb: CallbackQuery):
    uid=cb.from_user.id;did=int(cb.data.replace("deal_",""));d=deal_get(did)
    if not d: await cb.answer("❌",show_alert=True);return
    if uid not in (d["buyer_uid"],d["seller_uid"]) and uid not in ADMIN_IDS: await cb.answer("⛔️",show_alert=True);return
    ib=uid==d["buyer_uid"];sm={"pending":"⏳","active":"🟢","completed":"✅","cancelled":"❌","rejected":"🚫"};st=sm.get(d["status"],"?")
    seller=get_user(d["seller_uid"]);buyer=get_user(d["buyer_uid"]);sn=seller.get("uname","");bn=buyer.get("uname","")
    bd=_disp(d["buyer_uid"],bn);sd=_disp(d["seller_uid"],sn)
    text=f"📋 **#{did}**\n\n`@{d['username']}` — {d['price']}\n{st} {d['status']}\n\n👤 {bd}\n📤 {sd}"
    kb=InlineKeyboardBuilder()
    if d["status"]=="active":
        if ib:
            kb.button(text="✅ Получил",callback_data=f"deal_ok_{did}");kb.button(text="🚨 Апелляция",callback_data=f"deal_appeal_{did}")
            if sn: kb.button(text=f"💬 @{sn}",url=f"https://t.me/{sn}")
        else:
            if bn: kb.button(text=f"💬 @{bn}",url=f"https://t.me/{bn}")
        if uid in ADMIN_IDS: kb.button(text="❌ Отменить",callback_data=f"deal_cancel_{did}")
    elif d["status"]=="pending":
        if uid in ADMIN_IDS: kb.button(text="✅",callback_data=f"deal_confirm_{did}");kb.button(text="❌",callback_data=f"deal_reject_{did}")
    kb.button(text="📥 TXT",callback_data=f"deal_txt_{did}");kb.button(text="🔙",callback_data="market");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data.startswith("deal_txt_"))
async def cb_dt(cb: CallbackQuery):
    uid=cb.from_user.id;did=int(cb.data.replace("deal_txt_",""));d=deal_get(did)
    if not d or (uid not in (d["buyer_uid"],d["seller_uid"]) and uid not in ADMIN_IDS): await cb.answer("⛔️",show_alert=True);return
    await cb.answer();await bot.send_document(uid,BufferedInputFile(generate_deal_txt(d).encode(),filename=f"deal_{did}.txt"),caption=f"📄 #{did}")

@dp.callback_query(F.data.startswith("deal_ok_"))
async def cb_do(cb: CallbackQuery):
    uid=cb.from_user.id;did=int(cb.data.replace("deal_ok_",""));d=deal_get(did)
    if not d or d["buyer_uid"]!=uid or d["status"]!="active": await cb.answer("❌",show_alert=True);return
    deal_complete(did);await cb.answer("✅");await cb.message.edit_text(f"✅ #{did} завершена! 🎉")
    try: await bot.send_message(d["seller_uid"],f"✅ #{did} завершена! 🎉")
    except: pass

@dp.callback_query(F.data.startswith("deal_cancel_"))
async def cb_dcan(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    did=int(cb.data.replace("deal_cancel_",""));d=deal_get(did);
    if d: deal_cancel(did)
    await cb.answer("❌");await cb.message.edit_text(f"❌ #{did}")

@dp.callback_query(F.data.startswith("deal_appeal_"))
async def cb_da(cb: CallbackQuery):
    did=int(cb.data.replace("deal_appeal_",""));user_states[cb.from_user.id]={"action":"appeal_reason","deal_id":did}
    kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data=f"deal_{did}")
    await cb.message.edit_text("🚨 Опишите проблему:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data.startswith("appeal_buyer_"))
async def cb_ab(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    aid=int(cb.data.replace("appeal_buyer_",""));a=appeal_get(aid)
    if not a: return
    d=deal_get(a["deal_id"]);appeal_resolve(aid,"buyer",cb.from_user.id)
    if d: deal_cancel(a["deal_id"])
    await cb.answer("✅");await cb.message.edit_text(f"✅ #{aid} → покупателю")
    if d:
        try: await bot.send_message(d["buyer_uid"],"✅ Апелляция в вашу пользу!")
        except: pass
        try: await bot.send_message(d["seller_uid"],"❌ Не в вашу пользу")
        except: pass

@dp.callback_query(F.data.startswith("appeal_seller_"))
async def cb_as(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    aid=int(cb.data.replace("appeal_seller_",""));a=appeal_get(aid)
    if not a: return
    d=deal_get(a["deal_id"]);appeal_resolve(aid,"seller",cb.from_user.id)
    if d: deal_complete(a["deal_id"])
    await cb.answer("✅");await cb.message.edit_text(f"✅ #{aid} → продавцу")
    if d:
        try: await bot.send_message(d["seller_uid"],"✅ В вашу пользу!")
        except: pass
        try: await bot.send_message(d["buyer_uid"],f"ℹ️ В пользу продавца. @{ADMIN_CONTACT}")
        except: pass

# ── TikTok ──

@dp.callback_query(F.data=="tiktok")
async def cb_tt(cb: CallbackQuery):
    text=(f"🎁 **TikTok**\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"1️⃣ Найди «словил юз тг»\n2️⃣ {TIKTOK_SCREENSHOTS_NEEDED} комментов:\n"
          f"💬 `{TIKTOK_COMMENT_TEXT}`\n3️⃣ Скрины боту\n4️⃣ 🎁 **{TIKTOK_REWARD_GIFT}**")
    kb=InlineKeyboardBuilder();kb.button(text="📸 Начать",callback_data="tiktok_go");kb.button(text="🔙",callback_data="menu");kb.adjust(1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="tiktok_go")
async def cb_tg(cb: CallbackQuery):
    uid=cb.from_user.id
    if not tiktok_can_submit(uid): await cb.answer("⛔️ Лимит!",show_alert=True);return
    tid=task_create(uid);user_states[uid]={"action":"tiktok_proof","task_id":tid,"photos":0}
    kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="tiktok_cancel")
    await cb.message.edit_text(f"📸 #{tid}\n`0/{TIKTOK_SCREENSHOTS_NEEDED}`\n\n💬 `{TIKTOK_COMMENT_TEXT}`",reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="tiktok_cancel")
async def cb_tc(cb: CallbackQuery):
    user_states.pop(cb.from_user.id,None);await cb.answer("❌")
    t,k=build_menu(cb.from_user.id)
    try: await cb.message.edit_text(t,reply_markup=k,parse_mode="Markdown")
    except: pass

@dp.callback_query(F.data.startswith("ta_"))
async def cb_ta(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    tid=int(cb.data.replace("ta_",""));uid=task_approve(tid,cb.from_user.id)
    if uid:
        await cb.answer("✅");await cb.message.edit_text(f"✅ #{tid}")
        try: await bot.send_message(uid,f"🎉 TikTok одобрено! 🎁 {TIKTOK_REWARD_GIFT}\n@{ADMIN_CONTACT}")
        except: pass

@dp.callback_query(F.data.startswith("tr_"))
async def cb_tr(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    tid=int(cb.data.replace("tr_",""));uid=task_reject(tid,cb.from_user.id);await cb.answer("❌");await cb.message.edit_text(f"❌ #{tid}")
    if uid:
        try: await bot.send_message(uid,f"❌ TikTok отклонено. @{ADMIN_CONTACT}")
        except: pass

# ── Админ ──

@dp.callback_query(F.data=="admin")
async def cb_admin(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: await cb.answer("⛔️",show_alert=True);return
    s=get_stats();method="🔗 Сессия" if (USE_SESSION_SEARCH and session_ready) else "🌐 HTTP"
    text=(f"👑 **Админ** [{method}]\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
          f"👥 `{s['users']}` 💎 `{s['subs']}` 🔍 `{s['searches']}`\n"
          f"📊 Сегодня: 👤`{s['today_users']}` 🔍`{s['today_searches']}`\n"
          f"🔄 Авто-продление: `{s['auto_renew']}`\n\n"
          f"🛒`{s['market']}` ⏳`{s['pending']}` 🤝`{s['pending_deals']}` 🟢`{s['deals']}` 🚨`{s['appeals']}` 📱`{s['tasks']}`")
    kb=InlineKeyboardBuilder()
    kb.button(text="🔑 Ключ",callback_data="admin_keys");kb.button(text="📩 Подписка",callback_data="admin_give")
    kb.button(text=f"🛒 ({s['pending']})",callback_data="admin_market");kb.button(text=f"🤝 ({s['pending_deals']})",callback_data="admin_deals")
    kb.button(text=f"🚨 ({s['appeals']})",callback_data="admin_appeals");kb.button(text="📤 Рассылка",callback_data="admin_broadcast")
    kb.button(text="📊 Экспорт",callback_data="admin_export");kb.button(text="🔙",callback_data="menu");kb.adjust(2,2,2,1,1)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="admin_keys")
async def cb_ak(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    kb=InlineKeyboardBuilder()
    for k,p in PRICES.items(): kb.button(text=p["label"],callback_data=f"gk_{p['days']}")
    kb.button(text="✏️ Своё",callback_data="gk_custom");kb.button(text="🔙",callback_data="admin");kb.adjust(2)
    await cb.message.edit_text("🔑 Срок:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data.startswith("gk_"))
async def cb_gk(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    val=cb.data.replace("gk_","")
    if val=="custom":
        user_states[cb.from_user.id]={"action":"admin_key_days"};await cb.message.edit_text("✏️ Дней:");await cb.answer();return
    days=int(val);tariff=f"{days}дн"
    for p in PRICES.values():
        if p["days"]==days: tariff=p["label"];break
    if days>=99999: tariff="Навсегда"
    key=generate_key(days,f"D{days}");await cb.answer("🔑")
    await cb.message.edit_text(f"🔑 `{key}` — {tariff}",parse_mode="Markdown")
    bu=bot_info.username if bot_info else "bot"
    await bot.send_message(cb.from_user.id,f"🎁 **КЛЮЧ** {tariff}\n🔑 `{key}`\n@{bu}",parse_mode="Markdown")

@dp.callback_query(F.data=="admin_give")
async def cb_ag(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    user_states[cb.from_user.id]={"action":"admin_give_user"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="admin")
    await cb.message.edit_text("📩 ID/@username:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data=="admin_market")
async def cb_am(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    items=market_list(approved=0);text=f"🛒 Модерация ({len(items)})\n\n";kb=InlineKeyboardBuilder()
    for i in items:
        sd=_disp(i["seller_uid"],get_user(i["seller_uid"]).get("uname",""))
        text+=f"@{i['username']} — {i['price']} ({sd})\n"
        kb.button(text=f"✅ {i['username']}",callback_data=f"mka_{i['id']}");kb.button(text="❌",callback_data=f"mkr_{i['id']}")
    if not items: text+="_Нет_"
    kb.button(text="🔙",callback_data="admin");kb.adjust(2)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="admin_deals")
async def cb_ad(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    deals=deal_list_pending();text=f"🤝 ({len(deals)})\n\n";kb=InlineKeyboardBuilder()
    for d in deals:
        bd=_disp(d["buyer_uid"],get_user(d["buyer_uid"]).get("uname",""))
        sd=_disp(d["seller_uid"],get_user(d["seller_uid"]).get("uname",""))
        text+=f"#{d['id']} `@{d['username']}` {bd}→{sd}\n"
        kb.button(text=f"✅ #{d['id']}",callback_data=f"deal_confirm_{d['id']}");kb.button(text=f"❌ #{d['id']}",callback_data=f"deal_reject_{d['id']}")
    if not deals: text+="_Нет_"
    kb.button(text="🔙",callback_data="admin");kb.adjust(2)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="admin_appeals")
async def cb_aa(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    apps=appeal_list_pending();text=f"🚨 ({len(apps)})\n\n";kb=InlineKeyboardBuilder()
    for a in apps:
        d=deal_get(a["deal_id"]);un=d["username"] if d else "?"
        text+=f"#{a['id']} @{un} {a['reason'][:50]}\n"
        kb.button(text=f"👤 #{a['id']}",callback_data=f"appeal_buyer_{a['id']}");kb.button(text=f"📤 #{a['id']}",callback_data=f"appeal_seller_{a['id']}")
    if not apps: text+="_Нет_"
    kb.button(text="🔙",callback_data="admin");kb.adjust(2)
    await cb.message.edit_text(text,reply_markup=kb.as_markup(),parse_mode="Markdown");await cb.answer()

@dp.callback_query(F.data=="admin_broadcast")
async def cb_abr(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    user_states[cb.from_user.id]={"action":"admin_broadcast_text"};kb=InlineKeyboardBuilder();kb.button(text="❌",callback_data="admin")
    await cb.message.edit_text("📤 Текст рассылки:",reply_markup=kb.as_markup());await cb.answer()

@dp.callback_query(F.data=="admin_export")
async def cb_ae(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS: return
    conn=sqlite3.connect(DB);c=conn.cursor()
    c.execute("SELECT uid,uname,joined,searches,sub_end,ref_count,auto_renew FROM users ORDER BY uid")
    rows=c.fetchall();conn.close();now_s=datetime.now().strftime("%Y-%m-%d %H:%M")
    content=f"USERS {len(rows)}\n\n"
    for r in rows:
        sub="💎" if (r[4] and r[4]>now_s) else "  ";ar="🔄" if r[6] else "  "
        content+=f"{sub}{ar} {r[0]} @{r[1] or '—'} s:{r[3] or 0} r:{r[5] or 0}\n"
    await cb.answer();await bot.send_document(cb.from_user.id,BufferedInputFile(content.encode(),filename=f"users.txt"),caption=f"📊 {len(rows)}")


# ═══════════════════════ ЗАПУСК ═══════════════════════

async def init_telethon():
    """Инициализация Telethon клиента для сессионного поиска"""
    global telethon_client, session_ready
    if not TELETHON_AVAILABLE:
        logger.warning("⚠️ Telethon не установлен: pip install telethon")
        return
    if not API_ID or not API_HASH:
        logger.warning("⚠️ API_ID/API_HASH не указаны — используется HTTP")
        return
    try:
        telethon_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await telethon_client.start()
        session_ready = True
        logger.info("✅ Telethon сессия подключена!")
    except Exception as e:
        logger.error(f"❌ Telethon ошибка: {e}")
        session_ready = False


async def main():
    global http_session, bot_info
    init_db()
    bot_info = await bot.get_me()
    http_session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 Chrome/120.0"})

    # Инициализация сессионного поиска
    if USE_SESSION_SEARCH:
        await init_telethon()

    method = "🔗 Сессия" if session_ready else "🌐 HTTP"
    logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"🚀 @{bot_info.username} v16.0")
    logger.info(f"🔍 Метод поиска: {method}")
    logger.info(f"🔥 АКЦИЯ: Навсегда 1999₽/1599⭐")
    logger.info(f"🔄 Напоминания: каждые {REMINDER_CHECK_INTERVAL}с")
    logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Запускаем фоновую задачу напоминаний
    asyncio.create_task(reminder_loop())

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await http_session.close()
        if telethon_client:
            await telethon_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())