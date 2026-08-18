"""ZEUS FIND — public Telegram username finder with premium, subscription gate and admin panel.

This edition intentionally does not use TELEGRAM_API_ID, TELEGRAM_API_HASH or Telethon.
Candidate usernames are checked by public signals only:
1. Ordinary public t.me occupancy check.
2. Fast Fragment AJAX marketplace / reservation check.

Public checks cannot guarantee that Telegram will allow assigning every candidate.
The bot always asks the user to verify a result manually before using it.
"""

from __future__ import annotations

import asyncio
import csv
import html
import logging
import math
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message


# ═══════════════════════════════ НАСТРОЙКИ ═══════════════════════════════

# Новый токен Telegram-бота от @BotFather. Старый опубликованный токен лучше отозвать.
BOT_TOKEN = "8660751586:AAG-uIP9U6sCdH26ZPr44KRaR_SwSjqtyUA"

# ID администратора — обычное число без кавычек. Можно добавить несколько ID через запятую.
# Пример: ADMIN_IDS = [5969266721, 123456789]
ADMIN_IDS = [5969266721]

# Начальный канал обязательной подписки. Можно оставить заглушку и настроить канал
# позднее прямо в инлайн-админке: Админ-панель -> Канал -> Изменить канал.
# Допустимые варианты: "@channel_username" или числовой ID вида -1001234567890.
REQUIRED_CHANNEL = "@ZeusFind"

# Для публичного канала можно оставить пустым: ссылка создастся автоматически.
# Для приватного канала укажи invite-ссылку вида https://t.me/+xxxx.
REQUIRED_CHANNEL_LINK = ""

# Username поддержки. Используется в меню покупки Premium.
SUPPORT_USERNAME = "@AssistantZeusFind"

# Стартовые значения. Их можно менять из админ-панели без перезапуска.
DEFAULT_REQUIRE_SUBSCRIPTION = True
DEFAULT_FREE_DAILY_SEARCHES = 3
DEFAULT_SEARCH_COOLDOWN_SECONDS = 5
DEFAULT_CHECK_LIMIT = 20

REQUEST_DELAY = 0.08
REQUEST_TIMEOUT = 5
SEARCH_TIMEOUT = 40
DATABASE_PATH = "zeus_find.db"
EXPORT_PATH = "zeus_find_users.csv"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
STARTED_AT = time.monotonic()

PREMIUM_PRICES = {
    "1d": {"label": "1 день", "stars": 25, "rub": 25, "days": 1},
    "3d": {"label": "3 дня", "stars": 50, "rub": 60, "days": 3},
    "7d": {"label": "7 дней", "stars": 100, "rub": 130, "days": 7},
    "1m": {"label": "1 месяц", "stars": 300, "rub": 400, "days": 30},
    "3m": {"label": "3 месяца", "stars": 900, "rub": 1100, "days": 90},
}

CONSONANTS = "bcdfghjklmnprstvxz"
VOWELS = "aeiouy"
PATTERNS = ("VVCCV", "VCCVV", "VCVVC")
USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{3,31}$")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | ⚡ %(levelname)s | %(message)s",
)
logger = logging.getLogger("zeus_find")


# ═════════════════════════════ ВСПОМОГАТЕЛЬНОЕ ═════════════════════════════

def now_msk() -> datetime:
    return datetime.now(MOSCOW_TZ)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def parse_channel_ref(raw: str) -> int | str:
    value = raw.strip()
    if not value:
        return ""
    if value.lstrip("-").isdigit():
        return int(value)
    if value.startswith("https://t.me/"):
        value = value.removeprefix("https://t.me/").split("?")[0].strip("/")
    return value if value.startswith("@") else f"@{value}"


def support_link() -> str:
    username = SUPPORT_USERNAME.strip().lstrip("@")
    return f"https://t.me/{username}" if username else "https://t.me/"


def format_msk_datetime(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(MOSCOW_TZ).strftime("%d.%m.%Y · %H:%M МСК")
    except ValueError:
        return "—"


def human_bool(value: bool) -> str:
    return "✅ Включено" if value else "❌ Выключено"


def human_uptime() -> str:
    seconds = int(time.monotonic() - STARTED_AT)
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} дн.")
    if hours:
        parts.append(f"{hours} ч.")
    if minutes:
        parts.append(f"{minutes} мин.")
    parts.append(f"{seconds} сек.")
    return " ".join(parts)


# ═══════════════════════════════ БАЗА ДАННЫХ ═══════════════════════════════

@dataclass(frozen=True, slots=True)
class UserLimits:
    is_premium: bool
    premium_until: Optional[str]
    used_today: int
    remaining: Optional[int]
    cooldown_left: int
    is_banned: bool
    banned_reason: Optional[str]


@dataclass(frozen=True, slots=True)
class SearchAdmission:
    allowed: bool
    reason: str = ""
    cooldown_left: int = 0
    remaining: Optional[int] = None
    banned_reason: Optional[str] = None


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}

    def _add_column_if_missing(
        self,
        connection: sqlite3.Connection,
        table: str,
        column: str,
        declaration: str,
    ) -> None:
        if column not in self._table_columns(connection, table):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    searches_used INTEGER NOT NULL DEFAULT 0,
                    search_date TEXT,
                    last_search_at TEXT,
                    premium_until TEXT,
                    joined_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL,
                    is_banned INTEGER NOT NULL DEFAULT 0,
                    banned_reason TEXT,
                    total_searches INTEGER NOT NULL DEFAULT 0,
                    total_found INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self._add_column_if_missing(connection, "users", "is_banned", "INTEGER NOT NULL DEFAULT 0")
            self._add_column_if_missing(connection, "users", "banned_reason", "TEXT")
            self._add_column_if_missing(connection, "users", "total_searches", "INTEGER NOT NULL DEFAULT 0")
            self._add_column_if_missing(connection, "users", "total_found", "INTEGER NOT NULL DEFAULT 0")

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS search_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    pattern TEXT,
                    checked INTEGER NOT NULL,
                    request_errors INTEGER NOT NULL DEFAULT 0,
                    is_found INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    target_user_id INTEGER,
                    details TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_users_premium_until ON users(premium_until)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_users_activity ON users(last_activity)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_history_created ON search_history(created_at)")

            defaults = {
                "require_subscription": "1" if DEFAULT_REQUIRE_SUBSCRIPTION else "0",
                "required_channel": REQUIRED_CHANNEL,
                "required_channel_link": REQUIRED_CHANNEL_LINK,
                "free_daily_searches": str(DEFAULT_FREE_DAILY_SEARCHES),
                "search_cooldown_seconds": str(DEFAULT_SEARCH_COOLDOWN_SECONDS),
                "check_limit": str(DEFAULT_CHECK_LIMIT),
            }
            for key, value in defaults.items():
                connection.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (key, value),
                )

    def get_setting(self, key: str, fallback: str = "") -> str:
        with self._connect() as connection:
            row = connection.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else fallback

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_bool_setting(self, key: str, fallback: bool = False) -> bool:
        return self.get_setting(key, "1" if fallback else "0") == "1"

    def get_int_setting(self, key: str, fallback: int) -> int:
        try:
            return int(self.get_setting(key, str(fallback)))
        except ValueError:
            return fallback

    def ensure_user(self, user_id: int, username: Optional[str], first_name: str) -> None:
        timestamp = now_msk().isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO users (user_id, username, first_name, joined_at, last_activity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_activity = excluded.last_activity
                """,
                (user_id, username, first_name, timestamp, timestamp),
            )

    def get_user(self, user_id: int) -> Optional[dict[str, object]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            return dict(row) if row else None

    def find_user(self, raw: str) -> Optional[dict[str, object]]:
        value = raw.strip()
        with self._connect() as connection:
            if value.lstrip("-").isdigit():
                row = connection.execute("SELECT * FROM users WHERE user_id = ?", (int(value),)).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM users WHERE LOWER(username) = LOWER(?)",
                    (value.lstrip("@"),),
                ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _premium_active(row: sqlite3.Row | dict[str, object], current: datetime) -> bool:
        raw_expiry = row["premium_until"]
        if not raw_expiry:
            return False
        try:
            return datetime.fromisoformat(str(raw_expiry)) > current
        except ValueError:
            return False

    def _cooldown_left(self, row: sqlite3.Row, current: datetime) -> int:
        raw_last_search = row["last_search_at"]
        if not raw_last_search:
            return 0
        try:
            elapsed = (current - datetime.fromisoformat(str(raw_last_search))).total_seconds()
        except ValueError:
            return 0
        cooldown = max(0, self.get_int_setting("search_cooldown_seconds", DEFAULT_SEARCH_COOLDOWN_SECONDS))
        return max(0, math.ceil(cooldown - elapsed))

    def _normalize_row(self, connection: sqlite3.Connection, user_id: int) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row is None:
            raise RuntimeError("Пользователь не зарегистрирован. Отправь /start")

        current = now_msk()
        today = current.date().isoformat()
        updates: list[str] = []
        params: list[object] = []

        if row["search_date"] != today:
            updates.extend(["searches_used = 0", "search_date = ?"])
            params.append(today)

        if row["premium_until"] and not self._premium_active(row, current):
            updates.append("premium_until = NULL")

        if updates:
            params.append(user_id)
            connection.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?",
                tuple(params),
            )
            row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

        return row

    def get_limits(self, user_id: int) -> UserLimits:
        with self._connect() as connection:
            row = self._normalize_row(connection, user_id)
            current = now_msk()
            premium = self._premium_active(row, current)
            daily_limit = max(1, self.get_int_setting("free_daily_searches", DEFAULT_FREE_DAILY_SEARCHES))
            used = int(row["searches_used"])
            remaining = None if premium else max(0, daily_limit - used)
            return UserLimits(
                is_premium=premium,
                premium_until=str(row["premium_until"]) if row["premium_until"] else None,
                used_today=used,
                remaining=remaining,
                cooldown_left=self._cooldown_left(row, current),
                is_banned=bool(row["is_banned"]),
                banned_reason=str(row["banned_reason"]) if row["banned_reason"] else None,
            )

    def try_start_search(self, user_id: int) -> SearchAdmission:
        with self._connect() as connection:
            row = self._normalize_row(connection, user_id)
            current = now_msk()
            if bool(row["is_banned"]):
                return SearchAdmission(
                    allowed=False,
                    reason="banned",
                    banned_reason=str(row["banned_reason"] or "Причина не указана"),
                )

            premium = self._premium_active(row, current)
            cooldown_left = self._cooldown_left(row, current)
            if cooldown_left > 0:
                return SearchAdmission(allowed=False, reason="cooldown", cooldown_left=cooldown_left)

            daily_limit = max(1, self.get_int_setting("free_daily_searches", DEFAULT_FREE_DAILY_SEARCHES))
            used = int(row["searches_used"])
            if not premium and used >= daily_limit:
                return SearchAdmission(allowed=False, reason="daily_limit", remaining=0)

            new_used = used if premium else used + 1
            connection.execute(
                """
                UPDATE users
                SET searches_used = ?, search_date = ?, last_search_at = ?,
                    last_activity = ?, total_searches = total_searches + 1
                WHERE user_id = ?
                """,
                (new_used, current.date().isoformat(), current.isoformat(), current.isoformat(), user_id),
            )
            remaining = None if premium else max(0, daily_limit - new_used)
            return SearchAdmission(allowed=True, remaining=remaining)

    def record_search(
        self,
        user_id: int,
        username: Optional[str],
        pattern: Optional[str],
        checked: int,
        request_errors: int,
    ) -> None:
        timestamp = now_msk().isoformat()
        found = username is not None
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO search_history (
                    user_id, username, pattern, checked, request_errors, is_found, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, username, pattern, checked, request_errors, 1 if found else 0, timestamp),
            )
            if found:
                connection.execute(
                    "UPDATE users SET total_found = total_found + 1 WHERE user_id = ?",
                    (user_id,),
                )

    def grant_premium(self, user_id: int, days: int) -> str:
        current = now_msk()
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, joined_at, last_activity)
                    VALUES (?, NULL, 'Пользователь', ?, ?)
                    """,
                    (user_id, current.isoformat(), current.isoformat()),
                )
                base = current
            else:
                try:
                    expiry = datetime.fromisoformat(str(row["premium_until"])) if row["premium_until"] else None
                except ValueError:
                    expiry = None
                base = expiry if expiry and expiry > current else current

            premium_until = base + timedelta(days=days)
            connection.execute(
                "UPDATE users SET premium_until = ?, last_activity = ? WHERE user_id = ?",
                (premium_until.isoformat(), current.isoformat(), user_id),
            )
            return premium_until.isoformat()

    def remove_premium(self, user_id: int) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE users SET premium_until = NULL WHERE user_id = ?", (user_id,))

    def ban_user(self, user_id: int, reason: str) -> None:
        current = now_msk().isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO users (user_id, username, first_name, joined_at, last_activity, is_banned, banned_reason)
                VALUES (?, NULL, 'Пользователь', ?, ?, 1, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    is_banned = 1,
                    banned_reason = excluded.banned_reason,
                    last_activity = excluded.last_activity
                """,
                (user_id, current, current, reason),
            )

    def unban_user(self, user_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE users SET is_banned = 0, banned_reason = NULL WHERE user_id = ?",
                (user_id,),
            )

    def reset_user_limit(self, user_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE users SET searches_used = 0, search_date = ? WHERE user_id = ?",
                (now_msk().date().isoformat(), user_id),
            )

    def reset_all_limits(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE users SET searches_used = 0, search_date = ?",
                (now_msk().date().isoformat(),),
            )
            return int(cursor.rowcount)

    def list_users(self, mode: str, page: int, page_size: int = 8) -> tuple[list[dict[str, object]], int]:
        current = now_msk().isoformat()
        conditions = {
            "all": ("1 = 1", ()),
            "premium": ("premium_until IS NOT NULL AND premium_until > ?", (current,)),
            "banned": ("is_banned = 1", ()),
            "active": ("last_activity >= ?", ((now_msk() - timedelta(days=1)).isoformat(),)),
        }
        where, params = conditions.get(mode, conditions["all"])
        with self._connect() as connection:
            total = int(connection.execute(f"SELECT COUNT(*) AS total FROM users WHERE {where}", params).fetchone()["total"])
            rows = connection.execute(
                f"""
                SELECT * FROM users
                WHERE {where}
                ORDER BY last_activity DESC
                LIMIT ? OFFSET ?
                """,
                (*params, page_size, max(0, page) * page_size),
            ).fetchall()
            return [dict(row) for row in rows], total

    def get_all_user_ids(self) -> list[int]:
        with self._connect() as connection:
            return [int(row["user_id"]) for row in connection.execute("SELECT user_id FROM users")]

    def get_stats(self) -> dict[str, int]:
        current = now_msk()
        today = current.date().isoformat()
        day_start = datetime.combine(current.date(), datetime.min.time(), tzinfo=MOSCOW_TZ).isoformat()
        week_start = (current - timedelta(days=7)).isoformat()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_users,
                    SUM(CASE WHEN premium_until IS NOT NULL AND premium_until > ? THEN 1 ELSE 0 END) AS premium_users,
                    SUM(CASE WHEN is_banned = 1 THEN 1 ELSE 0 END) AS banned_users,
                    SUM(CASE WHEN last_activity >= ? THEN 1 ELSE 0 END) AS active_today,
                    SUM(CASE WHEN last_activity >= ? THEN 1 ELSE 0 END) AS active_week,
                    SUM(total_searches) AS total_searches,
                    SUM(total_found) AS total_found,
                    SUM(CASE WHEN search_date = ? THEN searches_used ELSE 0 END) AS free_searches_today
                FROM users
                """,
                (current.isoformat(), day_start, week_start, today),
            ).fetchone()
            history_today = connection.execute(
                "SELECT COUNT(*) AS total FROM search_history WHERE created_at >= ?",
                (day_start,),
            ).fetchone()["total"]
            found_today = connection.execute(
                "SELECT COUNT(*) AS total FROM search_history WHERE created_at >= ? AND is_found = 1",
                (day_start,),
            ).fetchone()["total"]
            return {
                "total_users": int(row["total_users"] or 0),
                "premium_users": int(row["premium_users"] or 0),
                "banned_users": int(row["banned_users"] or 0),
                "active_today": int(row["active_today"] or 0),
                "active_week": int(row["active_week"] or 0),
                "total_searches": int(row["total_searches"] or 0),
                "total_found": int(row["total_found"] or 0),
                "free_searches_today": int(row["free_searches_today"] or 0),
                "history_today": int(history_today or 0),
                "found_today": int(found_today or 0),
            }

    def recent_searches(self, limit: int = 12) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT h.*, u.username AS account_username
                FROM search_history h
                LEFT JOIN users u ON u.user_id = h.user_id
                ORDER BY h.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def log_admin(self, admin_id: int, action: str, target_user_id: Optional[int] = None, details: str = "") -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO admin_log (admin_id, action, target_user_id, details, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (admin_id, action, target_user_id, details, now_msk().isoformat()),
            )

    def recent_admin_log(self, limit: int = 12) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM admin_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def export_users_csv(self, path: str) -> int:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM users ORDER BY joined_at ASC").fetchall()
        columns = [
            "user_id", "username", "first_name", "is_banned", "banned_reason",
            "searches_used", "search_date", "last_search_at", "premium_until",
            "total_searches", "total_found", "joined_at", "last_activity",
        ]
        with open(path, "w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row[column] for column in columns])
        return len(rows)


db = Database(DATABASE_PATH)


# ═══════════════════════════════ ГЕНЕРАЦИЯ ═══════════════════════════════

@dataclass(frozen=True, slots=True)
class GeneratedUsername:
    username: str
    pattern: str


class PatternGenerator:
    @staticmethod
    def _generate_one(pattern: str) -> str:
        return "".join(random.choice(VOWELS if symbol == "V" else CONSONANTS) for symbol in pattern)

    def generate_usernames(self, max_results: int) -> list[GeneratedUsername]:
        usernames: list[GeneratedUsername] = []
        seen: set[str] = set()
        attempts = 0
        while len(usernames) < max_results and attempts < max_results * 100:
            pattern = random.choice(PATTERNS)
            username = self._generate_one(pattern)
            attempts += 1
            if len(set(username)) != 5 or username in seen:
                continue
            seen.add(username)
            usernames.append(GeneratedUsername(username=username, pattern=pattern))
        return usernames


pattern_generator = PatternGenerator()


# ═══════════════════════════════ ПРОВЕРКА USERNAME ═══════════════════════════════

class UsernameChecker:
    """Ordinary t.me public check followed by fast Fragment AJAX check."""

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        self.fragment_session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=timeout)
        if self.fragment_session is None or self.fragment_session.closed:
            self.fragment_session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
        if self.fragment_session and not self.fragment_session.closed:
            await self.fragment_session.close()

    @staticmethod
    def _user_agent() -> str:
        return random.choice(USER_AGENTS)

    @staticmethod
    def is_valid_username(username: str) -> bool:
        return bool(USERNAME_RE.fullmatch(username))

    async def check_telegram_username(self, username: str) -> Optional[bool]:
        if not self.is_valid_username(username):
            return False
        await self.start()
        assert self.session is not None
        try:
            async with self.session.get(
                f"https://t.me/{username}",
                headers={"User-Agent": self._user_agent()},
                allow_redirects=True,
            ) as response:
                if response.status != 200:
                    return None
                text = await response.text(errors="ignore")
                if "tgme_page_title" in text and username.lower() in text.lower():
                    return False
                if "If you have Telegram" in text:
                    return False
                return True
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Telegram check failed for @%s: %s", username, exc)
            return None

    @staticmethod
    def _fragment_headers(username: str) -> dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "X-Aj-Referer": f"https://fragment.com/?query={username}",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.5",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
        }

    async def check_fragment_username(self, username: str) -> Optional[bool]:
        if not self.is_valid_username(username):
            return False
        await self.start()
        assert self.fragment_session is not None
        try:
            async with self.fragment_session.get(
                f"https://fragment.com/username/{username}",
                headers=self._fragment_headers(username),
                allow_redirects=True,
            ) as response:
                if response.status != 200:
                    return None
                try:
                    payload = await response.json(content_type=None)
                except (aiohttp.ContentTypeError, ValueError):
                    return None
                if not isinstance(payload, dict):
                    return None
                if "h" not in payload:
                    return True
                status_html = str(payload.get("h", "")).lower()
                blocked_markers = ("tm-status-taken", "tm-status-avail", "tm-status-unavail")
                if any(marker in status_html for marker in blocked_markers):
                    return False
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Fragment check failed for @%s: %s", username, exc)
            return None

    async def comprehensive_check(self, username: str) -> dict[str, Optional[bool] | str]:
        telegram_available = await self.check_telegram_username(username)
        result: dict[str, Optional[bool] | str] = {
            "username": username,
            "telegram_available": telegram_available,
            "fragment_available": None,
            "fully_free": False,
        }
        if telegram_available is not True:
            return result
        fragment_available = await self.check_fragment_username(username)
        result["fragment_available"] = fragment_available
        result["fully_free"] = fragment_available is True
        return result

    async def find_free_username(
        self,
        limit: int,
        progress_callback: Optional[Callable[[int, int], Awaitable[None]]] = None,
    ) -> tuple[Optional[GeneratedUsername], int, int]:
        checked = 0
        request_errors = 0
        for generated in pattern_generator.generate_usernames(limit):
            result = await self.comprehensive_check(generated.username)
            checked += 1
            telegram_failed = result["telegram_available"] is None
            fragment_failed = result["telegram_available"] is True and result["fragment_available"] is None
            if telegram_failed or fragment_failed:
                request_errors += 1
            if progress_callback and (checked == 1 or checked % 5 == 0 or checked == limit):
                await progress_callback(checked, limit)
            if result["fully_free"] is True:
                return generated, checked, request_errors
            await asyncio.sleep(REQUEST_DELAY)
        return None, checked, request_errors


checker = UsernameChecker()
dp = Dispatcher()
active_searches: set[int] = set()


# ═══════════════════════════════ ПОДПИСКА И КАНАЛ ═══════════════════════════════

def effective_channel_raw() -> str:
    return db.get_setting("required_channel", REQUIRED_CHANNEL).strip()


def effective_channel_link() -> str:
    custom = db.get_setting("required_channel_link", REQUIRED_CHANNEL_LINK).strip()
    if custom:
        return custom
    raw = effective_channel_raw()
    parsed = parse_channel_ref(raw)
    if isinstance(parsed, str) and parsed.startswith("@"):
        return f"https://t.me/{parsed.lstrip('@')}"
    return support_link()


def subscription_enabled() -> bool:
    return db.get_bool_setting("require_subscription", DEFAULT_REQUIRE_SUBSCRIPTION)


async def inspect_channel(bot: Bot) -> dict[str, object]:
    raw = effective_channel_raw()
    parsed = parse_channel_ref(raw)
    result: dict[str, object] = {
        "configured": bool(parsed and parsed != "@your_channel"),
        "raw": raw or "—",
        "title": "—",
        "chat_id": "—",
        "members": "—",
        "bot_status": "—",
        "bot_is_admin": False,
        "error": "",
    }
    if not result["configured"]:
        result["error"] = "Канал ещё не настроен"
        return result
    try:
        chat = await bot.get_chat(parsed)
        result["title"] = chat.title or getattr(chat, "full_name", None) or "—"
        result["chat_id"] = chat.id
        result["members"] = await bot.get_chat_member_count(parsed)
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(parsed, me.id)
        result["bot_status"] = str(bot_member.status)
        result["bot_is_admin"] = bot_member.status in {
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
    except Exception as exc:
        result["error"] = str(exc)
    return result


async def has_required_subscription(bot: Bot, user_id: int) -> bool:
    if is_admin(user_id) or not subscription_enabled():
        return True
    parsed = parse_channel_ref(effective_channel_raw())
    if not parsed or parsed == "@your_channel":
        return False
    try:
        member = await bot.get_chat_member(parsed, user_id)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        logger.warning("Cannot verify subscription for %s: %s", user_id, exc)
        return False
    except Exception as exc:
        logger.warning("Subscription check failed for %s: %s", user_id, exc)
        return False

    if member.status in {ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER}:
        return True
    if member.status == ChatMemberStatus.RESTRICTED:
        return bool(getattr(member, "is_member", False))
    return False


async def require_subscription(bot: Bot, user_id: int, message: Message) -> bool:
    if await has_required_subscription(bot, user_id):
        return True
    await message.answer(subscription_text(), reply_markup=subscription_menu())
    return False


# ═══════════════════════════════ ТЕКСТЫ И КЛАВИАТУРЫ ═══════════════════════════════

def subscription_text() -> str:
    if parse_channel_ref(effective_channel_raw()) in {"", "@your_channel"}:
        return (
            "⚠️ <b>КАНАЛ ЕЩЁ НЕ НАСТРОЕН</b>\n\n"
            "Администратору нужно открыть админ-панель и указать канал обязательной подписки."
        )
    return (
        "⚡ <b>ZEUS FIND · ДОСТУП К ОЛИМПУ</b>\n\n"
        "Чтобы использовать поиск, подпишись на наш канал.\n\n"
        "После подписки нажми кнопку <b>«Проверить подписку»</b>."
    )


ABOUT_TEXT = (
    "🏛 <b>О ZEUS FIND</b>\n\n"
    "ZEUS FIND ищет короткие пятибуквенные username по трём схемам и проверяет их "
    "через публичную страницу Telegram и быстрый AJAX-запрос Fragment.\n\n"
    "⚡ Схемы: <code>VVCCV · VCCVV · VCVVC</code>\n"
    "⏳ Между поисками действует задержка.\n\n"
    "Публичная проверка не является официальной гарантией. Перед установкой "
    "открой найденное имя в Telegram и проверь его вручную."
)


PREMIUM_TEXT = (
    "💎 <b>ZEUS PREMIUM</b>\n\n"
    "Подними лимиты до уровня богов:\n\n"
    "⚡ <b>Безлимитные поиски</b> каждый день\n"
    "⏳ Кулдаун между поисками сохраняется для стабильной работы\n"
    "🏛 Поддержка проекта ZEUS FIND\n\n"
    "Выбери подходящий тариф:"
)


def subscription_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📣 Подписаться на канал", url=effective_channel_link())],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="subscription:check")],
        ]
    )


def main_menu(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="⚡ Найти username", callback_data="search:mixed")],
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
            InlineKeyboardButton(text="💎 Premium", callback_data="premium"),
        ],
        [
            InlineKeyboardButton(text="📣 Канал", url=effective_channel_link()),
        ],
    ]
    if is_admin(user_id):
        rows.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="menu")]]
    )


def premium_menu() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for key, plan in PREMIUM_PRICES.items():
        rows.append([
            InlineKeyboardButton(
                text=f"⚡ {plan['label']} · {plan['stars']} ⭐ / {plan['rub']} ₽",
                callback_data=f"premium:plan:{key}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Вернуться в меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def retry_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Повторить поиск", callback_data="search:mixed")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
        ]
    )


def result_menu(username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Проверить в Telegram", url=f"https://t.me/{username}")],
            [InlineKeyboardButton(text="💎 Открыть Fragment", url=f"https://fragment.com/username/{username}")],
            [InlineKeyboardButton(text="🔄 Найти ещё", callback_data="search:mixed")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
        ]
    )


def render_main_text(user_id: int, first_name: str) -> str:
    limits = db.get_limits(user_id)
    safe_name = html.escape(first_name or "путник")
    daily_limit = max(1, db.get_int_setting("free_daily_searches", DEFAULT_FREE_DAILY_SEARCHES))
    if limits.is_premium:
        status = "💎 <b>ZEUS PREMIUM</b>"
        searches = "⚡ Поиски сегодня: <b>безлимит</b>"
    else:
        status = "🌩 <b>FREE</b>"
        searches = f"⚡ Поиски сегодня: <b>{limits.used_today}/{daily_limit}</b>"
    return (
        "⚡ <b>ZEUS FIND</b>\n"
        "<i>Олимп коротких Telegram username</i>\n\n"
        f"Привет, <b>{safe_name}</b>.\n"
        "Зевс готов начать поиск по трём схемам:\n"
        "<code>VVCCV · VCCVV · VCVVC</code>\n\n"
        f"Твой статус: {status}\n"
        f"{searches}\n"
        "🕛 Free-лимит обновляется ежедневно в <b>00:00 МСК</b>.\n\n"
        "Выбери действие:"
    )


def render_profile_text(user_id: int, first_name: str, username: Optional[str]) -> str:
    limits = db.get_limits(user_id)
    safe_name = html.escape(first_name or "—")
    safe_username = f"@{html.escape(username)}" if username else "—"
    daily_limit = max(1, db.get_int_setting("free_daily_searches", DEFAULT_FREE_DAILY_SEARCHES))
    status = "💎 ZEUS PREMIUM" if limits.is_premium else "🌩 FREE"
    expiry = format_msk_datetime(limits.premium_until) if limits.is_premium else "—"
    searches = "безлимит" if limits.is_premium else f"{limits.used_today}/{daily_limit}"
    cooldown = "можно искать" if limits.cooldown_left == 0 else f"ещё {limits.cooldown_left} сек."
    return (
        "👤 <b>ПРОФИЛЬ ЖИТЕЛЯ ОЛИМПА</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📛 Имя: <b>{safe_name}</b>\n"
        f"🔖 Username: <code>{safe_username}</code>\n\n"
        f"🏛 Статус: <b>{status}</b>\n"
        f"📅 Premium до: <b>{expiry}</b>\n"
        f"⚡ Поиски сегодня: <b>{searches}</b>\n"
        f"⏳ Новый поиск: <b>{cooldown}</b>"
    )


# ═══════════════════════════════ АДМИН-ПАНЕЛЬ ═══════════════════════════════

class AdminStates(StatesGroup):
    waiting_user_lookup = State()
    waiting_ban_reason = State()
    waiting_custom_premium_days = State()
    waiting_broadcast_text = State()
    waiting_setting_value = State()


def admin_panel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
                InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users:all:0"),
            ],
            [
                InlineKeyboardButton(text="💎 Premium", callback_data="admin:users:premium:0"),
                InlineKeyboardButton(text="🚫 Баны", callback_data="admin:users:banned:0"),
            ],
            [
                InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin:user:lookup"),
                InlineKeyboardButton(text="⚡ Находки", callback_data="admin:history"),
            ],
            [
                InlineKeyboardButton(text="📣 Рассылка", callback_data="admin:broadcast"),
                InlineKeyboardButton(text="📡 Канал", callback_data="admin:channel"),
            ],
            [
                InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin:settings"),
                InlineKeyboardButton(text="🧾 Журнал", callback_data="admin:log"),
            ],
            [
                InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="admin:export"),
                InlineKeyboardButton(text="🩺 Система", callback_data="admin:system"),
            ],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="menu")],
        ]
    )


def admin_back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin:panel")]]
    )


def admin_settings_menu() -> InlineKeyboardMarkup:
    subscription = subscription_enabled()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"📣 Подписка: {'ВКЛ' if subscription else 'ВЫКЛ'}",
                callback_data="admin:settings:toggle_subscription",
            )],
            [
                InlineKeyboardButton(text="📡 Изменить канал", callback_data="admin:settings:set_channel"),
                InlineKeyboardButton(text="🔗 Ссылка канала", callback_data="admin:settings:set_channel_link"),
            ],
            [
                InlineKeyboardButton(text="🌩 Free-лимит", callback_data="admin:settings:set_free_limit"),
                InlineKeyboardButton(text="⏳ Кулдаун", callback_data="admin:settings:set_cooldown"),
            ],
            [InlineKeyboardButton(text="🔎 Кандидатов за поиск", callback_data="admin:settings:set_check_limit")],
            [InlineKeyboardButton(text="♻️ Сбросить дневные лимиты всем", callback_data="admin:settings:reset_all_confirm")],
            [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin:panel")],
        ]
    )


def admin_reset_confirm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, сбросить всем", callback_data="admin:settings:reset_all")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin:settings")],
        ]
    )


def admin_channel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить диагностику", callback_data="admin:channel")],
            [InlineKeyboardButton(text="📡 Изменить канал", callback_data="admin:settings:set_channel")],
            [InlineKeyboardButton(text="🔗 Изменить ссылку", callback_data="admin:settings:set_channel_link")],
            [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin:panel")],
        ]
    )


def user_label(user: dict[str, object]) -> str:
    username = f"@{user['username']}" if user.get("username") else str(user["user_id"])
    icon = "🚫" if bool(user.get("is_banned")) else "💎" if Database._premium_active(user, now_msk()) else "👤"
    return f"{icon} {username}"


def admin_users_menu(users: list[dict[str, object]], mode: str, page: int, total: int, page_size: int = 8) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="👥 Все", callback_data="admin:users:all:0"),
            InlineKeyboardButton(text="💎 Premium", callback_data="admin:users:premium:0"),
            InlineKeyboardButton(text="🚫 Баны", callback_data="admin:users:banned:0"),
        ],
        [InlineKeyboardButton(text="🟢 Активные сутки", callback_data="admin:users:active:0")],
    ]
    for user in users:
        rows.append([InlineKeyboardButton(text=user_label(user), callback_data=f"admin:user:view:{user['user_id']}")])

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:users:{mode}:{page - 1}"))
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:users:{mode}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin:user:lookup")])
    rows.append([InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_user_menu(user: dict[str, object]) -> InlineKeyboardMarkup:
    user_id = int(user["user_id"])
    rows = [
        [
            InlineKeyboardButton(text="💎 +1 день", callback_data=f"admin:user:premium:{user_id}:1"),
            InlineKeyboardButton(text="💎 +7 дней", callback_data=f"admin:user:premium:{user_id}:7"),
            InlineKeyboardButton(text="💎 +30 дней", callback_data=f"admin:user:premium:{user_id}:30"),
        ],
        [
            InlineKeyboardButton(text="➕ Свой срок", callback_data=f"admin:user:premium_custom:{user_id}"),
            InlineKeyboardButton(text="➖ Убрать Premium", callback_data=f"admin:user:unpremium:{user_id}"),
        ],
        [InlineKeyboardButton(text="♻️ Сбросить лимит", callback_data=f"admin:user:reset:{user_id}")],
    ]
    if bool(user.get("is_banned")):
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin:user:unban:{user_id}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin:user:ban:{user_id}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить карточку", callback_data=f"admin:user:view:{user_id}")],
        [InlineKeyboardButton(text="⬅️ К пользователям", callback_data="admin:users:all:0")],
        [InlineKeyboardButton(text="🏠 В админ-панель", callback_data="admin:panel")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def render_user_card(user: dict[str, object]) -> str:
    premium = Database._premium_active(user, now_msk())
    return (
        "👤 <b>КАРТОЧКА ПОЛЬЗОВАТЕЛЯ</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"🔖 Username: <code>{('@' + str(user['username'])) if user.get('username') else '—'}</code>\n"
        f"📛 Имя: <b>{html.escape(str(user.get('first_name') or '—'))}</b>\n\n"
        f"💎 Premium: <b>{'активен' if premium else 'нет'}</b>\n"
        f"📅 Premium до: <b>{format_msk_datetime(str(user['premium_until'])) if user.get('premium_until') else '—'}</b>\n"
        f"🚫 Бан: <b>{'да' if bool(user.get('is_banned')) else 'нет'}</b>\n"
        f"📝 Причина: <b>{html.escape(str(user.get('banned_reason') or '—'))}</b>\n\n"
        f"⚡ Поисков сегодня: <b>{user.get('searches_used', 0)}</b>\n"
        f"📊 Всего поисков: <b>{user.get('total_searches', 0)}</b>\n"
        f"🏛 Найдено: <b>{user.get('total_found', 0)}</b>\n"
        f"🗓 Регистрация: <b>{format_msk_datetime(str(user.get('joined_at') or ''))}</b>\n"
        f"🕒 Активность: <b>{format_msk_datetime(str(user.get('last_activity') or ''))}</b>"
    )


def render_settings_text() -> str:
    return (
        "⚙️ <b>НАСТРОЙКИ ZEUS FIND</b>\n\n"
        f"📣 Обязательная подписка: <b>{human_bool(subscription_enabled())}</b>\n"
        f"📡 Канал: <code>{html.escape(effective_channel_raw() or '—')}</code>\n"
        f"🔗 Ссылка: <code>{html.escape(db.get_setting('required_channel_link', '') or 'автоматически')}</code>\n"
        f"🌩 Free-поисков в сутки: <b>{db.get_int_setting('free_daily_searches', DEFAULT_FREE_DAILY_SEARCHES)}</b>\n"
        f"⏳ Кулдаун: <b>{db.get_int_setting('search_cooldown_seconds', DEFAULT_SEARCH_COOLDOWN_SECONDS)} сек.</b>\n"
        f"🔎 Кандидатов за запуск: <b>{db.get_int_setting('check_limit', DEFAULT_CHECK_LIMIT)}</b>"
    )


# ═══════════════════════════════ ОБЩИЕ ОБРАБОТЧИКИ ═══════════════════════════════

def register_user(message: Message) -> None:
    user = message.from_user
    db.ensure_user(user.id, user.username, user.first_name or "Пользователь")


def register_callback_user(callback: CallbackQuery) -> None:
    user = callback.from_user
    db.ensure_user(user.id, user.username, user.first_name or "Пользователь")


async def safe_edit(message: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


async def reject_non_admin(callback: CallbackQuery) -> bool:
    if is_admin(callback.from_user.id):
        return False
    await callback.answer("⛔ Нет доступа", show_alert=True)
    return True


@dp.message(CommandStart())
async def start_handler(message: Message) -> None:
    register_user(message)
    limits = db.get_limits(message.from_user.id)
    if limits.is_banned:
        await message.answer(f"🚫 Доступ заблокирован.\nПричина: <b>{html.escape(limits.banned_reason or 'не указана')}</b>")
        return
    if not await require_subscription(message.bot, message.from_user.id, message):
        return
    await message.answer(render_main_text(message.from_user.id, message.from_user.first_name or "Пользователь"), reply_markup=main_menu(message.from_user.id))


@dp.message(Command("myid"))
async def myid_handler(message: Message) -> None:
    register_user(message)
    await message.answer(f"🆔 Твой Telegram ID: <code>{message.from_user.id}</code>")


@dp.message(Command("admin"))
async def admin_command_handler(message: Message) -> None:
    register_user(message)
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return
    stats = db.get_stats()
    await message.answer(
        "⚙️ <b>АДМИН-ПАНЕЛЬ ZEUS FIND</b>\n\n"
        f"👥 Пользователей: <b>{stats['total_users']}</b>\n"
        f"⚡ Поисков всего: <b>{stats['total_searches']}</b>\n"
        f"🏛 Найдено: <b>{stats['total_found']}</b>\n\n"
        "Выбери раздел:",
        reply_markup=admin_panel_menu(),
    )


@dp.callback_query(F.data == "subscription:check")
async def subscription_check_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    if await has_required_subscription(callback.bot, callback.from_user.id):
        await callback.answer("✅ Подписка подтверждена", show_alert=True)
        if callback.message:
            await safe_edit(callback.message, render_main_text(callback.from_user.id, callback.from_user.first_name or "Пользователь"), main_menu(callback.from_user.id))
        return
    await callback.answer("Подписка пока не найдена", show_alert=True)
    if callback.message:
        await safe_edit(callback.message, subscription_text(), subscription_menu())


@dp.callback_query(F.data == "menu")
async def menu_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    await callback.answer()
    if not callback.message:
        return
    limits = db.get_limits(callback.from_user.id)
    if limits.is_banned:
        await callback.message.answer(f"🚫 Доступ заблокирован. Причина: <b>{html.escape(limits.banned_reason or 'не указана')}</b>")
        return
    if not await require_subscription(callback.bot, callback.from_user.id, callback.message):
        return
    await safe_edit(callback.message, render_main_text(callback.from_user.id, callback.from_user.first_name or "Пользователь"), main_menu(callback.from_user.id))


@dp.callback_query(F.data == "profile")
async def profile_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    await callback.answer()
    if not callback.message or not await require_subscription(callback.bot, callback.from_user.id, callback.message):
        return
    await safe_edit(callback.message, render_profile_text(callback.from_user.id, callback.from_user.first_name or "Пользователь", callback.from_user.username), back_menu(callback.from_user.id))


@dp.callback_query(F.data == "about")
async def about_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    await callback.answer()
    if callback.message and await require_subscription(callback.bot, callback.from_user.id, callback.message):
        await safe_edit(callback.message, ABOUT_TEXT, back_menu(callback.from_user.id))


@dp.callback_query(F.data == "premium")
async def premium_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    await callback.answer()
    if callback.message and await require_subscription(callback.bot, callback.from_user.id, callback.message):
        await safe_edit(callback.message, PREMIUM_TEXT, premium_menu())


@dp.callback_query(F.data.startswith("premium:plan:"))
async def premium_plan_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    await callback.answer()
    if not callback.message or not await require_subscription(callback.bot, callback.from_user.id, callback.message):
        return
    key = (callback.data or "").split(":")[-1]
    plan = PREMIUM_PRICES.get(key)
    if not plan:
        await safe_edit(callback.message, "⚠️ Тариф не найден.", premium_menu())
        return
    note = quote(f"Здравствуйте! Хочу купить ZEUS PREMIUM: {plan['label']} ({plan['stars']} Stars / {plan['rub']} руб.). Мой ID: {callback.from_user.id}")
    await safe_edit(
        callback.message,
        "💎 <b>ПОКУПКА ZEUS PREMIUM</b>\n\n"
        f"⚡ Тариф: <b>{plan['label']}</b>\n"
        f"⭐ Telegram Stars: <b>{plan['stars']}</b>\n"
        f"💳 Стоимость: <b>{plan['rub']} ₽</b>\n\n"
        "Напиши поддержке. После подтверждения администратор активирует Premium.",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать поддержке", url=f"{support_link()}?text={note}")],
            [InlineKeyboardButton(text="⬅️ К тарифам", callback_data="premium")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
        ]),
    )


@dp.callback_query(F.data == "search:mixed")
async def search_handler(callback: CallbackQuery) -> None:
    register_callback_user(callback)
    user_id = callback.from_user.id
    if not callback.message:
        await callback.answer()
        return
    if not await has_required_subscription(callback.bot, user_id):
        await callback.answer("Сначала подпишись на канал", show_alert=True)
        await callback.message.answer(subscription_text(), reply_markup=subscription_menu())
        return
    if user_id in active_searches:
        await callback.answer("⏳ У тебя уже идёт поиск", show_alert=True)
        return

    admission = db.try_start_search(user_id)
    if not admission.allowed:
        if admission.reason == "banned":
            await callback.answer("🚫 Доступ заблокирован", show_alert=True)
            await callback.message.answer(f"🚫 Причина блокировки: <b>{html.escape(admission.banned_reason or 'не указана')}</b>")
            return
        if admission.reason == "cooldown":
            await callback.answer(f"⏳ Подожди ещё {admission.cooldown_left} сек.", show_alert=True)
            return
        daily_limit = max(1, db.get_int_setting("free_daily_searches", DEFAULT_FREE_DAILY_SEARCHES))
        await callback.answer("Free-лимит на сегодня исчерпан", show_alert=True)
        await callback.message.answer(
            "🌩 <b>ЛИМИТ FREE ИСЧЕРПАН</b>\n\n"
            f"Для Free доступно <b>{daily_limit}</b> поиска в сутки.\n"
            "Лимит восстановится в <b>00:00 МСК</b>.\n\n"
            "Оформи ZEUS PREMIUM, чтобы искать без ограничений.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Открыть Premium", callback_data="premium")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
            ]),
        )
        return

    await callback.answer("⚡ Зевс начинает поиск")
    active_searches.add(user_id)
    check_limit = min(100, max(1, db.get_int_setting("check_limit", DEFAULT_CHECK_LIMIT)))
    remaining_text = "безлимит" if admission.remaining is None else str(admission.remaining)
    progress_message = await callback.message.answer(
        "⚡ <b>ЗЕВС ИЩЕТ ДОСТОЙНОЕ ИМЯ...</b>\n\n"
        "Схемы: <code>VVCCV · VCCVV · VCVVC</code>\n"
        f"Проверено: <code>0/{check_limit}</code>\n"
        f"Осталось поисков сегодня: <b>{remaining_text}</b>"
    )

    async def update_progress(checked: int, limit: int) -> None:
        try:
            await progress_message.edit_text(
                "⚡ <b>МОЛНИИ БЬЮТ ПО USERNAME...</b>\n\n"
                "Схемы: <code>VVCCV · VCCVV · VCVVC</code>\n"
                f"Проверено: <code>{checked}/{limit}</code>\n"
                "📱 Telegram + 💎 Fragment"
            )
        except Exception:
            pass

    try:
        generated, checked, request_errors = await asyncio.wait_for(
            checker.find_free_username(check_limit, update_progress),
            timeout=SEARCH_TIMEOUT,
        )
        db.record_search(user_id, generated.username if generated else None, generated.pattern if generated else None, checked, request_errors)
        if generated is None:
            if request_errors >= max(5, checked // 2):
                text = (
                    "⚠️ <b>ОЛИМП ВРЕМЕННО НЕ ОТВЕЧАЕТ</b>\n\n"
                    f"Проверено: <code>{checked}/{check_limit}</code>\n"
                    f"Ошибок запросов: <code>{request_errors}</code>\n\n"
                    "Подожди немного и повтори поиск."
                )
            else:
                text = (
                    f"🌩 <b>ЗЕВС НЕ НАШЁЛ ИМЯ ЗА {check_limit} ПРОВЕРОК</b>\n\n"
                    "Нажми «Повторить поиск» через несколько секунд: бот создаст новые варианты."
                )
            await progress_message.edit_text(text, reply_markup=retry_menu())
            return
        await progress_message.edit_text(
            "⚡ <b>МОЛНИЯ УКАЗАЛА НА USERNAME</b>\n\n"
            f"🏛 Имя: <code>@{html.escape(generated.username)}</code>\n"
            f"🧬 Схема: <code>{html.escape(generated.pattern)}</code>\n"
            f"🔎 Проверено вариантов: <code>{checked}</code>\n\n"
            "Перед установкой открой Telegram и проверь имя вручную.",
            reply_markup=result_menu(generated.username),
        )
    except asyncio.TimeoutError:
        db.record_search(user_id, None, None, check_limit, 1)
        await progress_message.edit_text("⏱ <b>ПОИСК ОСТАНОВЛЕН ПО ВРЕМЕНИ</b>\n\nПодожди несколько секунд и попробуй снова.", reply_markup=retry_menu())
    except Exception:
        logger.exception("Search failed for user %s", user_id)
        await progress_message.edit_text("⚠️ Во время поиска произошла ошибка. Попробуй ещё раз позже.", reply_markup=retry_menu())
    finally:
        active_searches.discard(user_id)


# ═══════════════════════════════ ОБРАБОТЧИКИ АДМИНКИ ═══════════════════════════════

@dp.callback_query(F.data == "admin:panel")
async def admin_panel_handler(callback: CallbackQuery, state: FSMContext) -> None:
    register_callback_user(callback)
    if await reject_non_admin(callback) or not callback.message:
        return
    await state.clear()
    stats = db.get_stats()
    await callback.answer()
    await safe_edit(
        callback.message,
        "⚙️ <b>АДМИН-ПАНЕЛЬ ZEUS FIND</b>\n\n"
        f"👥 Пользователей: <b>{stats['total_users']}</b>\n"
        f"🟢 Активных за сутки: <b>{stats['active_today']}</b>\n"
        f"💎 Premium: <b>{stats['premium_users']}</b>\n"
        f"🚫 Заблокировано: <b>{stats['banned_users']}</b>\n"
        f"⚡ Поисков всего: <b>{stats['total_searches']}</b>\n"
        f"🏛 Находок: <b>{stats['total_found']}</b>\n\n"
        "Выбери раздел:",
        admin_panel_menu(),
    )


@dp.callback_query(F.data == "admin:stats")
async def admin_stats_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    stats = db.get_stats()
    await callback.answer()
    await safe_edit(
        callback.message,
        "📊 <b>СТАТИСТИКА ZEUS FIND</b>\n\n"
        f"👥 Всего пользователей: <b>{stats['total_users']}</b>\n"
        f"🟢 Активны за сутки: <b>{stats['active_today']}</b>\n"
        f"📅 Активны за 7 дней: <b>{stats['active_week']}</b>\n"
        f"💎 Premium-пользователи: <b>{stats['premium_users']}</b>\n"
        f"🚫 Заблокировано: <b>{stats['banned_users']}</b>\n\n"
        f"⚡ Всего запусков поиска: <b>{stats['total_searches']}</b>\n"
        f"🌩 Free-поисков сегодня: <b>{stats['free_searches_today']}</b>\n"
        f"📜 Записей поиска сегодня: <b>{stats['history_today']}</b>\n"
        f"🏛 Находок сегодня: <b>{stats['found_today']}</b>\n"
        f"🏆 Находок за всё время: <b>{stats['total_found']}</b>",
        admin_back_menu(),
    )


@dp.callback_query(F.data.startswith("admin:users:"))
async def admin_users_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    parts = (callback.data or "").split(":")
    mode = parts[2] if len(parts) > 2 else "all"
    try:
        page = max(0, int(parts[3]))
    except (IndexError, ValueError):
        page = 0
    users, total = db.list_users(mode, page)
    await callback.answer()
    await safe_edit(
        callback.message,
        "👥 <b>ПОЛЬЗОВАТЕЛИ ZEUS FIND</b>\n\n"
        f"Фильтр: <code>{html.escape(mode)}</code>\n"
        f"Всего в разделе: <b>{total}</b>\n"
        f"Страница: <b>{page + 1}</b>",
        admin_users_menu(users, mode, page, total),
    )


@dp.callback_query(F.data == "admin:user:lookup")
async def admin_user_lookup_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    await state.set_state(AdminStates.waiting_user_lookup)
    await callback.answer()
    await safe_edit(callback.message, "🔎 <b>ПОИСК ПОЛЬЗОВАТЕЛЯ</b>\n\nОтправь числовой ID или @username пользователя.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:panel")]]))


@dp.message(StateFilter(AdminStates.waiting_user_lookup))
async def admin_user_lookup_message(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    user = db.find_user(raw)
    await state.clear()
    if not user:
        await message.answer("❌ Пользователь не найден.", reply_markup=admin_back_menu())
        return
    await message.answer(render_user_card(user), reply_markup=admin_user_menu(user))


@dp.callback_query(F.data.startswith("admin:user:view:"))
async def admin_user_view_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    user = db.get_user(user_id)
    await callback.answer()
    if not user:
        await safe_edit(callback.message, "❌ Пользователь не найден.", admin_back_menu())
        return
    await safe_edit(callback.message, render_user_card(user), admin_user_menu(user))


@dp.callback_query(F.data.startswith("admin:user:premium:"))
async def admin_user_premium_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    parts = (callback.data or "").split(":")
    user_id, days = int(parts[-2]), int(parts[-1])
    expiry = db.grant_premium(user_id, days)
    db.log_admin(callback.from_user.id, "grant_premium", user_id, f"{days} days")
    user = db.get_user(user_id)
    await callback.answer(f"✅ Premium +{days} дн.", show_alert=True)
    if user:
        await safe_edit(callback.message, render_user_card(user) + f"\n\n✅ Выдано <b>{days} дн.</b> до <b>{format_msk_datetime(expiry)}</b>", admin_user_menu(user))


@dp.callback_query(F.data.startswith("admin:user:premium_custom:"))
async def admin_user_premium_custom_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    await state.set_state(AdminStates.waiting_custom_premium_days)
    await state.update_data(target_user_id=user_id)
    await callback.answer()
    await safe_edit(callback.message, f"💎 <b>СВОЙ СРОК PREMIUM</b>\n\nПользователь: <code>{user_id}</code>\nОтправь количество дней числом.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin:user:view:{user_id}")]]))


@dp.message(StateFilter(AdminStates.waiting_custom_premium_days))
async def admin_user_premium_custom_message(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    user_id = int(data["target_user_id"])
    try:
        days = int((message.text or "").strip())
        if not 1 <= days <= 3650:
            raise ValueError
    except ValueError:
        await message.answer("❌ Отправь число от 1 до 3650.")
        return
    expiry = db.grant_premium(user_id, days)
    db.log_admin(message.from_user.id, "grant_premium_custom", user_id, f"{days} days")
    await state.clear()
    user = db.get_user(user_id)
    await message.answer(f"✅ Premium выдан до <b>{format_msk_datetime(expiry)}</b>.", reply_markup=admin_user_menu(user) if user else admin_back_menu())


@dp.callback_query(F.data.startswith("admin:user:unpremium:"))
async def admin_user_unpremium_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    db.remove_premium(user_id)
    db.log_admin(callback.from_user.id, "remove_premium", user_id)
    user = db.get_user(user_id)
    await callback.answer("✅ Premium отключён", show_alert=True)
    if user:
        await safe_edit(callback.message, render_user_card(user), admin_user_menu(user))


@dp.callback_query(F.data.startswith("admin:user:reset:"))
async def admin_user_reset_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    db.reset_user_limit(user_id)
    db.log_admin(callback.from_user.id, "reset_daily_limit", user_id)
    user = db.get_user(user_id)
    await callback.answer("✅ Лимит сброшен", show_alert=True)
    if user:
        await safe_edit(callback.message, render_user_card(user), admin_user_menu(user))


@dp.callback_query(F.data.startswith("admin:user:ban:"))
async def admin_user_ban_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    await state.set_state(AdminStates.waiting_ban_reason)
    await state.update_data(target_user_id=user_id)
    await callback.answer()
    await safe_edit(callback.message, f"🚫 <b>БЛОКИРОВКА</b>\n\nПользователь: <code>{user_id}</code>\nОтправь причину блокировки текстом.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin:user:view:{user_id}")]]))


@dp.message(StateFilter(AdminStates.waiting_ban_reason))
async def admin_user_ban_message(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    user_id = int(data["target_user_id"])
    reason = (message.text or "Без причины").strip()[:500] or "Без причины"
    db.ban_user(user_id, reason)
    db.log_admin(message.from_user.id, "ban_user", user_id, reason)
    await state.clear()
    user = db.get_user(user_id)
    await message.answer("🚫 Пользователь заблокирован.", reply_markup=admin_user_menu(user) if user else admin_back_menu())


@dp.callback_query(F.data.startswith("admin:user:unban:"))
async def admin_user_unban_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    user_id = int((callback.data or "").split(":")[-1])
    db.unban_user(user_id)
    db.log_admin(callback.from_user.id, "unban_user", user_id)
    user = db.get_user(user_id)
    await callback.answer("✅ Пользователь разбанен", show_alert=True)
    if user:
        await safe_edit(callback.message, render_user_card(user), admin_user_menu(user))


@dp.callback_query(F.data == "admin:history")
async def admin_history_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    rows = db.recent_searches()
    text = "⚡ <b>ПОСЛЕДНИЕ ПОИСКИ</b>\n\n"
    if not rows:
        text += "История пока пуста."
    for row in rows:
        account = f"@{row['account_username']}" if row.get("account_username") else str(row["user_id"])
        found = f"@{row['username']} · {row['pattern']}" if row.get("is_found") else "не найдено"
        text += f"• <code>{html.escape(account)}</code> → <b>{html.escape(str(found))}</b> · {row['checked']} пров.\n"
    await callback.answer()
    await safe_edit(callback.message, text, admin_back_menu())


@dp.callback_query(F.data == "admin:log")
async def admin_log_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    rows = db.recent_admin_log()
    text = "🧾 <b>ЖУРНАЛ АДМИНИСТРАТОРА</b>\n\n"
    if not rows:
        text += "Записей пока нет."
    for row in rows:
        target = f" → {row['target_user_id']}" if row.get("target_user_id") else ""
        text += f"• <code>{html.escape(str(row['action']))}</code>{html.escape(target)}\n"
    await callback.answer()
    await safe_edit(callback.message, text, admin_back_menu())


@dp.callback_query(F.data == "admin:channel")
async def admin_channel_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    info = await inspect_channel(callback.bot)
    status = "✅ Бот администратор" if info["bot_is_admin"] else "❌ Бот НЕ администратор"
    error = f"\n⚠️ Ошибка: <code>{html.escape(str(info['error']))}</code>" if info["error"] else ""
    text = (
        "📡 <b>ДИАГНОСТИКА КАНАЛА</b>\n\n"
        f"📣 Настройка: <code>{html.escape(str(info['raw']))}</code>\n"
        f"🏛 Название: <b>{html.escape(str(info['title']))}</b>\n"
        f"🆔 Chat ID: <code>{info['chat_id']}</code>\n"
        f"👥 Подписчиков: <b>{info['members']}</b>\n"
        f"🤖 Статус бота: <code>{html.escape(str(info['bot_status']))}</code>\n"
        f"🔐 Проверка подписки: <b>{status}</b>"
        f"{error}\n\n"
        "Для надёжной проверки подписки добавь бота администратором канала."
    )
    await callback.answer()
    await safe_edit(callback.message, text, admin_channel_menu())


@dp.callback_query(F.data == "admin:settings")
async def admin_settings_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    await state.clear()
    await callback.answer()
    await safe_edit(callback.message, render_settings_text(), admin_settings_menu())


@dp.callback_query(F.data == "admin:settings:toggle_subscription")
async def admin_settings_toggle_subscription_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    new_value = not subscription_enabled()
    db.set_setting("require_subscription", "1" if new_value else "0")
    db.log_admin(callback.from_user.id, "toggle_subscription", details=str(new_value))
    await callback.answer("✅ Настройка изменена", show_alert=True)
    await safe_edit(callback.message, render_settings_text(), admin_settings_menu())


SETTING_PROMPTS = {
    "set_channel": ("required_channel", "📡 Отправь @username канала или его числовой ID вида <code>-100...</code>.", "channel"),
    "set_channel_link": ("required_channel_link", "🔗 Отправь ссылку канала. Для автоматической ссылки публичного канала отправь <code>-</code>.", "channel_link"),
    "set_free_limit": ("free_daily_searches", "🌩 Отправь количество Free-поисков в сутки: число от 1 до 100.", "free_limit"),
    "set_cooldown": ("search_cooldown_seconds", "⏳ Отправь кулдаун в секундах: число от 0 до 3600.", "cooldown"),
    "set_check_limit": ("check_limit", "🔎 Отправь максимум кандидатов за запуск: число от 1 до 100.", "check_limit"),
}


@dp.callback_query(F.data.startswith("admin:settings:set_"))
async def admin_settings_value_start_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    action = (callback.data or "").split(":")[-1]
    config = SETTING_PROMPTS.get(action)
    if not config:
        await callback.answer("Настройка не найдена", show_alert=True)
        return
    key, prompt, kind = config
    await state.set_state(AdminStates.waiting_setting_value)
    await state.update_data(setting_key=key, setting_kind=kind)
    await callback.answer()
    await safe_edit(callback.message, f"⚙️ <b>ИЗМЕНЕНИЕ НАСТРОЙКИ</b>\n\n{prompt}", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:settings")]]))


@dp.message(StateFilter(AdminStates.waiting_setting_value))
async def admin_settings_value_message(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    key, kind = str(data["setting_key"]), str(data["setting_kind"])
    raw = (message.text or "").strip()
    try:
        if kind == "channel":
            parsed = parse_channel_ref(raw)
            if not parsed:
                raise ValueError("Канал не может быть пустым")
            value = str(parsed)
        elif kind == "channel_link":
            value = "" if raw == "-" else raw
            if value and not value.startswith("https://t.me/"):
                raise ValueError("Ссылка должна начинаться с https://t.me/")
        else:
            number = int(raw)
            limits = {"free_limit": (1, 100), "cooldown": (0, 3600), "check_limit": (1, 100)}
            minimum, maximum = limits[kind]
            if not minimum <= number <= maximum:
                raise ValueError(f"Допустимый диапазон: {minimum}–{maximum}")
            value = str(number)
    except (ValueError, KeyError) as exc:
        await message.answer(f"❌ Некорректное значение: {html.escape(str(exc))}")
        return
    db.set_setting(key, value)
    db.log_admin(message.from_user.id, "change_setting", details=f"{key}={value}")
    await state.clear()
    await message.answer("✅ Настройка сохранена. Она применяется сразу.", reply_markup=admin_settings_menu())


@dp.callback_query(F.data == "admin:settings:reset_all_confirm")
async def admin_settings_reset_all_confirm_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    await callback.answer()
    await safe_edit(callback.message, "♻️ <b>СБРОС ДНЕВНЫХ ЛИМИТОВ</b>\n\nСбросить счётчик поиска всем пользователям?", admin_reset_confirm_menu())


@dp.callback_query(F.data == "admin:settings:reset_all")
async def admin_settings_reset_all_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    count = db.reset_all_limits()
    db.log_admin(callback.from_user.id, "reset_all_daily_limits", details=f"users={count}")
    await callback.answer("✅ Лимиты сброшены", show_alert=True)
    await safe_edit(callback.message, f"✅ Дневные лимиты сброшены у <b>{count}</b> пользователей.", admin_settings_menu())


@dp.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await callback.answer()
    await safe_edit(callback.message, "📣 <b>РАССЫЛКА</b>\n\nОтправь текст сообщения. Перед отправкой бот покажет предпросмотр.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:panel")]]))


@dp.message(StateFilter(AdminStates.waiting_broadcast_text))
async def admin_broadcast_text_message(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Отправь текстовое сообщение.")
        return
    if len(text) > 4000:
        await message.answer("❌ Сообщение слишком длинное. Максимум 4000 символов.")
        return
    await state.update_data(broadcast_text=text)
    await message.answer(
        "📣 <b>ПРЕДПРОСМОТР РАССЫЛКИ</b>\n\n"
        f"{html.escape(text)}\n\n"
        "Отправить всем пользователям?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="admin:broadcast:send")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin:panel")],
        ]),
    )


@dp.callback_query(F.data == "admin:broadcast:send")
async def admin_broadcast_send_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    data = await state.get_data()
    text = str(data.get("broadcast_text", "")).strip()
    if not text:
        await callback.answer("Текст рассылки потерян", show_alert=True)
        await state.clear()
        return
    await callback.answer("Рассылка началась", show_alert=True)
    await safe_edit(callback.message, "📣 <b>РАССЫЛКА ЗАПУЩЕНА...</b>")
    success = 0
    failed = 0
    for user_id in db.get_all_user_ids():
        try:
            await callback.bot.send_message(user_id, text, parse_mode=None)
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.04)
    db.log_admin(callback.from_user.id, "broadcast", details=f"success={success}; failed={failed}")
    await state.clear()
    await safe_edit(callback.message, f"✅ <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n\nДоставлено: <b>{success}</b>\nОшибок: <b>{failed}</b>", admin_back_menu())


@dp.callback_query(F.data == "admin:export")
async def admin_export_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    count = db.export_users_csv(EXPORT_PATH)
    db.log_admin(callback.from_user.id, "export_users_csv", details=f"users={count}")
    await callback.answer("✅ CSV сформирован", show_alert=True)
    await callback.message.answer_document(FSInputFile(EXPORT_PATH), caption=f"📤 Экспорт пользователей ZEUS FIND: {count}")


@dp.callback_query(F.data == "admin:system")
async def admin_system_handler(callback: CallbackQuery) -> None:
    if await reject_non_admin(callback) or not callback.message:
        return
    db_size = Path(DATABASE_PATH).stat().st_size if Path(DATABASE_PATH).exists() else 0
    await callback.answer()
    await safe_edit(
        callback.message,
        "🩺 <b>СИСТЕМА ZEUS FIND</b>\n\n"
        f"⏱ Аптайм: <b>{human_uptime()}</b>\n"
        f"🔎 Активных поисков: <b>{len(active_searches)}</b>\n"
        f"🗄 База данных: <b>{db_size / 1024:.1f} КБ</b>\n"
        f"📣 Обязательная подписка: <b>{human_bool(subscription_enabled())}</b>\n"
        f"🌩 Free-лимит: <b>{db.get_int_setting('free_daily_searches', DEFAULT_FREE_DAILY_SEARCHES)}</b>\n"
        f"⏳ Кулдаун: <b>{db.get_int_setting('search_cooldown_seconds', DEFAULT_SEARCH_COOLDOWN_SECONDS)} сек.</b>\n"
        f"🔎 Проверок за запуск: <b>{db.get_int_setting('check_limit', DEFAULT_CHECK_LIMIT)}</b>",
        admin_back_menu(),
    )


# ═══════════════════════════════ ЗАПУСК ═══════════════════════════════

async def on_startup() -> None:
    await checker.start()
    logger.info("ZEUS FIND started")


async def on_shutdown() -> None:
    await checker.close()
    logger.info("ZEUS FIND stopped")


def validate_config() -> None:
    if not BOT_TOKEN.strip():
        raise RuntimeError("Вставь новый токен бота в BOT_TOKEN в начале файла")
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст: инлайн-админка никому не будет видна")
    if REQUIRED_CHANNEL == "@your_channel":
        logger.warning("Канал пока не настроен. Зайди администратором и установи его через инлайн-админку.")


async def main() -> None:
    validate_config()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    try:
        await dp.start_polling(bot)
    finally:
        await checker.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
