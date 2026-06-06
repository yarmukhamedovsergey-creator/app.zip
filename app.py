import asyncio
import logging
import re
import random
import sqlite3
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from fake_useragent import UserAgent
from curl_cffi import requests as curl_requests

# ⚡══════════════════════════════════════════════════════════════════════════════
# ⚡ ZEUS FIND — БОЖЕСТВЕННЫЙ ПОИСК USERNAME
# ⚡══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | ⚡ %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)

# ⚡════════════════════ КОНФИГУРАЦИЯ ════════════════════
BOT_TOKEN = "8660751586:AAGZHf5TMcJVMMS1XR8xJ3XeNb4VTmYvWKQ"
ADMIN_IDS = [5969266721]
SUPPORT_USERNAME = "@emeuw"
PREMIUM_BUY_URL = "https://t.me/emeuw"

FREE_DAILY_SEARCHES = 3
PREMIUM_DAILY_SEARCHES = 999999
USERNAMES_PER_SEARCH = 1

# СТАРЫЙ USER_AGENTS УДАЛИТЬ!

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ⚡══════════════════════════════════════════════════════════════════════════════
# ⚡ ZEUS FIND — БОЖЕСТВЕННЫЙ ПОИСК USERNAME
# ⚡══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | ⚡ %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)

# ⚡════════════════════ КОНФИГУРАЦИЯ ════════════════════
BOT_TOKEN = "8660751586:AAGZHf5TMcJVMMS1XR8xJ3XeNb4VTmYvWKQ"
ADMIN_IDS = [5969266721]          # ID администраторов
SUPPORT_USERNAME = "@emeuw"                # юзернейм для поддержки/покупок
PREMIUM_BUY_URL = "https://t.me/emeuw"    # прямая ссылка для покупки

FREE_DAILY_SEARCHES = 3
PREMIUM_DAILY_SEARCHES = 999999
USERNAMES_PER_SEARCH = 1

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.43 Mobile Safari/537.36",
]

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# ⚡════════════════════ БАЗА ДАННЫХ ════════════════════

class Database:
    def __init__(self, db_path: str = "zeus_find.db"):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT DEFAULT 'ru',
                is_premium INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                banned_reason TEXT,
                subscription_type TEXT DEFAULT 'free',
                subscription_expiry TIMESTAMP,
                daily_searches_used INTEGER DEFAULT 0,
                last_search_date DATE,
                total_searches INTEGER DEFAULT 0,
                total_found INTEGER DEFAULT 0,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS found_usernames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                pattern_type TEXT,
                telegram_available INTEGER,
                fragment_available INTEGER,
                fully_free INTEGER DEFAULT 0,
                found_by_user_id INTEGER,
                found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_claimed INTEGER DEFAULT 0,
                claimed_by INTEGER,
                claimed_at TIMESTAMP,
                FOREIGN KEY (found_by_user_id) REFERENCES users(user_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                search_type TEXT,
                pattern_type TEXT,
                usernames_checked INTEGER,
                usernames_found INTEGER,
                usernames_list TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS premium_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                plan_type TEXT,
                duration_days INTEGER,
                price_stars REAL,
                price_rub REAL,
                status TEXT DEFAULT 'active',
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_premium ON users(is_premium)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_found_free ON found_usernames(fully_free)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_found_username ON found_usernames(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_search_user ON search_history(user_id)')
        # Колонка для задержки между поисками
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN last_search_time TIMESTAMP")
        except sqlite3.OperationalError:
            pass

        conn.commit()
        conn.close()
        logging.info("✅ База данных инициализирована")

    def create_user(self, user_data: dict) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            is_admin = 1 if user_data['user_id'] in ADMIN_IDS else 0
            cursor.execute('''
                INSERT OR IGNORE INTO users (
                    user_id, username, first_name, last_name,
                    language_code, is_admin, joined_at, last_activity
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (
                user_data['user_id'], user_data.get('username'),
                user_data.get('first_name'), user_data.get('last_name'),
                user_data.get('language_code', 'ru'), is_admin
            ))
            conn.commit()
            conn.close()
            return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Ошибка создания пользователя: {e}")
            return False

    def update_user_activity(self, user_id: int):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Ошибка обновления активности: {e}")

    def get_user(self, user_id: int) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
            conn.close()
            return dict(user) if user else None
        except Exception as e:
            logging.error(f"Ошибка получения пользователя: {e}")
            return None

    def reset_daily_searches(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users
                SET daily_searches_used = 0
                WHERE last_search_date < DATE('now') OR last_search_date IS NULL
            ''')
            reset_count = cursor.rowcount
            conn.commit()
            conn.close()
            if reset_count > 0:
                logging.info(f"🔄 Сброшены поиски: {reset_count} пользователей")
            return reset_count
        except Exception as e:
            logging.error(f"Ошибка сброса: {e}")
            return 0

    def get_remaining_searches(self, user_id: int) -> int:
        user = self.get_user(user_id)
        if not user:
            return 0
        today = datetime.now().date()
        last_search = None
        if user['last_search_date']:
            last_search = datetime.strptime(user['last_search_date'], '%Y-%m-%d').date()
        if last_search != today:
            self.reset_user_daily_searches(user_id)
            daily_used = 0
        else:
            daily_used = user['daily_searches_used']
        is_premium = self.check_premium_status(user_id)
        daily_limit = PREMIUM_DAILY_SEARCHES if is_premium else FREE_DAILY_SEARCHES
        return max(0, daily_limit - daily_used)

    def can_search(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user:
            return True
        last_time = user.get('last_search_time')
        if last_time:
            last = datetime.fromisoformat(last_time)
            if (datetime.now() - last).total_seconds() < 5:
                return False
        return True

    def reset_user_daily_searches(self, user_id: int):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET daily_searches_used = 0, last_search_date = DATE(\'now\') WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Ошибка сброса: {e}")

    def use_search(self, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users
                SET daily_searches_used = daily_searches_used + 1,
                    total_searches = total_searches + 1,
                    last_search_date = DATE('now'),
                    last_search_time = CURRENT_TIMESTAMP,
                    last_activity = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (user_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Ошибка поиска: {e}")
            return False

    def get_user_limits_info(self, user_id: int) -> Dict:
        user = self.get_user(user_id)
        is_premium = self.check_premium_status(user_id)
        remaining = self.get_remaining_searches(user_id)
        daily_limit = PREMIUM_DAILY_SEARCHES if is_premium else FREE_DAILY_SEARCHES
        used_today = user['daily_searches_used'] if user else 0
        return {
            'is_premium': is_premium,
            'daily_limit': "∞" if is_premium else daily_limit,
            'used_today': used_today,
            'remaining': "∞" if is_premium else remaining,
            'usernames_per_search': USERNAMES_PER_SEARCH,
            'total_searches': user['total_searches'] if user else 0,
            'total_found': user['total_found'] if user else 0
        }

    def add_found_usernames(self, user_id: int, count: int):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET total_found = total_found + ? WHERE user_id = ?', (count, user_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Ошибка статистики: {e}")

    def check_premium_status(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user:
            return False
        if user['is_premium'] and user['subscription_expiry']:
            expiry = datetime.fromisoformat(user['subscription_expiry'])
            if expiry < datetime.now():
                self.remove_premium(user_id)
                return False
            return True
        return False

    def set_premium(self, user_id: int, duration_days: int, plan_type: str = "monthly",
                    price_stars: int = 0, price_rub: int = 0) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            expires_at = datetime.now() + timedelta(days=duration_days)
            cursor.execute('''
                UPDATE users
                SET is_premium = 1, subscription_type = ?, subscription_expiry = ?
                WHERE user_id = ?
            ''', (plan_type, expires_at, user_id))
            cursor.execute('''
                INSERT INTO premium_subscriptions (
                    user_id, plan_type, duration_days, price_stars, price_rub,
                    started_at, expires_at, status
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 'active')
            ''', (user_id, plan_type, duration_days, price_stars, price_rub, expires_at))
            conn.commit()
            conn.close()
            logging.info(f"✅ Премиум выдан {user_id} на {duration_days} дн.")
            return True
        except Exception as e:
            logging.error(f"Ошибка выдачи премиума: {e}")
            return False

    def remove_premium(self, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET is_premium = 0, subscription_type = 'free', subscription_expiry = NULL
                WHERE user_id = ?
            ''', (user_id,))
            cursor.execute('''
                UPDATE premium_subscriptions SET status = 'expired'
                WHERE user_id = ? AND status = 'active'
            ''', (user_id,))
            conn.commit()
            conn.close()
            logging.info(f"❌ Премиум удалён у {user_id}")
            return True
        except Exception as e:
            logging.error(f"Ошибка удаления премиума: {e}")
            return False

    def check_expired_premiums(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id FROM users
                WHERE is_premium = 1 AND subscription_expiry < CURRENT_TIMESTAMP
            ''')
            expired_users = cursor.fetchall()
            for user in expired_users:
                self.remove_premium(user['user_id'])
            conn.close()
            if expired_users:
                logging.info(f"🔄 Истекших премиумов: {len(expired_users)}")
            return len(expired_users)
        except Exception as e:
            logging.error(f"Ошибка проверки: {e}")
            return 0

    def save_found_username(self, username: str, user_id: int,
                            telegram_available: bool, fragment_available: bool,
                            pattern_type: str = "manual") -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # ИСПРАВЛЕНО: fully_free = оба свободны
            fully_free = 1 if (telegram_available and fragment_available) else 0
            cursor.execute('''
                INSERT OR REPLACE INTO found_usernames (
                    username, pattern_type, telegram_available,
                    fragment_available, fully_free, found_by_user_id
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, pattern_type, int(telegram_available), int(fragment_available), fully_free, user_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения username: {e}")
            return False

    def save_search_history(self, user_id: int, search_type: str,
                            pattern_type: str, checked: int,
                            found: int, usernames_list: list):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            usernames_str = ','.join(usernames_list) if usernames_list else ''
            cursor.execute('''
                INSERT INTO search_history (
                    user_id, search_type, pattern_type,
                    usernames_checked, usernames_found, usernames_list
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, search_type, pattern_type, checked, found, usernames_str))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Ошибка сохранения истории: {e}")

    def get_user_search_history(self, user_id: int, limit: int = 5) -> List[Dict]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM search_history
                WHERE user_id = ? ORDER BY created_at DESC LIMIT ?
            ''', (user_id, limit))
            history = cursor.fetchall()
            conn.close()
            return [dict(r) for r in history]
        except Exception as e:
            logging.error(f"Ошибка получения истории: {e}")
            return []

    def get_premium_stats(self) -> Dict:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) as total FROM users WHERE is_premium = 1')
            total_premium = cursor.fetchone()['total']
            cursor.execute('SELECT COUNT(*) as total FROM users')
            total_users = cursor.fetchone()['total']
            conn.close()
            return {
                'total_premium': total_premium,
                'total_users': total_users,
                'premium_percentage': (total_premium / total_users * 100) if total_users > 0 else 0
            }
        except Exception as e:
            logging.error(f"Ошибка статистики: {e}")
            return {}

    def ban_user(self, user_id: int, reason: str = "") -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET is_banned = 1, banned_reason = ? WHERE user_id = ?', (reason, user_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Ошибка бана: {e}")
            return False

    def unban_user(self, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET is_banned = 0, banned_reason = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Ошибка разбана: {e}")
            return False

    def get_all_users(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?', (limit, offset))
            users = cursor.fetchall()
            conn.close()
            return [dict(user) for user in users]
        except Exception as e:
            logging.error(f"Ошибка получения пользователей: {e}")
            return []


db = Database()


# ⚡════════════════════ ПРОВЕРКА USERNAME ════════════════════

class UsernameChecker:
    def __init__(self):
        self.ua = UserAgent()
        self.session_telegram = None
        # Для Fragment используем синхронную сессию curl_cffi, 
        # но вызовы обернём в asyncio.to_thread

    async def init_sessions(self):
        """Инициализация сессии для Telegram (aiohttp)"""
        if not self.session_telegram:
            import aiohttp
            self.session_telegram = aiohttp.ClientSession(
                headers=self._get_headers(),
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def close_sessions(self):
        if self.session_telegram:
            await self.session_telegram.close()
        # curl_cffi сессии закрываются автоматически

    def _get_headers(self) -> dict:
        """Реалистичные заголовки браузера"""
        return {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    @staticmethod
    def is_valid_username(username: str) -> bool:
        return bool(re.match(r'^[a-zA-Z][a-zA-Z0-9_]{3,31}$', username))

    async def check_telegram_username(self, username: str) -> Optional[bool]:
        """
        True  – username свободен
        False – занят
        None  – ошибка проверки
        """
        try:
            url = f"https://t.me/{username}"
            headers = self._get_headers()
            async with self.session_telegram.get(url, headers=headers, allow_redirects=True) as resp:
                text = await resp.text()
                # 1) Признак занятости: страница содержит инфу о пользователе
                if 'tgme_page_extra' in text and 'Last seen' in text:
                    return False
                # 2) Если есть кнопка "Send Message" – тоже занят
                if 'class="tgme_page_button"' in text and 'Send Message' in text:
                    return False
                # 3) Если страница предлагает скачать Telegram – скорее всего свободен
                if 'If you have Telegram, you can contact' not in text and 'tgme_page_owner' not in text:
                    # нет информации о владельце → вероятно свободен
                    return True
                # 4) Доп. проверка: username в заголовке title
                if f'@{username}' in text and ('Telegram' in text or 'Contact' in text):
                    return False
                return True
        except Exception as e:
            print(f"Telegram check error for {username}: {e}")
            return None

    async def check_fragment_username(self, username: str) -> Optional[bool]:
        """
        True  – доступен для покупки
        False – уже занят/продан
        None  – ошибка
        """
        def _sync_check():
            url = f"https://fragment.com/username/{username}"
            try:
                # curl_cffi имитирует Chrome и обходит Cloudflare
                response = curl_requests.get(
                    url,
                    headers={"User-Agent": self.ua.random},
                    impersonate="chrome120",   # эмуляция браузера
                    timeout=20,
                    verify=False
                )
                text = response.text.lower()
                # Признаки, что username занят
                if "not available" in text or "already taken" in text or "sold" in text:
                    return False
                # Признаки, что доступен
                if "available for purchase" in text or "buy now" in text or "place a bid" in text:
                    return True
                # Если страница не содержит явных индикаторов – считаем недоступным
                return False
            except Exception as e:
                print(f"Fragment check error for {username}: {e}")
                return None

        try:
            # Выполняем синхронный curl_cffi в отдельном потоке
            return await asyncio.to_thread(_sync_check)
        except Exception as e:
            print(f"Fragment async error: {e}")
            return None

    async def comprehensive_check(self, username: str) -> dict:
        """Полная проверка с задержкой между запросами"""
        # Случайная задержка, чтобы не спамить
        await asyncio.sleep(random.uniform(0.5, 2.5))

        result = {
            "username": username,
            "telegram_available": None,
            "fragment_available": None,
            "fully_free": False
        }
        # Сначала проверяем Telegram (быстрее)
        result["telegram_available"] = await self.check_telegram_username(username)

        # Если Telegram занят, нет смысла проверять Fragment (username уже не свободен)
        if result["telegram_available"] is False:
            result["fragment_available"] = None
            result["fully_free"] = False
            return result

        # Пауза перед Fragment проверкой
        await asyncio.sleep(random.uniform(0.8, 2.0))
        result["fragment_available"] = await self.check_fragment_username(username)

        # Логика полностью свободного имени:
        # Telegram доступен (True) И Fragment доступен (True)
        if result["telegram_available"] is True and result["fragment_available"] is True:
            result["fully_free"] = True
        else:
            result["fully_free"] = False

        return result


class CVCVCPattern:
    def __init__(self):
        self.good_consonants = 'bcdfghjklmnprstvxz'
        self.good_vowels = 'aeiouy'

    def generate_usernames(self, max_results=100):
        usernames = []
        seen = set()
        attempts = 0
        while len(usernames) < max_results and attempts < max_results * 10:
            c1 = random.choice(self.good_consonants)
            v1 = random.choice(self.good_vowels)
            c2 = random.choice(self.good_consonants)
            v2 = random.choice(self.good_vowels)
            c3 = random.choice(self.good_consonants)
            username = c1 + v1 + c2 + v2 + c3
            if len(set(username)) == 5 and UsernameChecker.is_valid_username(username):
                if username not in seen:
                    usernames.append(username)
                    seen.add(username)
            attempts += 1
        return usernames


checker = UsernameChecker()
pattern_generator = CVCVCPattern()


# ⚡════════════════════ FSM для админки ════════════════════

class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_premium_days = State()
    waiting_for_ban_reason = State()


# ⚡════════════════════ КЛАВИАТУРЫ ════════════════════

def get_main_menu(user_id: int = None):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⚡ Поиск", callback_data="search_cvcvc"))
    builder.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile"))
    builder.row(InlineKeyboardButton(text="💎 Премиум", callback_data="premium_menu"))
    builder.row(InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}"))
    if user_id:
        user = db.get_user(user_id)
        if user and (user['is_admin'] or user_id in ADMIN_IDS):
            builder.row(InlineKeyboardButton(text="🔧 Админ-панель", callback_data="admin_panel"))
    return builder.as_markup()


def get_premium_menu():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="💎 Купить", url=PREMIUM_BUY_URL))
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def get_result_keyboard(username: str):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Новый поиск", callback_data="search_cvcvc"),
        InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
    )
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def get_back_button():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    return builder.as_markup()


def get_admin_panel():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Выдать премиум", callback_data="admin_give_premium"))
    builder.row(InlineKeyboardButton(text="➖ Убрать премиум", callback_data="admin_remove_premium"))
    builder.row(InlineKeyboardButton(text="🚫 Забанить", callback_data="admin_ban"))
    builder.row(InlineKeyboardButton(text="✅ Разбанить", callback_data="admin_unban"))
    builder.row(InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"))
    builder.row(InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    return builder.as_markup()


# ⚡════════════════════ КОМАНДЫ ════════════════════

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    db.create_user({
        'user_id': user.id,
        'username': user.username,
        'first_name': user.first_name,
        'last_name': user.last_name,
        'language_code': user.language_code or 'ru'
    })
    limits = db.get_user_limits_info(user.id)

    welcome = (
        "⚡━━━━━━━━━━━━━━━━━━━⚡\n"
        "      ⚡ 𝐙𝐄𝐔𝐒 𝐅𝐈𝐍𝐃 ⚡\n"
        "   Божественный поиск имён\n"
        "⚡━━━━━━━━━━━━━━━━━━━⚡\n\n"
        f"👤 {user.first_name}, добро пожаловать на Олимп!\n\n"
        f"💎 Статус: {'⚡ Премиум' if limits['is_premium'] else '💫 Free'}\n"
        f"🔍 Поисков сегодня: {limits['used_today']}/{limits['daily_limit']}\n"
        f"📦 Выдаётся: {limits['usernames_per_search']} username\n\n"
        "⚡━━━━━━━━━━━━━━━━━━━⚡"
    )
    await message.answer(welcome, reply_markup=get_main_menu(user.id), parse_mode='HTML')


@dp.message(Command("profile"))
@dp.message(Command("stats"))   # оставлено для совместимости
async def cmd_profile(message: Message):
    user_id = message.from_user.id
    user = db.get_user(user_id)
    if not user:
        await message.answer("Сначала /start")
        return
    limits = db.get_user_limits_info(user_id)
    history = db.get_user_search_history(user_id, 3)

    premium_status = "⚡ Премиум" if limits['is_premium'] else "💫 Free"
    if limits['is_premium'] and user['subscription_expiry']:
        expiry = datetime.fromisoformat(user['subscription_expiry'])
        days_left = (expiry - datetime.now()).days
        premium_status += f" | {days_left} дн."

    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"📛 Имя: {user['first_name']}"
    )
    if user['username']:
        text += f"\n🔖 Username: @{user['username']}"
    text += (
        f"\n💎 Статус: {premium_status}\n\n"
        f"📊 <b>Статистика поисков:</b>\n"
        f"🔍 Сегодня: {limits['used_today']}/{limits['daily_limit']}\n"
        f"📈 Всего: {limits['total_searches']}\n"
        f"✅ Найдено: {limits['total_found']}\n"
        f"⚡ За поиск: {limits['usernames_per_search']} username\n"
    )
    if history:
        text += "\n📜 <b>Последние находки:</b>\n"
        for h in history:
            usernames = h['usernames_list'] if h['usernames_list'] else '—'
            parts = usernames.split(',')
            if len(parts) > 2:
                display = ', '.join(parts[:2]) + '...'
            else:
                display = usernames
            emoji = "✅" if h['usernames_found'] > 0 else "❌"
            text += f"{emoji} {display}\n"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"))
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')


# ⚡════════════════════ ПОИСК ════════════════════

@dp.message(Command("search"))
async def cmd_search(message: Message):
    await process_search(message.from_user.id, message)


async def process_search(user_id: int, message: Message):
    user = db.get_user(user_id)
    if not user:
        await message.answer("❌ Сначала /start")
        return
    if user['is_banned']:
        await message.answer("🚫 Ты забанен на Олимпе")
        return
    if not db.can_search(user_id):
        await message.answer("⏳ Подожди 5 секунд перед следующим поиском!")
        return

    remaining = db.get_remaining_searches(user_id)
    if remaining <= 0:
        if not db.check_premium_status(user_id):
            await message.answer(
                "⚠️ Лимит поисков исчерпан!\n"
                "💎 Безлимит с Premium: /premium",
                reply_markup=get_premium_menu()
            )
            return

    db.use_search(user_id)

    loading = await message.answer(
        "⚡ Зевс просматривает мир...\n"
        "🔍 Ищу достойное имя..."
    )

    usernames_to_check = pattern_generator.generate_usernames(150)
    found_free = []
    checked_count = 0

    try:
        for username in usernames_to_check:
            result = await checker.comprehensive_check(username)
            checked_count += 1

            if result["fully_free"]:
                found_free.append(username)
                db.save_found_username(
                    username=username, user_id=user_id,
                    telegram_available=result["telegram_available"],
                    fragment_available=result["fragment_available"],
                    pattern_type="CVCVC"
                )
                if len(found_free) >= USERNAMES_PER_SEARCH:
                    break

            if checked_count % 40 == 0:
                try:
                    await loading.edit_text(
                        f"🔍 Просканировано: {checked_count}\n"
                        "⚡ Молнии Зевса в действии..."
                    )
                except:
                    pass
            await asyncio.sleep(0.05)

        await loading.delete()

        db.save_search_history(
            user_id=user_id, search_type="CVCVC", pattern_type="CVCVC",
            checked=checked_count, found=len(found_free),
            usernames_list=found_free
        )
        db.add_found_usernames(user_id, len(found_free))

        if found_free:
            username = found_free[0]
            limits = db.get_user_limits_info(user_id)

            text = (
                f"⚡ <b>Найдено:</b> <code>{username}</code>\n\n"
                f"🔗 <a href='https://t.me/{username}'>Забрать в Telegram</a>\n"
                f"💎 <a href='https://fragment.com/username/{username}'>Fragment</a>"
            )
            if not limits['is_premium']:
                rem = db.get_remaining_searches(user_id)
                text += f"\n\n🔍 Осталось поисков: {rem}/{limits['daily_limit']}"

            await message.answer(text, reply_markup=get_result_keyboard(username), parse_mode='HTML')
        else:
            await message.answer(
                "❌ Имя не найдено\nПопробуй другой поиск.",
                reply_markup=get_result_keyboard("")
            )
    except Exception as e:
        logging.error(f"Ошибка поиска: {e}")
        await message.answer("⚡ Гром разразился! Ошибка.")
    finally:
        try:
            await loading.delete()
        except:
            pass


# ⚡════════════════════ ПРОВЕРКА ════════════════════

@dp.message(Command("check"))
async def cmd_check(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("⚡ Укажи username:\n<code>/check name</code>", parse_mode='HTML')
        return

    username = args[1].replace("@", "").strip().lower()
    if not checker.is_valid_username(username):
        await message.answer("❌ Некорректный username")
        return

    user_id = message.from_user.id
    if db.get_remaining_searches(user_id) <= 0:
        await message.answer("⚠️ Лимит исчерпан!\n💎 Безлимит: /premium", reply_markup=get_premium_menu())
        return

    db.use_search(user_id)

    msg = await message.answer(f"🔍 Проверяю: <code>{username}</code>...", parse_mode='HTML')
    result = await checker.comprehensive_check(username)

    if result["fully_free"]:
        db.save_found_username(
            username=username, user_id=user_id,
            telegram_available=result["telegram_available"],
            fragment_available=result["fragment_available"]
        )
        db.add_found_usernames(user_id, 1)

    db.save_search_history(
        user_id=user_id, search_type="check", pattern_type="manual",
        checked=1, found=1 if result["fully_free"] else 0,
        usernames_list=[username] if result["fully_free"] else []
    )

    tg_status = "✅ Свободен" if result["telegram_available"] else "❌ Занят" if result["telegram_available"] == False else "⚠️ Ошибка"
    frag_status = "✅ Доступен" if result["fragment_available"] else "❌ Занят" if result["fragment_available"] == False else "⚠️ Не проверен"
    status_emoji = "⚡" if result["fully_free"] else "❌"
    status_text = "СВОБОДЕН!" if result["fully_free"] else "Занят"

    text = (
        f"👤 <code>{result['username']}</code>\n"
        f"📱 Telegram: {tg_status}\n"
        f"💎 Fragment: {frag_status}\n"
        f"{status_emoji} {status_text}"
    )
    if result["fully_free"]:
        text += (
            f"\n\n🔗 <a href='https://t.me/{username}'>Забрать в Telegram</a>\n"
            f"💎 <a href='https://fragment.com/username/{username}'>Fragment</a>"
        )

    limits = db.get_user_limits_info(user_id)
    if not limits['is_premium']:
        text += f"\n\n🔍 Осталось: {limits['remaining']}/{limits['daily_limit']}"

    await msg.delete()
    await message.answer(text, reply_markup=get_result_keyboard(username) if result["fully_free"] else get_main_menu(user_id), parse_mode='HTML')


# ⚡════════════════════ CALLBACK ОБРАБОТЧИКИ ════════════════════

@dp.callback_query(lambda c: c.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "⚡ 𝐙𝐄𝐔𝐒 𝐅𝐈𝐍𝐃 — божественный поиск имён\n\n"
        "Выбери действие:",
        reply_markup=get_main_menu(callback.from_user.id)
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "profile")
async def profile_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    limits = db.get_user_limits_info(user_id)
    history = db.get_user_search_history(user_id, 3)

    premium_status = "⚡ Премиум" if limits['is_premium'] else "💫 Free"
    if limits['is_premium'] and user['subscription_expiry']:
        expiry = datetime.fromisoformat(user['subscription_expiry'])
        days_left = (expiry - datetime.now()).days
        premium_status += f" | {days_left} дн."

    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"📛 Имя: {user['first_name']}"
    )
    if user['username']:
        text += f"\n🔖 Username: @{user['username']}"
    text += (
        f"\n💎 Статус: {premium_status}\n\n"
        f"📊 <b>Статистика поисков:</b>\n"
        f"🔍 Сегодня: {limits['used_today']}/{limits['daily_limit']}\n"
        f"📈 Всего: {limits['total_searches']}\n"
        f"✅ Найдено: {limits['total_found']}\n"
        f"⚡ За поиск: {limits['usernames_per_search']} username\n"
    )
    if history:
        text += "\n📜 <b>Последние находки:</b>\n"
        for h in history:
            usernames = h['usernames_list'] if h['usernames_list'] else '—'
            parts = usernames.split(',')
            if len(parts) > 2:
                display = ', '.join(parts[:2]) + '...'
            else:
                display = usernames
            emoji = "✅" if h['usernames_found'] > 0 else "❌"
            text += f"{emoji} {display}\n"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()


@dp.callback_query(lambda c: c.data == "premium_menu")
async def premium_menu_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_premium = db.check_premium_status(user_id)

    if is_premium:
        user = db.get_user(user_id)
        expiry = datetime.fromisoformat(user['subscription_expiry'])
        days_left = (expiry - datetime.now()).days
        text = (
            "⚡ <b>Премиум активен</b>\n\n"
            f"📅 Осталось: {days_left} дн.\n"
            "🔍 Безлимит поисков\n"
            "💎 Божественный статус"
        )
    else:
        text = (
            "💎 <b>Премиум</b>\n\n"
            "⚡ Безлимит поисков\n"
            "⚡ Максимальная скорость\n"
            "⚡ Приоритетная поддержка\n\n"
            "Для покупки нажми кнопку ниже:"
        )
    await callback.message.edit_text(text, reply_markup=get_premium_menu(), parse_mode='HTML')
    await callback.answer()


@dp.callback_query(lambda c: c.data == "search_cvcvc")
async def search_callback(callback: CallbackQuery):
    await callback.answer("⚡ Зевс начинает поиск...")
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    if not user or user['is_banned']:
        return

    if not db.can_search(user_id):
        await callback.message.answer("⏳ Подожди 5 секунд перед следующим поиском!")
        await callback.answer()
        return

    if db.get_remaining_searches(user_id) <= 0:
        if not db.check_premium_status(user_id):
            await callback.message.answer("⚠️ Лимит исчерпан!", reply_markup=get_premium_menu())
            return

    db.use_search(user_id)

    loading = await callback.message.answer("⚡ Зевс мечет молнии...\n🔍 Сканирую Олимп...")

    usernames_to_check = pattern_generator.generate_usernames(150)
    found_free = []
    checked_count = 0

    try:
        for username in usernames_to_check:
            result = await checker.comprehensive_check(username)
            checked_count += 1

            if result["fully_free"]:
                found_free.append(username)
                db.save_found_username(
                    username=username, user_id=user_id,
                    telegram_available=result["telegram_available"],
                    fragment_available=result["fragment_available"],
                    pattern_type="CVCVC"
                )
                if len(found_free) >= USERNAMES_PER_SEARCH:
                    break

            if checked_count % 40 == 0:
                try:
                    await loading.edit_text(f"🔍 Просканировано: {checked_count}\n⚡ Молнии в действии...")
                except:
                    pass
            await asyncio.sleep(0.05)

        await loading.delete()

        db.save_search_history(
            user_id=user_id, search_type="CVCVC", pattern_type="CVCVC",
            checked=checked_count, found=len(found_free),
            usernames_list=found_free
        )
        db.add_found_usernames(user_id, len(found_free))

        if found_free:
            username = found_free[0]
            limits = db.get_user_limits_info(user_id)

            text = (
                f"⚡ <b>Найдено:</b> <code>{username}</code>\n\n"
                f"🔗 <a href='https://t.me/{username}'>Забрать в Telegram</a>\n"
                f"💎 <a href='https://fragment.com/username/{username}'>Fragment</a>"
            )
            if not limits['is_premium']:
                rem = db.get_remaining_searches(user_id)
                text += f"\n\n🔍 Осталось поисков: {rem}/{limits['daily_limit']}"

            await callback.message.answer(text, reply_markup=get_result_keyboard(username), parse_mode='HTML')
        else:
            await callback.message.answer("❌ Не найдено\nПопробуй позже.", reply_markup=get_result_keyboard(""))
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await callback.message.answer("⚡ Ошибка! Гром разразился.")
    finally:
        try:
            await loading.delete()
        except:
            pass


# ⚡════════════════════ АДМИНКА ════════════════════

@dp.callback_query(lambda c: c.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    if not user or (not user['is_admin'] and callback.from_user.id not in ADMIN_IDS):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    stats = db.get_premium_stats()
    text = (
        "🔧 <b>Админ-панель</b>\n\n"
        f"👥 Пользователей: {stats['total_users']}\n"
        f"⚡ Премиум: {stats['total_premium']}\n"
        f"📊 % премиум: {stats['premium_percentage']:.1f}%"
    )
    await callback.message.edit_text(text, reply_markup=get_admin_panel(), parse_mode='HTML')
    await callback.answer()


@dp.callback_query(lambda c: c.data == "admin_give_premium")
async def admin_give_premium(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ID пользователя:")
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="give_premium")


@dp.callback_query(lambda c: c.data == "admin_remove_premium")
async def admin_remove_premium(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ID пользователя:")
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="remove_premium")


@dp.callback_query(lambda c: c.data == "admin_ban")
async def admin_ban(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ID пользователя для бана:")
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="ban")


@dp.callback_query(lambda c: c.data == "admin_unban")
async def admin_unban(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ID пользователя для разбана:")
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="unban")


@dp.message(StateFilter(AdminStates.waiting_for_user_id))
async def admin_get_user_id(message: Message, state: FSMContext):
    try:
        target_id = int(message.text.strip())
        data = await state.get_data()
        action = data.get("action")

        user = db.get_user(target_id)
        if not user and action != "unban":
            await message.answer("❌ Пользователь не найден.")
            await state.clear()
            return

        if action == "give_premium":
            await message.answer(
                f"Выберите срок премиума для {user['first_name']} (@{user['username']}):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="1 день", callback_data="prem_days_1")],
                    [InlineKeyboardButton(text="3 дня", callback_data="prem_days_3")],
                    [InlineKeyboardButton(text="7 дней", callback_data="prem_days_7")],
                    [InlineKeyboardButton(text="1 месяц", callback_data="prem_days_30")],
                    [InlineKeyboardButton(text="3 месяца", callback_data="prem_days_90")],
                ])
            )
            await state.update_data(target_id=target_id)
            await state.set_state(AdminStates.waiting_for_premium_days)
        elif action == "remove_premium":
            if db.remove_premium(target_id):
                await message.answer(f"✅ Премиум у пользователя {target_id} отозван.")
            else:
                await message.answer("❌ Ошибка отзыва.")
            await state.clear()
        elif action == "ban":
            await message.answer("Введите причину бана:")
            await state.update_data(target_id=target_id)
            await state.set_state(AdminStates.waiting_for_ban_reason)
        elif action == "unban":
            if db.unban_user(target_id):
                await message.answer(f"✅ Пользователь {target_id} разбанен.")
            else:
                await message.answer("❌ Ошибка разбана.")
            await state.clear()
    except ValueError:
        await message.answer("❌ Некорректный ID.")
        await state.clear()


@dp.callback_query(StateFilter(AdminStates.waiting_for_premium_days))
async def admin_premium_days_chosen(callback: CallbackQuery, state: FSMContext):
    days = int(callback.data.replace("prem_days_", ""))
    data = await state.get_data()
    target_id = data.get("target_id")

    if db.set_premium(target_id, days):
        await callback.message.edit_text(f"✅ Премиум на {days} дн. выдан пользователю {target_id}.")
    else:
        await callback.message.edit_text("❌ Не удалось выдать премиум.")
    await state.clear()
    await callback.answer()


@dp.message(StateFilter(AdminStates.waiting_for_ban_reason))
async def admin_ban_reason(message: Message, state: FSMContext):
    reason = message.text.strip() or "Без причины"
    data = await state.get_data()
    target_id = data.get("target_id")

    if db.ban_user(target_id, reason):
        await message.answer(f"🚫 Пользователь {target_id} забанен. Причина: {reason}")
    else:
        await message.answer("❌ Ошибка бана.")
    await state.clear()


@dp.callback_query(lambda c: c.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    users = db.get_all_users(limit=10)
    if not users:
        await callback.answer("Нет пользователей", show_alert=True)
        return
    text = "👥 <b>Последние 10 пользователей:</b>\n\n"
    for u in users:
        text += f"• {u['first_name']} (@{u['username']}) – ID: {u['user_id']} – {'⚡Премиум' if u['is_premium'] else 'Free'}\n"
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()


@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    stats = db.get_premium_stats()
    text = (
        "📊 <b>Общая статистика</b>\n\n"
        f"👥 Пользователей: {stats['total_users']}\n"
        f"⚡ Премиум: {stats['total_premium']}\n"
        f"📊 % премиум: {stats['premium_percentage']:.1f}%"
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()


# ⚡════════════════════ ТЕКСТОВЫЙ ОБРАБОТЧИК ════════════════════

@dp.message()
async def handle_text(message: Message):
    text = message.text.strip().lower()
    if 5 <= len(text) <= 32 and re.match(r'^[a-zA-Z][a-zA-Z0-9_]{3,31}$', text):
        user_id = message.from_user.id
        user = db.get_user(user_id)
        if not user or user['is_banned']:
            return

        if db.get_remaining_searches(user_id) <= 0:
            if not db.check_premium_status(user_id):
                await message.answer("⚠️ Лимит исчерпан!", reply_markup=get_premium_menu())
                return

        db.use_search(user_id)

        msg = await message.answer(f"🔍 Проверяю: <code>{text}</code>...", parse_mode='HTML')
        result = await checker.comprehensive_check(text)

        if result["fully_free"]:
            db.save_found_username(username=text, user_id=user_id,
                                   telegram_available=result["telegram_available"],
                                   fragment_available=result["fragment_available"])
            db.add_found_usernames(user_id, 1)

        tg_status = "✅ Свободен" if result["telegram_available"] else "❌ Занят" if result["telegram_available"] == False else "⚠️ Ошибка"
        frag_status = "✅ Доступен" if result["fragment_available"] else "❌ Занят" if result["fragment_available"] == False else "⚠️ Не проверен"
        status_emoji = "⚡" if result["fully_free"] else "❌"
        status_text = "СВОБОДЕН!" if result["fully_free"] else "Занят"

        out = (
            f"👤 <code>{result['username']}</code>\n"
            f"📱 Telegram: {tg_status}\n"
            f"💎 Fragment: {frag_status}\n"
            f"{status_emoji} {status_text}"
        )
        if result["fully_free"]:
            out += (
                f"\n\n🔗 <a href='https://t.me/{text}'>Забрать в Telegram</a>\n"
                f"💎 <a href='https://fragment.com/username/{text}'>Fragment</a>"
            )

        limits = db.get_user_limits_info(user_id)
        if not limits['is_premium']:
            out += f"\n\n🔍 Осталось: {limits['remaining']}/{limits['daily_limit']}"

        await msg.delete()
        await message.answer(out,
                             reply_markup=get_result_keyboard(text) if result["fully_free"] else get_main_menu(user_id),
                             parse_mode='HTML')
    else:
        await message.answer(
            "⚡ Используй меню или команды:\n"
            "/search — поиск\n"
            "/profile — профиль\n"
            "/premium — премиум",
            reply_markup=get_main_menu(message.from_user.id)
        )


# ⚡════════════════════ ФОНОВЫЕ ЗАДАЧИ ════════════════════

async def scheduled_tasks():
    while True:
        try:
            db.check_expired_premiums()
            db.reset_daily_searches()
        except Exception as e:
            logging.error(f"Ошибка фоновой задачи: {e}")
        await asyncio.sleep(3600)


async def on_startup():
    await checker.init_sessions()
    asyncio.create_task(scheduled_tasks())
    logging.info("⚡ ZEUS FIND — БОГИ НА СВЯЗИ ⚡")


async def on_shutdown():
    await checker.close_sessions()
    logging.info("⚡ ZEUS FIND — ОЛИМП ЗАКРЫТ ⚡")


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
