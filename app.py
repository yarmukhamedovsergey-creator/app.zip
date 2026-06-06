import asyncio
import logging
import re
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Конфигурация бота
BOT_TOKEN = "  # Замените на токен вашего бота

# Список user-agent для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

# База данных (пока в памяти, потом заменим на PostgreSQL)
free_usernames_db = set()
checked_usernames = set()

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


class UsernameChecker:
    """Класс для проверки доступности username"""
    
    def __init__(self):
        self.session = None
        self.fragment_session = None
    
    async def init_sessions(self):
        """Инициализация HTTP сессий"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        if not self.fragment_session:
            self.fragment_session = aiohttp.ClientSession(
                headers={"User-Agent": self.get_random_user_agent()}
            )
    
    async def close_sessions(self):
        """Закрытие HTTP сессий"""
        if self.session:
            await self.session.close()
        if self.fragment_session:
            await self.fragment_session.close()
    
    @staticmethod
    def get_random_user_agent():
        """Получение случайного User-Agent"""
        import random
        return random.choice(USER_AGENTS)
    
    @staticmethod
    def is_valid_username(username: str) -> bool:
        """Проверка валидности username"""
        pattern = r'^[a-zA-Z][a-zA-Z0-9_]{3,31}$'
        return bool(re.match(pattern, username))
    
    async def check_telegram_username(self, username: str) -> bool:
        """Проверка доступности username через Telegram API"""
        try:
            url = f"https://t.me/{username}"
            headers = {
                "User-Agent": self.get_random_user_agent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            
            async with self.session.get(url, headers=headers, 
                                       allow_redirects=True, 
                                       timeout=10) as response:
                text = await response.text()
                
                # Проверяем признаки недоступности username
                if "tgme_page_title" in text and username.lower() in text.lower():
                    return False  # Username занят
                
                # Дополнительные проверки
                if "If you have Telegram" in text and "t.me" in text:
                    return False
                
                # Если страница не содержит информации о пользователе - username свободен
                return True
                
        except Exception as e:
            logging.error(f"Ошибка при проверке {username} в Telegram: {e}")
            return None  # Неопределенный результат
    
    async def check_fragment_username(self, username: str) -> bool:
        """Проверка доступности username на Fragment"""
        try:
            url = f"https://fragment.com/username/{username}"
            headers = {
                "User-Agent": self.get_random_user_agent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            
            async with self.fragment_session.get(url, headers=headers, 
                                                allow_redirects=True, 
                                                timeout=10) as response:
                text = await response.text()
                
                # Проверяем признаки занятого username на Fragment
                if "This username is not available" in text:
                    return False
                elif "Available" in text and username.lower() in text.lower():
                    return True
                elif "Not available" in text:
                    return False
                
                # Дополнительная проверка через API Fragment
                api_url = f"https://fragment.com/api?hash={self._get_fragment_hash()}"
                # Здесь может быть более сложная логика проверки через API
                
                return True  # По умолчанию считаем свободным
                
        except Exception as e:
            logging.error(f"Ошибка при проверке {username} на Fragment: {e}")
            return None
    
    def _get_fragment_hash(self):
        """Получение хеша для Fragment API (заглушка)"""
        import hashlib
        import time
        return hashlib.md5(f"fragment{int(time.time())}".encode()).hexdigest()
    
    async def comprehensive_check(self, username: str) -> dict:
        """Комплексная проверка username"""
        result = {
            "username": username,
            "telegram_available": None,
            "fragment_available": None,
            "fully_free": False
        }
        
        # Проверка в Telegram
        result["telegram_available"] = await self.check_telegram_username(username)
        
        # Проверка на Fragment
        result["fragment_available"] = await self.check_fragment_username(username)
        
        # Username полностью свободен если доступен везде
        result["fully_free"] = (
            result["telegram_available"] == True and 
            result["fragment_available"] != False
        )
        
        return result


class CVCVCPattern:
    """Генератор username по паттерну CVCVC"""
    
    def __init__(self):
        self.consonants = 'bcdfghjklmnpqrstvwxyz'
        self.vowels = 'aeiou'
    
    def generate_usernames(self, max_results=100):
        """Генерация username по шаблону CVCVC"""
        usernames = []
        count = 0
        
        for c1 in self.consonants:
            for v1 in self.vowels:
                for c2 in self.consonants:
                    for v2 in self.vowels:
                        for c3 in self.consonants:
                            username = f"{c1}{v1}{c2}{v2}{c3}"
                            usernames.append(username)
                            count += 1
                            if count >= max_results:
                                return usernames
        
        return usernames


# Инициализация инструментов
checker = UsernameChecker()
pattern_generator = CVCVCPattern()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        "🔍 Бот для поиска свободных username\n\n"
        "Команды:\n"
        "/search_cvcvc - Поиск username по шаблону CVCVC\n"
        "/check <username> - Проверка конкретного username\n"
        "/stats - Статистика найденных username"
    )


@dp.message(Command("search_cvcvc"))
async def cmd_search_cvcvc(message: Message):
    """Поиск свободных username по шаблону CVCVC"""
    await message.answer("🔎 Начинаю поиск свободных username по шаблону CVCVC...")
    
    # Генерируем список username
    usernames = pattern_generator.generate_usernames(max_results=500)
    
    found_free = []
    checked = 0
    
    # Отправляем сообщение о прогрессе
    progress_msg = await message.answer(f"Проверено: 0/{len(usernames)}")
    
    try:
        # Проверяем каждый username
        for username in usernames:
            if username in checked_usernames:
                continue
            
            checked_usernames.add(username)
            result = await checker.comprehensive_check(username)
            checked += 1
            
            if result["fully_free"]:
                found_free.append(username)
                free_usernames_db.add(username)
            
            # Обновляем прогресс каждые 50 проверок
            if checked % 50 == 0:
                try:
                    await progress_msg.edit_text(
                        f"🔍 Проверено: {checked}/{len(usernames)}\n"
                        f"✅ Найдено свободных: {len(found_free)}"
                    )
                except:
                    pass
            
            # Небольшая задержка для избежания блокировки
            await asyncio.sleep(0.1)
        
        # Формируем результат
        if found_free:
            result_text = "✅ Найдены полностью свободные username:\n\n"
            for uname in found_free[:20]:  # Показываем первые 20
                result_text += f"• @{uname}\n"
            
            if len(found_free) > 20:
                result_text += f"\n... и еще {len(found_free) - 20} username"
        else:
            result_text = "❌ Свободных username не найдено"
        
        await message.answer(result_text)
        
    except Exception as e:
        logging.error(f"Ошибка при поиске: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")
    finally:
        await progress_msg.delete()


@dp.message(Command("check"))
async def cmd_check_username(message: Message):
    """Проверка конкретного username"""
    # Извлекаем username из сообщения
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите username для проверки\nПример: /check myusername")
        return
    
    username = args[1].replace("@", "").strip()
    
    if not checker.is_valid_username(username):
        await message.answer("❌ Некорректный формат username")
        return
    
    await message.answer(f"🔍 Проверяю username: @{username}...")
    
    result = await checker.comprehensive_check(username)
    
    # Формируем ответ
    response = f"📊 Результаты проверки @{result['username']}:\n\n"
    
    # Статус в Telegram
    if result["telegram_available"] == True:
        response += "✅ Telegram: свободен\n"
    elif result["telegram_available"] == False:
        response += "❌ Telegram: занят\n"
    else:
        response += "⚠️ Telegram: не удалось проверить\n"
    
    # Статус на Fragment
    if result["fragment_available"] == True:
        response += "✅ Fragment: доступен\n"
    elif result["fragment_available"] == False:
        response += "❌ Fragment: недоступен\n"
    else:
        response += "⚠️ Fragment: не удалось проверить\n"
    
    # Итоговый статус
    if result["fully_free"]:
        response += "\n🎉 Username полностью свободен!"
        free_usernames_db.add(username)
    else:
        response += "\n❌ Username недоступен"
    
    await message.answer(response)


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика найденных username"""
    stats = f"📊 Статистика бота:\n\n"
    stats += f"🔍 Проверено username: {len(checked_usernames)}\n"
    stats += f"✅ Свободных username: {len(free_usernames_db)}\n"
    
    if free_usernames_db:
        stats += f"\nПоследние найденные:\n"
        for uname in list(free_usernames_db)[-5:]:
            stats += f"• @{uname}\n"
    
    await message.answer(stats)


@dp.message()
async def handle_text(message: Message):
    """Обработчик текстовых сообщений"""
    text = message.text.strip()
    
    # Если прислали username для проверки
    if len(text) >= 5 and len(text) <= 32 and text.isalnum():
        if checker.is_valid_username(text):
            await cmd_check_username(message)
            return
    
    await message.answer("Используйте команды бота:\n/start - список команд")


async def on_startup():
    """Действия при запуске бота"""
    await checker.init_sessions()
    logging.info("Бот запущен и готов к работе")


async def on_shutdown():
    """Действия при остановке бота"""
    await checker.close_sessions()
    logging.info("Бот остановлен")


async def main():
    """Главная функция"""
    # Регистрируем функции старта и остановки
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Запускаем бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
