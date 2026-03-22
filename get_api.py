"""
🔥 TELEGRAM API GRABBER v3.0
Автоматически получает api_id и api_hash
Сохраняет в файл, добавляет в бота, всё сам
"""

import requests
import re
import json
import os
import sys
import random
import string
import time

class TelegramAPIGrabber:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Origin": "https://my.telegram.org",
            "Referer": "https://my.telegram.org",
            "Connection": "keep-alive",
        })
        self.api_id = None
        self.api_hash = None
        self.phone = None
        
    def banner(self):
        print("\n" + "="*50)
        print("🔥 TELEGRAM API GRABBER v3.0")
        print("="*50)
        print("📱 Автоматическое получение api_id и api_hash")
        print("💾 Сохранение в файл + добавление в бота")
        print("="*50 + "\n")

    def connect(self):
        print("🌐 [1/6] Подключаюсь к my.telegram.org...")
        try:
            resp = self.session.get("https://my.telegram.org/auth", timeout=15)
            if resp.status_code == 200:
                print("   ✅ Подключено!")
                return True
            else:
                print(f"   ❌ Статус: {resp.status_code}")
                return False
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            return False

    def send_code(self):
        print("\n📱 [2/6] Отправка кода...")
        self.phone = input("   Введите номер (с +): ").strip()
        
        if not self.phone.startswith("+"):
            self.phone = "+" + self.phone
            
        resp = self.session.post(
            "https://my.telegram.org/auth/send_password",
            data={"phone": self.phone}
        )
        
        if "Sorry" in resp.text or resp.status_code != 200:
            print("   ❌ Telegram отклонил запрос")
            print(f"   Ответ: {resp.text[:200]}")
            return False
            
        try:
            data = resp.json()
            self.random_hash = data.get("random_hash", "")
            if self.random_hash:
                print("   ✅ Код отправлен в Telegram!")
                return True
            else:
                print(f"   ❌ Нет хеша: {data}")
                return False
        except:
            print(f"   ❌ Ответ: {resp.text[:200]}")
            return False

    def login(self):
        print("\n🔑 [3/6] Вход...")
        code = input("   Введите код из Telegram: ").strip()
        
        resp = self.session.post(
            "https://my.telegram.org/auth/login",
            data={
                "phone": self.phone,
                "random_hash": self.random_hash,
                "password": code
            }
        )
        
        if resp.text == "true":
            print("   ✅ Вход выполнен!")
            return True
        else:
            print(f"   ❌ Ошибка: {resp.text}")
            return False

    def extract_keys(self, html):
        """Пытается вытащить ключи всеми возможными способами"""
        api_id = None
        api_hash = None
        
        # Способ 1: regex api_id + число
        patterns_id = [
            r'api_id["\s:>]*(\d{5,12})',
            r'name="api_id"[^>]*value="(\d+)"',
            r'App api_id:\s*<[^>]*>(\d+)',
            r'>(\d{7,10})<',
        ]
        for p in patterns_id:
            m = re.search(p, html)
            if m:
                api_id = m.group(1)
                break
        
        # Способ 2: regex api_hash + 32 символа hex
        patterns_hash = [
            r'api_hash["\s:>]*([a-f0-9]{32})',
            r'name="api_hash"[^>]*value="([a-f0-9]{32})"',
            r'App api_hash:\s*<[^>]*>([a-f0-9]{32})',
        ]
        for p in patterns_hash:
            m = re.search(p, html)
            if m:
                api_hash = m.group(1)
                break
        
        # Способ 3: просто найти 32-символьный hex
        if not api_hash:
            all_hex = re.findall(r'[a-f0-9]{32}', html)
            # Отфильтровываем hash формы (он обычно другой длины или содержит цифры)
            for h in all_hex:
                if not h.isdigit():
                    api_hash = h
                    break
        
        return api_id, api_hash

    def get_keys(self):
        print("\n🔍 [4/6] Ищу API ключи...")
        
        resp = self.session.get("https://my.telegram.org/apps")
        html = resp.text
        
        self.api_id, self.api_hash = self.extract_keys(html)
        
        if self.api_id and self.api_hash:
            print(f"   ✅ Найдены существующие ключи!")
            return True
        
        print("   ⚠️ Ключей нет, создаю приложение...")
        return self.create_app(html)

    def create_app(self, html):
        print("\n⚙️ [5/6] Создаю приложение...")
        
        # Ищем hash формы
        hash_match = re.search(r'name="hash"[^>]*value="([^"]+)"', html)
        if not hash_match:
            # Пробуем другой паттерн
            hash_match = re.search(r'"hash"\s*:\s*"([^"]+)"', html)
        if not hash_match:
            hash_match = re.search(r'hash["\s:>]*([a-zA-Z0-9]{10,})', html)
        
        if not hash_match:
            print("   ❌ Форма создания не найдена")
            self.save_debug(html)
            return False
        
        form_hash = hash_match.group(1)
        rand = ''.join(random.choices(string.ascii_lowercase, k=6))
        
        data = {
            "hash": form_hash,
            "app_title": f"MyApp{rand}",
            "app_shortname": f"myapp{rand}",
            "app_url": "",
            "app_platform": "android",
            "app_desc": ""
        }
        
        print(f"   📝 Название: MyApp{rand}")
        resp = self.session.post("https://my.telegram.org/apps/create", data=data)
        
        # Проверяем ответ
        if "error" in resp.text.lower():
            print(f"   ❌ Ошибка создания: {resp.text[:200]}")
            # Может приложение уже создалось — пробуем получить ключи
            time.sleep(2)
        
        # Перезагружаем страницу с ключами
        print("   🔄 Загружаю страницу с ключами...")
        time.sleep(1)
        resp = self.session.get("https://my.telegram.org/apps")
        
        self.api_id, self.api_hash = self.extract_keys(resp.text)
        
        if self.api_id and self.api_hash:
            print("   ✅ Приложение создано!")
            return True
        
        # Последняя попытка — ждём и пробуем снова
        print("   ⏳ Жду 3 секунды и пробую снова...")
        time.sleep(3)
        resp = self.session.get("https://my.telegram.org/apps")
        self.api_id, self.api_hash = self.extract_keys(resp.text)
        
        if self.api_id and self.api_hash:
            print("   ✅ Получилось!")
            return True
        
        print("   ❌ Не удалось получить ключи")
        self.save_debug(resp.text)
        return False

    def save_debug(self, html):
        """Сохраняет HTML для отладки"""
        with open("tg_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        
        # Также сохраняем чистый текст
        text = re.sub(r'<[^>]+>', '\n', html)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        with open("tg_debug.txt", "w", encoding="utf-8") as f:
            f.write(text)
        
        print("\n   📄 HTML сохранён в tg_debug.html")
        print("   📄 Текст сохранён в tg_debug.txt")
        print("   Посмотри: cat tg_debug.txt | grep -i api")

    def save_results(self):
        print("\n💾 [6/6] Сохраняю результаты...")
        
        # 1. Сохраняем в JSON
        result = {
            "api_id": int(self.api_id),
            "api_hash": self.api_hash,
            "phone": self.phone,
            "created": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        filename = "api_credentials.json"
        # Если файл уже есть — добавляем
        existing = []
        if os.path.exists(filename):
            try:
                with open(filename, "r") as f:
                    existing = json.load(f)
                if not isinstance(existing, list):
                    existing = [existing]
            except:
                existing = []
        
        existing.append(result)
        with open(filename, "w") as f:
            json.dump(existing, f, indent=2)
        print(f"   ✅ Сохранено в {filename}")
        
        # 2. Сохраняем в текстовый файл
        with open("api_keys.txt", "a") as f:
            f.write(f"\n{'='*40}\n")
            f.write(f"Телефон: {self.phone}\n")
            f.write(f"api_id = {self.api_id}\n")
            f.write(f"api_hash = '{self.api_hash}'\n")
            f.write(f"Дата: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        print("   ✅ Добавлено в api_keys.txt")
        
        # 3. Пробуем добавить в бота
        self.add_to_bot()

    def add_to_bot(self):
        """Пробует добавить сессию в added_sessions.json"""
        sessions_file = "added_sessions.json"
        
        if not os.path.exists("bot.py"):
            print("   ℹ️ bot.py не найден — пропускаю добавление в бота")
            return
        
        new_session = {
            "api_id": int(self.api_id),
            "api_hash": self.api_hash,
            "phone": self.phone
        }
        
        existing = []
        if os.path.exists(sessions_file):
            try:
                with open(sessions_file, "r") as f:
                    existing = json.load(f)
            except:
                existing = []
        
        # Проверяем дубли
        for s in existing:
            if s.get("phone") == self.phone:
                print(f"   ⚠️ Телефон {self.phone} уже в {sessions_file}")
                return
        
        existing.append(new_session)
        with open(sessions_file, "w") as f:
            json.dump(existing, f, indent=2)
        print(f"   ✅ Добавлено в {sessions_file}")
        print(f"   ℹ️ При следующем запуске бот подключит эту сессию")

    def show_results(self):
        print("\n" + "🎉"*20)
        print("\n   🔥 ГОТОВО! ВАШИ API ДАННЫЕ:\n")
        print(f"   ╔══════════════════════════════════════╗")
        print(f"   ║  api_id   = {self.api_id:<26}║")
        print(f"   ║  api_hash = '{self.api_hash}'  ║")
        print(f"   ║  phone    = {self.phone:<26}║")
        print(f"   ╚══════════════════════════════════════╝")
        print(f"\n   📁 Файлы:")
        print(f"   • api_credentials.json — JSON")
        print(f"   • api_keys.txt — текстовый")
        if os.path.exists("added_sessions.json"):
            print(f"   • added_sessions.json — для бота")
        print("\n   📋 Для вставки в код бота:")
        print(f'   {{"api_id": {self.api_id}, "api_hash": "{self.api_hash}", "phone": "{self.phone}"}}')
        print("\n" + "🎉"*20)

    def run(self):
        self.banner()
        
        if not self.connect():
            print("\n💡 Попробуйте:")
            print("   1. Cloudflare WARP (1.1.1.1)")
            print("   2. VPN")
            print("   3. Другой сервер")
            return False
        
        if not self.send_code():
            return False
            
        if not self.login():
            return False
            
        if not self.get_keys():
            return False
        
        self.save_results()
        self.show_results()
        
        # Спрашиваем про ещё один аккаунт
        again = input("\n🔄 Добавить ещё один аккаунт? (y/n): ").strip().lower()
        if again in ("y", "yes", "д", "да"):
            grabber = TelegramAPIGrabber()
            grabber.run()
        
        return True


if __name__ == "__main__":
    grabber = TelegramAPIGrabber()
    success = grabber.run()
    
    if not success:
        print("\n❌ Не удалось получить ключи")
        print("💡 Альтернативы:")
        print("   • Установите WARP: https://one.one.one.one")
        print("   • Используйте Opera с VPN")
        print("   • Попросите друга за границей")
    
    print("\n👋 Готово!")
