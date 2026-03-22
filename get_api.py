import requests
from bs4 import BeautifulSoup
import random
import string

# Заголовки как у обычного браузера
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://my.telegram.org",
    "Referer": "https://my.telegram.org/auth"
}

def get_api_credentials():
    s = requests.Session()
    s.headers.update(HEADERS)

    print("🚀 Подключаюсь к my.telegram.org...")
    
    # 1. Загружаем страницу авторизации, чтобы получить случайный хэш
    try:
        resp = s.get("https://my.telegram.org/auth")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    # 2. Вводим номер
    phone = input("📱 Введите номер телефона (например, +79991234567): ").strip()
    
    data = {"phone": phone}
    resp = s.post("https://my.telegram.org/auth/send_password", data=data)
    
    if resp.text == "Sorry, something went wrong.":
        print("❌ Telegram заблокировал вход с этого IP (сервера).")
        print("💡 Решение: Запустите этот скрипт на домашнем ПК.")
        return

    try:
        random_hash = resp.json()["random_hash"]
    except:
        print(f"❌ Не удалось отправить код. Ответ сайта: {resp.text}")
        return

    # 3. Вводим код
    code = input("📩 Введите код из Telegram (пришел в приложение): ").strip()
    
    data = {"phone": phone, "random_hash": random_hash, "password": code}
    resp = s.post("https://my.telegram.org/auth/login", data=data)
    
    if resp.text == "true":
        print("✅ Успешный вход!")
    else:
        print(f"❌ Ошибка входа (неверный код?): {resp.text}")
        return

    # 4. Переходим в раздел API
    resp = s.get("https://my.telegram.org/apps")
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Проверяем, есть ли уже ключи
    api_id_input = soup.find("input", {"name": "api_id"})
    api_hash_input = soup.find("input", {"name": "api_hash"})

    if api_id_input and api_hash_input:
        print("\n🎉 ВАШИ ДАННЫЕ НАЙДЕНЫ:")
        print(f"api_id = {api_id_input.get('value')}")
        print(f"api_hash = '{api_hash_input.get('value')}'")
        return

    # 5. Если ключей нет — создаем новое приложение
    print("⚙️ API ключей нет, создаю новое приложение...")
    
    # Генерируем случайное название
    rand_str = ''.join(random.choices(string.ascii_lowercase, k=8))
    app_title = f"App{rand_str}"
    app_short = f"app{rand_str}"
    
    # Ищем hash для формы создания
    hash_code = soup.find("input", {"name": "hash"}).get("value")
    
    data = {
        "hash": hash_code,
        "app_title": app_title,
        "app_shortname": app_short,
        "app_url": "",
        "app_platform": "android",
        "app_desc": ""
    }
    
    resp = s.post("https://my.telegram.org/apps/create", data=data)
    
    # 6. Парсим полученные ключи
    soup = BeautifulSoup(resp.text, "html.parser")
    api_id_input = soup.find("input", {"name": "api_id"})
    api_hash_input = soup.find("input", {"name": "api_hash"})
    
    if api_id_input and api_hash_input:
        print("\n🎉 ПРИЛОЖЕНИЕ СОЗДАНО! ВАШИ ДАННЫЕ:")
        print(f"api_id = {api_id_input.get('value')}")
        print(f"api_hash = '{api_hash_input.get('value')}'")
    else:
        print("❌ Не удалось создать приложение. Возможно, ошибка на сайте.")

if __name__ == "__main__":
    get_api_credentials()