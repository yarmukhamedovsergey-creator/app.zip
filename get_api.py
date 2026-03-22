import requests
from bs4 import BeautifulSoup
import random
import string

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://my.telegram.org",
    "Referer": "https://my.telegram.org/auth"
}

def get_api_credentials():
    s = requests.Session()
    s.headers.update(HEADERS)

    print("🚀 Подключаюсь к my.telegram.org...")
    
    try:
        resp = s.get("https://my.telegram.org/auth")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    phone = input("📱 Введите номер телефона (например, +79991234567): ").strip()
    data = {"phone": phone}
    resp = s.post("https://my.telegram.org/auth/send_password", data=data)
    
    if "Sorry, something went wrong" in resp.text:
        print("❌ Telegram заблокировал вход с этого IP.")
        return

    try:
        random_hash = resp.json()["random_hash"]
    except:
        print(f"❌ Не удалось отправить код. Ответ: {resp.text}")
        return

    code = input("📩 Введите код из Telegram: ").strip()
    data = {"phone": phone, "random_hash": random_hash, "password": code}
    resp = s.post("https://my.telegram.org/auth/login", data=data)
    
    if resp.text == "true":
        print("✅ Вход выполнен!")
    else:
        print(f"❌ Ошибка входа: {resp.text}")
        return

    print("🔍 Ищу API ключи...")
    resp = s.get("https://my.telegram.org/apps")
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Попытка 1: Ищем готовые ключи
    try:
        # Ищем через label, так надежнее
        api_id_label = soup.find(string="App api_id:")
        api_hash_label = soup.find(string="App api_hash:")
        
        if api_id_label and api_hash_label:
            api_id = api_id_label.find_next("span").text.strip()
            api_hash = api_hash_label.find_next("span").text.strip()
            print("\n🎉 ВАШИ ДАННЫЕ НАЙДЕНЫ:")
            print(f"api_id = {api_id}")
            print(f"api_hash = '{api_hash}'")
            return
    except: pass

    # Попытка 2: Ищем поля input (старый дизайн)
    api_id_input = soup.find("input", {"name": "api_id"})
    api_hash_input = soup.find("input", {"name": "api_hash"})
    if api_id_input and api_hash_input:
        print("\n🎉 ВАШИ ДАННЫЕ НАЙДЕНЫ (input):")
        print(f"api_id = {api_id_input.get('value')}")
        print(f"api_hash = '{api_hash_input.get('value')}'")
        return

    print("⚙️ Ключей нет, пробую создать...")
    
    # Ищем hash для формы
    hash_input = soup.find("input", {"name": "hash"})
    if not hash_input:
        print("❌ Не нашел форму создания приложения. Возможно, аккаунт ограничен или сайт изменился.")
        # Выведем часть HTML для отладки
        print("HTML страницы (первые 500 символов):")
        print(resp.text[:500])
        return

    hash_code = hash_input.get("value")
    
    rand_str = ''.join(random.choices(string.ascii_lowercase, k=8))
    data = {
        "hash": hash_code,
        "app_title": f"App{rand_str}",
        "app_shortname": f"app{rand_str}",
        "app_url": "",
        "app_platform": "android",
        "app_desc": ""
    }
    
    resp = s.post("https://my.telegram.org/apps/create", data=data)
    
    # Проверяем результат создания
    if "Error" in resp.text:
        print("❌ Ошибка при создании: Telegram вернул ошибку.")
        return

    # Снова парсим страницу, чтобы найти ключи
    soup = BeautifulSoup(resp.text, "html.parser")
    try:
        api_id_label = soup.find(string="App api_id:")
        api_hash_label = soup.find(string="App api_hash:")
        
        if api_id_label and api_hash_label:
            api_id = api_id_label.find_next("span").text.strip()
            api_hash = api_hash_label.find_next("span").text.strip()
            print("\n🎉 ПРИЛОЖЕНИЕ СОЗДАНО!")
            print(f"api_id = {api_id}")
            print(f"api_hash = '{api_hash}'")
        else:
            print("⚠️ Приложение вроде создано, но ключи не нашел. Зайдите на сайт вручную.")
    except Exception as e:
        print(f"❌ Ошибка парсинга после создания: {e}")

if __name__ == "__main__":
    get_api_credentials()
