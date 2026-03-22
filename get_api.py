import requests
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://my.telegram.org",
    "Referer": "https://my.telegram.org/auth"
}

s = requests.Session()
s.headers.update(HEADERS)

print("🚀 Подключаюсь...")
s.get("https://my.telegram.org/auth")

phone = input("📱 Номер телефона: ").strip()
resp = s.post("https://my.telegram.org/auth/send_password", data={"phone": phone})

try:
    random_hash = resp.json()["random_hash"]
except:
    print(f"❌ Ошибка: {resp.text}")
    exit()

code = input("📩 Код из Telegram: ").strip()
resp = s.post("https://my.telegram.org/auth/login", data={
    "phone": phone, "random_hash": random_hash, "password": code
})

if resp.text != "true":
    print(f"❌ Ошибка входа: {resp.text}")
    exit()

print("✅ Вошли! Ищу ключи...")

resp = s.get("https://my.telegram.org/apps")
html = resp.text

# Ищем api_id и api_hash любым способом
api_id = None
api_hash = None

# Способ 1: через регулярки
m1 = re.search(r'api_id.*?(\d{5,12})', html)
m2 = re.search(r'api_hash.*?([a-f0-9]{32})', html)
if m1: api_id = m1.group(1)
if m2: api_hash = m2.group(1)

# Способ 2: через value в input
if not api_id:
    m1 = re.search(r'name="api_id"[^>]*value="(\d+)"', html)
    if m1: api_id = m1.group(1)
if not api_hash:
    m2 = re.search(r'name="api_hash"[^>]*value="([a-f0-9]+)"', html)
    if m2: api_hash = m2.group(1)

# Способ 3: просто ищем 32-символьный хеш
if not api_hash:
    m2 = re.search(r'[a-f0-9]{32}', html)
    if m2: api_hash = m2.group(0)

if api_id and api_hash:
    print(f"\n🎉 НАЙДЕНО!")
    print(f"api_id = {api_id}")
    print(f"api_hash = '{api_hash}'")
else:
    print("⚠️ Ключи не найдены на странице. Пробую создать...")
    
    # Ищем hash формы
    m = re.search(r'name="hash"[^>]*value="([^"]+)"', html)
    if not m:
        # Сохраняем HTML для просмотра
        with open("tg_page.html", "w") as f:
            f.write(html)
        print("❌ Форма не найдена. HTML сохранен в tg_page.html")
        print("Посмотри его командой: cat tg_page.html")
        exit()
    
    import random, string
    r = ''.join(random.choices(string.ascii_lowercase, k=6))
    resp = s.post("https://my.telegram.org/apps/create", data={
        "hash": m.group(1),
        "app_title": f"App{r}",
        "app_shortname": f"app{r}",
        "app_url": "",
        "app_platform": "android",
        "app_desc": ""
    })
    
    html2 = resp.text
    m1 = re.search(r'api_id.*?(\d{5,12})', html2)
    m2 = re.search(r'[a-f0-9]{32}', html2)
    
    if m1 and m2:
        print(f"\n🎉 СОЗДАНО!")
        print(f"api_id = {m1.group(1)}")
        print(f"api_hash = '{m2.group(0)}'")
    else:
        # Перезагрузим страницу
        resp = s.get("https://my.telegram.org/apps")
        html3 = resp.text
        m1 = re.search(r'api_id.*?(\d{5,12})', html3)
        m2 = re.search(r'[a-f0-9]{32}', html3)
        
        if m1 and m2:
            print(f"\n🎉 НАЙДЕНО!")
            print(f"api_id = {m1.group(1)}")
            print(f"api_hash = '{m2.group(0)}'")
        else:
            with open("tg_page.html", "w") as f:
                f.write(html3)
            print("❌ Не удалось. HTML сохранен в tg_page.html")
            print("Посмотри: cat tg_page.html")
