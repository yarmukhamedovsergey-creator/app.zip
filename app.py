#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import asyncio
import random
import logging
import requests
from typing import Dict, List

from twitchio import Client as TwitchClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# ========== НАСТРОЙКИ ==========
TELEGRAM_TOKEN = "8885296149:AAEmYTNxvJC24Q3tKP_pMM58ag3m5nr_knA"
OPENROUTER_API_KEY = "sk-or-v1-788bafdcdffd8d3bc4344cee8d44027a881db1bc6f1d2d083651a93c19aba9b"  # получите на https://openrouter.io/keys
OPENROUTER_MODEL = "openai/gpt-4o-mini"     # можно заменить на любую модель
ACCOUNTS_FILE = "twitch_accounts.json"
# ================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- ХРАНИЛИЩЕ АККАУНТОВ ----------
def load_accounts() -> dict:
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_accounts(accounts: dict):
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

# ---------- AI-ФУНКЦИЯ ЧЕРЕЗ OPENROUTER (без сторонней библиотеки) ----------
async def generate_ai_reply(prompt: str, context_messages: List[str] = None) -> str:
    """Генерирует ответ через OpenRouter API с историей чата."""
    system_prompt = (
        "Ты — реальный зритель стрима на Twitch. Твоя цель — общаться в чате естественно, "
        "как обычный человек. Не говори, что ты бот. Отвечай кратко (1-2 предложения), "
        "иногда используй сленг стримерского чата (Pog, LUL, KEKW). Не спамь."
    )
    
    messages = [{"role": "system", "content": system_prompt}]
    if context_messages:
        for msg in context_messages[-5:]:
            messages.append({"role": "user", "content": msg})
    messages.append({"role": "user", "content": prompt})
    
    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": messages,
                "max_tokens": 60,
                "temperature": 0.9,
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"AI ошибка: {e}")
        return "nice!"  # fallback-ответ

# ---------- TWITCH-ЗРИТЕЛЬ (ОДИН АККАУНТ) ----------
class TwitchViewer(TwitchClient):
    def __init__(self, email: str, password: str, oauth_token: str, channel_name: str):
        # oauth_token должен быть с префиксом "oauth:" например "oauth:abc123..."
        super().__init__(token=oauth_token, initial_channels=[channel_name])
        self.email = email
        self.password = password
        self.channel_name = channel_name
        self.running = False
        self._message_history: List[str] = []
        
    async def event_ready(self):
        logger.info(f"✅ {self.email} зашёл на канал {self.channel_name}")
        self.running = True
        
    async def event_message(self, message):
        if message.echo:
            return
        if message.channel.name != self.channel_name.lstrip("#"):
            return
            
        self._message_history.append(f"{message.author.name}: {message.content}")
        if len(self._message_history) > 20:
            self._message_history.pop(0)
        
        # отвечаем на ~30% сообщений
        if random.random() < 0.3:
            reply = await generate_ai_reply(message.content, self._message_history)
            await message.channel.send(reply)
            logger.info(f"{self.email} написал: {reply}")
    
    async def run_viewer(self):
        await self.start()
        
    async def stop_viewer(self):
        self.running = False
        await self.close()

# ---------- ГЛОБАЛЬНЫЙ МЕНЕДЖЕР ЗРИТЕЛЕЙ ----------
active_viewers: Dict[str, TwitchViewer] = {}

async def start_viewer(email: str, password: str, oauth_token: str, channel: str):
    if email in active_viewers:
        return
    viewer = TwitchViewer(email, password, oauth_token, channel)
    active_viewers[email] = viewer
    asyncio.create_task(viewer.run_viewer())

async def stop_viewer(email: str):
    if email in active_viewers:
        await active_viewers[email].stop_viewer()
        del active_viewers[email]

# ---------- TELEGRAM-ПАНЕЛЬ ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📋 Список аккаунтов", callback_data="list_accounts")],
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("🚀 Запустить всех на канал", callback_data="run_all")],
        [InlineKeyboardButton("🛑 Остановить всех", callback_data="stop_all")],
        [InlineKeyboardButton("🔑 Инструкция по OAuth", callback_data="help_tokens")]
    ]
    await update.message.reply_text(
        "👋 Панель управления Twitch-зрителями с AI-ответами в чате.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "list_accounts":
        accounts = load_accounts()
        if not accounts:
            await query.edit_message_text("❌ Нет сохранённых аккаунтов.")
            return
        msg = "📋 **Список аккаунтов:**\n" + "\n".join(f"• {email}" for email in accounts)
        await query.edit_message_text(msg, parse_mode="Markdown")
    
    elif query.data == "add_account":
        await query.edit_message_text("Введите в чат: `email:пароль:oauth_токен`\nПример: `my@mail.com:pass123:oauth:abc...`")
        context.user_data["waiting_for_account"] = True
    
    elif query.data == "run_all":
        accounts = load_accounts()
        if not accounts:
            await query.edit_message_text("Нет аккаунтов.")
            return
        await query.edit_message_text("Введите название канала (без #):")
        context.user_data["channel_to_run"] = list(accounts.keys())
    
    elif query.data == "stop_all":
        for email in list(active_viewers.keys()):
            await stop_viewer(email)
        await query.edit_message_text("✅ Все зрители остановлены.")
    
    elif query.data == "help_tokens":
        await query.edit_message_text(
            "🔐 **Как получить OAuth токен для Twitch:**\n"
            "1. Перейдите на https://twitchtokengenerator.com/\n"
            "2. Выберите **Custom Scope** → отметьте `chat:read` и `chat:edit`\n"
            "3. Авторизуйтесь и скопируйте **access_token** (начинается с `oauth:`)\n"
            "4. Вставьте этот токен при добавлении аккаунта.\n\n"
            "⚠️ Для каждого аккаунта нужен СВОЙ токен, полученный под его логином."
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("waiting_for_account"):
        text = update.message.text.strip()
        parts = text.split(":")
        if len(parts) != 3:
            await update.message.reply_text("❌ Неверный формат. Нужно: email:пароль:oauth_токен")
            return
        email, password, oauth = parts
        accounts = load_accounts()
        accounts[email] = {"password": password, "oauth": oauth}
        save_accounts(accounts)
        await update.message.reply_text(f"✅ Аккаунт {email} добавлен.")
        context.user_data["waiting_for_account"] = False
    
    elif context.user_data.get("channel_to_run"):
        channel = update.message.text.strip()
        accounts_to_run = context.user_data["channel_to_run"]
        for email in accounts_to_run:
            data = load_accounts()[email]
            await start_viewer(email, data["password"], data["oauth"], channel)
        await update.message.reply_text(f"🚀 Запущено {len(accounts_to_run)} зрителей на канал {channel}.")
        context.user_data["channel_to_run"] = None

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("✅ Бот запущен. Напишите /start в Telegram.")
    app.run_polling()

if __name__ == "__main__":
    main()
