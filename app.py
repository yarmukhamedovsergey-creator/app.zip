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

# ================= КОНФИГУРАЦИЯ =================
TELEGRAM_TOKEN = "8885296149:AAEmYTNxvJC24Q3tKP_pMM58ag3m5nr_knA"
OPENROUTER_API_KEY = "sk-or-v1-788bafdcdffd8d3bc4344cee8d44027a881db1bc6f1d2d083651a93c19aba9b"  # получите на https://openrouter.io/keys
OPENROUTER_MODEL = "openai/gpt-4o-mini"     # можно заменить на любую модель
ACCOUNTS_FILE = "twitch_accounts.json"
# =================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- РАБОТА С ФАЙЛОМ АККАУНТОВ ----------
def load_accounts() -> dict:
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_accounts(accounts: dict):
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

# ---------- AI-ОТВЕТЫ ЧЕРЕЗ OPENROUTER ----------
async def generate_ai_reply(prompt: str, context_messages: List[str] = None) -> str:
    system_prompt = (
        "Ты — реальный зритель стрима на Twitch. Общайся как обычный человек, кратко (1-2 предложения), "
        "иногда используй сленг типа Pog, LUL, KEKW. Не говори, что ты бот."
    )
    messages = [{"role": "system", "content": system_prompt}]
    if context_messages:
        for msg in context_messages[-5:]:
            messages.append({"role": "user", "content": msg})
    messages.append({"role": "user", "content": prompt})

    try:
        resp = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
            json={"model": OPENROUTER_MODEL, "messages": messages, "max_tokens": 60, "temperature": 0.9},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"AI error: {e}")
        return "nice!"

# ---------- TWITCH-ЗРИТЕЛЬ (ОДИН АККАУНТ) ----------
class TwitchViewer(TwitchClient):
    def __init__(self, username: str, oauth_token: str, channel_name: str):
        # oauth_token должен начинаться с "oauth:"
        super().__init__(token=oauth_token, initial_channels=[channel_name])
        self.username = username
        self.channel_name = channel_name
        self._history: List[str] = []

    async def event_ready(self):
        logger.info(f"✅ {self.username} зашёл на канал {self.channel_name}")
        # Отправить приветствие в чат (опционально)
        # channel = self.get_channel(self.channel_name)
        # await channel.send("Привет, стрим!")

    async def event_message(self, message):
        if message.echo or message.channel.name != self.channel_name.lstrip("#"):
            return
        self._history.append(f"{message.author.name}: {message.content}")
        if len(self._history) > 20:
            self._history.pop(0)

        # Отвечаем на 30% сообщений
        if random.random() < 0.3:
            reply = await generate_ai_reply(message.content, self._history)
            await message.channel.send(reply)
            logger.info(f"{self.username} -> {reply}")

    async def run_viewer(self):
        await self.start()

    async def stop_viewer(self):
        await self.close()

# ---------- МЕНЕДЖЕР ЗРИТЕЛЕЙ ----------
active_viewers: Dict[str, TwitchViewer] = {}

async def start_viewer(username: str, oauth_token: str, channel: str):
    if username in active_viewers:
        return
    viewer = TwitchViewer(username, oauth_token, channel)
    active_viewers[username] = viewer
    asyncio.create_task(viewer.run_viewer())

async def stop_viewer(username: str):
    if username in active_viewers:
        await active_viewers[username].stop_viewer()
        del active_viewers[username]

# ---------- TELEGRAM ПАНЕЛЬ ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📋 Список аккаунтов", callback_data="list")],
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add")],
        [InlineKeyboardButton("🚀 Запустить всех на канал", callback_data="run")],
        [InlineKeyboardButton("🛑 Остановить всех", callback_data="stop")],
        [InlineKeyboardButton("❓ Как получить OAuth токен", callback_data="help")]
    ]
    await update.message.reply_text(
        "👾 **Панель управления Twitch-зрителями**\n"
        "Бот заходит на стрим и отвечает в чате через ИИ.\n\n"
        "Формат добавления: `ник:oauth_токен`\n"
        "Токен должен начинаться с `oauth:`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "list":
        accs = load_accounts()
        if not accs:
            await query.edit_message_text("❌ Нет аккаунтов.")
            return
        msg = "📋 **Аккаунты:**\n" + "\n".join(f"• {u}" for u in accs.keys())
        await query.edit_message_text(msg, parse_mode="Markdown")

    elif query.data == "add":
        await query.edit_message_text(
            "Введи в чат данные в формате:\n"
            "`ник:oauth_токен`\n\n"
            "Пример: `mystreamer:oauth:abc123def456...`"
        )
        context.user_data["waiting_account"] = True

    elif query.data == "run":
        accs = load_accounts()
        if not accs:
            await query.edit_message_text("Нет аккаунтов для запуска.")
            return
        await query.edit_message_text("Введите имя канала (без #):")
        context.user_data["run_channel"] = list(accs.keys())

    elif query.data == "stop":
        for u in list(active_viewers.keys()):
            await stop_viewer(u)
        await query.edit_message_text("✅ Все зрители остановлены.")

    elif query.data == "help":
        await query.edit_message_text(
            "🔑 **Как получить OAuth токен для Twitch**\n"
            "1. Перейди на https://twitchtokengenerator.com/\n"
            "2. Выбери **Custom Scope** → отметь `chat:read` и `chat:edit`\n"
            "3. Нажми **Generate** и авторизуйся под нужным аккаунтом\n"
            "4. Скопируй **access_token** (начинается с `oauth:`)\n"
            "5. Вставь его при добавлении аккаунта.\n\n"
            "⚠️ Для каждого аккаунта нужен свой токен."
        )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("waiting_account"):
        text = update.message.text.strip()
        if ":" not in text:
            await update.message.reply_text("❌ Неверный формат. Нужно: ник:токен")
            return
        username, token = text.split(":", 1)
        accs = load_accounts()
        accs[username] = token
        save_accounts(accs)
        await update.message.reply_text(f"✅ Аккаунт {username} добавлен.")
        context.user_data["waiting_account"] = False

    elif context.user_data.get("run_channel"):
        channel = update.message.text.strip()
        usernames = context.user_data["run_channel"]
        for un in usernames:
            token = load_accounts()[un]
            await start_viewer(un, token, channel)
        await update.message.reply_text(f"🚀 Запущено {len(usernames)} зрителей на канал {channel}.")
        context.user_data["run_channel"] = None

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    logger.info("Бот запущен. Напишите /start в Telegram.")
    app.run_polling()

if __name__ == "__main__":
    main()
