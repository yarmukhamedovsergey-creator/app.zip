#!/usr/bin/env python3
import subprocess, sys, os

BOT_PATH='/root/bot.py'
VENV_PATH='/root/venv/bin/activate'

# Функция проверяет запущен ли бот
def is_bot_running():
    result = subprocess.run(['pgrep', '-f', BOT_PATH], stdout=subprocess.PIPE)
    return bool(result.stdout.strip())

# Если запущен, ничего не делаем
if is_bot_running():
    print('Бот уже запущен, пропуск.')
    sys.exit(0)

# Активируем виртуальное окружение
activate_cmd = f'source {VENV_PATH}'
subprocess.call(activate_cmd, shell=True, executable='/bin/bash')

# Запускаем бот в фоне с логированием
log_file='/root/bot.log'
subprocess.Popen([sys.executable, BOT_PATH], stdout=open(log_file,'a'), stderr=subprocess.STDOUT)
print('Бот запущен.')
