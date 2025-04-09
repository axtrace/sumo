import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from functools import wraps

import telebot
from telebot import types


# Импорт класса GPTAdapter
from gpt_adapter import GPTAdapter
from ydb_adapter import YdbAdapter

# Инициализация бота
TOKEN = os.environ['PRODUCTION_TOKEN']
bot = telebot.TeleBot(TOKEN)

ydb = YdbAdapter()

MAX_CALLS = int(os.environ.get('MAX_CALLS', 30))

# Контекст для хранения ограничений вызовов
context = {}

# Декоратор для обработки ошибок
def telegram_error_handler(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {f.__name__}: {str(e)}", exc_info=True)
            if args and isinstance(args[0], types.Message):
                try:
                    args[0].reply_text("⚠️ Произошла ошибка. Пожалуйста, попробуйте позже")
                except Exception as send_error:
                    print(f"Failed to send error message: {send_error}")
            return None
    return wrapped


@telegram_error_handler
def handler(event, context):
    try:
        message = telebot.types.Update.de_json(event['body'])
        bot.process_new_updates([message])
        return {'statusCode': 200, 'body': 'OK'}
    except Exception as e:
        logger.error(f"Handler error: {str(e)}", exc_info=True)
        return {'statusCode': 500, 'body': 'Error'}
    
def normalize_command(text, bot_username):
    if not text:
        return None
    # Удаляем упоминание бота если есть
    if text.startswith('@' + bot_username):
        text = text.split(' ', 1)[-1]
    return text.strip()

@bot.message_handler(func=lambda m: normalize_command(m.text, bot.get_me().username) in ['/summarize', '/summary'])
@telegram_error_handler
def summarize(message: types.Message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # 1. Проверяем лимиты вызовов
        current_date = datetime.now().date()
        usage_key = f"{chat_id}_{current_date}"
        
        current_usage = ydb.get_usage_today(chat_id)
        if current_usage >= MAX_CALLS:
            bot.reply_to(message, f"⚠️ Лимит саммаризаций исчерпан ({MAX_CALLS}/день)")
            return

        # 2. Получаем время последней саммаризации
        last_summary_time = ydb.get_last_summary_time(chat_id) or datetime.fromtimestamp(0)
        
        # 3. Получаем новые сообщения
        messages = ydb.get_messages_since(chat_id, last_summary_time)
        
        if not messages:
            bot.reply_to(message, "🔄 Нет новых сообщений для саммаризации")
            return

        # 4. Генерируем summary
        chat_history = "\n".join(
            f"{msg['username']}: {msg['text']}" 
            for msg in messages
        )
        
        gpt = GPTAdapter()
        summary = gpt.summarize(chat_history[:6000])
        
        if not summary:
            bot.reply_to(message, "❌ Ошибка генерации summary")
            return

        
        # 5. Сохраняем результат и обновляем статистику
        ydb.save_summary_record(
            chat_id=chat_id, 
            summary_time=datetime.now(),
            user_id=user_id
        )
        
        
        # 6. Отправляем пользователю (форматируем как цитаты)
        response = f"📝 Summary ({len(messages)} новых сообщений):\n\n"
        response += f"```\n{summary}\n```"
        
        bot.reply_to(message, response, parse_mode='Markdown')

    except Exception as e:
        print(f"Summary error: {str(e)}")
        bot.reply_to(message, "⚠️ Техническая ошибка. Попробуйте позже")


@bot.message_handler(func=lambda message: True)
@telegram_error_handler
def save_message(message: types.Message):
    """Сохраняет все входящие сообщения в ydb"""
    try:
        if not message.text or len(message.text.strip()) == 0:
            return
        # Подготовка данных
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or ""
        first_name = getattr(message.from_user, 'first_name', '').strip()
        last_name = getattr(message.from_user, 'last_name', '').strip()
        text = message.text or message.caption or ''
        message_date = datetime.fromtimestamp(message.date)
        
        # Формирование raw-данных
        raw_data = {
            'message_id': message.message_id,
            'from': {
                'id': user_id,
                'is_bot': message.from_user.is_bot,
                'first_name': message.from_user.first_name,
                'last_name': message.from_user.last_name,
                'username': message.from_user.username,
                'language_code': message.from_user.language_code
            },
            'chat': {
                'id': chat_id,
                'type': message.chat.type,
                'title': getattr(message.chat, 'title', None),
                'username': getattr(message.chat, 'username', None)
            },
            'date': datetime.fromtimestamp(message.date).isoformat(),
            'text': text,
            'entities': [e.to_dict() for e in message.entities] if message.entities else None,
            'caption_entities': [e.to_dict() for e in message.caption_entities] if message.caption_entities else None
        }
        
        # Сохранение сообщения
        ydb.save_message(
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            text=text,
            raw_data=raw_data,
            message_date=message_date
        )
        
    except Exception as e:
        print(f"Ошибка сохранения сообщения: {str(e)}")


