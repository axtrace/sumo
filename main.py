import os
import telebot

# Импорт класса GPTAdapter
from gpt_adapter import GPTAdapter
from ybd_adapter import YdbAdapter

# Инициализация бота
TOKEN = os.environ['PRODUCTION_TOKEN']
bot = telebot.TeleBot(TOKEN)

ybd = YdbAdapter()

MAX_CALLS = int(os.environ.get('MAX_CALLS', 10))

# Контекст для хранения ограничений вызовов
context = {}

def handler(event, context):
    message = telebot.types.Update.de_json(event['body'])
    bot.process_new_updates([message])
    return {
        'statusCode': 200,
        'body': 'OK'
    }


@bot.message_handler(func=lambda message: True)
def save_message(message: types.Message):
    """Сохраняет все входящие сообщения в YDB"""
    try:
        # Подготовка данных
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or \
                  f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
        text = message.text or message.caption or ''
        
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
            'date': message.date,
            'text': text,
            'entities': [e.to_dict() for e in message.entities] if message.entities else None,
            'caption_entities': [e.to_dict() for e in message.caption_entities] if message.caption_entities else None
        }
        
        # Сохранение сообщения
        ybd.save_message(
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            text=text,
            raw_data=raw_data
        )
        
    except Exception as e:
        print(f"Ошибка сохранения сообщения: {str(e)}")


@tb.message_handler(commands=['summurize', 'summary'])
def summarize(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
  try:
    chat_id = update.effective_chat.id
    if chat_id not in context:
        context[chat_id] = {'calls': 0, 'last_call': None}

    # Проверка ограничений вызовов
    if context[chat_id]['calls'] >= MAX_CALLS:
        bot.reply_to(message, "Достигнут суточный лимит вызовов. Попробуйте повторить запрос спустя несколько часов")
        return

    # Получение сообщений с последней синхронизации
    messages = ybd.get_messages(chat_id)
    
    if not messages:
        bot.reply_to(message, "Нет сообщений для саммаризации")
        return

    chat_history = "\n".join(
        f"{msg['username']}: {msg['text']}" 
        for msg in reversed(messages)  # В хронологическом порядке
    )
      
    # Создание экземпляра GPTAdapter
    gpt = GPTAdapter()

    # Суммирование сообщений
    summary = gpt.summarize(chat_history)

    # Отправка суммирования
    bot.reply_to(message, summary)

    # Обновление контекста вызовов
    context[chat_id]['calls'] += 1
    context[chat_id]['last_call'] = datetime.now()
    
  except Exception as e:
    print(message.text)
    bot.reply_to(message, f"⚠️ Ошибка: {str(e)}")
