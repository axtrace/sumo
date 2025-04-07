import os
import telebot

# Импорт класса GPTAdapter
from gpt_adapter import GPTAdapter

# Инициализация бота
TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN)

MAX_CALLS = int(os.environ['MAX_CALLS'])

# Контекст для хранения ограничений вызовов
context = {}

def handler(event, context):
    message = telebot.types.Update.de_json(event['body'])
    bot.process_new_updates([message])
    return {
        'statusCode': 200,
        'body': 'OK'
    }

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
    messages = bot.history(chat_id, limit=500)
    
    if not messages:
        bot.reply_to(message, "Нет сообщений для суммирования")
        return

    # Создание экземпляра GPTAdapter
    gpt = GPTAdapter()

    # Суммирование сообщений
    summary = gpt.summarize(messages)

    # Отправка суммирования
    bot.reply_to(message, summary)

    # Обновление контекста вызовов
    context[chat_id]['calls'] += 1
    context[chat_id]['last_call'] = datetime.now()
    
  except Exception as e:
    print(message.text)
    bot.reply_to(message, f"⚠️ Ошибка: {str(e)}")


def get_messages_from_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Реализуйте получение сообщений из чата здесь
    # Для примера возвращаем пустой список
    return []

def main() -> None:
    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler(['summarize', 'summary'], summarize))
    application.run_polling()

if __name__ == '__main__':
    main()
