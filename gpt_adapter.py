import os
from yandex_cloud_ml_sdk import YCloudML

class GPTAdapter:
    
    def __init__(self):
        self.sdk = YCloudML(
            folder_id=os.environ['FOLDER_ID'],
            auth=os.environ['API_KEY']
        )
        self.model = self.sdk.models.completions("yandexgpt", model_version="rc")
        self.model = self.model.configure(temperature=0.8)
        self.model = self.model.configure(reasoning_mode='enabled_hidden')
        self.messages = [
            {
               "role": "system", 
                          "text": """Анализируй сообщения из чата и создавай структурированное краткое содержание:
              1. Выдели основные темы обсуждения и выводы
              2. Отметь ключевых участников по имени и их позиции
              3. Укажи важные выводы или решения
              4. Сохраняй нейтральный тон
              5. Используй маркированные списки для наглядности"""
            }
        ]
     
    def summarize(self, messages_text):
        user_message = {
            "role": "user",
            "text": f"Сообщения чата:\n{messages_text}"
        }
        
        result = self.model.run(self.messages + [user_message])
        return result.alternatives[0].text






