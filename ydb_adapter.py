import os
import ydb
import ydb.iam
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List

class YdbAdapter:
    def __init__(self):
        self.driver = ydb.Driver(
            endpoint=os.getenv('YDB_ENDPOINT'),
            database=os.getenv('YDB_DATABASE'),
            # credentials=ydb.iam.ServiceAccountCredentials.from_file(YDB_SERVICE_ACCOUNT_KEY_FILE)
            credentials = ydb.iam.MetadataUrlCredentials()
        )
        self.driver.wait(timeout=5, fail_fast=True)
        self.pool = ydb.SessionPool(self.driver)
    
    def _prepare_and_execute(self, session, query, parameters):
        # 1. Подготавливаем запрос
        prepared = session.prepare(query)
        print("Query prepared successfully")
        
        # 2. Выполняем с параметрами
        return session.transaction().execute(
            prepared,
            parameters,
            commit_tx=True
        )

    def execute_query(self, query, parameters=None):
        def callee(session):
            return self._prepare_and_execute(session, query, parameters)
        try:
            result = self.pool.retry_operation_sync(callee)
            return result
        except Exception as e:
            # print(f"Query failed: {str(e)}")
            raise

    def save_message(self, chat_id: int, user_id: int, username: str, 
                   text: str, raw_data: Dict[str, Any], message_date: datetime) -> None:
        insert_query = """
        DECLARE $raw_json AS Json;
        DECLARE $chat_id AS Int64;
        DECLARE $user_id AS Int64;
        DECLARE $username AS Utf8;
        DECLARE $text AS Utf8;
        DECLARE $date AS Datetime;
        DECLARE $uuid AS Utf8;

        UPSERT INTO chat_messages (
            id, chat_id, user_id, username, date, text, raw
        ) VALUES (
            CAST(Digest::CityHash($uuid) AS Int64),
            $chat_id,
            $user_id,
            $username,
            $date,
            $text,
            $raw_json
        );
        """
        
        # Подготавливаем параметры с явным контролем типов
        parameters = {
            '$raw_json': json.dumps(raw_data),
            '$chat_id': int(chat_id),
            '$user_id': int(user_id),
            '$username': str(username),
            '$text': str(text),
            '$date': int(message_date.timestamp()),
            '$uuid': str(uuid.uuid4())
        }
        
        try:
            self.execute_query(insert_query, parameters)
            print("Message saved successfully")
        except Exception as e:
            print(f"Failed to save message: {e}")
            raise


    def get_messages(self, chat_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получает сообщения из указанного чата
        
        Args:
            chat_id: ID чата для получения сообщений
            limit: Максимальное количество сообщений (по умолчанию 1000)
            
        Returns:
            Список сообщений в формате словарей
        """
        query = """
        DECLARE $chat_id AS Int64;
        DECLARE $limit AS Uint64;
        
        SELECT 
            id,
            chat_id,
            user_id,
            username,
            date,
            text,
            raw
        FROM chat_messages
        WHERE chat_id = $chat_id
        ORDER BY date DESC
        LIMIT $limit;
        """
        
        parameters = {
            '$chat_id': int(chat_id),
            '$limit': min(limit, 1000)  # Ограничиваем максимальный лимит
        }
        
        try:
            result = self.execute_query(query, parameters)
            messages = []
            
            for row in result[0].rows:
                messages.append({
                    'id': row.id,
                    'chat_id': row.chat_id,
                    'user_id': row.user_id,
                    'username': row.username,
                    'date': datetime.fromtimestamp(row.date),  # Конвертируем timestamp обратно в datetime
                    'text': row.text
                })
                
            return messages
            
        except Exception as e:
            print(f"Failed to get messages: {e}")
            raise

