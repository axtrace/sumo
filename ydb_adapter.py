import os
import ydb
import ydb.iam
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

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

    def save_message(self, chat_id: int, user_id: int, username: str, first_name: str, last_name: str,
               text: str, raw_data: Dict[str, Any], message_date: datetime) -> None:
        insert_query = """
        DECLARE $raw_json AS Json;
        DECLARE $chat_id AS Int64;
        DECLARE $user_id AS Int64;
        DECLARE $username AS Utf8;
        DECLARE $first_name AS Utf8;
        DECLARE $last_name AS Utf8;
        DECLARE $text AS Utf8;
        DECLARE $date AS Datetime;
        DECLARE $uuid AS Utf8;
    
        UPSERT INTO chat_messages (
            id, chat_id, user_id, username, first_name, last_name, date, text, raw
        ) VALUES (
            CAST(Digest::CityHash($uuid) AS Int64),
            $chat_id,
            $user_id,
            $username,
            $first_name,
            $last_name,
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
            '$first_name': str(first_name),
            '$last_name': str(last_name),
            '$text': str(text),
            '$date': int(message_date.timestamp()),
            '$uuid': str(uuid.uuid4())
        }
        
        try:
            self.execute_query(insert_query, parameters)
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
            first_name,
            last_name,
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
                    'first_name': row.first_name,
                    'last_name': row.last_name,
                    'date': datetime.fromtimestamp(row.date),  # Конвертируем timestamp обратно в datetime
                    'text': row.text
                })
                
            return messages
            
        except Exception as e:
            print(f"Failed to get messages: {e}")
            raise

    def get_last_summary_time(self, chat_id: int) -> datetime | None:
        """Возвращает время последней саммаризации для чата"""
        query = """
        DECLARE $chat_id AS Int64;
        
        SELECT MAX(summary_time) as last_time 
        FROM chat_summary_history
        WHERE chat_id = $chat_id;
        """
        
        result = self.execute_query(query, {'$chat_id': chat_id})
        if result[0].rows and result[0].rows[0].last_time:
            return datetime.fromtimestamp(result[0].rows[0].last_time)
        return None

    def save_summary_record(self, chat_id: int, summary_time: datetime):
        """Сохраняет запись о выполненной саммаризации"""
        query = """
        DECLARE $chat_id AS Int64;
        DECLARE $summary_time AS Int64;
        
        UPSERT INTO chat_summary_history (chat_id, summary_time)
        VALUES ($chat_id, $summary_time);
        """
        
        self.execute_query(query, {
            '$chat_id': chat_id,
            '$summary_time': int(summary_time.timestamp())
        })

    def get_messages_since(self, chat_id: int, since: datetime) -> List[Dict[str, Any]]:
        """Возвращает сообщения после указанной даты"""
        try:
            query = """
            DECLARE $chat_id AS Int64;
            DECLARE $since_date AS Int64;
            
            SELECT 
                id,
                chat_id,
                user_id,
                username,
                first_name,
                last_name,
                date,
                text,
                raw
            FROM chat_messages
            WHERE chat_id = $chat_id 
              AND date > $since_date
            ORDER BY date ASC;
            """
            
            result = self.execute_query(query, {
                '$chat_id': chat_id,
                '$since_date': int(since.timestamp())
            })
            
            messages = []
            for row in result[0].rows:
                messages.append({
                    'id': row.id,
                    'chat_id': row.chat_id,
                    'user_id': row.user_id,
                    'username': row.username,
                    'first_name': row.first_name,
                    'last_name': row.last_name,
                    'date': datetime.fromtimestamp(row.date),
                    'text': row.text,
                    'raw': json.loads(row.raw) if row.raw else None
                })
            return messages
            
        except Exception as e:
            print(f"Get messages error: {e}")
            return []

    
    def get_usage_today(self, chat_id: int) -> int:
        """Возвращает количество саммаризаций за последние N часов"""
        try:
            hours_limit = max(1, int(os.getenv('SUMMARY_HOURS_LIMIT', '24')))
            time_threshold = int((datetime.now(timezone.utc) - timedelta(hours=hours_limit)).timestamp())
            
            query = """
            DECLARE $chat_id AS Int64;
            DECLARE $time_threshold AS Int64;
            
            SELECT COUNT(*) AS usage_count 
            FROM chat_summary_history
            WHERE chat_id = $chat_id
              AND summary_time >= $time_threshold;
            """
            
            params = {
                '$chat_id': chat_id,
                '$time_threshold': time_threshold
            }
            
            result = self.execute_query(query, params)
            return int(result[0].rows[0].usage_count) if result and result[0].rows else 0
            
        except Exception as e:
            print(f"Usage count error: {e}")
            return 0
