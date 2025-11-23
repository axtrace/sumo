import os
import ydb
import ydb.iam
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

class YdbAdapter:
    def __init__(self):
        if os.getenv('CLOUD_ENV'):
            env_credentials = ydb.iam.MetadataUrlCredentials()
        else:
            env_credentials=ydb.iam.ServiceAccountCredentials.from_file(YDB_SERVICE_ACCOUNT_KEY_FILE)
        
        self.driver = ydb.Driver(
            endpoint=os.getenv('YDB_ENDPOINT'),
            database=os.getenv('YDB_DATABASE'),
            credentials=env_credentials
        )
        self.driver.wait(timeout=5, fail_fast=True)
        self.pool = ydb.SessionPool(self.driver)
    
    def _prepare_and_execute(self, session, query, parameters):
        # 1. Подготавливаем запрос
        prepared = session.prepare(query)
        
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
            print(f"Query failed: {str(e)}\n query:{query}\n parameters:{parameters}")
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
            
    def save_summary_record(self, chat_id: int, summary_time: datetime, user_id: int):
        """Сохраняет запись саммаризации в таблицу chat_summary_history"""
        try:
            # Генерация уникального идентификатора для summary_id
            summary_id = str(uuid.uuid4())
            
            query = """
                DECLARE $chat_id AS Int64;
                DECLARE $summary_id AS Utf8;
                DECLARE $summary_time AS Timestamp;
                DECLARE $user_id AS Int64;
    
                UPSERT INTO chat_summary_history 
                (chat_id, summary_id, summary_time, user_id)
                VALUES ($chat_id, $summary_id, $summary_time, $user_id);
            """
    
            # Параметры запроса
            params = {
                '$chat_id': int(chat_id),  # Убедитесь, что передается как Int64
                '$summary_id': summary_id,  # Передаем как Utf8
                '$summary_time': int(summary_time.timestamp()),  # Передаем как Unix Timestamp целое число
                '$user_id': int(user_id)  # Убедитесь, что передается как Int64
            }
    
            # Выполнение запроса
            self.execute_query(query, params)
    
        except Exception as e:
            print(f"Error while saving summary record: {e}")
            raise
    
            
    def get_messages_since(self, chat_id: int, since: datetime) -> List[Dict[str, Any]]:
        """Возвращает сообщения после указанной даты"""
        try:
            query = """
            DECLARE $chat_id AS Int64;
            DECLARE $since_date AS Datetime;
    
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
            ORDER BY date DESC;
            """
            
            # Преобразуем `since` в Unix Timestamp, преобразованный в секунды
            unix_timestamp = int(since.timestamp())
            
            parameters = {
                '$chat_id': chat_id,
                '$since_date': unix_timestamp
            }
    
            # Логирование для отладки
            print(f"Querying messages since {since} (timestamp: {unix_timestamp}) for chat_id {chat_id}")
            
            # Выполнение запроса
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
                    # Предполагая, что 'row.date' уже объект datetime
                    'date': row.date,
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
            # Лимит времени для выборки
            hours_limit = max(1, int(os.getenv('SUMMARY_HOURS_LIMIT', '24')))
            time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_limit)
    
            # SQL-запрос с параметрами
            query = """
                DECLARE $chat_id AS Int64;
                DECLARE $time_threshold AS Timestamp;
    
                SELECT COUNT(*) AS usage_count
                FROM chat_summary_history
                WHERE chat_id = $chat_id
                  AND summary_time >= $time_threshold;
            """
    
            # Параметры запроса
            params = {
                '$chat_id': int(chat_id),  # Преобразуем chat_id в целое число для соответствия типу Int64
                '$time_threshold': int(time_threshold.timestamp())  # Unix timestamp как целое число
            }
    
            # Выполнение запроса
            result = self.execute_query(query, params)
    
            # Логирование результата для отладки
            print(f"Query result: {result}")
    
            # Обработка результата
            if result and result[0].rows:
                usage_count = result[0].rows[0].get('usage_count', 0)
    
                # Проверяем тип usage_count и преобразуем его в целое число при необходимости
                if isinstance(usage_count, str):
                    try:
                        usage_count = int(usage_count)
                    except ValueError:
                        print(f"Error converting usage_count to int: {usage_count}")
                        return 0
    
                return usage_count
    
            return 0  # Если нет строк в результате
    
        except Exception as e:
            print(f"Error: {e}")
            return 0
