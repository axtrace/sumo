import os
import ydb
import ydb.iam
import json
from datetime import datetime
from typing import Dict, List, Any

class YdbAdapter:
    """Connection to YDB"""
    
    def __init__(self):
        # Получаем параметры подключения из переменных окружения
        endpoint = os.getenv('YDB_ENDPOINT', 'grpcs://ydb.serverless.yandexcloud.net:2135')
        database = os.getenv('YDB_DATABASE')
        
        # Используем сервисный аккаунт через IAM-токен
        self.driver = ydb.Driver(
            endpoint=endpoint,
            database=database,
            credentials=ydb.iam.MetadataUrlCredentials()  # Автоматически получает токен из метаданных
        )
        
        try:
            self.driver.wait(timeout=5)
        except Exception as e:
            print(f"Failed to initialize driver: {str(e)}")
            raise

        self.pool = self.driver.table_client.session_pool()
        self.table_path = "chat_messages"
        self.message_limit = 1000

    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> Any:
        def callee(session):
            return session.transaction().execute(
                query,
                commit_tx=True,
                parameters=parameters,
                settings=ydb.BaseRequestSettings(
                    timeout=3,
                    operation_timeout=2
                )
            )
        return self.pool.retry_operation(callee)

    def save_message(self, chat_id: int, user_id: int, username: str, 
                   text: str, raw_data: Dict[str, Any]) -> None:
        """Save message to YDB"""
        insert_query = f"""
        DECLARE $raw_json AS Json;
        
        UPSERT INTO `{self.table_path}` (
            id, chat_id, user_id, username, date, text, raw
        ) VALUES (
            CAST(RandomUuid() AS Uint64),
            {chat_id},
            {user_id},
            "{username.replace('"', '""')}",
            DateTime::MakeDatetime(DateTime::Now()),
            "{text.replace('"', '""')}",
            $raw_json
        );
        """
        
        self._execute_query(insert_query, {
            '$raw_json': json.dumps(raw_data)
        })

    def get_messages(self, chat_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get messages from chat"""
        query = f"""
        SELECT * FROM `{self.table_path}`
        WHERE chat_id = {chat_id}
        ORDER BY date DESC
        LIMIT {min(limit, self.message_limit)};
        """
        
        result = self._execute_query(query)
        return [
            {
                "id": row["id"],
                "chat_id": row["chat_id"],
                "user_id": row["user_id"],
                "username": row["username"],
                "date": row["date"],
                "text": row["text"],
                "raw": json.loads(row["raw"])
            }
            for row in result[0].rows
        ]
