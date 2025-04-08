import os
import ydb
import ydb.iam


class YdbAdapter:
    """connection to YDB"""

    def __init__(self):
        # Create driver in global space.
        driver = ydb.Driver(
            endpoint=os.getenv('YDB_ENDPOINT'),
            database=os.getenv('YDB_DATABASE'),
            # credentials=ydb.iam.MetadataUrlCredentials(),
            # root_certificates=ydb.load_ydb_root_certificate()
            credentials=ydb.AccessTokenCredentials(os.getenv('YDB_SERVICE_ACCOUNT_TOKEN'))
        )

        # Wait for the driver to become active for requests.
        driver.wait(fail_fast=True, timeout=5)

        # Create the session pool instance to manage YDB sessions.
        self.pool = ydb.SessionPool(driver)
        self.table_path = "chat_messages"
        self.message_limit = 1000

    def _run_transaction(self, session, query):
        # Create the transaction and execute query.
        return session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )

    async def _execute_query(self, query: str, parameters: Dict = None) -> Any:
        """Универсальный метод выполнения запросов"""
        async def callee(session):
            return await session.transaction().execute(
                query,
                commit_tx=True,
                parameters=parameters
            )
        return await self.pool.retry_operation(callee)
    
    async def save_message(self, chat_id: int, user_id: int, username: str, text: str, raw_data: Dict) -> None:
        """
        Сохраняет сообщение в YDB
        :param chat_id: ID чата
        :param user_id: ID пользователя
        :param username: Имя пользователя
        :param text: Текст сообщения
        :param raw_data: Исходные данные сообщения в формате JSON
        """
        # 1. Сохраняем новое сообщение
        insert_query = f"""
            DECLARE $raw_json AS Json;
            
            UPSERT INTO `{self.table_path}`
            (id, chat_id, user_id, username, date, text, raw)
            VALUES (
                CAST(RandomUuid() AS Uint64),
                {chat_id},
                {user_id},
                "{username}",
                DateTime::MakeDatetime(DateTime::Now()),
                "{text.replace('"', '""')}",
                $raw_json
            );
        """
        
        return await self._execute_query(insert_query, {'$raw_json': json.dumps(raw_data)})
    

    async def get_messages(self, chat_id: int, limit: int = 1000) -> List[Dict]:
        """
        Возвращает последние сообщения из указанного чата
        :param chat_id: ID чата
        :param limit: Максимальное количество сообщений
        :return: Список сообщений в формате [{"id": ..., "text": ..., ...}]
        """
        query = f"""
            SELECT * FROM `{self.table_path}`
            WHERE chat_id = {chat_id}
            ORDER BY date DESC
            LIMIT {min(limit, self.message_limit)};
        """
        
        result = await self._execute_query(query)
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

