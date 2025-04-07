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

    def _run_transaction(self, session, query):
        # Create the transaction and execute query.
        return session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )

    def execute_query(self, query):
        result = self.pool.retry_operation_sync(
            lambda session: self._run_transaction(session, query))
        return result
