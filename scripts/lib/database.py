"""
Class for creating snowflake connections
"""

from etc.variables import *
from lib.utils import create_connection


class Connection:
    """
    Class for database connection
    """

    user = None
    account = None
    database = None
    db_ctx: SnwfConn = None

    def __init__(self, user: str, account: str, database: str, password: str):
        self.user = user
        self.account = account
        self.database = database

        if database:
            self.db_ctx = create_connection(
                user=user,
                account=account,
                database=database,
                password=password,
            )

        self.get_info()

    def close_connection(self):
        """Close the snowflake connections"""

        self.db_ctx.close() if self.db_ctx else None

    def get_info(self):
        """Get snowflake connection information"""

        return dict(
            user=self.user,
            account=self.account,
            database=self.database,
        )
