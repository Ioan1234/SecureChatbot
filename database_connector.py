import pymysql
from pymysql.err import MySQLError
import logging

class DatabaseConnector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            if self.connection.open:
                self.logger.info(f"Connected to MySQL database: {self.database}")
                return True
        except MySQLError as e:
            self.logger.error(f"Error connecting to MySQL database: {e}")
            return False

    def is_connected(self):
        return self.connection and self.connection.open

    def disconnect(self):
        try:
            if self.connection and self.connection.open:
                self.connection.close()
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error disconnecting from database: {e}")

    def execute_query(self, query, params=None):
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False
            )
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())

                if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return {"affected_rows": cursor.rowcount}

        except MySQLError as e:
            self.logger.error(f"Error executing query: {e}")
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def get_table_schema(self, table_name):
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)

    def get_all_tables(self):
        query = "SHOW TABLES"
        return self.execute_query(query)
