import mysql.connector
from mysql.connector import Error
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
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                self.logger.info(f"Connected to MySQL database: {self.database}")
                return True
        except Error as e:
            self.logger.error(f"Error connecting to MySQL database: {e}")
            return False

    def disconnect(self):
        try:
            if self.connection:
                try:
                    if hasattr(self.connection, 'is_connected') and self.connection.is_connected():
                        cursor = self.connection.cursor()
                        cursor.fetchall()
                        cursor.close()
                except Error:
                    pass

                self.connection.close()
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error disconnecting from database: {e}")

    def execute_query(self, query, params=None):
        cursor = None
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()

            cursor = self.connection.cursor(dictionary=True)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                result = cursor.fetchall()  # This ensures we consume all results
                return result
            else:
                self.connection.commit()
                return {"affected_rows": cursor.rowcount}

        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def get_table_schema(self, table_name):
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)

    def get_all_tables(self):
        query = "SHOW TABLES"
        return self.execute_query(query)

    def handle_unread_results(self):
        try:
            if self.connection and self.connection.is_connected():
                cursor = self.connection.cursor()
                while self.connection.unread_result:
                    cursor.fetchall()
                cursor.close()
        except Error as e:
            self.logger.error(f"Error handling unread results: {e}")