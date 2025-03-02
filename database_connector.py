import mysql.connector
from mysql.connector import Error
import logging


class DatabaseConnector:
    def __init__(self, host, user, password, database):
        """Initialize database connection"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish database connection"""
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
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def execute_query(self, query, params=None):
        """Execute a regular SQL query"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()

            cursor = self.connection.cursor(dictionary=True)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith(('SELECT', 'SHOW')):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return {"affected_rows": cursor.rowcount}

        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_table_schema(self, table_name):
        """Get schema information for a table"""
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)

    def get_all_tables(self):
        """Get all tables in the database"""
        query = "SHOW TABLES"
        return self.execute_query(query)