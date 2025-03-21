from flask import Flask, request, jsonify, session, render_template
from werkzeug.security import generate_password_hash, check_password_hash
import logging
import os
import secrets
import re


class FlaskAPI:
    def __init__(self, chatbot_engine, db_connector, secret_key=None):
        self.logger = logging.getLogger(__name__)
        self.chatbot_engine = chatbot_engine
        self.db_connector = db_connector
        self.app = Flask(__name__,
                         template_folder='../templates',
                         static_folder='../static')

        self.app.secret_key = secret_key or os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(16))

        self.setup_routes()

    def setup_routes(self):

        @self.app.route('/')
        def index():
            return render_template('index.html')

        @self.app.route('/api/chat', methods=['POST'])
        def chat():
            data = request.json
            if not data or 'message' not in data:
                return jsonify({"error": "No message provided"}), 400

            result = self.chatbot_engine.process_user_input(data['message'])



            return jsonify(result)
        @self.app.route('/api/schema', methods=['GET'])
        def schema():
            tables = {}

            table_query = "SHOW TABLES"
            table_results = self.db_connector.execute_query(table_query)

            if table_results:
                for table_row in table_results:
                    table_name = list(table_row.values())[0]

                    schema_query = f"DESCRIBE {table_name}"
                    schema_results = self.db_connector.execute_query(schema_query)

                    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                    count_result = self.db_connector.execute_query(count_query)
                    row_count = count_result[0]['count'] if count_result else 0

                    tables[table_name] = {
                        "columns": schema_results,
                        "row_count": row_count
                    }

            return jsonify({"schema": tables})

        @self.app.route('/api/execute_sql', methods=['POST'])
        def execute_sql():
            data = request.json
            if not data or 'sql' not in data:
                return jsonify({"error": "SQL query required"}), 400

            sql = data['sql']

            if not re.match(r'^\s*SELECT', sql, re.IGNORECASE):
                return jsonify({"error": "Only SELECT queries are allowed for security reasons"}), 403

            results = self.db_connector.execute_query(sql)


            return jsonify({
                "results": results,
                "row_count": len(results) if results else 0
            })

        @self.app.route('/api/table_preview/<table_name>', methods=['GET'])
        def table_preview(table_name):
            # Safety check - only allow alphanumeric table names
            if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                return jsonify({"error": "Invalid table name"}), 400

            preview_query = f"SELECT * FROM {table_name} LIMIT 100"
            preview_results = self.db_connector.execute_query(preview_query)

            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.db_connector.execute_query(count_query)
            row_count = count_result[0]['count'] if count_result else 0

            return jsonify({
                "table": table_name,
                "data": preview_results,
                "row_count": row_count
            })

        @self.app.route('/api/db_stats', methods=['GET'])
        def db_stats():
            stats = {}

            tables_query = "SHOW TABLES"
            tables_result = self.db_connector.execute_query(tables_query)

            if tables_result:
                table_stats = []
                total_rows = 0
                total_size_mb = 0

                for table_row in tables_result:
                    table_name = list(table_row.values())[0]

                    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                    count_result = self.db_connector.execute_query(count_query)
                    row_count = count_result[0]['count'] if count_result else 0
                    total_rows += row_count

                    size_query = f"""
                        SELECT 
                            table_name AS 'table',
                            round(((data_length + index_length) / 1024 / 1024), 2) AS 'size_mb'
                        FROM information_schema.TABLES
                        WHERE table_schema = DATABASE()
                        AND table_name = '{table_name}'
                    """
                    size_result = self.db_connector.execute_query(size_query)
                    size_mb = size_result[0]['size_mb'] if size_result and size_result[0]['size_mb'] else 0
                    total_size_mb += size_mb

                    table_stats.append({
                        "name": table_name,
                        "row_count": row_count,
                        "size_mb": size_mb
                    })

                stats["tables"] = table_stats
                stats["total_tables"] = len(table_stats)
                stats["total_rows"] = total_rows
                stats["total_size_mb"] = total_size_mb

            return jsonify({"stats": stats})

        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            return jsonify({
                "status": "ok",
                "authenticated": True,
                "is_admin": True
            })


    def start_server(self, host='0.0.0.0', port=5000, debug=False):
        self.logger.info(f"Starting server on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)