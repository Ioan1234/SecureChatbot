

from flask import Flask, request, jsonify, session, render_template
from werkzeug.security import generate_password_hash, check_password_hash
from api.speech_routes import SpeechRoutes
from speech.speech_recognition import SecureSpeechRecognition
import logging
import os
import secrets
import re
import base64
import datetime

class FlaskAPI:
    def __init__(self, chatbot_engine, db_connector, secret_key=None):
        self.logger = logging.getLogger(__name__)
        self.chatbot_engine = chatbot_engine
        self.db_connector = db_connector
        self.app = Flask(__name__,
                         template_folder='../templates',
                         static_folder='../static')

        self._he_logs = []

        try:
            speech_rec = SecureSpeechRecognition()
            SpeechRoutes(self.app, speech_rec)
            self.logger.info("Registered /api/speech_recognition")
        except Exception as e:
            self.logger.warning(f"Speech recognition not available: {e}")

        class TripwireHandler(logging.Handler):
            def __init__(self, target_list):
                super().__init__(level=logging.DEBUG)
                self.target_list = target_list
                self.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))

            def emit(self, record):
                raw = record.getMessage()
                if raw.startswith("HE-") or raw.startswith("HE-TRIPWIRE") or "HE-" in raw:

                    self.target_list.append(self.format(record))


        trip = TripwireHandler(self._he_logs)
        logging.getLogger('secure_database_connector').addHandler(trip)
        logging.getLogger('encryption_manager').addHandler(trip)



        self.setup_routes()



    def _process_value_for_json_safe(self, value):
        
        if isinstance(value, bytes):

            return "[BINARY DATA]"


        elif isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        elif isinstance(value, datetime.timedelta):

            total_seconds = int(value.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)

            parts = []
            if hours > 0: parts.append(f"{hours}h")
            if minutes > 0: parts.append(f"{minutes}m")
            if seconds > 0 or not parts: parts.append(f"{seconds}s")
            return " ".join(parts)



        else:

            return value

    def _process_results_for_json(self, results):
        
        if results is None:
            return None

        if not isinstance(results, list):

             if isinstance(results, dict):

                 return {k: self._process_value_for_json_safe(v) for k, v in results.items()}

             return self._process_value_for_json_safe(results)


        processed_results = []
        for row in results:
            processed_row = {}
            if isinstance(row, dict):
                for key, value in row.items():
                    processed_row[key] = self._process_value_for_json_safe(value)
            else:

                 processed_row = self._process_value_for_json_safe(row)
            processed_results.append(processed_row)
        return processed_results


    def setup_routes(self):

        @self.app.route('/')
        def index():
            return render_template('index.html')

        @self.app.route('/api/chat', methods=['POST'])
        def chat():
            data = request.json
            if not data or 'message' not in data:
                return jsonify({"error": "No message provided"}), 400



            self._he_logs.clear()
            result = self.chatbot_engine.process_user_input(data['message'])



            if self._he_logs:
                result.setdefault('metadata', {})['encrypted'] = True
                result['metadata']['he_tripwire'] = list(self._he_logs)


            return jsonify(result)

        @self.app.route('/api/schema', methods=['GET'])
        def schema():
            tables = {}
            try:
                table_query = "SHOW TABLES"

                raw_table_results = self.db_connector.execute_query(table_query)
                processed_table_results = self._process_results_for_json(raw_table_results)


                if processed_table_results:
                    for table_row in processed_table_results:
                        table_name = list(table_row.values())[0]


                        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                           self.logger.warning(f"Skipping invalid table name from SHOW TABLES: {table_name}")
                           continue

                        schema_query = f"DESCRIBE `{table_name}`"
                        raw_schema_results = self.db_connector.execute_query(schema_query)

                        processed_schema_results = self._process_results_for_json(raw_schema_results)

                        count_query = f"SELECT COUNT(*) as count FROM `{table_name}`"

                        raw_count_result = self.db_connector.execute_query(count_query)
                        processed_count_result = self._process_results_for_json(raw_count_result)
                        row_count = processed_count_result[0]['count'] if processed_count_result else 0

                        tables[table_name] = {
                            "columns": processed_schema_results or [],
                            "row_count": row_count
                        }

                return jsonify({"schema": tables})
            except Exception as e:
                self.logger.error(f"Error fetching schema: {e}", exc_info=True)
                return jsonify({"error": "Failed to retrieve database schema"}), 500


        @self.app.route('/api/execute_sql', methods=['POST'])
        def execute_sql():
            data = request.json
            if not data or 'sql' not in data:
                return jsonify({"error": "SQL query required"}), 400

            sql = data['sql']

            params = data.get('params')


            if not re.match(r'^\s*SELECT', sql, re.IGNORECASE):
                return jsonify({"error": "Only SELECT queries are allowed for security reasons"}), 403


            default_limit = 1000
            if not re.search(r'\bLIMIT\s+\d+\b', sql, re.IGNORECASE):
                 self.logger.warning(f"Adding default LIMIT {default_limit} to SQL query.")
                 sql = f"{sql.rstrip(';')} LIMIT {default_limit}"


            try:

                raw_results = self.db_connector.execute_query(sql, params)


                processed_results = self._process_results_for_json(raw_results)

                return jsonify({
                    "success": True,
                    "results": processed_results,
                    "row_count": len(processed_results) if isinstance(processed_results, list) else (1 if processed_results else 0)
                })
            except Exception as e:
                self.logger.error(f"Error executing raw SQL query: {e}", exc_info=True)

                error_message = f"Database query failed: {type(e).__name__}"
                return jsonify({"success": False, "error": error_message}), 500


        @self.app.route('/api/table_preview/<table_name>', methods=['GET'])
        def table_preview(table_name):

            if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                return jsonify({"error": "Invalid table name format"}), 400


            preview_limit = 100
            preview_query = f"SELECT * FROM `{table_name}` LIMIT {preview_limit}"

            try:
                raw_preview_results = self.db_connector.execute_query(preview_query)

                processed_preview_results = self._process_results_for_json(raw_preview_results)

                count_query = f"SELECT COUNT(*) as count FROM `{table_name}`"

                raw_count_result = self.db_connector.execute_query(count_query)
                processed_count_result = self._process_results_for_json(raw_count_result)
                row_count = processed_count_result[0]['count'] if processed_count_result else 0

                return jsonify({
                    "table": table_name,
                    "data": processed_preview_results,
                    "row_count": row_count,
                    "preview_limit": preview_limit
                })
            except Exception as e:
                self.logger.error(f"Error fetching table preview for {table_name}: {e}", exc_info=True)

                if "Table" in str(e) and "doesn't exist" in str(e):
                     return jsonify({"error": f"Table '{table_name}' not found"}), 404
                return jsonify({"error": "Failed to retrieve table preview"}), 500


        @self.app.route('/api/db_stats', methods=['GET'])
        def db_stats():
            stats = {}
            try:
                tables_query = "SHOW TABLES"

                raw_tables_result = self.db_connector.execute_query(tables_query)
                processed_tables_result = self._process_results_for_json(raw_tables_result)

                if processed_tables_result:
                    table_stats = []
                    total_rows = 0
                    total_size_mb = 0.0

                    for table_row in processed_tables_result:
                        table_name = list(table_row.values())[0]

                        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                            self.logger.warning(f"Skipping invalid table name from SHOW TABLES in db_stats: {table_name}")
                            continue

                        count_query = f"SELECT COUNT(*) as count FROM `{table_name}`"
                        raw_count_result = self.db_connector.execute_query(count_query)
                        processed_count_result = self._process_results_for_json(raw_count_result)
                        row_count = processed_count_result[0]['count'] if processed_count_result else 0
                        total_rows += row_count


                        db_name = self.db_connector.database
                        size_query = f"""
                                                    SELECT
                                                        round(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                                                    FROM information_schema.TABLES
                                                    WHERE table_schema = %s
                                                    AND table_name = %s
                                                """
                        raw_size_result = self.db_connector.execute_query(size_query, (db_name, table_name))
                        processed_size_result = self._process_results_for_json(raw_size_result)

                        size_mb_value = processed_size_result[0]['size_mb'] if processed_size_result and 'size_mb' in processed_size_result[0] else 0.0
                        size_mb = float(size_mb_value) if size_mb_value is not None else 0.0
                        total_size_mb += size_mb

                        table_stats.append({
                            "name": table_name,
                            "row_count": row_count,
                            "size_mb": size_mb
                        })

                    stats["tables"] = table_stats
                    stats["total_tables"] = len(table_stats)
                    stats["total_rows"] = total_rows

                    stats["total_size_mb"] = round(total_size_mb, 2)

                return jsonify({"stats": stats})
            except Exception as e:
                self.logger.error(f"Error fetching db stats: {e}", exc_info=True)
                return jsonify({"error": "Failed to retrieve database statistics"}), 500


        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            db_connected = False
            db_error = None
            try:

                if self.db_connector.is_connected():
                     db_status = self.db_connector.execute_query("SELECT 1")
                     db_connected = bool(db_status)
                else:

                    if self.db_connector.connect():
                        db_status = self.db_connector.execute_query("SELECT 1")
                        db_connected = bool(db_status)
                    else:
                         db_error = "Failed to connect initially"
            except Exception as e:
                self.logger.error(f"Database health check failed: {e}")
                db_error = str(e)
                db_connected = False

            return jsonify({
                "status": "ok" if db_connected else "error",
                "database_connected": db_connected,
                "database_error": db_error,

                "authenticated": True,
                "is_admin": True
            })

    def start_server(self, host='0.0.0.0', port=5000, debug=False):
        self.logger.info(f"Starting Flask server on http://{host}:{port}")

        use_reloader = debug
        self.app.run(host=host, port=port, debug=debug, use_reloader=False)

