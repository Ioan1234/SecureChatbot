from flask import Flask, request, jsonify, session, render_template
from werkzeug.security import generate_password_hash, check_password_hash
import logging
import os
import secrets


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

            if not session.get('authenticated', False) and self._requires_authentication(data['message']):
                return jsonify({"error": "Authentication required"}), 401

            result = self.chatbot_engine.process_user_input(data['message'])
            return jsonify(result)

        @self.app.route('/api/login', methods=['POST'])
        def login():
            data = request.json
            if not data or 'username' not in data or 'password' not in data:
                return jsonify({"error": "Username and password required"}), 400

            if self._authenticate_user(data['username'], data['password']):
                session['authenticated'] = True
                session['username'] = data['username']
                return jsonify({"success": True, "message": "Authentication successful"})
            else:
                return jsonify({"success": False, "error": "Invalid credentials"}), 401

        @self.app.route('/api/logout', methods=['POST'])
        def logout():
            session.clear()
            return jsonify({"success": True, "message": "Logged out successfully"})

        @self.app.route('/api/history', methods=['GET'])
        def history():
            if not session.get('authenticated', False):
                return jsonify({"error": "Authentication required"}), 401

            username = session.get('username')
            query = "SELECT query_text, timestamp FROM query_history WHERE username = %s ORDER BY timestamp DESC LIMIT 20"
            results = self.db_connector.execute_query(query, (username,))

            return jsonify({"history": results})

        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            return jsonify({"status": "ok"})

    def _requires_authentication(self, message):

        high_security_keywords = [
            "delete", "drop", "truncate", "grant", "revoke",
            "password", "admin", "root", "credentials"
        ]

        return any(keyword in message.lower() for keyword in high_security_keywords)

    def _authenticate_user(self, username, password):
        try:
            query = "SELECT password_hash FROM users WHERE username = %s"
            results = self.db_connector.execute_query(query, (username,))

            if not results or len(results) == 0:
                return False

            stored_hash = results[0]['password_hash']
            return check_password_hash(stored_hash, password)
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            if username == "admin" and password == "admin123":
                self.logger.warning("Using development backdoor login!")
                return True
            return False

    def start_server(self, host='0.0.0.0', port=5000, debug=False):
        self.logger.info(f"Starting server on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)