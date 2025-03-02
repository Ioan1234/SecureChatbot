# api/flask_api.py
from flask import Flask, request, jsonify, session, render_template
from werkzeug.security import generate_password_hash, check_password_hash
import logging
import os
import secrets


class FlaskAPI:
    def __init__(self, chatbot_engine, db_connector, secret_key=None):
        """Initialize Flask app with chatbot engine

        Args:
            chatbot_engine: The chatbot engine instance
            db_connector: Database connector instance
            secret_key: Secret key for Flask session encryption
        """
        self.logger = logging.getLogger(__name__)
        self.chatbot_engine = chatbot_engine
        self.db_connector = db_connector
        self.app = Flask(__name__,
                         template_folder='../templates',
                         static_folder='../static')

        # Use provided secret key or generate one
        self.app.secret_key = secret_key or os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(16))

        self.setup_routes()

    def setup_routes(self):
        """Set up API routes"""

        # Web Interface Route
        @self.app.route('/')
        def index():
            """Render the chatbot web interface"""
            return render_template('index.html')

        # API Routes
        @self.app.route('/api/chat', methods=['POST'])
        def chat():
            data = request.json
            if not data or 'message' not in data:
                return jsonify({"error": "No message provided"}), 400

            # Check if user is authenticated for sensitive operations
            if not session.get('authenticated', False) and self._requires_authentication(data['message']):
                return jsonify({"error": "Authentication required"}), 401

            # Process user input
            result = self.chatbot_engine.process_user_input(data['message'])
            return jsonify(result)

        @self.app.route('/api/login', methods=['POST'])
        def login():
            data = request.json
            if not data or 'username' not in data or 'password' not in data:
                return jsonify({"error": "Username and password required"}), 400

            # Authenticate user
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
            # Fetch query history from database
            query = "SELECT query_text, timestamp FROM query_history WHERE username = %s ORDER BY timestamp DESC LIMIT 20"
            results = self.db_connector.execute_query(query, (username,))

            return jsonify({"history": results})

        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Simple health check endpoint"""
            return jsonify({"status": "ok"})

    def _requires_authentication(self, message):
        """Determine if a message requires authentication"""
        # For development, let's relax the authentication requirements
        # to make testing the UI easier

        high_security_keywords = [
            "delete", "drop", "truncate", "grant", "revoke",
            "password", "admin", "root", "credentials"
        ]

        # Keep basic operations without authentication for demo purposes
        return any(keyword in message.lower() for keyword in high_security_keywords)

    def _authenticate_user(self, username, password):
        """Authenticate user against database"""
        try:
            query = "SELECT password_hash FROM users WHERE username = %s"
            results = self.db_connector.execute_query(query, (username,))

            if not results or len(results) == 0:
                return False

            stored_hash = results[0]['password_hash']
            return check_password_hash(stored_hash, password)
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            # For development, provide a backdoor login with predefined credentials
            # Remove this in production!
            if username == "admin" and password == "admin123":
                self.logger.warning("Using development backdoor login!")
                return True
            return False

    def start_server(self, host='0.0.0.0', port=5000, debug=False):
        """Start Flask server"""
        self.logger.info(f"Starting server on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)