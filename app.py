#!/usr/bin/env python3
"""
Secure Chatbot for DB Access with Homomorphic Encryption
Master's Degree Project

This is the main entry point for the secure chatbot application.
"""

import os
import argparse
import json
import logging
from pathlib import Path

# Import application components
from database_connector import DatabaseConnector
from encryption_manager import HomomorphicEncryptionManager
from model.intent_classifier import IntentClassifier
from training.trainer import DistributedTrainer
from query_processor import QueryProcessor
from model.chatbot_engine import ChatbotEngine
from api.flask_api import FlaskAPI


class SecureChatbotApplication:
    """Main application class for the secure chatbot"""

    def __init__(self, config_path):
        """Initialize secure chatbot application"""
        self.logger = self._setup_logging()
        self.config = self._load_config(config_path)
        self.components = {}

    def _setup_logging(self):
        """Set up logging configuration"""
        log_config = self.config.get("logging", {}) if hasattr(self, "config") else {}
        log_level = getattr(logging, log_config.get("level", "INFO"))
        log_file = log_config.get("file", "chatbot.log")

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)

    def _load_config(self, config_path):
        """Load configuration from file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            print(f"Error loading configuration: {e}")
            # Use default configuration
            return {
                "database": {
                    "host": "localhost",
                    "user": "root",
                    "password": "",
                    "database": "secure_chatbot"
                },
                "encryption": {
                    "key_size": 2048
                },
                "model": {
                    "path": "models/intent_classifier"
                },
                "training": {
                    "strategy": "mirrored"
                },
                "api": {
                    "host": "0.0.0.0",
                    "port": 5000,
                    "debug": False
                },
                "logging": {
                    "level": "INFO",
                    "file": "chatbot.log"
                }
            }

    def initialize_components(self):
        """Initialize all components with proper dependencies"""
        try:
            # Initialize database connector
            db_config = self.config.get("database", {})
            self.components["db_connector"] = DatabaseConnector(
                host=db_config.get("host", "localhost"),
                user=db_config.get("user", "root"),
                password=db_config.get("password", ""),
                database=db_config.get("database", "secure_chatbot")
            )

            # Initialize encryption manager
            enc_config = self.config.get("encryption", {})
            self.components["encryption_manager"] = HomomorphicEncryptionManager(
                key_size=enc_config.get("key_size", 2048),
                context_params=enc_config.get("context_parameters", {})
            )

            # Initialize intent classifier
            model_config = self.config.get("model", {})
            model_params = model_config.get("parameters", {})
            self.components["intent_classifier"] = IntentClassifier(
                vocab_size=model_params.get("vocab_size", 5000),
                embedding_dim=model_params.get("embedding_dim", 128),
                max_sequence_length=model_params.get("max_sequence_length", 50),
                model_path=model_config.get("path")
            )

            # Initialize distribution strategy for single machine
            self.components["distributed_trainer"] = DistributedTrainer()

            # Initialize query processor
            security_config = self.config.get("security", {})
            self.components["query_processor"] = QueryProcessor(
                db_connector=self.components["db_connector"],
                encryption_manager=self.components["encryption_manager"],
                sensitive_fields=security_config.get("sensitive_fields", [])
            )

            # Initialize chatbot engine
            self.components["chatbot_engine"] = ChatbotEngine(
                intent_classifier=self.components["intent_classifier"],
                query_processor=self.components["query_processor"]
            )

            # Initialize Flask API
            api_config = self.config.get("api", {})
            self.components["flask_api"] = FlaskAPI(
                chatbot_engine=self.components["chatbot_engine"],
                db_connector=self.components["db_connector"],
                secret_key=api_config.get("secret_key")
            )

            self.logger.info("All components initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing components: {e}")
            return False

    def start(self):
        """Start the application"""
        try:
            if not self.initialize_components():
                self.logger.error("Failed to initialize components. Exiting.")
                return False

            # Connect to database
            db_connector = self.components["db_connector"]
            if not db_connector.connect():
                self.logger.error("Failed to connect to database. Exiting.")
                return False

            # Start Flask API server
            api_config = self.config.get("api", {})
            flask_api = self.components["flask_api"]
            flask_api.start_server(
                host=api_config.get("host", "0.0.0.0"),
                port=api_config.get("port", 5000),
                debug=api_config.get("debug", False)
            )

            return True
        except Exception as e:
            self.logger.error(f"Error starting application: {e}")
            return False

    def shutdown(self):
        """Graceful shutdown"""
        try:
            # Close database connection
            db_connector = self.components.get("db_connector")
            if db_connector:
                db_connector.disconnect()

            self.logger.info("Application shutdown completed")
            return True
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            return False


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Secure Chatbot for DB Access")
    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Path to the configuration file"
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Create and start application
    app = SecureChatbotApplication(args.config)
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        app.shutdown()
        print("Application terminated.")