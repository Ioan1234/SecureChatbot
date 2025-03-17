import os
import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import importlib.util

from database_connector import DatabaseConnector
from encryption_manager import HomomorphicEncryptionManager
from model.intent_classifier import EnhancedIntentClassifier
from query_processor import QueryProcessor
from model.chatbot_engine import ChatbotEngine
from api.flask_api import FlaskAPI

try:
    spec = importlib.util.spec_from_file_location("train_model", "train_model.py")
    train_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(train_module)
    TRAINING_AVAILABLE = True
except Exception as e:
    TRAINING_AVAILABLE = False
    logging.warning(f"Training module not available: {e}")

try:
    from speech.speech_recognition import SecureSpeechRecognition
    from api.speech_routes import SpeechRoutes

    SPEECH_AVAILABLE = True
except ImportError:
    SPEECH_AVAILABLE = False
    logging.warning("Speech recognition components not available. "
                    "Install required dependencies with: pip install librosa soundfile SpeechRecognition pydub")


class SecureChatbotApplication:

    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.components = {}
        self.model_last_loaded = 0

    def _setup_logging(self):
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
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            print(f"Error loading configuration: {e}")
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
                "speech": {
                    "enabled": True,
                    "use_encryption": True,
                    "model_path": "models/speech_recognition"
                },
                "logging": {
                    "level": "INFO",
                    "file": "chatbot.log"
                }
            }

    def _check_and_update_model(self):
        training_config = self.config.get("training", {})

        if not training_config.get("auto_train", False):
            return

        model_path = self.config.get("model", {}).get("path")
        training_data_path = training_config.get("data_path", "training/generated_training_data.json")

        if not os.path.exists(training_data_path):
            self.logger.warning(f"Training data not found at {training_data_path}. Skipping auto-training.")
            return

        model_file = f"{model_path}/model.h5"
        if not os.path.exists(model_file):
            self.logger.info("Model not found. Will train a new model.")
            self.run_train_model()
            return

        data_mtime = os.path.getmtime(training_data_path)
        model_mtime = os.path.getmtime(model_file)

        if data_mtime > model_mtime:
            self.logger.info("Training data is newer than model. Retraining...")
            self.run_train_model()

    def initialize_components(self):
        try:
            db_config = self.config.get("database", {})
            self.components["db_connector"] = DatabaseConnector(
                host=db_config.get("host", "localhost"),
                user=db_config.get("user", "root"),
                password=db_config.get("password", ""),
                database=db_config.get("database", "secure_chatbot")
            )

            enc_config = self.config.get("encryption", {})
            self.components["encryption_manager"] = HomomorphicEncryptionManager(
                key_size=enc_config.get("key_size", 2048),
                context_params=enc_config.get("context_parameters", {})
            )

            if TRAINING_AVAILABLE:
                self._check_and_update_model()

            model_config = self.config.get("model", {})
            model_params = model_config.get("parameters", {})
            self.components["intent_classifier"] = EnhancedIntentClassifier(
                vocab_size=model_params.get("vocab_size", 5000),
                embedding_dim=model_params.get("embedding_dim", 128),
                max_sequence_length=model_params.get("max_sequence_length", 50)
            )

            model_path = model_config.get("path")
            if model_path and os.path.exists(f"{model_path}/model.h5"):
                self.logger.info(f"Loading intent classifier model from {model_path}")
                self.components["intent_classifier"].load_model(model_path)
                self.model_last_loaded = os.path.getmtime(f"{model_path}/model.h5")
            else:
                self.logger.warning(f"Model not found at {model_path}. Using uninitialized classifier.")

            security_config = self.config.get("security", {})
            self.components["query_processor"] = QueryProcessor(
                db_connector=self.components["db_connector"],
                encryption_manager=self.components["encryption_manager"],
                sensitive_fields=security_config.get("sensitive_fields", [])
            )

            self.components["chatbot_engine"] = ChatbotEngine(
                intent_classifier=self.components["intent_classifier"],
                query_processor=self.components["query_processor"]
            )

            speech_config = self.config.get("speech", {})
            if speech_config.get("enabled", False) and SPEECH_AVAILABLE:
                try:
                    self.components["speech_recognition"] = SecureSpeechRecognition(
                        encryption_manager=self.components["encryption_manager"],
                        model_path=speech_config.get("model_path"),
                        use_encryption=speech_config.get("use_encryption", True)
                    )
                    self.logger.info("Speech recognition initialized successfully")
                except Exception as e:
                    self.logger.error(f"Error initializing speech recognition: {e}")
                    self.logger.warning("Speech recognition will be disabled")
                    self.components["speech_recognition"] = None
            else:
                self.components["speech_recognition"] = None
                if not SPEECH_AVAILABLE:
                    self.logger.warning("Speech recognition is not available. "
                                        "Required packages not installed.")
                else:
                    self.logger.info("Speech recognition is disabled in configuration")

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

    def run_train_model(self, **kwargs):
        if not TRAINING_AVAILABLE:
            self.logger.error("Training module not available. Cannot train model.")
            return False

        try:
            original_argv = sys.argv.copy()
            argv = [sys.argv[0]]

            if "config" in kwargs and kwargs["config"]:
                config_path = kwargs["config"]
                if isinstance(config_path, dict):
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                        json.dump(config_path, f)
                        config_path = f.name
                argv.extend(["--config", config_path])
            elif self.config:
                argv.extend(["--config", "config.json"])
            if kwargs.get("generate_only"):
                argv.append("--generate-only")
            if kwargs.get("enrich"):
                argv.append("--enrich")
            if kwargs.get("augment"):
                argv.append("--augment")
            if kwargs.get("output"):
                argv.extend(["--output", kwargs["output"]])
            if kwargs.get("model_output"):
                argv.extend(["--model-output", kwargs["model_output"]])
            if kwargs.get("cross_validation", 0) > 0:
                argv.extend(["--cross-validation", str(kwargs["cross_validation"])])
            if kwargs.get("early_stopping"):
                argv.append("--early-stopping")
            if kwargs.get("patience", 0) > 0:
                argv.extend(["--patience", str(kwargs["patience"])])
            if kwargs.get("debug"):
                argv.append("--debug")
            if kwargs.get("reduce_complexity"):
                argv.append("--reduce-complexity")

            sys.argv = argv

            self.logger.info(f"Running train_model.py with args: {' '.join(argv[1:])}")
            success = train_module.main()

            sys.argv = original_argv

            if success:
                self.logger.info("Training completed successfully")

                model_path = kwargs.get("model_output",
                                        self.config.get("model", {}).get("path", "models/intent_classifier"))
                model_file = f"{model_path}/model.h5"
                if os.path.exists(model_file):
                    self.model_last_loaded = os.path.getmtime(model_file)
            else:
                self.logger.error("Training failed")

            return success

        except Exception as e:
            self.logger.error(f"Error running train_model.py: {e}")
            return False

    def reload_model(self):
        try:
            model_config = self.config.get("model", {})
            model_path = model_config.get("path")

            if not os.path.exists(f"{model_path}/model.h5"):
                self.logger.error(f"Model not found at {model_path}")
                return False

            model_mtime = os.path.getmtime(f"{model_path}/model.h5")
            if model_mtime <= self.model_last_loaded:
                self.logger.info("Model has not been updated. No need to reload.")
                return True

            self.logger.info(f"Reloading model from {model_path}")

            model_params = model_config.get("parameters", {})
            self.components["intent_classifier"] = EnhancedIntentClassifier(
                vocab_size=model_params.get("vocab_size", 5000),
                embedding_dim=model_params.get("embedding_dim", 128),
                max_sequence_length=model_params.get("max_sequence_length", 50)
            )

            if model_path and os.path.exists(f"{model_path}/model.h5"):
                self.logger.info(f"Loading intent classifier model from {model_path}")
                success = self.components["intent_classifier"].load_model(model_path)
                if success:
                    self.model_last_loaded = os.path.getmtime(f"{model_path}/model.h5")
                else:
                    self.logger.error(f"Failed to load model from {model_path}")
            else:
                model_path_str = model_path if model_path else "undefined path"
                self.logger.warning(f"Model not found at {model_path_str}. Using uninitialized classifier.")

                self.components["intent_classifier"].model = None
                self.components["intent_classifier"].tokenizer = None
                self.components["intent_classifier"].intent_classes = ["database_query_list", "greeting", "help",
                                                                       "goodbye"]

            self.components["chatbot_engine"] = ChatbotEngine(
                intent_classifier=self.components["intent_classifier"],
                query_processor=self.components["query_processor"]
            )

            self.model_last_loaded = model_mtime

            self.logger.info("Model reloaded successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error reloading model: {e}")
            return False

    def start(self):
        try:
            if not self.initialize_components():
                self.logger.error("Failed to initialize components. Exiting.")
                return False

            db_connector = self.components["db_connector"]
            if not db_connector.connect():
                self.logger.error("Failed to connect to database. Exiting.")
                return False

            flask_api = self.components["flask_api"]
            speech_recognition = self.components.get("speech_recognition")

            if speech_recognition and SPEECH_AVAILABLE:
                try:
                    self.logger.info("Initializing speech recognition routes")
                    SpeechRoutes(flask_api.app, speech_recognition)
                except Exception as e:
                    self.logger.error(f"Error setting up speech recognition routes: {e}")

            if TRAINING_AVAILABLE:
                self._add_training_routes(flask_api.app)

            api_config = self.config.get("api", {})
            flask_api.start_server(
                host=api_config.get("host", "0.0.0.0"),
                port=api_config.get("port", 5000),
                debug=api_config.get("debug", False)
            )

            return True
        except Exception as e:
            self.logger.error(f"Error starting application: {e}")
            return False

    def _add_training_routes(self, app):
        if not TRAINING_AVAILABLE:
            self.logger.warning("Training components not available. Training routes will not be added.")
            return

        try:
            from flask import request, jsonify

            @app.route('/api/training/generate_data', methods=['POST'])
            def generate_training_data():
                data = request.json or {}

                args = {
                    "generate_only": True,
                    "output": data.get('output_path'),
                    "enrich": data.get('enrich_existing', False)
                }

                success = self.run_train_model(**args)
                return jsonify({"success": success})

            @app.route('/api/training/train_model', methods=['POST'])
            def train_model():
                data = request.json or {}

                args = {
                    "output": data.get('data_path'),
                    "model_output": data.get('save_path'),
                    "augment": data.get('use_augmentation', True),
                    "early_stopping": data.get('early_stopping', True),
                    "reduce_complexity": data.get('reduce_complexity', True)
                }

                success = self.run_train_model(**args)
                return jsonify({"success": success})

            @app.route('/api/training/reload_model', methods=['POST'])
            def reload_model():
                success = self.reload_model()
                return jsonify({"success": success})

            self.logger.info("Training API routes added successfully")

        except Exception as e:
            self.logger.error(f"Error adding training routes: {e}")

    def shutdown(self):
        try:
            db_connector = self.components.get("db_connector")
            if db_connector:
                db_connector.disconnect()

            self.logger.info("Application shutdown completed")
            return True
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            return False


def parse_arguments():
    parser = argparse.ArgumentParser(description="Secure Chatbot for DB Access")

    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Path to the configuration file"
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    subparsers.add_parser('server', help='Run the chatbot server')

    if TRAINING_AVAILABLE:
        subparsers.add_parser('train', help='Train the intent classifier model')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    if not hasattr(args, 'command') or args.command is None or args.command == 'server':
        app = SecureChatbotApplication(args.config)
        try:
            app.start()
        except KeyboardInterrupt:
            print("\nShutting down...")
            app.shutdown()
            print("Application terminated.")
    elif args.command == 'train' and TRAINING_AVAILABLE:
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        train_module.main()
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)