import decimal
import logging
import threading
import time
import os
import json
import datetime

from auto_learning.feedback_analyzer import AutoLearningAnalyzer


class AutonomousLearningEngine:

    def __init__(self, chatbot_engine, intent_classifier, db_connector,
                 config=None, learning_path="auto_learning_data"):

        self.logger = logging.getLogger(__name__)
        self.chatbot_engine = chatbot_engine
        self.intent_classifier = intent_classifier
        self.db_connector = db_connector
        self.learning_path = learning_path

        self.analyzer = AutoLearningAnalyzer(db_connector, learning_path)

        self.config = {
            "enabled": True,
            "analysis_interval": 1800,
            "training_interval": 43200,
            "min_interactions": 20,
            "min_confidence": 0.6,
            "max_training_samples": 10000,
            "continuous_analysis": True
        }

        if config:
            self.config.update(config)

        self.stop_event = threading.Event()
        self.analysis_thread = None
        self.training_thread = None

        self.last_analysis_time = 0
        self.last_training_time = 0
        self.sessions_analyzed = 0
        self.improvements = []

        self._load_state()

        if self.config["enabled"] and self.config["continuous_analysis"]:
            self._start_background_threads()

    def _load_state(self):
        state_path = os.path.join(self.learning_path, "learning_state.json")
        if os.path.exists(state_path):
            try:
                with open(state_path, 'r') as f:
                    state = json.load(f)

                self.last_analysis_time = state.get("last_analysis_time", 0)
                self.last_training_time = state.get("last_training_time", 0)
                self.sessions_analyzed = state.get("sessions_analyzed", 0)
                self.improvements = state.get("improvements", [])

                self.logger.info(
                    f"Loaded learning state: last training {datetime.fromtimestamp(self.last_training_time)}")
            except Exception as e:
                self.logger.error(f"Error loading learning state: {e}")

    def _save_state(self):
        state_path = os.path.join(self.learning_path, "learning_state.json")
        try:
            state = {
                "last_analysis_time": self.last_analysis_time,
                "last_training_time": self.last_training_time,
                "sessions_analyzed": self.sessions_analyzed,
                "improvements": self.improvements
            }

            with open(state_path, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving learning state: {e}")

    def _start_background_threads(self):
        self.stop_event.clear()

        self.analysis_thread = threading.Thread(
            target=self._background_analysis_worker,
            daemon=True
        )

        self.training_thread = threading.Thread(
            target=self._background_training_worker,
            daemon=True
        )

        self.analysis_thread.start()
        self.training_thread.start()

        self.logger.info("Started autonomous learning background threads")

    def stop_background_threads(self):
        if self.analysis_thread or self.training_thread:
            self.logger.info("Stopping autonomous learning background threads")
            self.stop_event.set()

            if self.analysis_thread:
                self.analysis_thread.join(timeout=5)

            if self.training_thread:
                self.training_thread.join(timeout=5)

            self.logger.info("Autonomous learning threads stopped")

    def _background_analysis_worker(self):
        self.logger.info("Analysis background worker started")

        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                if current_time - self.last_analysis_time >= self.config["analysis_interval"]:
                    self.logger.info("Running scheduled session analysis")

                    result = self.analyzer.analyze_all_unprocessed_sessions(limit=50)

                    if result["processed"] > 0:
                        self.sessions_analyzed += result["processed"]
                        self.last_analysis_time = current_time

                        self._record_learning_event(
                            "analysis",
                            result["processed"],
                            notes=f"Processed {result['processed']} of {result['total']} sessions"
                        )

                        if self.sessions_analyzed % 5 == 0:
                            self._discover_patterns()

                        self._save_state()

                    self.logger.info(f"Analysis complete: {result['processed']} sessions processed")

                time.sleep(60)

            except Exception as e:
                self.logger.error(f"Error in analysis worker: {e}")
                time.sleep(300)

    def _background_training_worker(self):
        self.logger.info("Training background worker started")

        time.sleep(300)

        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                if current_time - self.last_training_time >= self.config["training_interval"]:
                    stats = self.analyzer.get_stats()

                    if stats["analyzed_interactions"] >= self.config["min_interactions"]:
                        self.logger.info("Running scheduled model retraining")

                        result = self.retrain_model()

                        if result["success"]:
                            self.last_training_time = current_time

                            if result.get("accuracy_before") and result.get("accuracy_after"):
                                improvement = {
                                    "time": current_time,
                                    "accuracy_before": result["accuracy_before"],
                                    "accuracy_after": result["accuracy_after"],
                                    "improvement": result["accuracy_after"] - result["accuracy_before"],
                                    "training_samples": result["training_samples"]
                                }

                                self.improvements.append(improvement)

                            self._save_state()

                        self.logger.info(f"Training complete: {result}")
                    else:
                        self.logger.info(
                            f"Not enough interactions for training: {stats['analyzed_interactions']}/{self.config['min_interactions']}")

                time.sleep(600)

            except Exception as e:
                self.logger.error(f"Error in training worker: {e}")
                time.sleep(900)

    def _record_learning_event(self, event_type, processed_items, success=True,
                               accuracy_before=None, accuracy_after=None, notes=None):
        try:
            query = """
            INSERT INTO chatbot_learning_events 
            (event_type, timestamp, processed_items, success, accuracy_before, accuracy_after, notes)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s)
            """

            self.db_connector.execute_query(
                query,
                (event_type, processed_items, success, accuracy_before, accuracy_after, notes)
            )
        except Exception as e:
            self.logger.error(f"Error recording learning event: {e}")

    def _discover_patterns(self):
        try:
            self.logger.info("Starting pattern discovery")

            patterns = self.analyzer.extract_common_patterns(min_occurrences=3)

            for pattern in patterns.get("positive_patterns", []):
                check_query = """
                SELECT id FROM chatbot_discovered_patterns
                WHERE pattern = %s AND is_positive = TRUE
                """

                existing = self.db_connector.execute_query(check_query, (pattern,))

                if existing:
                    update_query = """
                    UPDATE chatbot_discovered_patterns
                    SET occurrences = occurrences + 1
                    WHERE id = %s
                    """

                    self.db_connector.execute_query(update_query, (existing[0]['id'],))
                else:
                    insert_query = """
                    INSERT INTO chatbot_discovered_patterns
                    (pattern, intent, is_positive, occurrences, confidence)
                    VALUES (%s, %s, TRUE, 1, 0.8)
                    """

                    self.db_connector.execute_query(insert_query, (pattern, "unknown"))

            for pattern in patterns.get("negative_patterns", []):
                check_query = """
                SELECT id FROM chatbot_discovered_patterns
                WHERE pattern = %s AND is_positive = FALSE
                """

                existing = self.db_connector.execute_query(check_query, (pattern,))

                if existing:
                    update_query = """
                    UPDATE chatbot_discovered_patterns
                    SET occurrences = occurrences + 1
                    WHERE id = %s
                    """

                    self.db_connector.execute_query(update_query, (existing[0]['id'],))
                else:
                    insert_query = """
                    INSERT INTO chatbot_discovered_patterns
                    (pattern, intent, is_positive, occurrences, confidence)
                    VALUES (%s, %s, FALSE, 1, 0.8)
                    """

                    self.db_connector.execute_query(insert_query, (pattern, "unknown"))

            total_patterns = len(patterns.get("positive_patterns", [])) + len(patterns.get("negative_patterns", []))
            self._record_learning_event(
                "pattern_discovery",
                total_patterns,
                notes=f"Discovered {len(patterns.get('positive_patterns', []))} positive and {len(patterns.get('negative_patterns', []))} negative patterns"
            )

            self.logger.info(f"Pattern discovery complete: {total_patterns} patterns found")
            return {"success": True, "patterns": total_patterns}
        except Exception as e:
            self.logger.error(f"Error in pattern discovery: {e}")
            return {"success": False, "error": str(e)}

    def process_user_input(self, user_input, session_id=None):

        if not session_id:
            session_id = f"session_{int(time.time())}"

            try:
                insert_query = """
                INSERT INTO chatbot_sessions
                (session_id, start_time, user_agent, ip_address)
                VALUES (%s, NOW(), %s, %s)
                """

                self.db_connector.execute_query(
                    insert_query,
                    (session_id, "autonomous_learning", "127.0.0.1")
                )
            except Exception as e:
                self.logger.error(f"Error creating session: {e}")

        start_time = time.time()
        response = self.chatbot_engine.process_user_input(user_input)
        processing_time = time.time() - start_time

        intent_data = None
        if hasattr(self.chatbot_engine, 'intent_classifier'):
            intent_data = self.chatbot_engine.intent_classifier.classify_intent(user_input)

        try:
            interaction_id = f"int_{int(time.time())}_{hash(user_input) % 10000}"

            intent = intent_data.get('intent') if intent_data else None
            confidence = intent_data.get('confidence', 0.0) if intent_data else 0.0

            insert_query = """
            INSERT INTO chatbot_interactions
            (interaction_id, session_id, timestamp, user_query, intent, intent_confidence, 
             response, processing_time)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s)
            """

            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (datetime.date)):
                        return obj.isoformat()
                    elif isinstance(obj, decimal.Decimal):
                        return float(obj)
                    return super(DateTimeEncoder, self).default(obj)

            self.db_connector.execute_query(
                insert_query,
                (
                    interaction_id,
                    session_id,
                    user_input,
                    intent,
                    confidence,
                    json.dumps(response, cls=DateTimeEncoder) if isinstance(response, dict) else str(response),
                    processing_time
                )
            )

            if isinstance(response, dict):
                response["interaction_id"] = interaction_id

        except Exception as e:
            self.logger.error(f"Error recording interaction: {e}")

        return response

    def end_session(self, session_id):
        try:
            update_query = """
            UPDATE chatbot_sessions
            SET end_time = NOW()
            WHERE session_id = %s
            """

            self.db_connector.execute_query(update_query, (session_id,))

            check_query = """
            SELECT COUNT(*) as count
            FROM chatbot_sessions
            WHERE end_time IS NOT NULL
            AND session_id NOT IN (
                SELECT session_id FROM chatbot_session_analysis
            )
            """

            result = self.db_connector.execute_query(check_query)
            waiting_count = result[0]['count'] if result else 0

            if waiting_count < 10:
                return self.analyzer.analyze_session(session_id)
            else:
                return {"queued": True, "message": "Session queued for later analysis"}

        except Exception as e:
            self.logger.error(f"Error ending session {session_id}: {e}")
            return {"success": False, "error": str(e)}

    def retrain_model(self, output_path=None):
        try:
            from train_model import main as train_main
            import sys
            import tempfile

            training_data = self.analyzer.get_learning_data(
                min_confidence=self.config["min_confidence"],
                max_samples=self.config["max_training_samples"]
            )

            if not training_data["texts"]:
                self.logger.warning("No training data available from inferred feedback")
                return {"success": False, "reason": "No training data available"}

            accuracy_before = self._evaluate_current_model()

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                temp_data_path = f.name
                json.dump(training_data, f, indent=2)

            self.logger.info(f"Saved {len(training_data['texts'])} examples to {temp_data_path}")

            original_argv = sys.argv

            if not output_path:
                if hasattr(self.intent_classifier, 'model_dir'):
                    output_path = self.intent_classifier.model_dir
                else:
                    output_path = "models/intent_classifier_auto_learned"

            sys.argv = [
                "train_model.py",
                "--output", temp_data_path,
                "--model-output", output_path,
                "--early-stopping",
                "--epochs", "20",
                "--augment"
            ]

            self.logger.info(f"Starting model retraining with args: {' '.join(sys.argv[1:])}")
            success = train_main()

            sys.argv = original_argv

            if success:
                self.logger.info("Model retraining completed successfully")

                self._reload_model(output_path)

                accuracy_after = self._evaluate_current_model()

                self._record_learning_event(
                    "training",
                    len(training_data["texts"]),
                    success=True,
                    accuracy_before=accuracy_before,
                    accuracy_after=accuracy_after,
                    notes=f"Autonomous retraining with {len(training_data['texts'])} examples"
                )

                if os.path.exists(temp_data_path):
                    os.remove(temp_data_path)

                return {
                    "success": True,
                    "training_samples": len(training_data["texts"]),
                    "accuracy_before": accuracy_before,
                    "accuracy_after": accuracy_after,
                    "improvement": accuracy_after - accuracy_before
                }
            else:
                self.logger.error("Model retraining failed")
                return {"success": False, "reason": "Training process failed"}

        except Exception as e:
            self.logger.error(f"Error retraining model: {e}")
            return {"success": False, "error": str(e)}

    def _reload_model(self, model_path):
        try:
            success = self.intent_classifier.load_model(model_path)

            if success:
                self.logger.info(f"Successfully reloaded model from {model_path}")

                self.chatbot_engine.intent_classifier = self.intent_classifier
                return True
            else:
                self.logger.error(f"Failed to reload model from {model_path}")
                return False
        except Exception as e:
            self.logger.error(f"Error reloading model: {e}")
            return False

    def _evaluate_current_model(self):
        try:
            query = """
            SELECT user_query, intent
            FROM chatbot_interactions
            WHERE inferred_feedback = TRUE
            AND feedback = 'positive'
            AND feedback_confidence >= 0.9
            ORDER BY RAND()
            LIMIT 200
            """

            results = self.db_connector.execute_query(query)

            if not results or len(results) < 10:
                return 0.5

            correct = 0
            total = len(results)

            for test_item in results:
                prediction = self.intent_classifier.classify_intent(test_item['user_query'])
                if prediction and prediction.get('intent') == test_item['intent']:
                    correct += 1

            accuracy = correct / total if total > 0 else 0.5

            self.logger.info(f"Current model evaluation: {correct}/{total} correct ({accuracy:.4f})")
            return accuracy

        except Exception as e:
            self.logger.error(f"Error evaluating current model: {e}")
            return 0.5

    def get_learning_stats(self):
        try:
            analyzer_stats = self.analyzer.get_stats()

            feedback_query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN feedback = 'positive' THEN 1 ELSE 0 END) as positive,
                SUM(CASE WHEN feedback = 'negative' THEN 1 ELSE 0 END) as negative,
                SUM(CASE WHEN feedback = 'neutral' THEN 1 ELSE 0 END) as neutral,
                AVG(feedback_confidence) as avg_confidence
            FROM chatbot_interactions
            WHERE inferred_feedback = TRUE
            """

            feedback_stats = self.db_connector.execute_query(feedback_query)

            patterns_query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_positive = TRUE THEN 1 ELSE 0 END) as positive,
                SUM(CASE WHEN is_positive = FALSE THEN 1 ELSE 0 END) as negative
            FROM chatbot_discovered_patterns
            """

            pattern_stats = self.db_connector.execute_query(patterns_query)

            return {
                "analyzer": analyzer_stats,
                "feedback": feedback_stats[0] if feedback_stats else {},
                "patterns": pattern_stats[0] if pattern_stats else {},
                "improvements": self.improvements,
                "sessions_analyzed": self.sessions_analyzed,
                "last_analysis_time": datetime.fromtimestamp(
                    self.last_analysis_time).isoformat() if self.last_analysis_time else None,
                "last_training_time": datetime.fromtimestamp(
                    self.last_training_time).isoformat() if self.last_training_time else None
            }

        except Exception as e:
            self.logger.error(f"Error getting learning stats: {e}")
            return {"error": str(e)}



    def build_interaction_timeline(self, session_id):

        try:
            query = """
            SELECT 
                interaction_id,
                timestamp,
                user_query,
                intent,
                intent_confidence,
                inferred_feedback,
                feedback,
                feedback_confidence,
                processing_time
            FROM 
                chatbot_interactions
            WHERE 
                session_id = %s
            ORDER BY 
                timestamp ASC
            """

            interactions = self.db_connector.execute_query(query, (session_id,))

            if not interactions:
                return []

            timeline = []

            for i, interaction in enumerate(interactions):
                item = {
                    "interaction_id": interaction["interaction_id"],
                    "timestamp": interaction["timestamp"],
                    "query": interaction["user_query"],
                    "intent": interaction["intent"],
                    "intent_confidence": interaction["intent_confidence"],
                    "processing_time": interaction["processing_time"]
                }

                if interaction["feedback"]:
                    item["feedback"] = interaction["feedback"]
                    item["feedback_confidence"] = interaction["feedback_confidence"]
                    item["inferred"] = interaction["inferred_feedback"]

                if i < len(interactions) - 1:
                    next_interaction = interactions[i + 1]
                    response_time = (datetime.fromisoformat(str(next_interaction["timestamp"])) -
                                     datetime.fromisoformat(str(interaction["timestamp"]))).total_seconds()
                    item["response_time"] = response_time

                timeline.append(item)

            return timeline

        except Exception as e:
            self.logger.error(f"Error building interaction timeline: {e}")
            return []

    def get_active_sessions(self):

        try:
            query = """
            SELECT 
                s.session_id,
                s.start_time,
                COUNT(i.interaction_id) as interaction_count,
                MAX(i.timestamp) as last_activity
            FROM 
                chatbot_sessions s
            JOIN 
                chatbot_interactions i ON s.session_id = i.session_id
            WHERE 
                s.end_time IS NULL
            GROUP BY 
                s.session_id, s.start_time
            ORDER BY 
                last_activity DESC
            """

            active_sessions = self.db_connector.execute_query(query)

            for session in active_sessions:
                last_activity = datetime.datetime.fromisoformat(str(session["last_activity"]))
                inactive_threshold = datetime.datetime.now() - datetime.timedelta(minutes=30)

                session["inactive"] = last_activity < inactive_threshold

                if session["inactive"]:
                    self.end_session(session["session_id"])

            return active_sessions

        except Exception as e:
            self.logger.error(f"Error getting active sessions: {e}")
            return []

    def get_uncertain_interactions(self, min_interactions=100, confidence_threshold=0.6):

        try:
            count_query = """
            SELECT COUNT(*) as count FROM chatbot_interactions
            """

            count_result = self.db_connector.execute_query(count_query)
            if not count_result or count_result[0]["count"] < min_interactions:
                return []

            query = """
            SELECT 
                interaction_id,
                user_query,
                intent,
                intent_confidence,
                timestamp
            FROM 
                chatbot_interactions
            WHERE 
                intent_confidence < %s
                AND intent_confidence > 0.3
                AND feedback IS NULL
            ORDER BY 
                intent_confidence ASC
            LIMIT 50
            """

            uncertain = self.db_connector.execute_query(query, (confidence_threshold,))
            return uncertain

        except Exception as e:
            self.logger.error(f"Error getting uncertain interactions: {e}")
            return []