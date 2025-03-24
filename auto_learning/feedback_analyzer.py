import logging
import json
import os
import time
from datetime import datetime, timedelta
import numpy as np
from scipy.spatial.distance import cosine
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


class AutoLearningAnalyzer:

    def __init__(self, db_connector, learning_path="auto_learning_data"):
        self.db_connector = db_connector
        self.logger = logging.getLogger(__name__)
        self.learning_path = learning_path
        self.stop_words = set(stopwords.words('english'))

        os.makedirs(learning_path, exist_ok=True)

        self.stats = {
            "analyzed_sessions": 0,
            "analyzed_interactions": 0,
            "inferred_positive": 0,
            "inferred_negative": 0,
            "inferred_neutral": 0,
            "avg_response_time": 0,
            "reformulation_count": 0
        }

        self._load_stats()

        self.thresholds = {
            "quick_reformulation_time": 20,
            "adequate_reading_time": 20,
            "query_similarity_threshold": 0.6,
            "intent_confidence_threshold": 0.5,
            "engagement_threshold": 3
        }

    def _load_stats(self):
        stats_path = os.path.join(self.learning_path, "analyzer_stats.json")
        if os.path.exists(stats_path):
            try:
                with open(stats_path, 'r') as f:
                    self.stats = json.load(f)
                self.logger.info(f"Loaded analyzer stats: {self.stats}")
            except Exception as e:
                self.logger.error(f"Error loading analyzer stats: {e}")

    def _save_stats(self):
        stats_path = os.path.join(self.learning_path, "analyzer_stats.json")
        try:
            with open(stats_path, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving analyzer stats: {e}")

    def _get_session_interactions(self, session_id):
        try:
            query = """
            SELECT interaction_id, timestamp, user_query, intent, intent_confidence, response
            FROM chatbot_interactions
            WHERE session_id = %s
            ORDER BY timestamp ASC
            """

            interactions = self.db_connector.execute_query(query, (session_id,))
            return interactions
        except Exception as e:
            self.logger.error(f"Error getting session interactions: {e}")
            return []

    def _calculate_time_difference(self, time1, time2):
        try:
            if isinstance(time1, str):
                time1 = datetime.fromisoformat(time1.replace('Z', '+00:00'))
            if isinstance(time2, str):
                time2 = datetime.fromisoformat(time2.replace('Z', '+00:00'))

            diff = (time2 - time1).total_seconds()
            return max(0, diff)
        except Exception as e:
            self.logger.warning(f"Error calculating time difference: {e}")
            return 0

    def _calculate_query_similarity(self, query1, query2):
        try:
            def preprocess(text):
                text = text.lower()
                text = re.sub(r'[^\w\s]', '', text)
                tokens = word_tokenize(text)
                tokens = [word for word in tokens if word not in self.stop_words]
                return tokens

            tokens1 = preprocess(query1)
            tokens2 = preprocess(query2)

            set1 = set(tokens1)
            set2 = set(tokens2)

            if not set1 or not set2:
                return 0.0

            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))

            return intersection / union
        except Exception as e:
            self.logger.warning(f"Error calculating query similarity: {e}")
            return 0.0

    def _analyze_response_complexity(self, response):

        try:
            text = response
            if isinstance(response, dict):
                text = response.get("response", "")

            if isinstance(text, dict):
                text = json.dumps(text)

            if not isinstance(text, str):
                text = str(text)

            words = text.split()
            word_count = len(words)

            unique_words = set(word.lower() for word in words)
            unique_count = len(unique_words)

            avg_word_length = sum(len(word) for word in words) / max(1, word_count)

            norm_word_count = min(1.0, word_count / 100)
            norm_unique_ratio = unique_count / max(1, word_count)
            norm_avg_length = min(1.0, avg_word_length / 10)

            complexity = (0.4 * norm_word_count +
                          0.4 * norm_unique_ratio +
                          0.2 * norm_avg_length)

            return complexity
        except Exception as e:
            self.logger.warning(f"Error analyzing response complexity: {e}")
            return 0.5

    def _infer_feedback(self, response_time, query_similarity, intent_confidence,
                        response_complexity, is_last_in_session):
        feedback_confidence = intent_confidence

        positive_score = 0.5

        if response_time < self.thresholds["quick_reformulation_time"] and query_similarity > self.thresholds[
            "query_similarity_threshold"]:
            positive_score -= 0.3
            self.stats["reformulation_count"] += 1

        if response_time > self.thresholds["adequate_reading_time"] and query_similarity < 0.3:
            positive_score += 0.2

        if response_complexity > 0.7 and response_time > response_complexity * 30:
            positive_score += 0.15

        if is_last_in_session and response_time > 10:
            positive_score += 0.1

        if positive_score > 0.6:
            return {
                'feedback': 'positive',
                'confidence': min(0.95, positive_score * feedback_confidence),
                'score': positive_score
            }
        elif positive_score < 0.4:
            return {
                'feedback': 'negative',
                'confidence': min(0.95, (1 - positive_score) * feedback_confidence),
                'score': positive_score
            }
        else:
            return {
                'feedback': 'neutral',
                'confidence': min(0.8, (1 - abs(0.5 - positive_score)) * feedback_confidence),
                'score': positive_score
            }

    def _record_inferred_feedback(self, interaction_id, feedback, confidence):
        try:
            check_query = """
            SELECT feedback FROM chatbot_interactions
            WHERE interaction_id = %s AND feedback IS NOT NULL
            """
            existing = self.db_connector.execute_query(check_query, (interaction_id,))

            if existing and existing[0].get('feedback'):
                return

            update_query = """
            UPDATE chatbot_interactions
            SET feedback = %s, 
                inferred_feedback = TRUE,
                feedback_confidence = %s
            WHERE interaction_id = %s
            """

            self.db_connector.execute_query(update_query, (feedback, confidence, interaction_id))

            if feedback == 'positive':
                self.stats["inferred_positive"] += 1
            elif feedback == 'negative':
                self.stats["inferred_negative"] += 1
            else:
                self.stats["inferred_neutral"] += 1

            self.logger.debug(
                f"Recorded inferred {feedback} feedback for {interaction_id} with confidence {confidence}")
        except Exception as e:
            self.logger.error(f"Error recording inferred feedback: {e}")

    def analyze_session(self, session_id):
        try:
            interactions = self._get_session_interactions(session_id)
            if not interactions or len(interactions) < 2:
                return {"success": False, "reason": "Not enough interactions"}

            self.stats["analyzed_sessions"] += 1
            total_interactions = len(interactions)
            self.stats["analyzed_interactions"] += total_interactions

            inferred_count = 0
            response_times = []

            for i in range(len(interactions) - 1):
                current = interactions[i]
                next_interaction = interactions[i + 1]

                self.logger.debug(
                    f"Analyzing interaction pair: {current['interaction_id']} -> {next_interaction['interaction_id']}")

                response_time = self._calculate_time_difference(
                    current['timestamp'], next_interaction['timestamp']
                )

                response_times.append(response_time)

                query_similarity = self._calculate_query_similarity(
                    current['user_query'], next_interaction['user_query']
                )

                response_complexity = self._analyze_response_complexity(current.get('response', ''))

                intent_confidence = current.get('intent_confidence', 0.5)
                is_last = (i == len(interactions) - 2)

                inferred_feedback = self._infer_feedback(
                    response_time,
                    query_similarity,
                    intent_confidence,
                    response_complexity,
                    is_last
                )

                if inferred_feedback['confidence'] > self.thresholds["intent_confidence_threshold"]:
                    self._record_inferred_feedback(
                        current['interaction_id'],
                        inferred_feedback['feedback'],
                        inferred_feedback['confidence']
                    )
                    inferred_count += 1

            if response_times:
                avg_response_time = sum(response_times) / len(response_times)
                count = self.stats["analyzed_interactions"]
                current_avg = self.stats["avg_response_time"]
                self.stats["avg_response_time"] = ((count - total_interactions) * current_avg +
                                                   sum(response_times)) / count

            self._save_stats()

            return {
                "success": True,
                "session_id": session_id,
                "total_interactions": total_interactions,
                "inferred_feedback_count": inferred_count
            }

        except Exception as e:
            self.logger.error(f"Error analyzing session {session_id}: {e}")
            return {"success": False, "reason": str(e)}

    def get_learning_data(self, min_confidence=0.7, max_samples=5000):
        try:
            query = """
            SELECT user_query, intent, feedback
            FROM chatbot_interactions
            WHERE inferred_feedback = TRUE
            AND feedback_confidence >= %s
            AND feedback != 'neutral'
            ORDER BY feedback_confidence DESC
            LIMIT %s
            """

            results = self.db_connector.execute_query(query, (min_confidence, max_samples))

            texts = []
            labels = []

            for row in results:
                texts.append(row['user_query'])
                labels.append(row['intent'])

            self.logger.info(f"Extracted {len(texts)} examples for autonomous learning")
            return {"texts": texts, "labels": labels}

        except Exception as e:
            self.logger.error(f"Error getting learning data: {e}")
            return {"texts": [], "labels": []}

    def extract_common_patterns(self, min_occurrences=3):

        try:
            positive_query = """
            SELECT user_query, intent
            FROM chatbot_interactions
            WHERE (feedback = 'positive' OR
                  (inferred_feedback = TRUE AND feedback = 'positive' AND feedback_confidence >= 0.8))
            LIMIT 1000
            """

            positive_results = self.db_connector.execute_query(positive_query)

            negative_query = """
            SELECT user_query, intent
            FROM chatbot_interactions
            WHERE (feedback = 'negative' OR
                  (inferred_feedback = TRUE AND feedback = 'negative' AND feedback_confidence >= 0.8))
            LIMIT 1000
            """

            negative_results = self.db_connector.execute_query(negative_query)

            positive_patterns = self._extract_patterns([r['user_query'] for r in positive_results], min_occurrences)
            negative_patterns = self._extract_patterns([r['user_query'] for r in negative_results], min_occurrences)

            return {
                "positive_patterns": positive_patterns,
                "negative_patterns": negative_patterns
            }

        except Exception as e:
            self.logger.error(f"Error extracting patterns: {e}")
            return {"positive_patterns": [], "negative_patterns": []}

    def _extract_patterns(self, queries, min_occurrences):
        word_patterns = {}

        for query in queries:
            words = query.lower().split()

            for length in range(2, 4):
                if len(words) >= length:
                    for i in range(len(words) - length + 1):
                        pattern = " ".join(words[i:i + length])
                        word_patterns[pattern] = word_patterns.get(pattern, 0) + 1

        return [pattern for pattern, count in word_patterns.items()
                if count >= min_occurrences]

    def analyze_all_unprocessed_sessions(self, limit=100):
        try:
            query = """
            SELECT DISTINCT s.session_id, s.start_time 
            FROM chatbot_sessions s
            LEFT JOIN chatbot_session_analysis sa ON s.session_id = sa.session_id
            WHERE s.end_time IS NOT NULL 
            AND sa.session_id IS NULL
            AND s.start_time < NOW() - INTERVAL 10 MINUTE
            ORDER BY s.start_time DESC
            LIMIT %s
            """

            sessions = self.db_connector.execute_query(query, (limit,))

            if not sessions:
                return {"processed": 0, "message": "No unprocessed sessions found"}

            processed_count = 0
            results = []

            for session in sessions:
                session_id = session['session_id']
                result = self.analyze_session(session_id)
                results.append(result)

                if result['success']:
                    processed_count += 1

                    insert_query = """
                    INSERT INTO chatbot_session_analysis 
                    (session_id, analysis_time, total_interactions, inferred_feedback_count)
                    VALUES (%s, NOW(), %s, %s)
                    """

                    self.db_connector.execute_query(
                        insert_query,
                        (session_id, result['total_interactions'], result['inferred_feedback_count'])
                    )

            return {
                "processed": processed_count,
                "total": len(sessions),
                "results": results
            }

        except Exception as e:
            self.logger.error(f"Error analyzing unprocessed sessions: {e}")
            return {"processed": 0, "error": str(e)}

    def get_stats(self):
        return self.stats.copy()