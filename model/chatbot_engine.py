import logging
import json
import re


class ChatbotEngine:
    def __init__(self, intent_classifier, query_processor):
        self.logger = logging.getLogger(__name__)
        self.intent_classifier = intent_classifier
        self.query_processor = query_processor
        self.current_query = None

    def process_user_input(self, user_input):
        try:
            self.current_query = user_input

            intent_result = None
            try:
                intent_result = self.intent_classifier.classify_intent(user_input)
            except Exception as e:
                self.logger.error(f"Error classifying intent: {e}")

            if not intent_result:
                self.logger.info("Using direct query mode (intent classification unavailable)")
                query_result = self.query_processor.secure_process_query(user_input)

                if query_result:
                    return self.generate_response("database_query", query_result)
                else:
                    return {"response": "I couldn't find any information for your query."}

            intent = intent_result['intent']
            confidence = intent_result['confidence']

            if intent == "database_query" and confidence > 0.7:
                query_result = self.query_processor.secure_process_query(user_input)
                return self.generate_response(intent, query_result)
            elif intent == "help":
                return {
                    "response": "I can help you query the financial database securely. Try asking questions like:\n- What markets are available?\n- Show me all traders\n- Which assets are stocks?\n- Find completed orders\n- Show recent trades"}
            elif intent == "greeting":
                return {"response": "Hello! I'm your secure financial database assistant. How can I help you today?"}
            elif intent == "goodbye":
                return {"response": "Goodbye! Feel free to come back if you have more questions."}
            else:
                return {"response": "I'm not sure I understand. Could you rephrase your question?"}
        except Exception as e:
            self.logger.error(f"Error processing user input: {e}")
            return {"response": "An error occurred while processing your request."}

    def generate_response(self, intent, query_result):
        if not query_result:
            return {"response": "I couldn't find any information for your query."}

        if isinstance(query_result, dict) and "error" in query_result:
            return {"response": f"There was an error: {query_result['error']}"}

        if isinstance(query_result, dict) and "message" in query_result:
            return {"response": query_result["message"]}

        if intent == "database_query":
            if isinstance(query_result, list):
                if len(query_result) == 0:
                    return {"response": "I found no matching records."}

                primary_table = self._determine_primary_table(query_result[0])
                self.logger.info(f"Determined primary table: {primary_table}")

                detailed_view = self._is_detailed_request()
                self.logger.info(f"Detailed view requested: {detailed_view}")

                contains_encrypted = False
                has_sensitive_fields = False

                processed_results = []
                for item in query_result:
                    processed_item = {}

                    for key, value in item.items():
                        if key.endswith('_id') and not detailed_view:
                            continue

                        if isinstance(value, str) and value.startswith("[ENCRYPTED:"):
                            contains_encrypted = True
                            has_sensitive_fields = True
                            continue

                        if self._is_sensitive_field(key) and not detailed_view:
                            has_sensitive_fields = True
                            continue

                        processed_item[key] = value

                    processed_results.append(processed_item)

                if len(query_result) == 1:
                    response_text = "Here's what I found:"
                    data_to_return = processed_results[0]
                else:
                    response_text = f"I found {len(query_result)} matching records."

                    if len(query_result) <= 10:
                        sample = query_result[0]

                        if "name" in sample:
                            try:
                                names = [r["name"] for r in query_result if "name" in r]
                                response_text += f" Names include: {', '.join(names[:5])}"
                                if len(names) > 5:
                                    response_text += " and others."
                            except:
                                pass
                        elif primary_table == "traders":
                            response_text += " These are trader records."
                        elif primary_table == "assets":
                            response_text += " These are asset records."
                        elif primary_table == "markets":
                            response_text += " These are market records."
                        elif primary_table == "trades":
                            response_text += " These are trading transactions."
                        elif primary_table == "orders":
                            response_text += " These are orders."
                        elif primary_table == "accounts":
                            response_text += " These are account records."
                        elif primary_table == "transactions":
                            response_text += " These are financial transactions."

                    data_to_return = processed_results

                if has_sensitive_fields:
                    encryption_message = "\n\nSome fields contain sensitive information that is encrypted with Homomorphic Encryption. This advanced encryption allows computations on encrypted data without decrypting it first, providing enhanced security for sensitive information like email addresses, license numbers, and other personal details."
                    response_text += encryption_message

                if self._is_requesting_sensitive_data():
                    response_text += "\n\nI'm sorry, but I cannot display the actual values of sensitive fields such as contact emails, license numbers, and phone numbers. These fields are protected with Homomorphic Encryption, a privacy-preserving technology that allows us to perform operations on encrypted data without exposing the actual values."

                return {
                    "response": response_text,
                    "data": data_to_return
                }
            else:
                affected_rows = query_result.get("affected_rows", 0) if isinstance(query_result, dict) else 0
                return {"response": f"Operation completed successfully. {affected_rows} rows affected."}

        return {"response": "Request processed successfully."}

    def _is_sensitive_field(self, field_name):
        sensitive_patterns = [
            "email", "contact_email", "phone", "license",
            "password", "secret", "token", "key", "ssn",
            "social_security", "tax_id", "credit_card", "card_number"
        ]

        return any(pattern in field_name.lower() for pattern in sensitive_patterns)

    def _determine_primary_table(self, result_item):

        if not result_item:
            return None

        if "market_id" in result_item:
            return "markets"
        elif "broker_id" in result_item:
            return "brokers"
        elif "trader_id" in result_item:
            return "traders"
        elif "asset_id" in result_item:
            return "assets"
        elif "trade_id" in result_item:
            return "trades"
        elif "account_id" in result_item:
            return "accounts"
        elif "transaction_id" in result_item:
            return "transactions"
        elif "order_id" in result_item:
            return "orders"
        elif "status_id" in result_item:
            return "order_status"
        elif "price_id" in result_item:
            return "price_history"

        for key in result_item.keys():
            if key == "location" and "operating_hours" in result_item:
                return "markets"
            elif key == "license_number":
                return "brokers"

        return None

    def _is_requesting_sensitive_data(self):

        if not hasattr(self, "current_query") or not self.current_query:
            return False

        query = self.current_query.lower()
        sensitive_indicators = [
            "email", "contact email", "license number", "phone", "encrypted", "sensitive",
            "private information", "personal details", "personal information", "contact details"
        ]

        return any(indicator in query for indicator in sensitive_indicators)

    def _is_detailed_request(self):
        if not hasattr(self, "current_query") or not self.current_query:
            return False

        query = self.current_query.lower()

        self.logger.info(f"Checking if detailed view is requested: '{query}'")

        detail_indicators = [
            "all details", "more details", "detailed", "complete", "all information",
            "everything about", "all data", "show all", "all fields", "full record",
            "details about", "details", "detail"  # Added these patterns
        ]

        for indicator in detail_indicators:
            if indicator in query:
                self.logger.info(f"Detailed view requested - found indicator: '{indicator}'")
                return True

        self.logger.info("No detailed view requested")
        return False

    def handle_error(self, error_type, error_message=None):
        error_responses = {
            "connection": "I'm having trouble connecting to the database. Please try again later.",
            "authentication": "There seems to be an authentication issue. Please check your credentials.",
            "query": "There was a problem with your query. Please try again with a different question.",
            "permission": "You don't have permission to access this information.",
            "encryption": "There was an issue with the encryption. Your data remains secure.",
            "sensitive_data": "I'm sorry, but I cannot display sensitive information such as emails, license numbers, and phone numbers as they are encrypted for security reasons."
        }

        response = error_responses.get(error_type, "An unexpected error occurred.")
        if error_message and isinstance(error_message, str):
            safe_message = re.sub(r'(password|key|credential|token)=\S+', r'\1=[REDACTED]', error_message)
            response += f" Details: {safe_message}"

        return {"response": response, "error": True}