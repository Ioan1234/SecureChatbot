# chatbot/chatbot_engine.py
import logging
import json
import re


class ChatbotEngine:
    def __init__(self, intent_classifier, query_processor):
        """Initialize chatbot engine"""
        self.logger = logging.getLogger(__name__)
        self.intent_classifier = intent_classifier
        self.query_processor = query_processor

    def process_user_input(self, user_input):
        """Process user input and generate response"""
        try:
            # Try to classify user intent
            intent_result = None
            try:
                intent_result = self.intent_classifier.classify_intent(user_input)
            except Exception as e:
                self.logger.error(f"Error classifying intent: {e}")
                # Continue with fallback approach

            # Direct database query mode (fallback if intent classification fails)
            if not intent_result:
                self.logger.info("Using direct query mode (intent classification unavailable)")
                query_result = self.query_processor.secure_process_query(user_input)

                if query_result:
                    return self.generate_response("database_query", query_result)
                else:
                    return {"response": "I couldn't find any information for your query."}

            # If intent classification worked, proceed normally
            intent = intent_result['intent']
            confidence = intent_result['confidence']

            # Process based on intent
            if intent == "database_query" and confidence > 0.7:
                # Handle database query
                query_result = self.query_processor.secure_process_query(user_input)
                return self.generate_response(intent, query_result)
            elif intent == "help":
                # Handle help request
                return {
                    "response": "I can help you query the financial database securely. Try asking questions like:\n- What markets are available?\n- Show me all traders\n- Which assets are stocks?\n- Find completed orders\n- Show recent trades"}
            elif intent == "greeting":
                # Handle greeting
                return {"response": "Hello! I'm your secure financial database assistant. How can I help you today?"}
            elif intent == "goodbye":
                # Handle goodbye
                return {"response": "Goodbye! Feel free to come back if you have more questions."}
            else:
                # Handle unknown intent
                return {"response": "I'm not sure I understand. Could you rephrase your question?"}
        except Exception as e:
            self.logger.error(f"Error processing user input: {e}")
            return {"response": "An error occurred while processing your request."}

    def generate_response(self, intent, query_result):
        """Generate natural language response"""
        if not query_result:
            return {"response": "I couldn't find any information for your query."}

        if isinstance(query_result, dict) and "error" in query_result:
            return {"response": f"There was an error: {query_result['error']}"}

        if isinstance(query_result, dict) and "message" in query_result:
            return {"response": query_result["message"]}

        if intent == "database_query":
            # Check if query_result is a list (query results)
            if isinstance(query_result, list):
                if len(query_result) == 0:
                    return {"response": "I found no matching records."}

                # Format results as response
                if len(query_result) == 1:
                    response_text = "Here's what I found:"
                    return {
                        "response": response_text,
                        "data": query_result[0]
                    }
                else:
                    response_text = f"I found {len(query_result)} matching records."

                    # Add some intelligent interpretation of the data
                    if len(query_result) <= 10:
                        # Try to determine what kind of data this is
                        sample = query_result[0]

                        # Look for specific types of data
                        if "name" in sample:
                            names = [r["name"] for r in query_result if "name" in r]
                            response_text += f" Names include: {', '.join(names[:5])}"
                            if len(names) > 5:
                                response_text += " and others."
                        elif "trader_id" in sample:
                            response_text += " These are trader records."
                        elif "asset_id" in sample:
                            response_text += " These are asset records."
                        elif "market_id" in sample:
                            response_text += " These are market records."
                        elif "trade_id" in sample:
                            response_text += " These are trading transactions."
                        elif "order_id" in sample:
                            response_text += " These are orders."
                        elif "account_id" in sample:
                            response_text += " These are account records."
                        elif "transaction_id" in sample:
                            response_text += " These are financial transactions."

                        # Determine if any values are encrypted
                        has_encrypted = False
                        for row in query_result:
                            for key, value in row.items():
                                if isinstance(value, str) and value.startswith("[ENCRYPTED:"):
                                    has_encrypted = True
                                    break

                        if has_encrypted:
                            response_text += " Some sensitive data is encrypted for security."

                    return {
                        "response": response_text,
                        "data": query_result
                    }
            else:
                # For non-select queries
                affected_rows = query_result.get("affected_rows", 0) if isinstance(query_result, dict) else 0
                return {"response": f"Operation completed successfully. {affected_rows} rows affected."}

        # Default response
        return {"response": "Request processed successfully."}

    def handle_error(self, error_type, error_message=None):
        """Handle different types of errors"""
        error_responses = {
            "connection": "I'm having trouble connecting to the database. Please try again later.",
            "authentication": "There seems to be an authentication issue. Please check your credentials.",
            "query": "There was a problem with your query. Please try again with a different question.",
            "permission": "You don't have permission to access this information.",
            "encryption": "There was an issue with the encryption. Your data remains secure."
        }

        response = error_responses.get(error_type, "An unexpected error occurred.")
        if error_message and isinstance(error_message, str):
            # Sanitize error message to avoid leaking sensitive info
            safe_message = re.sub(r'(password|key|credential|token)=\S+', r'\1=[REDACTED]', error_message)
            response += f" Details: {safe_message}"

        return {"response": response, "error": True}