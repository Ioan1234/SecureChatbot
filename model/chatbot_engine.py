import decimal
import logging
import re
from collections import defaultdict
import tenseal as ts
import base64
from datetime import datetime, timedelta

class ChatbotEngine:
    def __init__(self, intent_classifier, query_processor,prompt_evolver=None):
        self.logger = logging.getLogger(__name__)
        self.intent_classifier = intent_classifier
        self.query_processor = query_processor
        self.current_query = None
        self.prompt_evolver = prompt_evolver
        self.raw_query = None

        self._history_path = "prompt_refinements.json"
        self.prompt_history = self._load_history()

        self.table_display_names = {
            "traders": "Traders",
            "assets": "Assets",
            "markets": "Markets",
            "trades": "Trades",
            "orders": "Orders",
            "accounts": "Accounts",
            "transactions": "Transactions",
            "brokers": "Brokers",
            "price_history": "Price History",
            "order_status": "Order Status"
        }

        self._init_entity_question_handlers()
        self.entity_patterns = [
            (re.compile(p, re.IGNORECASE), h)
            for p, h in self.base_patterns.items()
        ]

    def _load_history(self) -> dict:
        import json, os

        if not os.path.exists(self._history_path):
            # create empty file
            with open(self._history_path, "w", encoding="utf8") as f:
                json.dump({}, f)
            return {}

        try:
            with open(self._history_path, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load prompt history: {e}")
            return {}

    def _store_refinement(self, original: str, refined: str):
        import json

        self.prompt_history[original] = refined
        try:
            with open(self._history_path, "w", encoding="utf8") as f:
                json.dump(self.prompt_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Failed to persist refinements: {e}")

    def _init_entity_question_handlers(self):

        self.base_patterns = {

            r"(?:how|who|list|show).*(?:largest|highest|top)\s+traders?.*balance": self._handle_highest_balance_account,
            r"(?:who has|which trader has).*(?:the)?\s*(?:most|highest)\s+(?:money|balance)": self._handle_highest_balance_account,
            r"(?:top)\s+\d+\s+traders?\s+by\s+(?:account\s+)?balance": self._handle_highest_balance_account,
            r"traders?\s+ranked\s+by\s+highest\s+(?:account\s+)?balance": self._handle_highest_balance_account,
            r".*(?:traders?\sranked\sby\shighest\sbalance).*": self._handle_highest_balance_query,
            r".*(?:top\s\d\straders?\sby\s(?:account\s)?balance).*": self._handle_highest_balance_query,
            r".*(?:who\shas\s(?:the\s)?most\smoney\sin\s(?:their|the)\saccount).*": self._handle_highest_balance_query,
            r".*(?:largest\saccount\sbalances?).*": self._handle_highest_balance_query,
            r".*(?:trader\swith\smost\smoney).*": self._handle_highest_balance_query,
            r"(?:what are|show|list|get|find|display).*(?:recent|latest|newest|current) trades": self._handle_recent_trades_query,
            r"(?:show|list|get|find|display).*(?:recent|latest|newest|current) transactions(?: in the market)?": self._handle_recent_transactions_query,
            r"(?:show|list|rank|compare).*(?:markets?).*(?:trade (?:volume|count)|trading activity|most activity)": self._handle_market_trade_activity,
            r"(?:show|display).*(?:current asset prices|latest price records|most recent price)": self._handle_latest_prices,
            r"(?:show|list|get|display).*(?:account types)": self._handle_account_types,
            r"(?:which|list|find).*(?:no trading activity|inactive traders|without any trades)": self._handle_traders_without_trades,
            r"(?:which|show|display).*(?:most traded asset|popular assets by trade count|trade most frequently)": self._handle_most_traded_asset,
            r"(?:show|find|display).*(?:largest trade|biggest trade|most units)": self._handle_highest_trade_quantity,
            r"(?:show|give me).*(?:price range for trades|statistics on trade prices)": self._handle_trade_price_range,
            r"(?:show|give me).*(?:statistics on account balances|summary of account balances)": self._handle_average_account_balance,
            r"(?:show|what time|list).*(?:markets open and close|time do markets operate|opening and closing times)": self._handle_market_trading_hours_summary,
            r"(?:show|who registered|list).*(?:earliest registration|registered first|oldest first)": self._handle_oldest_traders,
            r"(?:show|list|get|display).*(?:transaction types|transaction categories|kinds of transactions)": self._handle_transaction_types,
            r"(?:show|display).*(?:most active account|highest transaction count)": self._handle_account_with_most_transactions,
            r"(?:how efficiently|show|list).*(?:orders completed|order completion times|order types processed most efficiently)": self._handle_order_completion_efficiency,
            r"(?:how|show).*(?:brokers distribute their assets|asset distribution across brokers|types of assets do brokers manage)": self._handle_broker_asset_distribution,
            r"(?:what|show|list|get|display).*(?:account types)(?: available)?": self._handle_account_types,
            r"(?:how|who|list|show).*(?:traders?).*(?:largest|highest|top)\s+balances?": self._handle_highest_balance_account,


            r"(?:who|show|find|identify).*(?:most active trader|highest number of trades|trades the most frequently)":
                self._handle_trader_most_trades,


            r"(?:who|show|find|rank|list).*(?:broker).*?(?:largest portfolio|most assets managed|assets under management)":
                self._handle_broker_with_most_assets,



            r"(?:show|find|display).*(?:broker).*(?:largest portfolio)":
                self._handle_broker_with_most_assets,


            r"(?:who|show|find|identify).*(?:broker).*(?:highest number of assets)":
                self._handle_broker_with_most_assets,


            r"(?:rank|list).*(?:brokers).*(?:assets managed)":
                self._handle_broker_with_most_assets,


            r"(?:find|show).*(?:broker).*(?:assets under management)":
                self._handle_broker_with_most_assets
        }


        synonym_groups = {

            self._handle_recent_trades_query: [
                "What are the latest trades?",
                "Display the newest trading activity",
                "List the most recent trades",
                "Show the most recent trades from the last hour",
                "List the latest transactions in the market",
            ],
            self._handle_recent_transactions_query: [
                "List the most recent transactions in the market",
            ],
            lambda match: self._handle_asset_type_query("Cryptocurrency"): ["List cryptocurrency assets"],
            lambda match: self._handle_asset_type_query("Stock"): ["What stocks do we have?"],
            lambda match: self._handle_asset_type_query("Bond"): ["Display all bond assets"],
            lambda match: self._handle_asset_type_query("ETF"): ["Show all ETF assets"],
            lambda match: self._handle_asset_type_query("Commodity"): ["Display commodity assets"],
            self._handle_market_trade_activity: [
                "Show me trading activity across markets",
                "List markets by trade volume",
                "Compare trading activity between markets",
                "Rank markets by trade count",
                "Show markets sorted by total trading volume",
                "Compare trading activity among different markets",
            ],
            self._handle_latest_prices: [
                "Show current asset prices",
                "What is the most recent price for each asset?",
                "Display latest price records",
            ],
            self._handle_account_types: [
                "Show me all account types",
                "List types of accounts and their statistics",
                "What account types are available?",
                "Show all available account types",
                "List account types and their respective counts",
            ],
            self._handle_traders_without_trades: [
                "Which users have no trading activity?",
                "List inactive traders",
                "Find traders without any trades",
                "Identify traders with no trading history",
                "List traders who have never placed a trade",
            ],
            self._handle_most_traded_asset: [
                "Which asset has the highest trading volume?",
                "Show me the most popular assets by trade count",
                "What do traders trade most frequently?",
                "Which asset has the highest trade volume this month?",
                "Display the top traded asset over the last 30 days",
            ],
            self._handle_highest_trade_quantity: [
                "Show me the largest trade by quantity",
                "Which trade had the most units?",
                "Find the trade with the maximum quantity",
                "Show the largest trade by unit count",
            ],
            self._handle_trade_price_range: [
                "Show me the price range for trades",
                "What's the trading price range?",
                "Give me statistics on trade prices",
                "Show minimum and maximum trade prices",
                "Display trade price statistics",
            ],
            self._handle_average_account_balance: [
                "Show me statistics on account balances",
                "What's the typical balance across accounts?",
                "Give me a summary of account balances",
                "Show a summary of account balances across all users",
                "Display a statistical overview of account balances",
            ],
            self._handle_market_trading_hours_summary: [
                "Show me when markets open and close",
                "What time do markets operate?",
                "List the opening and closing times of each market",
                "Show market hours for all exchanges",
                "List opening and closing times of each market",
            ],
            self._handle_oldest_traders: [
                "Show me traders with earliest registration",
                "Who registered first on the platform?",
                "List traders by registration date, oldest first",
                "List traders by earliest registration date",
                "Identify which trader registered first",
            ],
            self._handle_transaction_types: [
                "Show me statistics on transaction types",
                "List all types of transactions",
                "What kinds of transactions are recorded?",
                "Show all transaction types",
                "List the different transaction categories",
            ],
            self._handle_account_with_most_transactions: [
                "Show me the most active account",
                "What account performs the most transactions?",
                "Which account has the highest transaction count?",
                "Display the account with the highest transaction count",
                "List the most active account by number of transactions",
            ],
            self._handle_order_completion_efficiency: [
                "How efficiently are orders completed?",
                "Show me order completion times by type",
                "Which order types are processed most efficiently?",
                "List order types sorted by completion speed",
                "Show which order types are processed most efficiently",
            ],
            self._handle_broker_asset_distribution: [
                "How do brokers distribute their assets?",
                "Show me asset distribution across brokers",
                "What types of assets do brokers manage?",
                "Display how brokers allocate their assets across types",
            ],

        }

        self.entity_question_handlers = {}
        for pattern, handler in self.base_patterns.items():
            self.entity_question_handlers[pattern] = handler
        for handler_or_lambda, phrases in synonym_groups.items():
            for phrase in phrases:
                regex = rf".*{re.escape(phrase)}.*"
                self.entity_question_handlers[regex] = handler_or_lambda

        self.compiled_entity_patterns = {
            re.compile(pattern, re.IGNORECASE): handler
            for pattern, handler in self.entity_question_handlers.items()
        }

    def process_user_input(self, user_input):
        self.prompt_history = self._load_history()
        original = user_input
        if original in self.prompt_history:
            refined = self.prompt_history[original]
            self.logger.debug(f"[PromptEvolver] replay refinement: {original!r} → {refined!r}")
        elif self.prompt_evolver and self.prompt_evolver.should_refine(original):
            refined = self.prompt_evolver.evolve_prompt(original)
            self.logger.debug(f"[PromptEvolver] new refinement: {original!r} → {refined!r}")
            self._store_refinement(original, refined)
        else:
            refined = original
        try:
            self.current_query = original
            self.logger.info(f"Processing input: '{original}'")

            entity_query_result = self._check_entity_question_handlers(user_input)
            if entity_query_result is not None:

                formatted = self.generate_response(None, entity_query_result)
                return self._process_response_for_json(formatted)

            intent_data = self.intent_classifier.classify_intent(user_input)
            self.logger.info(f"Classified intent: {intent_data}")

            if not intent_data or 'intent' not in intent_data:
                return {"response": "I couldn't understand your query. Could you rephrase it?"}

            intent = intent_data.get('intent')
            confidence = intent_data.get('confidence', 0.0)

            if intent == "greeting":
                return {"response": "Hello! How can I help you with your financial data today?"}
            elif intent == "goodbye":
                return {"response": "Goodbye! Have a great day."}
            elif intent == "help":
                return {
                    "response": "I can help you query financial data including traders, assets, transactions, and more. Try asking about stocks, account balances, or recent trades."}

            result = self.query_processor.process_query(user_input, intent_data)
            response = self.generate_response(
                intent=intent_data["intent"],
                query_result=result,
                sub_intent=intent_data.get("sub_intent"),
                original_user=original,
                refined_user=user_input
            )
            return self._process_response_for_json(response)

        except Exception as e:
            self.logger.error(f"Error processing user input: {e}")
            return {"response": f"An error occurred while processing your request: {str(e)}"}

    def _check_entity_question_handlers(self, user_input):
        exact_matches = [
            "what are the trading hours for markets",
            "trading hours for markets",
            "market hours",
            "when do markets open",
            "when do markets close"
        ]

        user_input_lower = user_input.lower()

        if user_input_lower in exact_matches:
            return self._handle_market_trading_hours_summary()

        for pattern, handler in self.compiled_entity_patterns.items():
            match = pattern.search(user_input)
            if match:
                if handler.__name__ == '<lambda>' and handler.__code__.co_argcount == 1:
                    return handler(match)
                else:
                    capture_groups = match.groups()
                    return handler(*capture_groups) if capture_groups else handler()

        return None

    def _process_response_for_json(self, response):
        try:
            if isinstance(response, dict):
                processed = {}

                if 'response' in response:
                    response_text = response['response']
                    if response_text and len(response_text) > 500:
                        response_text = response_text[:497] + "..."
                    processed['response'] = response_text

                if 'data' in response:
                    data = response['data']
                    if isinstance(data, list):
                        processed_data = []
                        for item in data:
                            if isinstance(item, dict):
                                item_dict = {}
                                for k, v in item.items():
                                    if k.endswith('_encrypted'):
                                        continue
                                    item_dict[k] = self._process_value_for_json(v)
                                processed_data.append(item_dict)
                            else:
                                processed_data.append(self._process_value_for_json(item))
                        processed['data'] = processed_data
                    else:
                        processed['data'] = self._process_value_for_json(data)

                for key, value in response.items():
                    if key not in ['response', 'data']:
                        if key.endswith('_encrypted'):
                            continue
                        processed[key] = self._process_value_for_json(value)

                return processed

            elif isinstance(response, list):
                return [self._process_value_for_json(item) for item in response]

            else:
                return self._process_value_for_json(response)

        except Exception as e:
            self.logger.error(f"Error processing response for JSON: {e}")
            return {"response": "Error processing the response. Please try a simpler query."}

    def _process_value_for_json(self, value):
        if value is None:
            return None
        if isinstance(value, bytes):
            return "[BINARY DATA]"
        elif isinstance(value, timedelta):
            total_seconds = value.total_seconds()
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
        elif isinstance(value, dict):
            processed_dict = {}
            for k, v in value.items():
                if k == 'api_symbol':
                    continue
                if k.endswith('_encrypted'):
                    continue
                if isinstance(v, str) and len(v) > 500:
                    processed_dict[k] = v[:497] + "..."
                else:
                    processed_dict[k] = self._process_value_for_json(v)

            return processed_dict
        elif isinstance(value, list):
            return [self._process_value_for_json(item) for item in value]
        elif hasattr(value, 'isoformat'):
            return value.isoformat()
        elif isinstance(value, decimal.Decimal):
            return str(value)

        elif hasattr(value, '__dict__'):
            return self._process_value_for_json(value.__dict__)

        elif isinstance(value, str) and len(value) > 500:
            return value[:497] + "..."

        else:
            return value

    def generate_response(self, intent, query_result, sub_intent=None, original_user=None, refined_user=None):
        if not query_result:
            return {"response": "I couldn't find any information for your query."}
        if isinstance(query_result, dict) and "error" in query_result:
            return {"response": f"There was an error: {query_result['error']}"}
        if isinstance(query_result, dict) and "response" in query_result:
            return query_result
        if isinstance(query_result, list) and len(query_result) > 1:
            rows = query_result
            sample = rows[0]
            detected = self._determine_primary_table(sample)
            intent_parts = intent.split('_') if intent else []
            table_from_intent = next(
                (p for p in intent_parts if p in ["assets", "traders", "trades", "markets", "accounts", "orders"]),
                None)
            primary_table = table_from_intent or detected or "records"
            display_name = self.table_display_names.get(primary_table, primary_table.capitalize())
            total = len(rows)
            response_text = f"Found {total} {display_name} in the database:"
            if total <= 10:
                if 'trader_name' in sample:
                    names = [r.get('trader_name') for r in rows]
                    snippet = ', '.join(names[:5]) + (' and others' if len(names) > 5 else '')
                    response_text += f" Including traders: {snippet}."
                elif 'name' in sample:
                    names = [r.get('name') for r in rows]
                    snippet = ', '.join(names[:5]) + (' and others' if len(names) > 5 else '')
                    response_text += f" Including {display_name.rstrip('s')}: {snippet}."
            if primary_table == 'traders':
                if 'balance' in sample:
                    vals = [float(r.get('balance', 0)) for r in rows]
                    avg = sum(vals) / len(vals) if vals else 0
                    response_text += f" Average balance: ${avg:.2f}."
                if 'registration_date' in sample:
                    dates = [r.get('registration_date') for r in rows]
                    earliest = min(dates)
                    latest = max(dates)
                    response_text += f" Registration from {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}."
            return {"response": response_text, "data": rows}
        if isinstance(query_result, list) and len(query_result) == 1:
            query_result = query_result[0]
        if (intent == 'database_query_comparative'
                and sub_intent == 'database_query_comparative_highest'
                and not (isinstance(query_result, list) and len(query_result) > 1)):
            if isinstance(query_result, list):
                top = query_result[0]
            else:
                top = query_result

            name = top.get('trader_name') or top.get('name', 'Unknown')
            bal = float(top.get('balance', 0))
            return {"response": f"Top trader is {name} with ${bal:.2f}.", "data": [top]}
        return {"response": "Operation completed successfully.", "data": query_result}

    def _determine_primary_table(self, result_item):
        if not result_item:
            return None

        query_lower = self.current_query.lower() if hasattr(self, "current_query") else ""

        if "trader" in query_lower and ("trader_id" in result_item or "trader_name" in result_item):
            return "traders"
        elif "market" in query_lower and ("market_id" in result_item or "market_name" in result_item):
            return "markets"
        elif "asset" in query_lower and ("asset_id" in result_item or "asset_name" in result_item):
            return "assets"
        elif "account" in query_lower and "account_id" in result_item:
            return "accounts"
        elif "broker" in query_lower and ("broker_id" in result_item or "broker_name" in result_item):
            return "brokers"
        elif "trade" in query_lower and "trade_id" in result_item:
            return "trades"
        elif "order" in query_lower and "order_id" in result_item:
            return "orders"

        if "market_id" in result_item or "market_name" in result_item:
            return "markets"
        elif "broker_id" in result_item or "broker_name" in result_item:
            return "brokers"
        elif "trader_id" in result_item or "trader_name" in result_item:
            return "traders"
        elif "asset_id" in result_item or "asset_name" in result_item:
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
        elif "price_id" in result_item or "price_date" in result_item:
            return "price_history"

        return "records"

    def _handle_traders_before_date(self, date_str):
        try:
            result = self.query_processor.get_traders_by_registration_date(date_str)
            count = len(result) if result else 0

            return {
                "response": f"Found {count} traders registered before {date_str}.",
                "data": result[:10] if result else []
            }
        except Exception as e:
            self.logger.error(f"Error handling traders before date query: {e}")
            return {
                "response": f"Error retrieving traders registered before {date_str}. Please try again."
            }

    def _handle_trader_count(self):
        result = self.query_processor.get_trader_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0
        return {
            "response": f"There are {count} traders registered in the system.",
            "count": count
        }

    def _handle_average_asset_price(self):
        sql = """
        SELECT 
            AVG(ph.close_price) as avg_price
        FROM assets a
        JOIN price_history ph ON a.asset_id = ph.asset_id
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "Could not calculate average asset price."}

        avg_price = result[0].get('avg_price', 0)

        return {
            "response": f"The average price across all assets is ${avg_price:.2f}.",
            "data": result
        }

    def _handle_traders_without_contact(self, contact_field=None):
        if not contact_field:
            contact_field = "email"
        result = self.query_processor.get_traders_without_contact(contact_field)
        count = len(result) if result else 0
        return {
            "response": f"Found {count} traders without {contact_field} information.",
            "data": result
        }

    def _handle_common_email_domains(self):
        result = self.query_processor.get_common_email_domains()
        if not result or len(result) == 0:
            return {"response": "No email domain information found."}

        domains = []
        for item in result:
            domain = item.get('domain')
            count = item.get('count', 0)
            domains.append(f"{domain} ({count} users)")

        domain_list = ", ".join(domains[:5])
        return {
            "response": f"Most common email domains used by traders: {domain_list}.",
            "data": result
        }

    def _handle_traders_same_name(self):
        result = self.query_processor.get_traders_with_same_name()
        if not result or len(result) == 0:
            return {"response": "No traders share the same name."}

        count = len(result)
        sample = result[:3]
        examples = []
        for item in sample:
            name = item.get('name')
            name_count = item.get('count', 0)
            examples.append(f"{name} ({name_count} traders)")

        examples_str = ", ".join(examples)
        return {
            "response": f"Found {count} duplicate names among traders. Examples: {examples_str}.",
            "data": result
        }

    def _handle_oldest_traders(self):
        sql = """
        SELECT 
            t.trader_id,
            t.name,
            t.email_encrypted,
            t.phone_encrypted,
            t.registration_date,
            DATEDIFF(CURRENT_DATE, t.registration_date) as days_registered,
            (SELECT COUNT(*) FROM trades tr WHERE tr.trader_id = t.trader_id) as trade_count
        FROM traders t
        ORDER BY t.registration_date ASC
        LIMIT 10
        """

        result=self.query_processor.db_connector.execute_encrypted_raw(sql)


        if not result or len(result) == 0:
            return {"response": "No trader registration data found."}

        oldest = result[0]
        oldest_date = oldest.get('registration_date')
        oldest_name = oldest.get('name')
        days_registered = oldest.get('days_registered', 0)
        years_registered = days_registered / 365 if days_registered else 0

        trader_examples = []
        for i, trader in enumerate(result[:3]):
            if i >= 3:
                break
            name = trader.get('name')
            date = trader.get('registration_date')
            trader_examples.append(f"{name} (since {date})")

        traders_list = ", ".join(trader_examples)

        return {
            "response": f"The oldest registered trader is {oldest_name}, who registered on {oldest_date} (approximately {years_registered:.1f} years ago). Other longstanding traders include: {traders_list}.",
            "data": result
        }

    def _handle_traders_without_trades(self):
        sql = """
        SELECT 
            t.trader_id,
            t.name,
            t.registration_date,
            DATEDIFF(CURRENT_DATE, t.registration_date) as days_since_registration
        FROM traders t
        LEFT JOIN trades tr ON t.trader_id = tr.trader_id
        WHERE tr.trade_id IS NULL
        ORDER BY t.registration_date
        """

        result = self.query_processor.db_connector.execute_query(sql)
        count = len(result) if result else 0

        processed_results = []
        for item in result or []:
            processed_item = {}
            for key, value in item.items():
                if key in ['email', 'phone', 'email_encrypted', 'phone_encrypted']:
                    continue
                processed_item[key] = self._process_value_for_json(value)
            processed_results.append(processed_item)

        if count > 0:
            recent_count = 0
            old_count = 0

            for trader in processed_results:
                days = trader.get('days_since_registration', 0)
                if isinstance(days, str) and 'h' in days:
                    days = 0
                if days <= 30:
                    recent_count += 1
                else:
                    old_count += 1

            response = f"Found {count} traders who haven't made any trades yet. "
            if recent_count > 0 and old_count > 0:
                response += f"{recent_count} of them registered within the last 30 days and {old_count} have been registered for more than 30 days without trading."
            elif recent_count > 0:
                response += f"All of them registered within the last 30 days."
            else:
                response += f"All {count} have been registered for more than 30 days without trading."
        else:
            response = "All traders have made at least one trade."

        return {
            "response": response,
            "data": processed_results
        }
    def _handle_traders_multiple_accounts(self):
        result = self.query_processor.get_traders_with_multiple_accounts()
        if not result or len(result) == 0:
            return {"response": "No traders have multiple accounts."}

        count = len(result)
        return {
            "response": f"Found {count} traders with multiple accounts.",
            "data": result
        }

    def _handle_average_registration_date(self):
        result = self.query_processor.get_average_registration_date()
        if not result or len(result) == 0:
            return {"response": "Could not calculate average registration date."}

        avg_date = result[0].get('avg_date')
        return {
            "response": f"The average trader registration date is {avg_date}.",
            "data": result
        }

    def _handle_trader_registration_distribution(self):
        result = self.query_processor.get_trader_registration_distribution()
        if not result or len(result) == 0:
            return {"response": "No trader registration data found for distribution analysis."}

        years = []
        for item in result:
            year = item.get('year')
            count = item.get('count', 0)
            years.append(f"{year}: {count} traders")

        year_list = ", ".join(years[:5])
        if len(years) > 5:
            year_list += ", ..."

        return {
            "response": f"Trader registration by year: {year_list}",
            "data": result
        }

    def _handle_market_count(self):
        result = self.query_processor.get_market_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0
        return {
            "response": f"There are {count} markets in the system.",
            "count": count
        }

    def _handle_market_trading_hours_summary(self):
        sql = """
        SELECT 
            name, 
            location,
            opening_time, 
            closing_time,
            TIMEDIFF(closing_time, opening_time) as trading_duration
        FROM markets
        ORDER BY location, name
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No market trading hours information found."}

        count = len(result)

        locations = {}
        for market in result:
            loc = market.get('location', 'Unknown')
            if loc not in locations:
                locations[loc] = []
            locations[loc].append(market)

        location_count = len(locations)

        earliest_market = None
        earliest_time = "23:59:59"
        latest_market = None
        latest_time = "00:00:00"

        for market in result:
            opening = str(market.get('opening_time', '09:00:00'))
            closing = str(market.get('closing_time', '17:00:00'))

            if opening < earliest_time:
                earliest_time = opening
                earliest_market = market

            if closing > latest_time:
                latest_time = closing
                latest_market = market

        standard_hours = 0
        extended_hours = 0

        for market in result:
            opening = str(market.get('opening_time', '09:00:00'))
            closing = str(market.get('closing_time', '17:00:00'))

            if opening <= "09:00:00" and closing >= "17:00:00":
                standard_hours += 1
            else:
                extended_hours += 1

        summary = f"Found trading hours for {count} markets across {location_count} locations. "

        if earliest_market:
            summary += f"The earliest market to open is {earliest_market.get('name')} in {earliest_market.get('location')} at {earliest_market.get('opening_time')}. "

        if latest_market:
            summary += f"The latest market to close is {latest_market.get('name')} in {latest_market.get('location')} at {latest_market.get('closing_time')}. "

        top_locations = sorted(locations.items(), key=lambda x: len(x[1]), reverse=True)[:3]
        location_summary = []

        for loc, markets in top_locations:
            location_summary.append(f"{loc} ({len(markets)} markets)")

        if location_summary:
            summary += f"Top market locations: {', '.join(location_summary)}."

        return {
            "response": summary,
            "data": result
        }

    def _handle_earliest_latest_markets(self):
        result = self.query_processor.get_earliest_latest_markets()
        if not result or len(result) == 0:
            return {"response": "No market hours information found."}

        earliest = result[0].get('earliest_opening')
        earliest_time = result[0].get('earliest_time')
        latest = result[0].get('latest_closing')
        latest_time = result[0].get('latest_time')

        return {
            "response": f"The market that opens earliest is {earliest} at {earliest_time}. The market that closes latest is {latest} at {latest_time}.",
            "data": result
        }

    def _handle_overlapping_markets(self):
        result = self.query_processor.get_overlapping_markets()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "No markets with overlapping trading hours found."}

        return {
            "response": f"Found {count} pairs of markets with overlapping trading hours:",
            "data": result
        }

    def _handle_markets_by_location(self, location=None):
        result = self.query_processor.get_markets_by_location()
        if not result or len(result) == 0:
            return {"response": "No market location information found."}

        locations = []
        for item in result:
            loc = item.get('location')
            count = item.get('count', 0)
            locations.append(f"{loc}: {count} markets")

        location_list = ", ".join(locations[:5])
        if len(locations) > 5:
            location_list += ", ..."

        return {
            "response": f"Markets by location: {location_list}",
            "data": result
        }

    def _handle_market_name_uniqueness(self):
        result = self.query_processor.get_duplicate_market_names()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "All markets have unique names."}

        duplicate_names = [f"{item.get('name')} ({item.get('name_count')} occurrences)" for item in result[:3]]
        duplicates_str = ", ".join(duplicate_names)

        return {
            "response": f"Found {count} market names that are not unique. Examples: {duplicates_str}.",
            "data": result
        }

    def _handle_busiest_market(self):
        sql = """
        SELECT 
            m.market_id,
            m.name,
            m.location,
            m.opening_time,
            m.closing_time,
            COUNT(t.trade_id) as trade_count,
            SUM(t.quantity * t.price) as total_value,
            COUNT(DISTINCT t.trader_id) as unique_traders,
            COUNT(DISTINCT t.asset_id) as unique_assets
        FROM markets m
        LEFT JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.market_id, m.name, m.location, m.opening_time, m.closing_time
        ORDER BY trade_count DESC
        LIMIT 1
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No market trading activity information found."}

        processed_results = []
        for item in result:
            processed_item = {}
            for key, value in item.items():
                processed_item[key] = self._process_value_for_json(value)
            processed_results.append(processed_item)

        busiest = processed_results[0]
        market_name = busiest.get('name')
        location = busiest.get('location', '')
        trade_count = busiest.get('trade_count', 0)
        total_value = busiest.get('total_value', 0)
        unique_traders = busiest.get('unique_traders', 0)

        location_str = f" in {location}" if location else ""

        return {
            "response": f"The busiest market is {market_name}{location_str} with {trade_count} trades totaling ${total_value:.2f} from {unique_traders} different traders.",
            "data": processed_results
        }

    def _handle_broker_count(self):
        result = self.query_processor.get_broker_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0
        return {
                "response": f"There are {count} brokers registered in the system.",
                "count": count
            }

    def _handle_brokers_without_license(self):
        result = self.query_processor.get_brokers_without_license()
        count = len(result) if result else 0
        return {
                "response": f"Found {count} brokers without a license number.",
                "data": result
            }

    def _handle_brokers_with_same_email(self):
        result = self.query_processor.get_brokers_with_same_email()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "No brokers share the same contact email."}

        duplicate_emails = [f"{item.get('contact_email')} ({item.get('count')} brokers)" for item in result[:3]]
        duplicates_str = ", ".join(duplicate_emails)

        return {
                "response": f"Found {count} shared contact emails among brokers. Examples: {duplicates_str}.",
                "data": result
        }

    def _handle_broker_with_most_assets(self):
        sql = """
        SELECT 
            b.broker_id,
            b.name,
            b.contact_email_encrypted,
            b.license_number_encrypted,
            COUNT(a.asset_id) as asset_count,
            GROUP_CONCAT(DISTINCT a.asset_type) as asset_types
        FROM brokers b
        LEFT JOIN assets a ON b.broker_id = a.broker_id
        GROUP BY b.broker_id, b.name
        ORDER BY asset_count DESC
        LIMIT 10
        """

        result = self.query_processor.db_connector.execute_encrypted_raw(sql)


        if not result or len(result) == 0:
            return {"response": "No broker asset management information found."}

        top_broker = result[0]
        broker_name = top_broker.get('name')
        asset_count = top_broker.get('asset_count', 0)
        asset_types = top_broker.get('asset_types', '').split(',')
        unique_types = len(set(asset_types))

        return {
            "response": f"The broker managing the most assets is {broker_name} with {asset_count} assets across {unique_types} different asset types.",
            "data": result
        }

    def _handle_average_assets_per_broker(self):
        result = self.query_processor.get_average_assets_per_broker()
        if not result or len(result) == 0:
            return {"response": "Could not calculate average assets per broker."}

        avg_assets = result[0].get('avg_assets_per_broker', 0)

        return {
                "response": f"The average number of assets managed per broker is {avg_assets:.2f}.",
                "data": result
            }

    def _handle_brokers_without_assets(self):
        result = self.query_processor.get_brokers_without_assets()
        count = len(result) if result else 0
        return {
            "response": f"Found {count} brokers that don't manage any assets.",
            "data": result
            }

    def _handle_asset_count(self):
        result = self.query_processor.get_asset_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0
        return {
            "response": f"There are {count} assets in the system.",
            "count": count
        }

    def _handle_asset_types(self):
        result = self.query_processor.get_asset_types()
        if not result or len(result) == 0:
            return {"response": "No asset type information found."}

        types = []
        for item in result:
            asset_type = item.get('asset_type')
            count = item.get('count', 0)
            types.append(f"{asset_type}: {count} assets")

        type_list = ", ".join(types[:5])
        if len(types) > 5:
            type_list += ", ..."

        return {
             "response": f"Asset types in the system: {type_list}",
             "data": result
        }

    def _handle_most_traded_asset(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            a.asset_type,
            b.name as broker_name,
            COUNT(t.trade_id) as trade_count,
            SUM(t.quantity) as total_quantity,
            SUM(t.quantity * t.price) as total_value,
            COUNT(DISTINCT t.trader_id) as unique_traders
        FROM assets a
        JOIN trades t ON a.asset_id = t.asset_id
        LEFT JOIN brokers b ON a.broker_id = b.broker_id
        GROUP BY a.asset_id, a.name, a.asset_type, b.name
        ORDER BY trade_count DESC
        LIMIT 10
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No asset trading information found."}

        top_asset = result[0]
        asset_name = top_asset.get('name')
        asset_type = top_asset.get('asset_type')
        broker_name = top_asset.get('broker_name', 'Unknown')
        trade_count = top_asset.get('trade_count', 0)
        total_value = top_asset.get('total_value', 0)
        unique_traders = top_asset.get('unique_traders', 0)

        return {
            "response": f"The most commonly traded asset is {asset_name} ({asset_type}) managed by {broker_name} with {trade_count} trades worth ${total_value:.2f} by {unique_traders} different traders.",
            "data": result
        }

    def _handle_assets_without_broker(self):
        result = self.query_processor.get_assets_without_broker()
        count = len(result) if result else 0
        return {
            "response": f"Found {count} assets not associated with any broker.",
            "data": result
        }

    def _handle_actively_traded_assets(self):
        result = self.query_processor.get_actively_traded_assets()
        count = len(result) if result else 0
        return {
            "response": f"Found {count} assets that are actively being traded.",
            "data": result
        }

    def _handle_average_trades_per_asset(self):
        result = self.query_processor.get_average_trades_per_asset()
        if not result or len(result) == 0:
            return {"response": "Could not calculate average trades per asset."}

        avg_trades = result[0].get('avg_trades_per_asset', 0)

        return {
            "response": f"The average number of trades per asset is {avg_trades:.2f}.",
            "data": result
        }

    def _handle_trade_count(self):
        result = self.query_processor.get_trade_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0
        return {
            "response": f"There are {count} trades recorded in the system.",
            "count": count
        }

    def _handle_highest_trade_quantity(self):
        sql = """
              SELECT t.trade_id, \
                     t.quantity, \
                     t.price, \
                     t.trade_date, \
                     (t.quantity * t.price) AS trade_value, \

                     a.name                 AS asset_name, \
                     a.asset_type, \

                     tr.trader_id           AS trader_id, \
                     tr.name                AS trader_name, \
                     tr.email_encrypted     AS trader_email, -- alias to trigger built-in decryption \

                     m.name                 AS market_name, \
                     m.location             AS market_location
              FROM trades t
                       JOIN assets a ON t.asset_id = a.asset_id
                       JOIN traders tr ON t.trader_id = tr.trader_id
                       JOIN markets m ON t.market_id = m.market_id
              ORDER BY t.quantity DESC LIMIT 10 \
              """

        result = self.query_processor.db_connector.execute_encrypted_raw(sql)

        if not result:
            return {"response": "No trade quantity information found."}

        top = result[0]
        dec_email = top.get("trader_email", "Unknown")
        trader_name = top["trader_name"]
        asset_name = top["asset_name"]
        quantity = top["quantity"]
        trade_value = top["trade_value"]
        trade_date = top["trade_date"]
        market_name = top["market_name"]

        response = (
            f"The highest quantity traded was {quantity} units of {asset_name} "
            f"by {trader_name} <{dec_email}> on {trade_date} at {market_name}, "
            f"worth ${trade_value:.2f} total."
        )
        return {"response": response, "data": result}

    def _handle_trade_price_range(self):
        sql = """
        SELECT
        s.min_price,
        s.max_price,
        s.avg_price,
        s.price_stddev,
      (SELECT a.name
       FROM trades t
       JOIN assets a ON t.asset_id = a.asset_id
       WHERE t.price = s.min_price
       LIMIT 1
      ) AS min_price_asset,
      (SELECT a.name
       FROM trades t
       JOIN assets a ON t.asset_id = a.asset_id
       WHERE t.price = s.max_price
       LIMIT 1
      ) AS max_price_asset,
      s.total_trades,
      s.total_value
        FROM (
        SELECT
        MIN(price)       AS min_price,
        MAX(price)       AS max_price,
        AVG(price)       AS avg_price,
        STDDEV(price)    AS price_stddev,
        COUNT(*)         AS total_trades,
        SUM(quantity*price) AS total_value
      FROM trades
    ) s;
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No trade price information found."}

        min_price = result[0].get('min_price', 0)
        max_price = result[0].get('max_price', 0)
        avg_price = result[0].get('avg_price', 0)
        min_asset = result[0].get('min_price_asset', 'Unknown')
        max_asset = result[0].get('max_price_asset', 'Unknown')
        total_trades = result[0].get('total_trades', 0)

        price_range = max_price - min_price
        range_percent = (price_range / min_price) * 100 if min_price > 0 else 0

        return {
            "response": f"Trade price analysis across {total_trades} trades: lowest ${min_price:.2f} ({min_asset}), highest ${max_price:.2f} ({max_asset}), average ${avg_price:.2f}. The price range of ${price_range:.2f} represents a {range_percent:.1f}% spread from lowest to highest.",
            "data": result
        }

    def _handle_trader_most_trades(self):
        sql = """
        SELECT 
            tr.trader_id,
            tr.name,
            tr.email_encrypted,
            COUNT(t.trade_id) as trade_count,
            SUM(t.quantity * t.price) as total_value,
            MIN(t.trade_date) as first_trade_date,
            MAX(t.trade_date) as last_trade_date,
            COUNT(DISTINCT t.asset_id) as unique_assets_traded
        FROM traders tr
        JOIN trades t ON tr.trader_id = t.trader_id
        GROUP BY tr.trader_id, tr.name
        ORDER BY trade_count DESC
        LIMIT 10
        """

        result=self.query_processor.db_connector.execute_encrypted_raw(sql)


        if not result or len(result) == 0:
            return {"response": "No trader trading information found."}

        top_trader = result[0]
        trader_name = top_trader.get('name')
        trade_count = top_trader.get('trade_count', 0)
        total_value = top_trader.get('total_value', 0)
        unique_assets = top_trader.get('unique_assets_traded', 0)

        return {
            "response": f"The trader who conducted the most trades is {trader_name} with {trade_count} trades totaling ${total_value:.2f} across {unique_assets} different assets.",
            "data": result
        }

    def _handle_average_trade_quantity(self):
        result = self.query_processor.get_average_trade_quantity()
        if not result or len(result) == 0:
            return {"response": "Could not calculate average trade quantity."}

        avg_quantity = result[0].get('avg_quantity', 0)

        return {
            "response": f"The average trade quantity across all trades is {avg_quantity:.2f} units.",
            "data": result
        }

    def _handle_trades_with_nulls(self):
        result = self.query_processor.get_trades_with_nulls()
        if not result or len(result) == 0:
            return {"response": "Could not analyze trades for missing values."}

        total = result[0].get('total_trades', 0)
        missing_asset = result[0].get('missing_asset', 0)
        missing_trader = result[0].get('missing_trader', 0)
        missing_market = result[0].get('missing_market', 0)

        issues = []
        if missing_asset > 0:
            issues.append(f"{missing_asset} trades with missing asset information")
        if missing_trader > 0:
            issues.append(f"{missing_trader} trades with missing trader information")
        if missing_market > 0:
            issues.append(f"{missing_market} trades with missing market information")

        if not issues:
            return {
                "response": f"All {total} trades have complete information with no missing values.",
                "data": result
            }

        issues_str = ", ".join(issues)
        return {
            "response": f"Out of {total} trades, found: {issues_str}.",
            "data": result
        }

    def _handle_market_trade_activity(self):
        sql = """
        SELECT 
            m.market_id,
            m.name,
            m.location,
            m.opening_time,
            m.closing_time,
            COUNT(t.trade_id) as trade_count,
            SUM(t.quantity * t.price) as total_value,
            COUNT(DISTINCT t.trader_id) as unique_traders,
            COUNT(DISTINCT t.asset_id) as unique_assets,
            MIN(t.trade_date) as earliest_trade,
            MAX(t.trade_date) as latest_trade
        FROM markets m
        LEFT JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.market_id, m.name, m.location, m.opening_time, m.closing_time
        ORDER BY trade_count DESC
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No market trading activity information found."}

        processed_results = []
        for item in result:
            processed_item = {}
            for key, value in item.items():
                processed_item[key] = self._process_value_for_json(value)
            processed_results.append(processed_item)

        markets = []
        for item in processed_results[:5]:
            market_name = item.get('name')
            trade_count = item.get('trade_count', 0)
            location = item.get('location', '')
            if location:
                markets.append(f"{market_name} in {location} ({trade_count} trades)")
            else:
                markets.append(f"{market_name} ({trade_count} trades)")

        markets_str = ", ".join(markets)

        return {
            "response": f"Market trading activity: {markets_str}.",
            "data": processed_results
        }
    def _handle_average_open_price(self):
        result = self.query_processor.get_average_open_price()
        if not result or len(result) == 0:
            return {"response": "Could not calculate average open price."}

        avg_price = result[0].get('avg_open_price', 0)

        return {
            "response": f"The average open price for all assets is ${avg_price:.2f}.",
            "data": result
        }

    def _handle_price_records_per_asset(self):
        result = self.query_processor.get_price_records_per_asset()
        if not result or len(result) == 0:
            return {"response": "No price record information found."}

        assets = []
        for item in result[:5]:
            asset_name = item.get('name')
            record_count = item.get('price_record_count', 0)
            assets.append(f"{asset_name} ({record_count} records)")

        assets_str = ", ".join(assets)

        return {
            "response": f"Price records per asset: {assets_str}.",
            "data": result
        }

    def _handle_highest_price_fluctuation(self):
        result = self.query_processor.get_highest_price_fluctuation()
        if not result or len(result) == 0:
            return {"response": "No price fluctuation information found."}

        top_fluctuation = result[0]
        asset_name = top_fluctuation.get('name')
        price_date = top_fluctuation.get('price_date')
        open_price = top_fluctuation.get('open_price', 0)
        close_price = top_fluctuation.get('close_price', 0)
        change = top_fluctuation.get('price_change', 0)
        percent_change = top_fluctuation.get('percentage_change', 0)

        return {
            "response": f"The highest price fluctuation was for {asset_name} on {price_date}: opened at ${open_price:.2f}, closed at ${close_price:.2f}, changed by ${change:.2f} ({percent_change:.2f}%).",
            "data": result
        }

    def _handle_earliest_price_record(self):
        result = self.query_processor.get_earliest_price_record()
        if not result or len(result) == 0:
            return {"response": "No price history information found."}

        earliest = result[0]
        asset_name = earliest.get('asset_name')
        price_date = earliest.get('price_date')

        return {
            "response": f"The earliest price record is for {asset_name} on {price_date}.",
            "data": result
        }

    def _handle_orphaned_price_records(self):
        result = self.query_processor.get_orphaned_price_records()
        count = len(result) if result else 0

        return {
            "response": f"Found {count} price records for assets that are not currently traded.",
            "data": result
        }

    def _handle_latest_prices(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            a.asset_type,
            b.name as broker_name,
            p.price_date,
            p.open_price,
            p.close_price,
            ((p.close_price - p.open_price) / p.open_price * 100) as daily_change_percent
        FROM assets a
        JOIN (
            SELECT asset_id, MAX(price_date) as max_date
            FROM price_history
            GROUP BY asset_id
        ) latest ON a.asset_id = latest.asset_id
        JOIN price_history p ON latest.asset_id = p.asset_id AND latest.max_date = p.price_date
        LEFT JOIN brokers b ON a.broker_id = b.broker_id
        ORDER BY daily_change_percent DESC
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No latest price information found."}

        assets = []
        gainers = []
        losers = []

        for item in result:
            asset_name = item.get('name')
            price = item.get('close_price', 0)
            change_pct = item.get('daily_change_percent', 0)

            if len(assets) < 5:
                assets.append(f"{asset_name}: ${price:.2f}")

            if change_pct > 0 and len(gainers) < 3:
                gainers.append(f"{asset_name} (+{change_pct:.2f}%)")
            elif change_pct < 0 and len(losers) < 3:
                losers.append(f"{asset_name} ({change_pct:.2f}%)")

        assets_str = ", ".join(assets)
        response = f"Latest prices: {assets_str}."

        if gainers:
            response += f" Top gainers: {', '.join(gainers)}."
        if losers:
            response += f" Top losers: {', '.join(losers)}."

        return {
            "response": response,
            "data": result
        }

    def _handle_account_count(self):
        result = self.query_processor.get_account_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0

        return {
            "response": f"There are {count} accounts in the system.",
            "count": count
        }

    def _handle_highest_balance_query(self):
        try:
            self.logger.info("Using custom highest-balance handler")

            query_lower = getattr(self, 'current_query', '').lower()
            m = re.search(r'\b(?:top|show me)\s+(\d+)\b', query_lower)
            limit = int(m.group(1)) if m else 10

            sql = """
                  SELECT t.trader_id, \
                         t.name              AS trader_name, \
                         a.account_id, \
                         a.balance_encrypted AS balance, \
                         a.account_type
                  FROM traders t
                           JOIN accounts a ON t.trader_id = a.trader_id \
                  """
            rows = self.query_processor.db_connector.execute_encrypted_raw(sql)  # decrypts numeric for you
            if not rows:
                return {"response": "No account balances found."}

            rows.sort(key=lambda r: float(r.get("balance", 0)), reverse=True)
            top = rows[:limit]

            lines = [
                f"{r['trader_name']}: ${float(r['balance']):,.2f}"
                for r in top
            ]

            return {
                "response": f"Top {limit} traders by account balance:\n" + "\n".join(lines),
                "data": top
            }

        except Exception as e:
            self.logger.error(f"Error in custom highest-balance handler: {e}")
            return {
                "response": f"Could not retrieve top balances due to an error: {e}"
            }

    def _handle_average_account_balance(self):

        rows = self.query_processor.db_connector.execute_query(
            """
            SELECT a.balance_encrypted AS balance_encrypted,
                   t.name              AS trader_name
            FROM accounts a
                     JOIN traders t ON a.trader_id = t.trader_id
            """
        )
        if not rows:
            return {"response": "Could not calculate average account balance."}


        he_ctx  = self.query_processor.encryption_manager.ckks_context
        sec_ctx = self.query_processor.encryption_manager.secret_context


        total_vec = None
        plaintext_vals = []
        trader_names = []

        for row in rows:
            ct = row["balance_encrypted"]
            if ct is None:
                continue

            vec = ts.ckks_vector_from(he_ctx, ct)
            total_vec = vec if total_vec is None else total_vec + vec

            dec_vec = ts.ckks_vector_from(sec_ctx, ct)
            plain   = dec_vec.decrypt()[0]
            plaintext_vals.append(plain)
            trader_names.append(row["trader_name"])

        count = len(plaintext_vals)
        if count == 0:
            return {"response": "No encrypted balances found."}


        avg_vec = total_vec * (1.0 / count)
        avg_ct    = avg_vec.serialize()
        avg_plain = ts.ckks_vector_from(sec_ctx, avg_ct).decrypt()[0]


        min_balance = min(plaintext_vals)
        max_balance = max(plaintext_vals)
        total_balance = sum(plaintext_vals)

        negative_count = sum(1 for v in plaintext_vals if v < 0)
        negative_pct = (negative_count / count) * 100

        idx_high = plaintext_vals.index(max_balance)
        highest_trader = trader_names[idx_high]


        response = (
            f"Account balance summary: "
            f"average ${avg_plain:.2f}, "
            f"minimum ${min_balance:.2f}, "
            f"maximum ${max_balance:.2f} held by {highest_trader}, "
            f"total ${total_balance:.2f} across {count} accounts. "
            f"{negative_count} accounts ({negative_pct:.1f}%) have negative balances."
        )

        data = [{
            "avg_balance": round(avg_plain,2),
            "min_balance": round(min_balance,2),
            "max_balance": round(max_balance,2),
            "total_balance": round(total_balance,2),
            "total_accounts": round(count,2),
            "negative_balance_count": round(negative_count,2),
            "highest_balance_trader": highest_trader
        }]

        return {
            "response": response,
            "data": data
        }

    def _handle_non_positive_balance_accounts(self):
        result = self.query_processor.get_non_positive_balance_accounts()
        if not result or len(result) == 0:
            return {"response": "Could not analyze account balances."}

        total = result[0].get('total_accounts', 0)
        negative = result[0].get('negative_balance_count', 0)
        zero = result[0].get('zero_balance_count', 0)
        non_positive = result[0].get('non_positive_balance_count', 0)

        return {
            "response": f"Out of {total} accounts, {negative} have negative balances, {zero} have zero balances, for a total of {non_positive} accounts with non-positive balances.",
            "data": result
        }

    def _handle_highest_balance_account(self):
        try:
            rows = self.query_processor.get_highest_balance_account()
            if not rows:
                return {"response": "No account balance information found."}

            lines = [
                f"{r['trader_name']}: ${float(r['balance']):,.2f} ({r['account_type']} account)"
                for r in rows
            ]

            return {
                "response": "Top 10 traders by account balance:\n" + "\n".join(lines),
                "data": rows
            }

        except Exception as e:
            self.logger.error(f"Error in custom highest‐balance handler: {e}")
            return {
                "response": f"Could not retrieve top balances due to an error: {e}"
            }

    def _handle_account_types(self):

        rows = self.query_processor.db_connector.execute_query(
            "SELECT account_type, balance_encrypted FROM accounts"
        )
        if not rows:
            return {"response": "No account data found."}


        groups = defaultdict(list)
        for row in rows:
            acct_type = row["account_type"]
            cipher = row["balance_encrypted"]
            if cipher is not None:
                groups[acct_type].append(cipher)


        he_ctx = self.query_processor.encryption_manager.ckks_context
        sec_ctx = self.query_processor.encryption_manager.secret_context

        results = []
        for acct_type, ciphers in groups.items():

            total_vec = None
            for ct in ciphers:
                vec = ts.ckks_vector_from(he_ctx, ct)
                total_vec = vec if total_vec is None else total_vec+vec

            avg_vec = total_vec * (1.0 / len(ciphers))
            avg_plain = ts.ckks_vector_from(sec_ctx, avg_vec.serialize()).decrypt()[0]

            balances = [
                ts.ckks_vector_from(sec_ctx, ct).decrypt()[0]
                for ct in ciphers
            ]

            results.append({
                "account_type": acct_type,
                "count": len(ciphers),
                "avg_balance":   round(avg_plain,   2),
                "min_balance":   round(min(balances),   2),
                "max_balance":   round(max(balances),   2),
                "total_balance": round(sum(balances),    2)
            })


        return results

    def _handle_accounts_before_date(self, date_str):
        result = self.query_processor.get_accounts_before_date(date_str)
        count = len(result) if result else 0

        return {
            "response": f"Found {count} accounts created before {date_str}.",
            "data": result
        }

    def _handle_trader_multiple_accounts(self):
        result = self.query_processor.get_traders_with_multiple_accounts()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "No traders have multiple accounts."}

        sample_traders = []
        for i, trader in enumerate(result):
            if i >= 3:
                break
            name = trader.get('name', 'Unknown')
            accounts = trader.get('account_count', 0)
            sample_traders.append(f"{name} ({accounts} accounts)")

        sample_text = ""
        if sample_traders:
            sample_text = f" Examples include: {', '.join(sample_traders)}."

        return {
            "response": f"Found {count} traders who have multiple accounts.{sample_text}",
            "data": result[:10]
        }

    def _handle_transaction_count(self):
        result = self.query_processor.get_transaction_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0

        return {
            "response": f"There are {count} transactions recorded in the system.",
            "count": count
        }

    def _handle_transaction_types(self):
        sql = """
        SELECT 
            transaction_type,
            COUNT(*) as count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(transaction_date) as earliest_date,
            MAX(transaction_date) as latest_date
        FROM transactions
        GROUP BY transaction_type
        ORDER BY count DESC
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No transaction type information found."}

        types = []
        for item in result:
            trans_type = item.get('transaction_type')
            count = item.get('count', 0)
            total = item.get('total_amount', 0)
            sign = "+" if total >= 0 else ""
            types.append(f"{trans_type}: {count} transactions (${sign}{total:.2f} total)")

        types_str = ", ".join(types)

        most_common = result[0].get('transaction_type') if result else "None"
        most_common_count = result[0].get('count', 0) if result else 0

        total_transactions = sum(item.get('count', 0) for item in result)

        return {
            "response": f"Found {len(result)} transaction types across {total_transactions} total transactions: {types_str}. The most common type is {most_common} ({most_common_count} transactions).",
            "data": result
        }

    def _handle_transaction_amount_range(self):
        result = self.query_processor.get_transaction_amount_range()
        if not result or len(result) == 0:
            return {"response": "No transaction amount information found."}

        min_amount = result[0].get('min_amount', 0)
        max_amount = result[0].get('max_amount', 0)
        avg_amount = result[0].get('avg_amount', 0)
        total_amount = result[0].get('total_amount', 0)

        return {
            "response": f"Transaction amounts: lowest ${min_amount:.2f}, highest ${max_amount:.2f}, average ${avg_amount:.2f}, total ${total_amount:.2f}.",
            "data": result
        }

    def _handle_account_with_most_transactions(self):
        sql = """
              SELECT a.account_id, \
                     a.account_type, \
                     ANY_VALUE(a.balance_encrypted)             AS balance, \
                     a.creation_date, \
                     t.trader_id, \
                     t.name                                     AS trader_name, \
                     ANY_VALUE(t.email_encrypted)               AS trader_email, \
                     COUNT(tr.transaction_id)                   AS transaction_count, \
                     SUM(tr.amount)                             AS total_amount, \
                     MIN(tr.transaction_date)                   AS earliest_transaction, \
                     MAX(tr.transaction_date)                   AS latest_transaction, \
                     COUNT(DISTINCT DATE (tr.transaction_date)) AS active_days
              FROM accounts a
                       JOIN traders t ON a.trader_id = t.trader_id
                       JOIN transactions tr ON a.account_id = tr.account_id
              GROUP BY a.account_id, \
                       a.account_type, \
                       a.creation_date, \
                       t.trader_id, \
                       t.name
              ORDER BY transaction_count DESC LIMIT 10 \
              """
        return self.query_processor.db_connector.execute_encrypted_raw(sql)

        if not result or len(result) == 0:
            return {"response": "No account transaction information found."}

        top_account = result[0]
        account_id = top_account.get('account_id')
        account_type = top_account.get('account_type')
        trader_name = top_account.get('trader_name')
        trans_count = top_account.get('transaction_count', 0)
        total_amount = top_account.get('total_amount', 0)
        active_days = top_account.get('active_days', 0)
        latest = top_account.get('latest_transaction')

        avg_per_day = trans_count / active_days if active_days > 0 else 0

        return {
            "response": f"Account #{account_id} ({account_type}) belonging to {trader_name} has the most transactions with {trans_count} transactions totaling ${total_amount:.2f} over {active_days} active trading days (avg {avg_per_day:.1f} per day). Most recent activity: {latest}.",
            "data": result
        }

    def _handle_accounts_without_transactions(self):
        result = self.query_processor.get_accounts_without_transactions()
        count = len(result) if result else 0

        return {
            "response": f"Found {count} accounts without any transactions.",
            "data": result
        }

    def _handle_transactions_on_date(self, date_str):
        result = self.query_processor.get_transactions_on_date(date_str)
        count = len(result) if result else 0

        return {
            "response": f"Found {count} transactions on {date_str}.",
            "data": result
        }

    def _handle_total_transacted_amount(self):
        result = self.query_processor.get_total_transacted_amount()
        if not result or len(result) == 0:
            return {"response": "Could not calculate total transacted amount."}

        gross_amount = result[0].get('total_gross_amount', 0)
        net_amount = result[0].get('total_net_amount', 0)

        return {
            "response": f"Total gross transaction amount: ${gross_amount:.2f}, net amount: ${net_amount:.2f}.",
            "data": result
        }

    def _handle_order_count(self):
        result = self.query_processor.get_order_count()
        count = result[0].get('count', 0) if result and len(result) > 0 else 0

        return {
            "response": f"There are {count} orders recorded in the system.",
            "count": count
        }

    def _handle_order_types(self):
        result = self.query_processor.get_order_types()
        if not result or len(result) == 0:
            return {"response": "No order type information found."}

        types = []
        for item in result:
            order_type = item.get('order_type')
            count = item.get('count', 0)
            types.append(f"{order_type} ({count} orders)")

        types_str = ", ".join(types)

        return {
            "response": f"Order types: {types_str}.",
            "data": result
        }

    def _handle_trades_with_most_orders(self):
        result = self.query_processor.get_trades_with_most_orders()
        if not result or len(result) == 0:
            return {"response": "No trade order information found."}

        top_trade = result[0]
        trade_id = top_trade.get('trade_id')
        trader_name = top_trade.get('trader_name')
        asset_name = top_trade.get('asset_name')
        order_count = top_trade.get('order_count', 0)

        return {
            "response": f"Trade #{trade_id} ({trader_name} trading {asset_name}) has the most associated orders with {order_count} orders.",
            "data": result
        }

    def _handle_orders_on_date(self, date_str):
        result = self.query_processor.get_orders_on_date(date_str)
        count = len(result) if result else 0

        return {
            "response": f"Found {count} orders placed on {date_str}.",
            "data": result
        }

    def _handle_orphaned_orders(self):
        result = self.query_processor.get_orphaned_orders()
        count = len(result) if result else 0

        return {
            "response": f"Found {count} orders without a corresponding trade.",
            "data": result
        }

    def _handle_order_statuses(self):
        result = self.query_processor.get_order_statuses()
        if not result or len(result) == 0:
            return {"response": "No order status information found."}

        statuses = []
        for item in result:
            status = item.get('status')
            count = item.get('count', 0)
            statuses.append(f"{status} ({count} orders)")

        statuses_str = ", ".join(statuses)

        return {
            "response": f"Order statuses: {statuses_str}.",
            "data": result
        }

    def _handle_orders_by_status(self, status):
        result = self.query_processor.get_orders_by_status(status)
        count = len(result) if result else 0

        return {
            "response": f"Found {count} orders with status '{status}'.",
            "data": result
        }

    def _handle_slow_completion_orders(self):
        result = self.query_processor.get_slow_completion_orders()
        if not result or len(result) == 0:
            return {"response": "No order completion time information found."}

        slowest = result[0]
        order_id = slowest.get('order_id')
        hours = slowest.get('hours_to_complete', 0)

        return {
            "response": f"Order #{order_id} took the longest to complete at {hours} hours.",
            "data": result
        }

    def _handle_avg_status_change_time(self):
        result = self.query_processor.get_avg_status_change_time()
        if not result or len(result) == 0:
            return {"response": "No order status change time information found."}

        times = []
        for item in result:
            status = item.get('status')
            minutes = item.get('avg_minutes_to_status', 0)
            times.append(f"{status}: {minutes:.1f} minutes")

        times_str = ", ".join(times)

        return {
            "response": f"Average time to status: {times_str}.",
            "data": result
        }

    def _handle_orders_with_multiple_statuses(self):
        result = self.query_processor.get_orders_with_multiple_statuses()
        count = len(result) if result else 0

        return {
            "response": f"Found {count} orders with multiple status updates.",
            "data": result
        }

    def _handle_trader_activity_analysis(self):
        result = self.query_processor.analyze_trader_activity_over_time()
        if not result or len(result) == 0:
            return {"response": "No trader activity data available for analysis."}

        months = len(result)
        latest_month = result[-1].get('month') if result else "N/A"
        latest_traders = result[-1].get('active_traders', 0) if result else 0
        latest_trades = result[-1].get('total_trades', 0) if result else 0
        latest_value = result[-1].get('total_value', 0) if result else 0

        return {
            "response": f"Analyzed trader activity over {months} months. In the most recent month ({latest_month}), there were {latest_traders} active traders with {latest_trades} trades valued at ${latest_value:.2f}.",
            "data": result
        }

    def _handle_market_correlations(self):
        result = self.query_processor.analyze_market_correlations()
        if not result or len(result) == 0:
            return {"response": "No market correlation data available for analysis."}

        pairs = len(result)
        top_pair = result[0] if result else {}
        market1 = top_pair.get('market1', 'Unknown')
        market2 = top_pair.get('market2', 'Unknown')
        common = top_pair.get('common_traders', 0)

        return {
            "response": f"Analyzed correlations between {pairs} market pairs. The strongest correlation is between {market1} and {market2} with {common} common traders.",
            "data": result
        }

    def _handle_asset_price_volatility(self):
        result = self.query_processor.analyze_asset_price_volatility()
        if not result or len(result) == 0:
            return {"response": "No asset price volatility data available for analysis."}

        assets = len(result)
        most_volatile = result[0] if result else {}
        asset_name = most_volatile.get('name', 'Unknown')
        asset_type = most_volatile.get('asset_type', 'Unknown')
        variation = most_volatile.get('coefficient_of_variation', 0)

        return {
            "response": f"Analyzed price volatility for {assets} assets. The most volatile asset is {asset_name} ({asset_type}) with a coefficient of variation of {variation:.2f}%.",
            "data": result
        }

    def _handle_trader_portfolio_diversity(self):
        result = self.query_processor.analyze_trader_portfolio_diversity()
        if not result or len(result) == 0:
            return {"response": "No trader portfolio diversity data available for analysis."}

        traders = len(result)
        most_diverse = result[0] if result else {}
        trader_name = most_diverse.get('name', 'Unknown')
        unique_assets = most_diverse.get('unique_assets', 0)
        unique_types = most_diverse.get('unique_asset_types', 0)

        return {
            "response": f"Analyzed portfolio diversity for {traders} traders. The most diverse portfolio belongs to {trader_name} with {unique_assets} unique assets across {unique_types} asset types.",
            "data": result
        }

    def _handle_transaction_patterns(self):
        result = self.query_processor.analyze_transaction_patterns()
        if not result or len(result) == 0:
            return {"response": "No transaction pattern data available for analysis."}

        patterns = len(result)
        busiest = result[0] if result else {}
        hour = busiest.get('hour_of_day', 0)
        day = busiest.get('day_of_week', 'Unknown')
        count = busiest.get('transaction_count', 0)

        return {
            "response": f"Analyzed {patterns} transaction time patterns. The busiest time is {hour}:00 on {day} with {count} transactions.",
            "data": result
        }

    def _handle_account_type_performance(self):
        result = self.query_processor.analyze_account_type_performance()
        if not result or len(result) == 0:
            return {"response": "No account type performance data available for analysis."}

        types = len(result)
        best = result[0] if result else {}
        account_type = best.get('account_type', 'Unknown')
        avg_balance = best.get('avg_balance', 0)

        return {
            "response": f"Analyzed performance for {types} account types. The best performing type is {account_type} with an average balance of ${avg_balance:.2f}.",
            "data": result
        }

    def _handle_order_completion_efficiency(self):
        sql = """
        SELECT 
            o.order_type,
            COUNT(o.order_id) as total_orders,
            AVG(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as avg_completion_time,
            MIN(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as min_completion_time,
            MAX(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as max_completion_time,
            COUNT(DISTINCT t.trader_id) as unique_traders,
            COUNT(DISTINCT t.asset_id) as unique_assets,
            COUNT(DISTINCT t.market_id) as unique_markets
        FROM orders o
        JOIN trades t ON o.trade_id = t.trade_id
        JOIN (
            SELECT order_id, status_date
            FROM order_status
            WHERE status = 'Completed'
        ) completed ON o.order_id = completed.order_id
        GROUP BY o.order_type
        ORDER BY avg_completion_time
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            return {"response": "No order completion efficiency data available for analysis."}

        types = len(result)
        fastest = result[0] if result else {}
        order_type = fastest.get('order_type', 'Unknown')
        avg_time = fastest.get('avg_completion_time', 0)


        slowest = result[-1] if result and len(result) > 1 else {}
        slowest_type = slowest.get('order_type', 'Unknown')
        slowest_time = slowest.get('avg_completion_time', 0)

        fastest_time_formatted = f"{int(avg_time // 60)}h {int(avg_time % 60)}m" if avg_time >= 60 else f"{int(avg_time)}m"
        slowest_time_formatted = f"{int(slowest_time // 60)}h {int(slowest_time % 60)}m" if slowest_time >= 60 else f"{int(slowest_time)}m"

        efficiency_data = []
        for item in result:
            order_type = item.get('order_type')
            avg_min = item.get('avg_completion_time', 0)
            time_formatted = f"{int(avg_min // 60)}h {int(avg_min % 60)}m" if avg_min >= 60 else f"{int(avg_min)}m"
            efficiency_data.append(f"{order_type}: {time_formatted}")

        efficiency_str = ", ".join(efficiency_data)

        return {
            "response": f"Order completion efficiency by type: {efficiency_str}. The most efficient is {order_type} with average completion time of {fastest_time_formatted}, while {slowest_type} is the slowest at {slowest_time_formatted}.",
            "data": result
        }

    def _handle_broker_asset_distribution(self):
        sql = """
              SELECT b.broker_id, \
                     b.name                                                           AS broker_name, \
                     COUNT(a.asset_id)                                                AS total_assets, \
                     SUM(CASE WHEN a.asset_type = 'Stock' THEN 1 ELSE 0 END)          AS stocks, \
                     SUM(CASE WHEN a.asset_type = 'ETF' THEN 1 ELSE 0 END)            AS etfs, \
                     SUM(CASE WHEN a.asset_type = 'Bond' THEN 1 ELSE 0 END)           AS bonds, \
                     SUM(CASE WHEN a.asset_type = 'Cryptocurrency' THEN 1 ELSE 0 END) AS crypto, \
                     SUM(CASE WHEN a.asset_type = 'Commodity' THEN 1 ELSE 0 END)      AS commodities, \
                     SUM(CASE WHEN a.asset_type = 'Options' THEN 1 ELSE 0 END)        AS options, \
                     SUM(CASE WHEN a.asset_type = 'Futures' THEN 1 ELSE 0 END)        AS futures
              FROM brokers b
                       LEFT JOIN assets a ON b.broker_id = a.broker_id
              GROUP BY b.broker_id, b.name
              ORDER BY total_assets DESC \
              """

        rows = self.query_processor.db_connector.execute_query(sql)
        if not rows:
            return {"response": "No broker asset distribution data available for analysis."}

        for r in rows[:10]:
            broker_id = int(r["broker_id"])
            email_sql = f"""
                SELECT contact_email_encrypted AS contact_email
                FROM brokers
                WHERE broker_id = {broker_id}
            """
            email_result = self.query_processor.db_connector.execute_encrypted_raw(email_sql)

            if email_result and "contact_email" in email_result[0]:
                r["contact_email"] = email_result[0]["contact_email"]
            else:
                r["contact_email"] = "N/A"

        for r in rows:
            asset_types = ['stocks', 'etfs', 'bonds', 'crypto', 'commodities', 'options', 'futures']
            r["diversity_score"] = sum(1 for t in asset_types if r.get(t, 0) > 0)

        brokers = len(rows)
        top = rows[0]
        broker_name = top.get("broker_name", "Unknown")
        total_assets = top.get("total_assets", 0)
        stocks = top.get("stocks", 0)
        etfs = top.get("etfs", 0)
        crypto = top.get("crypto", 0)

        most_diverse_broker = max(rows, key=lambda r: r.get("diversity_score", 0))
        diverse_name = most_diverse_broker.get("broker_name", "Unknown")
        diverse_score = most_diverse_broker.get("diversity_score", 0)

        asset_distribution = f"{stocks} stocks, {etfs} ETFs, {crypto} cryptocurrencies"
        if top.get('bonds', 0): asset_distribution += f", {top['bonds']} bonds"
        if top.get('commodities', 0): asset_distribution += f", {top['commodities']} commodities"

        return {
            "response": f"Analyzed asset distribution for {brokers} brokers. {broker_name} manages the most assets "
                        f"({total_assets} total: {asset_distribution}). {diverse_name} has the most diverse portfolio "
                        f"with {diverse_score} different asset types.",
            "data": rows
        }

    def _handle_anomalous_trading(self):
        result = self.query_processor.identify_anomalous_trading_activity()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "No anomalous trading activity identified."}

        top_anomaly = result[0] if result else {}
        trader_name = top_anomaly.get('name', 'Unknown')
        asset_name = top_anomaly.get('asset_name', 'Unknown')
        trade_value = top_anomaly.get('trade_value', 0)

        return {
            "response": f"Identified {count} potentially anomalous trades. The most significant is {trader_name} trading {asset_name} with a value of ${trade_value:.2f}.",
            "data": result
        }

    def _handle_recent_transactions_query(self):
        try:
            sql = """
                  SELECT tr.transaction_id, \
                         tr.transaction_date, \
                         tr.transaction_type, \
                         tr.amount, \
                         a.account_type, \
                         t.name AS trader_name
                  FROM transactions tr
                           JOIN accounts a ON tr.account_id = a.account_id
                           JOIN traders t ON a.trader_id = t.trader_id
                  ORDER BY tr.transaction_date DESC LIMIT 20 \
                  """
            rows = self.query_processor.db_connector.execute_query(sql)

            if not rows:
                return {"response": "No transaction information found in the database."}

            count = len(rows)
            latest_date = rows[0].get('transaction_date')
            examples = []
            for tx in rows[:3]:
                ttype = tx.get('transaction_type')
                amt = tx.get('amount', 0)
                acct = tx.get('account_type')
                trader = tx.get('trader_name')
                date = tx.get('transaction_date')
                examples.append(
                    f"{trader} had a {ttype} of ${amt:.2f} in {acct} on {date}"
                )

            examples_str = ", ".join(examples)
            return {
                "response": (
                    f"Found {count} recent transactions. "
                    f"Most recent transaction was on {latest_date}. "
                    f"Examples include: {examples_str}."
                ),
                "data": rows
            }

        except Exception as e:
            self.logger.error(f"Error in recent transactions query: {e}")
            return {"response": "Could not retrieve recent transactions due to an error."}

    def _handle_recent_trades_query(self):
        try:
            sql = """
            SELECT 
                t.trade_id,
                t.trade_date,
                t.quantity,
                t.price,
                (t.quantity * t.price) as trade_value,
                tr.name as trader_name,
                a.name as asset_name,
                a.asset_type,
                m.name as market_name,
                m.location as market_location
            FROM trades t
            JOIN traders tr ON t.trader_id = tr.trader_id
            JOIN assets a ON t.asset_id = a.asset_id
            JOIN markets m ON t.market_id = m.market_id
            ORDER BY t.trade_date DESC
            LIMIT 20
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                count = len(result)
                latest_date = result[0].get('trade_date')

                recent_trades = []
                for trade in result[:3]:
                    trader = trade.get('trader_name')
                    asset = trade.get('asset_name')
                    price = trade.get('price', 0)
                    quantity = trade.get('quantity', 0)
                    recent_trades.append(f"{trader} traded {quantity} {asset} at ${price:.2f}")

                trade_examples = ", ".join(recent_trades)

                return {
                    "response": f"Found {count} recent trades. Most recent trade was on {latest_date}. Examples include: {trade_examples}.",
                    "data": result
                }
            else:
                return {
                    "response": "No trade information found in the database."
                }
        except Exception as e:
            self.logger.error(f"Error in recent trades query: {e}")
            return {
                "response": "Could not retrieve recent trades due to an error."
            }

    def _handle_asset_type_query(self, asset_type=None):
        try:
            if not asset_type and hasattr(self, 'current_query'):
                q = self.current_query.lower()
                mappings = {
                    'etf': 'ETF', 'etfs': 'ETF',
                    'stock': 'Stock', 'stocks': 'Stock', 'equity': 'Stock', 'equities': 'Stock',
                    'share': 'Stock', 'shares': 'Stock',
                    'bond': 'Bond', 'bonds': 'Bond',
                    'crypto': 'Cryptocurrency', 'cryptocurrency': 'Cryptocurrency',
                    'cryptocurrencies': 'Cryptocurrency',
                    'bitcoin': 'Cryptocurrency', 'ethereum': 'Cryptocurrency',
                    'commodity': 'Commodity', 'commodities': 'Commodity',
                    'future': 'Futures', 'futures': 'Futures',
                    'option': 'Options', 'options': 'Options',
                    'forex': 'Forex', 'currency': 'Forex',
                    'reit': 'REIT'
                }
                for term, atype in mappings.items():
                    if term in q:
                        asset_type = atype
                        break

            if not asset_type:
                return {"response": "Please specify an asset type (e.g. ETF, Stock, Bond, Cryptocurrency)."}

            self.logger.info(f"Querying for asset type: {asset_type}")

            sql = f"""
                SELECT
                    a.asset_id,
                    a.name,
                    a.asset_type,
                    b.name AS broker_name,
                    b.contact_email_encrypted AS contact_email
                FROM assets a
                LEFT JOIN brokers b ON a.broker_id = b.broker_id
                WHERE a.asset_type = '{asset_type}'
                ORDER BY a.name
                LIMIT 20
            """
            rows = self.query_processor.db_connector.execute_encrypted_raw(sql)

            for r in rows:
                r['broker_contact'] = r.pop('contact_email', None)

            if rows:
                count = len(rows)
                examples = ", ".join(r['name'] for r in rows[:5])
                return {
                    'response': f"Found {count} {asset_type} assets. Examples include: {examples}.",
                    'data': rows
                }

            fallback_sql = f"""
                SELECT
                    a.asset_id,
                    a.name,
                    a.asset_type,
                    b.name AS broker_name,
                    b.contact_email_encrypted AS contact_email
                FROM assets a
                LEFT JOIN brokers b ON a.broker_id = b.broker_id
                WHERE a.asset_type LIKE '%{asset_type}%'
                ORDER BY a.name
                LIMIT 20
            """
            fallback = self.query_processor.db_connector.execute_encrypted_raw(fallback_sql)
            for r in fallback:
                r['broker_contact'] = r.pop('contact_email', None)

            if fallback:
                count = len(fallback)
                return {
                    'response': f"Found {count} assets similar to {asset_type}.",
                    'data': fallback
                }

            return {'response': f"No {asset_type} assets found."}

        except Exception as e:
            self.logger.error(f"Error in asset type query: {e}")
            return {'response': f"Could not retrieve {asset_type} assets: {e}"}

    def _handle_market_peak_hours(self):
        result = self.query_processor.analyze_market_peak_hours()
        if not result or len(result) == 0:
            return {"response": "No market peak hours data available for analysis."}

        market_hours = {}
        for item in result:
            market = item.get('market_name')
            hour = item.get('hour_of_day')
            count = item.get('trade_count', 0)

            if market not in market_hours or count > market_hours[market]['count']:
                market_hours[market] = {'hour': hour, 'count': count}

        peaks = []
        for market, data in list(market_hours.items())[:3]:
            peaks.append(f"{market} at {data['hour']}:00 ({data['count']} trades)")

        peaks_str = ", ".join(peaks)

        return {
            "response": f"Peak trading hours: {peaks_str}.",
            "data": result
        }

    def _handle_market_inefficiencies(self):
        result = self.query_processor.identify_market_inefficiencies()
        count = len(result) if result else 0

        if count == 0:
            return {"response": "No significant market inefficiencies identified."}

        top_inefficiency = result[0] if result else {}
        asset_name = top_inefficiency.get('name', 'Unknown')
        market1 = top_inefficiency.get('market1', 'Unknown')
        market2 = top_inefficiency.get('market2', 'Unknown')
        diff_percent = top_inefficiency.get('percentage_difference', 0)

        return {
            "response": f"Identified {count} potential market inefficiencies. The largest is for {asset_name} with a {diff_percent:.2f}% price difference between {market1} and {market2}.",
            "data": result
        }

    def _handle_data_quality_issues(self):
        result = self.query_processor.analyze_data_quality_issues()
        if not result or len(result) == 0:
            return {"response": "No data quality issues analysis available."}

        issues = []
        for item in result:
            table = item.get('table_name')
            total = item.get('total_records', 0)
            missing = sum([item.get(f, 0) for f in item.keys() if f.startswith('missing')])

            if missing > 0:
                issues.append(f"{table}: {missing} issues out of {total} records ({(missing/total*100):.1f}%)")

        if not issues:
            return {"response": "No significant data quality issues found across the database tables."}

        issues_str = ", ".join(issues)

        return {
            "response": f"Data quality issues found: {issues_str}.",
            "data": result
        }

    def _is_sensitive_field(self, field_name):
        sensitive_fields = ['email', 'phone', 'license_number', 'contact_email', 'balance']

        if field_name in sensitive_fields:
            return True

        if field_name.endswith('_encrypted'):
            return True

        if '.' in field_name:
            table, field = field_name.split('.')
            if field in sensitive_fields or field.endswith('_encrypted'):
                return True

        return False