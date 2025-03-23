import logging
import re
from datetime import datetime, timedelta


class ChatbotEngine:
    def __init__(self, intent_classifier, query_processor):
        self.logger = logging.getLogger(__name__)
        self.intent_classifier = intent_classifier
        self.query_processor = query_processor
        self.current_query = None

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

    def process_user_input(self, user_input):
        try:
            self.current_query = user_input
            self.logger.info(f"Processing user input: {user_input}")

            specialized_result = self._check_specialized_queries(user_input)
            if specialized_result:
                return specialized_result

            entity_patterns = [
                r'(?:tell me|info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)',
                r'(?:who|what)(?:\s+\w+)?\s+(?:is|are)\s+([\w\s]+)',
                r'(?:show|display|get)(?:\s+\w+)?\s+(?:info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)',
                r'(?:lookup|find|search for)\s+([\w\s]+)'
            ]

            for pattern in entity_patterns:
                if re.search(pattern, user_input.lower()):
                    entity_result = self._handle_entity_query(user_input)
                    if entity_result:
                        return entity_result

            self.logger.info("No specialized handler, using intent classifier")
            intent_data = self.intent_classifier.classify_intent(user_input)
            self.logger.info(f"Classified intent: {intent_data}")

            if not intent_data or 'intent' not in intent_data:
                return {"response": "I couldn't understand your query. Could you rephrase it?"}

            intent = intent_data.get('intent')
            confidence = intent_data.get('confidence', 0.0)

            if "trader" in user_input.lower() and "balance" in user_input.lower():
                return self._handle_highest_balance_query()

            if intent == "database_query_list" or intent == "database_query_detailed":
                result = self._execute_list_query(user_input)
                return self.generate_response(intent, result, intent_data.get('sub_intent'))

            elif intent == "database_query_count":
                return self._handle_count_query(user_input)

            elif intent == "database_query_comparative":
                sub_intent = intent_data.get('sub_intent')
                if sub_intent == "database_query_comparative_highest":
                    if "balance" in user_input.lower():
                        return self._handle_highest_balance_query()
                    elif "price" in user_input.lower():
                        return self._handle_highest_price_query()
                return self._execute_comparative_query(user_input, sub_intent)

            else:
                result = self.query_processor.process_query(user_input, intent_data)
                return self.generate_response(intent, result)

        except Exception as e:
            self.logger.error(f"Error processing user input: {e}")
            return {"response": f"An error occurred while processing your request: {str(e)}"}


    def _handle_date_specific_query(self, user_input):
        from datetime import datetime, timedelta
        import re
        import calendar

        query_lower = user_input.lower()

        main_table = None
        date_field = None

        if "trade" in query_lower:
            main_table = "trades"
            date_field = "trade_date"
        elif "transaction" in query_lower:
            main_table = "transactions"
            date_field = "transaction_date"
        elif "order" in query_lower:
            main_table = "orders"
            date_field = "order_date"
        else:
            main_table = "trades"
            date_field = "trade_date"

        today = datetime.now()

        specific_date_pattern = r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})'
        specific_date_match = re.search(specific_date_pattern, query_lower)
        if specific_date_match:
            day = int(specific_date_match.group(1))
            month_name = specific_date_match.group(2).lower()
            year = int(specific_date_match.group(3))

            months = {
                "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
            }
            month = months[month_name]

            max_days = calendar.monthrange(year, month)[1]
            day = min(day, max_days)

            specific_date = datetime(year, month, day)

            date_str = specific_date.strftime('%Y-%m-%d')

            sql = f"""
            SELECT * FROM {main_table}
            WHERE DATE({date_field}) = '{date_str}'
            ORDER BY {date_field}
            LIMIT 100
            """

            result_description = f"on {day} {month_name.capitalize()} {year}"

        elif re.search(
                r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year',
                query_lower):
            month_rel_year_match = re.search(
                r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year',
                query_lower)
            month_name = month_rel_year_match.group(1).lower()
            relative_year = month_rel_year_match.group(2).lower()

            months = {
                "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
            }
            month = months[month_name]

            if relative_year == "last":
                year = today.year - 1
            elif relative_year == "next":
                year = today.year + 1
            else:  # "this"
                year = today.year

            days_in_month = calendar.monthrange(year, month)[1]

            start_date = datetime(year, month, 1)
            end_date = datetime(year, month, days_in_month)

            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            sql = f"""
            SELECT * FROM {main_table}
            WHERE {date_field} BETWEEN '{start_date_str}' AND '{end_date_str}'
            ORDER BY {date_field}
            LIMIT 100
            """

            result_description = f"from {month_name.capitalize()} {year}"

        else:
            start_date = None
            end_date = today

            year_pattern = r'from\s+(20\d{2})\b'
            year_match = re.search(year_pattern, query_lower)
            if year_match:
                year = int(year_match.group(1))
                start_date = datetime(year, 1, 1)
                end_date = datetime(year, 12, 31)
                result_description = f"from {year}"

            elif re.search(r'\b(20\d{2})\b', query_lower):
                year_alt_match = re.search(r'\b(20\d{2})\b', query_lower)
                year = int(year_alt_match.group(1))
                start_date = datetime(year, 1, 1)
                end_date = datetime(year, 12, 31)
                result_description = f"from {year}"

            elif "last week" in query_lower:
                start_date = today - timedelta(days=7)
                result_description = "from last week"
            elif "last month" in query_lower:
                start_date = today - timedelta(days=30)
                result_description = "from last month"
            elif "last year" in query_lower:
                start_date = datetime(today.year - 1, 1, 1)
                end_date = datetime(today.year - 1, 12, 31)
                result_description = "from last year"
            elif "this year" in query_lower:
                start_date = datetime(today.year, 1, 1)
                end_date = datetime(today.year, 12, 31)
                result_description = "from this year"
            elif "this month" in query_lower:
                start_date = datetime(today.year, today.month, 1)
                days_in_month = calendar.monthrange(today.year, today.month)[1]
                end_date = datetime(today.year, today.month, days_in_month)
                result_description = "from this month"
            elif "recent" in query_lower or "latest" in query_lower:
                start_date = today - timedelta(days=30)  # Default to last 30 days
                result_description = "from the last 30 days"
            else:
                start_date = today - timedelta(days=30)
                result_description = "from the last 30 days"

            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            sql = f"""
            SELECT * FROM {main_table}
            WHERE {date_field} BETWEEN '{start_date_str}' AND '{end_date_str}'
            ORDER BY {date_field} DESC
            LIMIT 100
            """

        self.logger.info(f"Generated date-specific SQL: {sql}")
        result = self.query_processor.db_connector.execute_query(sql)

        if result and len(result) > 0:
            return {
                "response": f"Found {len(result)} {main_table} {result_description}:",
                "data": result
            }
        else:
            return {
                "response": f"No {main_table} found {result_description}."
            }

    def _check_specialized_queries(self, user_input):
        query_lower = user_input.lower()

        if (
                "show" in query_lower or "list" in query_lower or "all" in query_lower or "get" in query_lower) and "asset" in query_lower:

            if "crypto" in query_lower or "cryptocurrency" in query_lower or "bitcoin" in query_lower or "litecoin" in query_lower or "ethereum" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Cryptocurrency'
                ORDER BY asset_id
                """
                result = self.query_processor.db_connector.execute_query(sql)
                if result and len(result) > 0:
                    return {
                        "response": f"Found {len(result)} cryptocurrency assets in the database:",
                        "data": result
                    }
                else:
                    sql = """
                    SELECT * FROM assets 
                    WHERE asset_type LIKE '%crypto%' OR asset_type LIKE '%Crypto%'
                    ORDER BY asset_id
                    """
                    result = self.query_processor.db_connector.execute_query(sql)
                    if result and len(result) > 0:
                        return {
                            "response": f"Found {len(result)} cryptocurrency assets in the database:",
                            "data": result
                        }
                    else:
                        return {
                            "response": "No cryptocurrency assets found in the database."
                        }

            elif "etf" in query_lower:
                return self._handle_etf_assets_query()

            elif any(asset_type in query_lower for asset_type in
                     ["stock", "bond", "option", "commodity", "future", "forex"]):
                asset_types = {
                    "stock": "Stock",
                    "bond": "Bond",
                    "option": "Options",
                    "commodity": "Commodity",
                    "future": "Futures",
                    "forex": "Forex"
                }

                asset_type = None
                for key, value in asset_types.items():
                    if key in query_lower:
                        asset_type = value
                        break

                sql = f"""
                SELECT * FROM assets 
                WHERE asset_type = '{asset_type}'
                ORDER BY asset_id
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Found {len(result)} {asset_type.lower()} assets in the database:",
                        "data": result
                    }
                else:

                    sql = f"""
                    SELECT * FROM assets 
                    WHERE asset_type LIKE '%{asset_type}%'
                    ORDER BY asset_id
                    """

                    result = self.query_processor.db_connector.execute_query(sql)

                    if result and len(result) > 0:
                        return {
                            "response": f"Found {len(result)} {asset_type.lower()} assets in the database:",
                            "data": result
                        }
                    else:
                        return {
                            "response": f"No {asset_type.lower()} assets found in the database."
                        }

        if re.search(r'(?i)etf\s+assets', user_input) or re.search(r'(?i)show\s+me\s+etf', user_input):
            return self._handle_etf_assets_query()

        if re.search(r'(?i)completed\s+orders', user_input):
            return self._handle_completed_orders_query()

        if re.search(r'(?i)etf\s+assets', user_input) or re.search(r'(?i)show\s+me\s+etf', user_input):
            return self._handle_etf_assets_query()

        if re.search(r'(?i)completed\s+orders', user_input):
            return self._handle_completed_orders_query()

        entity_patterns = [
            r'(?:tell me|info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)(?:\?)?$',
            r'(?:who|what)(?:\s+\w+)?\s+(?:is|are)\s+([\w\s]+)(?:\?)?$',
            r'(?:show|display|get)(?:\s+\w+)?\s+(?:info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)(?:\?)?$',
            r'(?:lookup|find|search for)\s+([\w\s]+)(?:\?)?$'
        ]

        for pattern in entity_patterns:
            if re.search(pattern, user_input.lower()):
                entity_result = self._handle_entity_query(user_input)
                if entity_result:
                    return entity_result

        if re.search(r'(?i)etf\s+assets', user_input) or re.search(r'(?i)show\s+me\s+etf', user_input):
            return self._handle_etf_assets_query()

        if re.search(r'(?i)completed\s+orders', user_input):
            return self._handle_completed_orders_query()

        if re.search(r'(?i)average\s+price', user_input) and re.search(r'(?i)assets', user_input):
            return self._handle_average_price_query()

        if (re.search(
                r'(?i)trades.*(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year',
                query_lower) or
                re.search(
                    r'(?i)(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year.*trades',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(
                r'(?i)trades.*\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b',
                query_lower) or
                re.search(
                    r'(?i)\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b.*trades',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(r'(?i)(recent|latest)\s+(trades|trade)', user_input) or
                re.search(r'(?i)trades\s+from\s+(last|this|previous|20\d{2})', user_input) or
                re.search(r'(?i)(20\d{2})\s+trades', user_input)):
            return self._handle_date_specific_query(user_input)

        if (re.search(
                r'(?i)transactions.*(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year',
                query_lower) or
                re.search(
                    r'(?i)(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year.*transactions',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(
                r'(?i)transactions.*\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b',
                query_lower) or
                re.search(
                    r'(?i)\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b.*transactions',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(r'(?i)(recent|latest)\s+(transactions|transaction)', user_input) or
                re.search(r'(?i)transactions\s+from\s+(last|this|previous|20\d{2})', user_input) or
                re.search(r'(?i)(20\d{2})\s+transactions', user_input)):
            return self._handle_date_specific_query(user_input)

        if (re.search(
                r'(?i)orders.*(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year',
                query_lower) or
                re.search(
                    r'(?i)(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year.*orders',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(
                r'(?i)orders.*\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b',
                query_lower) or
                re.search(
                    r'(?i)\b(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b.*orders',
                    query_lower)):
            return self._handle_date_specific_query(user_input)

        if (re.search(r'(?i)(recent|latest)\s+(orders|order)', user_input) or
                re.search(r'(?i)orders\s+from\s+(last|this|previous|20\d{2})', user_input) or
                re.search(r'(?i)(20\d{2})\s+orders', user_input)):
            return self._handle_date_specific_query(user_input)

        match = re.search(r'(?i)transactions\s+over\s+\$?(\d+[,\d]*)', user_input)
        if match:
            threshold = float(match.group(1).replace(',', ''))
            return self._handle_large_transactions_query(threshold)

        if re.search(r'(?i)traders\s+with\s+highest\s+account\s+balance', user_input):
            return self._handle_highest_balance_query()

        if re.search(r'(?i)list\s+all\s+traders', user_input) or re.search(r'(?i)show\s+all\s+traders', user_input):
            return self._handle_list_traders_query()

        if re.search(r'(?i)count\s+how\s+many\s+traders', user_input) or re.search(r'(?i)how\s+many\s+traders',
                                                                                   user_input):
            return self._handle_count_traders_query()

        if 'about' in query_lower and len(query_lower.split()) <= 8:
            entity_result = self._handle_entity_query(user_input)
            if entity_result and 'data' in entity_result:
                return entity_result

        return None

    def _get_entity_details(self, entity_type, entity_data):

        entity_id_field = f"{entity_type[:-1]}_id"
        entity_id = entity_data.get(entity_id_field)

        if not entity_id:
            return {"response": f"Found entity but couldn't retrieve its ID", "data": entity_data}

        response = {
            "response": f"Here's information about {entity_data.get('name')}:",
            "data": entity_data,
            "related_data": {}
        }

        try:
            if entity_type == "traders":
                accounts_sql = f"""
                SELECT * FROM accounts 
                WHERE trader_id = {entity_id}
                """
                accounts = self.query_processor.db_connector.execute_query(accounts_sql)

                if accounts and len(accounts) > 0:
                    response["related_data"]["accounts"] = accounts

                    if accounts and len(accounts) > 0:
                        account_ids = ", ".join([str(acc["account_id"]) for acc in accounts])
                        transactions_sql = f"""
                        SELECT * FROM transactions 
                        WHERE account_id IN ({account_ids})
                        ORDER BY transaction_date DESC
                        LIMIT 10
                        """
                        transactions = self.query_processor.db_connector.execute_query(transactions_sql)
                        if transactions and len(transactions) > 0:
                            response["related_data"]["recent_transactions"] = transactions

                trades_sql = f"""
                SELECT t.*, a.name as asset_name, m.name as market_name 
                FROM trades t
                JOIN assets a ON t.asset_id = a.asset_id
                JOIN markets m ON t.market_id = m.market_id
                WHERE t.trader_id = {entity_id}
                ORDER BY t.trade_date DESC
                LIMIT 10
                """
                trades = self.query_processor.db_connector.execute_query(trades_sql)

                if trades and len(trades) > 0:
                    response["related_data"]["recent_trades"] = trades

            elif entity_type == "assets":
                price_sql = f"""
                SELECT * FROM price_history 
                WHERE asset_id = {entity_id}
                ORDER BY price_date DESC
                LIMIT 10
                """
                prices = self.query_processor.db_connector.execute_query(price_sql)

                if prices and len(prices) > 0:
                    response["related_data"]["price_history"] = prices

                trades_sql = f"""
                SELECT t.*, tr.name as trader_name, m.name as market_name 
                FROM trades t
                JOIN traders tr ON t.trader_id = tr.trader_id
                JOIN markets m ON t.market_id = m.market_id
                WHERE t.asset_id = {entity_id}
                ORDER BY t.trade_date DESC
                LIMIT 10
                """
                trades = self.query_processor.db_connector.execute_query(trades_sql)

                if trades and len(trades) > 0:
                    response["related_data"]["recent_trades"] = trades

                if entity_data.get('broker_id'):
                    broker_sql = f"""
                    SELECT * FROM brokers 
                    WHERE broker_id = {entity_data.get('broker_id')}
                    """
                    broker = self.query_processor.db_connector.execute_query(broker_sql)

                    if broker and len(broker) > 0:
                        response["related_data"]["broker"] = broker[0]

            elif entity_type == "markets":
                trades_sql = f"""
                SELECT t.*, tr.name as trader_name, a.name as asset_name 
                FROM trades t
                JOIN traders tr ON t.trader_id = tr.trader_id
                JOIN assets a ON t.asset_id = a.asset_id
                WHERE t.market_id = {entity_id}
                ORDER BY t.trade_date DESC
                LIMIT 10
                """
                trades = self.query_processor.db_connector.execute_query(trades_sql)

                if trades and len(trades) > 0:
                    response["related_data"]["recent_trades"] = trades

            elif entity_type == "brokers":
                assets_sql = f"""
                SELECT * FROM assets 
                WHERE broker_id = {entity_id}
                """
                assets = self.query_processor.db_connector.execute_query(assets_sql)

                if assets and len(assets) > 0:
                    response["related_data"]["assets"] = assets

        except Exception as e:
            self.logger.error(f"Error getting related data for {entity_type}.{entity_id_field}={entity_id}: {e}")
            response["response"] += " (Note: Some related information could not be retrieved.)"

        return response
    def _extract_entity_name(self, query):
        import re

        patterns = [
            r'(?:tell me|info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)(?:\?)?$',
            r'(?:who|what)(?:\s+\w+)?\s+(?:is|are)\s+([\w\s]+)(?:\?)?$',
            r'(?:show|display|get)(?:\s+\w+)?\s+(?:info|information|details)(?:\s+\w+)?\s+(?:about|on|for)\s+([\w\s]+)(?:\?)?$',
            r'(?:lookup|find|search for)\s+([\w\s]+)(?:\?)?$'
        ]

        for pattern in patterns:
            match = re.search(pattern, query.lower())
            if match:
                return match.group(1).strip()

        prepositions = ["about", "on", "for", "regarding"]
        for prep in prepositions:
            if f" {prep} " in query.lower():
                parts = query.lower().split(f" {prep} ", 1)
                if len(parts) > 1:
                    return parts[1].strip().rstrip('?')

        return None

    def _handle_entity_query(self, user_input):
        entity_name = self._extract_entity_name(user_input)
        if not entity_name:
            return None

        self.logger.info(f"Searching for entity: {entity_name}")

        tables_to_check = ["traders", "brokers", "assets", "markets"]

        for table in tables_to_check:
            sql = f"""
            SELECT * FROM {table} 
            WHERE name = '{entity_name}'
            LIMIT 1
            """

            try:
                result = self.query_processor.db_connector.execute_query(sql)

                if not result or len(result) == 0:
                    sql = f"""
                    SELECT * FROM {table} 
                    WHERE name LIKE '%{entity_name}%'
                    LIMIT 1
                    """
                    result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return self._get_entity_details(table, result[0])

            except Exception as e:
                self.logger.error(f"Error searching for entity in {table}: {e}")

        return {"response": f"I couldn't find any information about '{entity_name}' in our database."}

    def _handle_asset_type_query(self, user_input):
        query_lower = user_input.lower()

        asset_type_mappings = {
            'etf': ['etf', 'etfs', 'exchange traded fund', 'exchange-traded fund', 'exchange traded funds'],
            'stock': ['stock', 'stocks', 'equity', 'equities', 'share', 'shares'],
            'bond': ['bond', 'bonds', 'fixed income', 'debt security', 'debt securities'],
            'futures': ['future', 'futures', 'futures contract', 'futures contracts'],
            'option': ['option', 'options', 'stock option', 'stock options'],
            'cryptocurrency': ['crypto', 'cryptocurrency', 'cryptocurrencies', 'digital currency', 'digital currencies',
                               'bitcoin', 'ethereum', 'altcoin', 'altcoins'],
            'forex': ['forex', 'foreign exchange', 'currency pair', 'currency pairs', 'fx'],
            'commodity': ['commodity', 'commodities', 'gold', 'silver', 'oil', 'natural gas'],
            'mutual fund': ['mutual fund', 'mutual funds', 'fund', 'funds'],
            'reit': ['reit', 'reits', 'real estate investment trust', 'real estate investment trusts']
        }

        asset_type = None
        for type_key, variations in asset_type_mappings.items():
            if any(variation in query_lower for variation in variations):
                asset_type = type_key.upper()
                break

        if not asset_type and (
                'asset type' in query_lower or 'asset types' in query_lower or 'types of asset' in query_lower):
            sql = """
            SELECT asset_type, COUNT(*) as count
            FROM assets
            GROUP BY asset_type
            ORDER BY count DESC
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Here are the different types of assets in the database:",
                    "data": result
                }
            else:
                return {
                    "response": "No asset type information found in the database."
                }

        if not asset_type:
            asset_type = 'ETF'

        sql = f"""
        SELECT * FROM assets 
        WHERE asset_type = '{asset_type}'
        ORDER BY asset_id
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if result and len(result) > 0:
            return {
                "response": f"Found {len(result)} {asset_type.lower()} assets in the database:",
                "data": result
            }
        else:
            sql = f"""
            SELECT * FROM assets 
            WHERE asset_type LIKE '%{asset_type}%'
            ORDER BY asset_id
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Found {len(result)} {asset_type.lower()} assets in the database:",
                    "data": result
                }
            else:
                return {
                    "response": f"No {asset_type.lower()} assets found in the database."
                }

    def _handle_etf_assets_query(self):

        return self._handle_asset_type_query("show me ETF assets")

    def _handle_asset_by_broker_query(self, user_input):
        query_lower = user_input.lower()

        broker_patterns = [
            r'(?:by|from|of|with)\s+([a-zA-Z0-9\s]+)(?:broker)?',
            r'([a-zA-Z0-9\s]+)(?:broker|brokerage|firm)\'s\s+assets'
        ]

        broker_name = None
        for pattern in broker_patterns:
            match = re.search(pattern, query_lower)
            if match:
                broker_name = match.group(1).strip()
                break

        if not broker_name:
            sql = """
            SELECT b.broker_id, b.name, COUNT(a.asset_id) as asset_count
            FROM brokers b
            LEFT JOIN assets a ON b.broker_id = a.broker_id
            GROUP BY b.broker_id, b.name
            ORDER BY asset_count DESC
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Here are the brokers and the number of assets they manage:",
                    "data": result
                }
            else:
                return {
                    "response": "No broker information found in the database."
                }

        sql = f"""
        SELECT a.*, b.name as broker_name
        FROM assets a
        JOIN brokers b ON a.broker_id = b.broker_id
        WHERE b.name = '{broker_name}'
        ORDER BY a.asset_id
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if not result or len(result) == 0:
            sql = f"""
            SELECT a.*, b.name as broker_name
            FROM assets a
            JOIN brokers b ON a.broker_id = b.broker_id
            WHERE b.name LIKE '%{broker_name}%'
            ORDER BY a.asset_id
            """

            result = self.query_processor.db_connector.execute_query(sql)

        if result and len(result) > 0:
            broker_name_display = result[0].get('broker_name', broker_name)
            return {
                "response": f"Found {len(result)} assets managed by {broker_name_display}:",
                "data": result
            }
        else:
            return {
                "response": f"No assets found for broker '{broker_name}'."
            }

    def _handle_asset_performance_query(self, user_input):
        query_lower = user_input.lower()

        sort_direction = "DESC"
        if any(term in query_lower for term in ["worst", "lowest", "poorest", "bottom", "least"]):
            sort_direction = "ASC"

        try:
            sql = f"""
            WITH latest_prices AS (
                SELECT 
                    asset_id,
                    close_price,
                    price_date,
                    ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY price_date DESC) as rn
                FROM price_history
                WHERE close_price > 0
            ),
            oldest_prices AS (
                SELECT 
                    asset_id,
                    close_price,
                    price_date,
                    ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY price_date ASC) as rn
                FROM price_history
                WHERE close_price > 0
            )
            SELECT 
                a.asset_id,
                a.name,
                a.asset_type,
                latest.close_price as current_price,
                oldest.close_price as starting_price,
                latest.price_date as latest_date,
                oldest.price_date as starting_date,
                ((latest.close_price - oldest.close_price) / oldest.close_price * 100) as percent_change
            FROM assets a
            JOIN latest_prices latest ON a.asset_id = latest.asset_id AND latest.rn = 1
            JOIN oldest_prices oldest ON a.asset_id = oldest.asset_id AND oldest.rn = 1
            WHERE oldest.close_price > 0
            ORDER BY percent_change {sort_direction}
            LIMIT 20
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                performance_term = "best" if sort_direction == "DESC" else "worst"
                return {
                    "response": f"Here are the {performance_term} performing assets based on price change:",
                    "data": result
                }
        except Exception as e:
            self.logger.error(f"Error in asset performance query: {e}")

        sql = f"""
        SELECT a.*, p.close_price as current_price, p.price_date
        FROM assets a
        JOIN (
            SELECT asset_id, close_price, price_date,
                   ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY price_date DESC) as rn
            FROM price_history
        ) p ON a.asset_id = p.asset_id AND p.rn = 1
        ORDER BY current_price {sort_direction}
        LIMIT 20
        """

        try:
            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                price_term = "highest" if sort_direction == "DESC" else "lowest"
                return {
                    "response": f"Here are the assets with the {price_term} current prices:",
                    "data": result
                }
            else:
                sql = f"""
                SELECT * FROM assets
                ORDER BY asset_id
                LIMIT 20
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Here are some assets from the database (performance data not available):",
                        "data": result
                    }
                else:
                    return {
                        "response": "No asset information found in the database."
                    }
        except Exception as e:
            self.logger.error(f"Error in fallback asset query: {e}")
            return {
                "response": "Could not retrieve asset performance information due to an error."
            }

    def _handle_completed_orders_query(self):
        try:
            sql = """
            SELECT o.*, os.status, os.status_date
            FROM orders o
            JOIN order_status os ON o.order_id = os.order_id
            WHERE os.status = 'Completed'
            ORDER BY os.status_date DESC
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Found {len(result)} completed orders:",
                    "data": result
                }
            else:
                return {
                    "response": "No completed orders found in the database."
                }
        except Exception as e:
            self.logger.error(f"Error in completed orders query: {e}")
            try:
                sql = """
                SELECT * FROM orders
                WHERE order_type LIKE '%Completed%'
                ORDER BY order_date DESC
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Found {len(result)} completed-type orders:",
                        "data": result
                    }
                else:
                    return {
                        "response": "No completed orders found in the database."
                    }
            except Exception as e2:
                self.logger.error(f"Error in fallback query: {e2}")
                return {
                    "response": "Could not retrieve completed orders due to an error."
                }

    def _handle_average_price_query(self):
        try:
            sql = """
            SELECT AVG(price_history.close_price) as average_price
            FROM price_history
            JOIN assets ON price_history.asset_id = assets.asset_id
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and result[0]['average_price'] is not None:
                avg_price = result[0]['average_price']

                sample_sql = """
                SELECT assets.asset_id, assets.name, assets.asset_type, 
                       price_history.close_price as price
                FROM assets
                JOIN price_history ON assets.asset_id = price_history.asset_id
                ORDER BY price_history.close_price DESC
                LIMIT 10
                """

                sample_result = self.query_processor.db_connector.execute_query(sample_sql)

                return {
                    "response": f"The average price of assets is ${avg_price:.2f}. Here are some sample assets:",
                    "data": sample_result
                }
            else:
                sql = """
                SELECT AVG(price) as average_price
                FROM assets
                WHERE price IS NOT NULL
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and result[0]['average_price'] is not None:
                    avg_price = result[0]['average_price']

                    sample_sql = """
                    SELECT *
                    FROM assets
                    WHERE price IS NOT NULL
                    ORDER BY price DESC
                    LIMIT 10
                    """

                    sample_result = self.query_processor.db_connector.execute_query(sample_sql)

                    return {
                        "response": f"The average price of assets is ${avg_price:.2f}. Here are some sample assets:",
                        "data": sample_result
                    }
                else:
                    sql = """
                    SELECT AVG(price) as average_price
                    FROM trades
                    """

                    result = self.query_processor.db_connector.execute_query(sql)

                    if result and result[0]['average_price'] is not None:
                        avg_price = result[0]['average_price']

                        sample_sql = """
                        SELECT assets.*
                        FROM assets
                        LIMIT 10
                        """

                        sample_result = self.query_processor.db_connector.execute_query(sample_sql)

                        return {
                            "response": f"The average price in trades is ${avg_price:.2f}. Here are some sample assets:",
                            "data": sample_result
                        }
                    else:
                        return {
                            "response": "Could not calculate the average price of assets. No price data found."
                        }
        except Exception as e:
            self.logger.error(f"Error in average price query: {e}")
            return {
                "response": "Could not calculate the average price of assets due to an error."
            }

    def _handle_recent_trades_query(self):
        try:
            today = datetime.now()
            one_week_ago = today - timedelta(days=7)
            one_week_ago_str = one_week_ago.strftime('%Y-%m-%d')

            sql = f"""
            SELECT t.*, a.name as asset_name, m.name as market_name, tr.name as trader_name
            FROM trades t
            JOIN assets a ON t.asset_id = a.asset_id
            JOIN markets m ON t.market_id = m.market_id
            JOIN traders tr ON t.trader_id = tr.trader_id
            WHERE t.trade_date >= '{one_week_ago_str}'
            ORDER BY t.trade_date DESC
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Found {len(result)} trades from the last week (since {one_week_ago_str}):",
                    "data": result
                }
            else:
                sql = """
                SELECT t.*, a.name as asset_name, m.name as market_name, tr.name as trader_name
                FROM trades t
                JOIN assets a ON t.asset_id = a.asset_id
                JOIN markets m ON t.market_id = m.market_id
                JOIN traders tr ON t.trader_id = tr.trader_id
                ORDER BY t.trade_date DESC
                LIMIT 10
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Showing the 10 most recent trades (could not filter by last week):",
                        "data": result
                    }
                else:
                    return {
                        "response": f"No trades found in the database."
                    }
        except Exception as e:
            self.logger.error(f"Error in recent trades query: {e}")
            try:
                sql = """
                SELECT * FROM trades
                ORDER BY trade_date DESC
                LIMIT 10
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Showing the 10 most recent trades:",
                        "data": result
                    }
                else:
                    return {
                        "response": f"No trades found in the database."
                    }
            except Exception as e2:
                self.logger.error(f"Error in fallback trades query: {e2}")
                return {
                    "response": "Could not retrieve recent trades due to an error."
                }

    def _handle_large_transactions_query(self, threshold):
        try:
            sql = f"""
            SELECT t.*, a.account_type, tr.name as trader_name
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN traders tr ON a.trader_id = tr.trader_id
            WHERE t.amount > {threshold}
            ORDER BY t.amount DESC
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": f"Found {len(result)} transactions over ${threshold:,.2f}:",
                    "data": result
                }
            else:
                sql = f"""
                SELECT * FROM transactions
                WHERE amount > {threshold}
                ORDER BY amount DESC
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": f"Found {len(result)} transactions over ${threshold:,.2f}:",
                        "data": result
                    }
                else:
                    return {
                        "response": f"No transactions found over ${threshold:,.2f}."
                    }
        except Exception as e:
            self.logger.error(f"Error in large transactions query: {e}")
            return {
                "response": f"Could not retrieve transactions over ${threshold:,.2f} due to an error."
            }

    def _handle_highest_balance_query(self):
        try:
            sql = """
            SELECT t.trader_id, t.name, t.email, a.account_id, a.balance, a.account_type
            FROM traders t
            JOIN accounts a ON t.trader_id = a.trader_id
            ORDER BY a.balance DESC
            LIMIT 10
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": "Traders with highest account balances:",
                    "data": result
                }
            else:
                return {
                    "response": "No account balance information found."
                }
        except Exception as e:
            self.logger.error(f"Error in highest balance query: {e}")
            return {
                "response": "Could not retrieve trader balance information due to an error."
            }

    def _handle_highest_price_query(self):
        try:
            sql = """
            SELECT a.asset_id, a.name, a.asset_type, p.close_price as price
            FROM assets a
            JOIN price_history p ON a.asset_id = p.asset_id
            ORDER BY p.close_price DESC
            LIMIT 10
            """

            result = self.query_processor.db_connector.execute_query(sql)

            if result and len(result) > 0:
                return {
                    "response": "Assets with highest prices:",
                    "data": result
                }
            else:
                sql = """
                SELECT * FROM assets
                WHERE price IS NOT NULL
                ORDER BY price DESC
                LIMIT 10
                """

                result = self.query_processor.db_connector.execute_query(sql)

                if result and len(result) > 0:
                    return {
                        "response": "Assets with highest prices:",
                        "data": result
                    }
                else:
                    sql = """
                    SELECT t.*, a.name as asset_name
                    FROM trades t
                    JOIN assets a ON t.asset_id = a.asset_id
                    ORDER BY t.price DESC
                    LIMIT 10
                    """

                    result = self.query_processor.db_connector.execute_query(sql)

                    if result and len(result) > 0:
                        return {
                            "response": "Trades with highest prices:",
                            "data": result
                        }
                    else:
                        return {
                            "response": "No price information found."
                        }
        except Exception as e:
            self.logger.error(f"Error in highest price query: {e}")
            return {
                "response": "Could not retrieve price information due to an error."
            }

    def _handle_list_traders_query(self):
        sql = """
        SELECT * FROM traders
        ORDER BY trader_id
        LIMIT 100
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if result and len(result) > 0:
            return {
                "response": f"Listing all traders ({len(result)} total):",
                "data": result
            }
        else:
            return {
                "response": "No traders found in the database."
            }

    def _handle_count_traders_query(self):
        sql = """
        SELECT COUNT(*) as trader_count FROM traders
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if result and result[0]['trader_count'] is not None:
            count = result[0]['trader_count']

            sample_sql = """
            SELECT * FROM traders
            LIMIT 5
            """

            sample_result = self.query_processor.db_connector.execute_query(sample_sql)

            return {
                "response": f"There are {count} traders in the database. Here's a sample:",
                "data": sample_result
            }
        else:
            return {
                "response": "Could not count traders in the database."
            }

    def _handle_count_query(self, query):
        query_context = self._analyze_query_context(query)
        table = query_context.get('table', 'traders')

        sql = f"""
        SELECT COUNT(*) as count FROM {table}
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if result and result[0]['count'] is not None:
            count = result[0]['count']

            sample_sql = f"""
            SELECT * FROM {table}
            LIMIT 5
            """

            sample_result = self.query_processor.db_connector.execute_query(sample_sql)

            return {
                "response": f"There are {count} {table} in the database. Here's a sample:",
                "data": sample_result
            }
        else:
            return {
                "response": f"Could not count {table} in the database."
            }

    def _execute_list_query(self, query):
        query_lower = query.lower()

        if "asset" in query_lower:
            table = "assets"

            if "crypto" in query_lower or "cryptocurrency" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Cryptocurrency'
                ORDER BY asset_id
                """
            elif "etf" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'ETF'
                ORDER BY asset_id
                """
            elif "stock" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Stock'
                ORDER BY asset_id
                """
            elif "bond" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Bond'
                ORDER BY asset_id
                """
            elif "commodity" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Commodity'
                ORDER BY asset_id
                """
            elif "option" in query_lower:
                sql = """
                SELECT * FROM assets 
                WHERE asset_type = 'Options'
                ORDER BY asset_id
                """
            else:
                sql = """
                SELECT * FROM assets
                ORDER BY asset_id
                """

            self.logger.info(f"Executing asset query: {sql}")
            return self.query_processor.db_connector.execute_query(sql)

        query_context = self._analyze_query_context(query)
        table = query_context.get('table', 'traders')

        if not table or table == 'none':
            table = 'traders'

        sql = f"""
        SELECT * FROM {table}
        LIMIT 100
        """

        self.logger.info(f"Executing list query: {sql}")
        return self.query_processor.db_connector.execute_query(sql)

    def _execute_comparative_query(self, query, sub_intent):
        query_context = self._analyze_query_context(query)
        table = query_context.get('table', 'traders')
        attribute = query_context.get('attribute')

        sort_field = None
        if attribute:
            sort_field = attribute
        elif 'balance' in query.lower():
            sort_field = 'balance'
        elif 'price' in query.lower():
            sort_field = 'price'

        if not sort_field:
            if table == 'accounts':
                sort_field = 'balance'
            elif table == 'assets' or table == 'trades':
                sort_field = 'price'
            elif table == 'transactions':
                sort_field = 'amount'
            else:
                sort_field = f"{table[:-1] if table.endswith('s') else table}_id"

        sort_direction = "DESC"
        if sub_intent and "lowest" in sub_intent:
            sort_direction = "ASC"

        sql = f"""
        SELECT * FROM {table}
        ORDER BY {sort_field} {sort_direction}
        LIMIT 10
        """

        result = self.query_processor.db_connector.execute_query(sql)

        return result

    def _analyze_query_context(self, query):
        result = {
            "table": None,
            "attribute": None,
            "comparative": None
        }

        query_lower = query.lower()

        table_keywords = {
            "traders": ["trader", "traders"],
            "assets": ["asset", "assets", "etf", "etfs", "stock", "stocks", "bond", "bonds",
                       "cryptocurrency", "crypto", "bitcoin", "ethereum", "commodity", "commodities",
                       "option", "options", "security", "securities"],
            "markets": ["market", "markets", "exchange", "exchanges"],
            "accounts": ["account", "accounts"],
            "trades": ["trade", "trades", "transaction", "transactions"],
            "brokers": ["broker", "brokers"],
            "orders": ["order", "orders"]
        }

        for table, keywords in table_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                result["table"] = table
                break

        if any(term in query_lower for term in ["crypto", "cryptocurrency", "bitcoin", "ethereum", "litecoin"]):
            result["table"] = "assets"
            result["attribute"] = "asset_type"
        elif "etf" in query_lower:
            result["table"] = "assets"
            result["attribute"] = "asset_type"
        elif any(term in query_lower for term in ["stock", "equity", "share"]):
            result["table"] = "assets"
            result["attribute"] = "asset_type"

        attribute_keywords = {
            "balance": ["balance", "money", "funds", "account balance"],
            "price": ["price", "cost", "value", "worth"],
            "date": ["date", "time", "when"],
            "name": ["name", "called", "named"]
        }

        for attribute, keywords in attribute_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                result["attribute"] = attribute
                break

        if any(term in query_lower for term in ["highest", "most", "maximum", "largest"]):
            result["comparative"] = "highest"
        elif any(term in query_lower for term in ["lowest", "least", "minimum", "smallest"]):
            result["comparative"] = "lowest"
        elif any(term in query_lower for term in ["middle", "median", "average", "mid"]):
            result["comparative"] = "middle"

        return result

    def generate_response(self, intent, query_result, sub_intent=None):
        if not query_result:
            return {"response": "I couldn't find any information for your query."}

        if isinstance(query_result, dict) and "error" in query_result:
            return {"response": f"There was an error: {query_result['error']}"}

        if isinstance(query_result, dict) and "message" in query_result:
            return {"response": query_result["message"]}

        if isinstance(query_result, list):
            if len(query_result) == 0:
                return {"response": "I found no matching records."}

            table_detected = self._determine_primary_table(query_result[0])

            intent_parts = intent.split('_')
            intent_table = None
            for part in intent_parts:
                if part in ["assets", "traders", "trades", "markets", "accounts", "orders"]:
                    intent_table = part
                    break

            query_context = self._analyze_query_context(self.current_query)

            primary_table = query_context.get("table") or intent_table or table_detected or "records"

            if len(query_result) == 1:
                response_text = "Here's what I found:"
                data_to_return = query_result[0]
            else:
                response_text = f"Here are some {primary_table} from the database:"

                sample = query_result[0]
                if "name" in sample:
                    names = [r["name"] for r in query_result if "name" in r]
                    name_list = ", ".join(names[:5])
                    if len(names) > 5:
                        name_list += " and others"
                    response_text += f" Including {name_list}."

                data_to_return = query_result

            return {
                "response": response_text,
                "data": data_to_return
            }
        else:
            return {"response": "Operation completed successfully."}

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