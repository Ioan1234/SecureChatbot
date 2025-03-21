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

        return None

    def _handle_etf_assets_query(self):
        sql = """
        SELECT * FROM assets 
        WHERE asset_type = 'ETF'
        ORDER BY asset_id
        """

        result = self.query_processor.db_connector.execute_query(sql)

        if result and len(result) > 0:
            return {
                "response": f"Found {len(result)} ETF assets in the database:",
                "data": result
            }
        else:
            return {
                "response": "No ETF assets found in the database."
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
        query_context = self._analyze_query_context(query)
        table = query_context.get('table', 'traders')

        sql = f"""
        SELECT * FROM {table}
        LIMIT 100
        """

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
            "assets": ["asset", "assets", "stock", "stocks", "security", "securities"],
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