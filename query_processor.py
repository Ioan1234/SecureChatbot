import logging
import re
import json
import tensorflow as tf
import numpy as np


class QueryProcessor:
    def __init__(self, db_connector, encryption_manager, sensitive_fields=None):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.encryption_manager = encryption_manager
        self.sensitive_fields = sensitive_fields or [
            "email", "contact_email", "phone", "license_number", "balance"
        ]

        self.dev_mode = True

        self.needs_highest_value_handling = None
        self.needs_lowest_value_handling = None
        self.needs_middle_value_handling = None

        self.table_info = {
            "traders": ["trader_id", "name", "email", "phone", "registration_date"],
            "brokers": ["broker_id", "name", "license_number", "contact_email"],
            "assets": ["asset_id", "name", "asset_type", "broker_id"],
            "markets": ["market_id", "name", "location", "operating_hours"],
            "trades": ["trade_id", "trader_id", "asset_id", "market_id", "trade_date", "quantity", "price"],
            "accounts": ["account_id", "trader_id", "balance", "account_type", "creation_date"],
            "transactions": ["transaction_id", "account_id", "transaction_date", "transaction_type", "amount"],
            "orders": ["order_id", "trade_id", "order_type", "order_date"],
            "order_status": ["status_id", "order_id", "status", "status_date"],
            "price_history": ["price_id", "asset_id", "price_date", "open_price", "close_price"]
        }

        self.entity_mapping = {
            "market": "markets",
            "markets": "markets",
            "stock exchange": "markets",
            "trader": "traders",
            "traders": "traders",
            "broker": "brokers",
            "brokers": "brokers",
            "asset": "assets",
            "assets": "assets",
            "stock": "assets",
            "bond": "assets",
            "trade": "trades",
            "trades": "trades",
            "transaction": "transactions",
            "transactions": "transactions",
            "account": "accounts",
            "accounts": "accounts",
            "order": "orders",
            "orders": "orders",
            "price": "price_history",
            "price history": "price_history",
            "prices": "price_history"
        }

        self.question_patterns = {
            "what": "SELECT",
            "which": "SELECT",
            "where": "SELECT",
            "who": "SELECT",
            "how many": "SELECT COUNT(*)",
            "list": "SELECT",
            "show": "SELECT",
            "find": "SELECT"
        }

        try:
            import spacy
            self.nlp = spacy.load("en_core_web_md")
            self.logger.info("NLP model loaded successfully")
        except Exception as e:
            self.logger.warning(f"Error loading NLP model: {e}")
            self.logger.warning("Using simplified query processing without NLP")
            self.nlp = None

    def get_essential_fields(self, table_name):

        essential_fields = {
            "markets": ["name", "location"],
            "brokers": ["name"],
            "traders": ["name", "registration_date"],
            "assets": ["name", "asset_type"],
            "trades": ["trade_date", "quantity", "price"],
            "accounts": ["account_type", "balance"],
            "transactions": ["transaction_date", "transaction_type", "amount"],
            "orders": ["order_type", "order_date"],
            "order_status": ["status", "status_date"],
            "price_history": ["price_date", "open_price", "close_price"]
        }

        return essential_fields.get(table_name, ["name"])

    def _should_encrypt_field(self, field_name):
        if field_name in self.sensitive_fields:
            return True

        sensitive_patterns = [
            "password", "pwd", "secret", "token", "key",
            "ssn", "social_security", "tax_id",
            "credit_card", "card_number", "cvv", "ccv",
            "license_number", "license_id",
        ]

        sensitive_email_fields = ["email", "contact_email"]
        if field_name in sensitive_email_fields:
            return True

        if "phone" in field_name:
            return True

        return any(pattern in field_name.lower() for pattern in sensitive_patterns)

    def _make_serializable(self, value):
        if hasattr(value, '__class__') and value.__class__.__name__ == 'CKKSVector':
            return {
                "type": "encrypted",
                "value": "[ENCRYPTED]"
            }
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        return value

    def extract_number_from_query(self, nl_query):
        self.logger.info(f"Attempting to extract number from: '{nl_query}'")

        word_to_number = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
            'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
            'sixteen': 16, 'seventeen': 17, 'eighteen': 18, 'nineteen': 19, 'twenty': 20,
            'thirty': 30, 'forty': 40, 'fifty': 50
        }

        for word, num in word_to_number.items():
            pattern = rf'\b{word}\b'
            if re.search(pattern, nl_query.lower()):
                self.logger.info(f"Found word number: {word} ({num})")
                return num

        all_numbers = re.findall(r'\b(\d+)\b', nl_query)
        self.logger.info(f"Found digit numbers in query: {all_numbers}")

        if all_numbers:
            first_num = int(all_numbers[0])
            self.logger.info(f"Using first digit number found: {first_num}")
            return first_num

        return None

    def handle_comparative_queries(self, nl_query, tables, columns, conditions, order_by, limit=100):
        nl_query = nl_query.lower()

        modified = False

        comparatives = {
            "highest": (
                "DESC", ["high", "highest", "maximum", "most expensive", "top", "largest", "greatest", "biggest"]),
            "lowest": ("ASC", ["low", "lowest", "minimum", "cheapest", "bottom", "smallest", "least"]),
            "median": ("MEDIAN", ["median", "middle", "mid", "center", "mid values", "middle values"]),
            "ascending": ("ASC", ["ascending", "increasing", "going up", "from low to high", "smallest to largest"]),
            "descending": (
                "DESC", ["descending", "decreasing", "going down", "from high to low", "largest to smallest"])
        }

        price_related = any(term in nl_query for term in ["price", "cost", "value", "worth", "expensive", "cheap"])

        sort_related = any(term in nl_query for term in ["sort", "order", "arrange", "rank"])

        requested_comparison = None
        for comp_type, (sort_order, terms) in comparatives.items():
            if any(term in nl_query for term in terms):
                requested_comparison = comp_type
                self.logger.info(f"Detected comparative query type: {requested_comparison}")
                break

        if requested_comparison:

            if requested_comparison in ["ascending", "descending"]:
                if "price_history" in tables or price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                    sort_direction = "ASC" if requested_comparison == "ascending" else "DESC"
                    order_by = [f"price_history.close_price {sort_direction}"]
                    self.logger.info(f"Sorting by price in {sort_direction} order")
                    modified = True

                elif "assets" in tables:
                    sort_direction = "ASC" if requested_comparison == "ascending" else "DESC"
                    order_by = [f"assets.asset_id {sort_direction}"]
                    modified = True
                elif "markets" in tables:
                    sort_direction = "ASC" if requested_comparison == "ascending" else "DESC"
                    order_by = [f"markets.market_id {sort_direction}"]
                    modified = True
                elif "trades" in tables:
                    sort_direction = "ASC" if requested_comparison == "ascending" else "DESC"
                    if price_related:
                        order_by = [f"trades.price {sort_direction}"]
                    else:
                        order_by = [f"trades.trade_id {sort_direction}"]
                    modified = True

            elif requested_comparison in ["highest", "lowest"]:
                self.logger.info(
                    f"Using special approach for {requested_comparison} to handle multiple identical values")
                if "price_history" in tables or price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                    if requested_comparison == "highest":
                        order_by = ["price_history.close_price DESC"]
                        self.needs_highest_value_handling = "price_history.close_price"
                        modified = True
                    elif requested_comparison == "lowest":
                        order_by = ["price_history.close_price ASC"]
                        self.needs_lowest_value_handling = "price_history.close_price"
                        modified = True

                elif "markets" in tables:
                    if price_related:
                        if "assets" not in tables:
                            tables.append("assets")
                            conditions.append("assets.market_id = markets.market_id")

                        if "price_history" not in tables:
                            tables.append("price_history")
                            conditions.append("price_history.asset_id = assets.asset_id")

                        if requested_comparison == "highest":
                            order_by = ["price_history.close_price DESC"]
                            self.needs_highest_value_handling = "price_history.close_price"
                            modified = True
                        elif requested_comparison == "lowest":
                            order_by = ["price_history.close_price ASC"]
                            self.needs_lowest_value_handling = "price_history.close_price"
                            modified = True

                elif price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                        if "assets" in tables:
                            conditions.append("price_history.asset_id = assets.asset_id")
                        elif "markets" in tables:
                            tables.append("assets")
                            conditions.extend([
                                "assets.market_id = markets.market_id",
                                "price_history.asset_id = assets.asset_id"
                            ])

                    if requested_comparison == "highest":
                        order_by = ["price_history.close_price DESC"]
                        self.needs_highest_value_handling = "price_history.close_price"
                        modified = True
                    elif requested_comparison == "lowest":
                        order_by = ["price_history.close_price ASC"]
                        self.needs_lowest_value_handling = "price_history.close_price"
                        modified = True

            elif requested_comparison == "median":
                self.logger.info("Processing median/middle value query")
                if "price_history" in tables or price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                    if "*" in columns:
                        columns = ["price_history.*", "assets.name"]
                        if "assets" not in tables:
                            tables.append("assets")
                            conditions.append("price_history.asset_id = assets.asset_id")

                    order_by = ["price_history.close_price ASC"]
                    self.needs_middle_value_handling = "price_history.close_price"
                    modified = True

                elif "assets" in tables:
                    order_by = ["assets.asset_id ASC"]
                    self.needs_middle_value_handling = "assets.asset_id"
                    modified = True
                elif "markets" in tables:
                    order_by = ["markets.market_id ASC"]
                    self.needs_middle_value_handling = "markets.market_id"
                    modified = True

        elif sort_related:
            sort_direction = "ASC"
            if "descending" in nl_query or "high to low" in nl_query:
                sort_direction = "DESC"

            if "price_history" in tables or price_related:
                order_by = [f"price_history.close_price {sort_direction}"]
                modified = True
            elif "assets" in tables:
                order_by = [f"assets.asset_id {sort_direction}"]
                modified = True
            elif "markets" in tables:
                order_by = [f"markets.market_id {sort_direction}"]
                modified = True

        return tables, columns, conditions, order_by, modified, limit

    def natural_language_to_sql(self, nl_query):
        try:
            self.logger.info(f"Processing natural language query: '{nl_query}'")
            nl_query = nl_query.lower()

            trade_id_match = re.search(r'trade\s+(\d+)', nl_query)
            if trade_id_match:
                trade_id = trade_id_match.group(1)
                self.logger.info(f"Detected query for specific trade ID: {trade_id}")
                return f"""
                    SELECT 
                        t.trade_id,
                        t.trade_date,
                        t.quantity,
                        t.price,
                        t.quantity * t.price as trade_value,
                        tr.name as trader_name,
                        a.name as asset_name,
                        a.asset_type,
                        m.name as market_name,
                        o.order_type,
                        os.status
                    FROM 
                        trades t
                        JOIN traders tr ON t.trader_id = tr.trader_id
                        JOIN assets a ON t.asset_id = a.asset_id
                        JOIN markets m ON t.market_id = m.market_id
                        LEFT JOIN orders o ON t.trade_id = o.trade_id
                        LEFT JOIN order_status os ON o.order_id = os.order_id
                    WHERE 
                        t.trade_id = {trade_id}
                    LIMIT 100
                """

            order_id_match = re.search(r'order\s+(\d+)', nl_query)
            if order_id_match:
                order_id = order_id_match.group(1)
                self.logger.info(f"Detected query for specific order ID: {order_id}")
                return f"""
                    SELECT 
                        o.order_id,
                        o.order_type,
                        o.order_date,
                        os.status,
                        os.status_date,
                        t.trade_id,
                        t.trade_date,
                        t.quantity,
                        t.price,
                        tr.name as trader_name,
                        a.name as asset_name
                    FROM 
                        orders o
                        JOIN trades t ON o.trade_id = t.trade_id
                        JOIN traders tr ON t.trader_id = tr.trader_id
                        JOIN assets a ON t.asset_id = a.asset_id
                        LEFT JOIN order_status os ON o.order_id = os.order_id
                    WHERE 
                        o.order_id = {order_id}
                    LIMIT 100
                """

            detailed_request = any(term in nl_query for term in [
                "all details", "detailed", "complete", "all information",
                "full record", "details about", "show all fields", "details"
            ])
            self.logger.info(f"Detailed request detected: {detailed_request}")

            entity_type, name_match = self._check_for_name_search(nl_query)

            if entity_type and name_match:
                self.logger.info(f"Detected search for specific {entity_type}: {name_match}")

                if detailed_request:
                    return self._generate_detailed_entity_query(entity_type, name_match)
                else:
                    return self._generate_basic_entity_query(entity_type, name_match)
            self.logger.info(f"Detailed request detected: {detailed_request}")

            action_type = "SELECT"
            tables = []
            columns = []  # Start with empty columns
            conditions = []
            order_by = []
            limit = 100
            price_comparison = any(term in nl_query for term in [
                "highest price", "highest priced", "most expensive",
                "lowest price", "cheapest", "price ranking"
            ])

            for entity, table in self.entity_mapping.items():
                if entity in nl_query:
                    tables.append(table)

            if not tables:
                if any(word in nl_query for word in ["market", "exchange"]):
                    tables.append("markets")
                elif any(word in nl_query for word in ["trader", "client", "customer"]):
                    tables.append("traders")
                elif any(word in nl_query for word in ["asset", "stock", "security", "etf", "bond"]):
                    tables.append("assets")
                elif any(word in nl_query for word in ["trade", "trading"]):
                    tables.append("trades")
                elif any(word in nl_query for word in ["order"]):
                    tables.append("orders")
                elif any(word in nl_query for word in ["account", "balance"]):
                    tables.append("accounts")
                elif any(word in nl_query for word in ["price", "value", "cost"]):
                    tables.append("price_history")
                elif any(word in nl_query for word in ["transaction", "payment"]):
                    tables.append("transactions")
                elif any(word in nl_query for word in ["broker", "dealer"]):
                    tables.append("brokers")
                else:
                    tables_info = self.db_connector.get_all_tables()
                    if tables_info:
                        if isinstance(tables_info[0], dict):
                            first_table = list(tables_info[0].values())[0]
                        else:
                            first_table = tables_info[0]
                        tables.append(first_table)

            tables = list(dict.fromkeys(tables))

            if "markets" in tables:
                if detailed_request:
                    columns = ["market_id", "name", "location", "operating_hours"]
                else:
                    columns = ["name", "location", "operating_hours"]

            elif "brokers" in tables:
                if detailed_request:
                    columns = ["broker_id", "name"]
                    columns.append("license_number")
                    columns.append("contact_email")
                else:
                    columns = ["name"]

            elif "assets" in tables:
                if "brokers" not in tables:
                    tables.append("brokers")
                    conditions.append("assets.broker_id = brokers.broker_id")

                if price_comparison or any(term in nl_query for term in
                                           ["price", "cost", "expensive", "cheap", "value"]):
                    if "price_history" not in tables:
                        tables.append("price_history")
                        conditions.append("assets.asset_id = price_history.asset_id")

                if detailed_request:
                    columns = [
                        "assets.asset_id",
                        "assets.name",
                        "assets.asset_type",
                        "brokers.name AS broker_name"
                    ]
                    if "price_history" in tables:
                        columns.append("price_history.close_price")
                else:
                    columns = [
                        "assets.name",
                        "assets.asset_type",
                        "brokers.name AS broker_name"
                    ]
                    if "price_history" in tables:
                        columns.append("price_history.close_price")

            elif "traders" in tables:
                if detailed_request:
                    columns = [
                        "traders.trader_id",
                        "traders.name",
                        "traders.registration_date",
                        "traders.email",
                        "traders.phone"
                    ]
                else:
                    columns = [
                        "traders.name",
                        "traders.registration_date"
                    ]

            elif "trades" in tables:
                if "assets" not in tables:
                    tables.append("assets")
                    conditions.append("trades.asset_id = assets.asset_id")

                if "traders" not in tables:
                    tables.append("traders")
                    conditions.append("trades.trader_id = traders.trader_id")

                if "markets" not in tables:
                    tables.append("markets")
                    conditions.append("trades.market_id = markets.market_id")

                if detailed_request:
                    columns = [
                        "trades.trade_id",
                        "trades.trade_date",
                        "trades.quantity",
                        "trades.price",
                        "assets.name AS asset_name",
                        "traders.name AS trader_name",
                        "markets.name AS market_name"
                    ]
                else:
                    columns = [
                        "trades.trade_date",
                        "trades.quantity",
                        "trades.price",
                        "assets.name AS asset_name",
                        "traders.name AS trader_name",
                        "markets.name AS market_name"
                    ]

            elif "accounts" in tables:
                if "traders" not in tables:
                    tables.append("traders")
                    conditions.append("accounts.trader_id = traders.trader_id")

                if detailed_request:
                    columns = [
                        "accounts.account_id",
                        "traders.name AS trader_name",
                        "accounts.balance",
                        "accounts.account_type",
                        "accounts.creation_date"
                    ]
                else:
                    columns = [
                        "traders.name AS trader_name",
                        "accounts.account_type",
                        "accounts.balance"
                    ]

            elif "transactions" in tables:
                if "accounts" not in tables:
                    tables.append("accounts")
                    conditions.append("transactions.account_id = accounts.account_id")

                if detailed_request:
                    columns = [
                        "transactions.transaction_id",
                        "transactions.transaction_date",
                        "transactions.transaction_type",
                        "transactions.amount",
                        "accounts.account_id"
                    ]
                else:
                    columns = [
                        "transactions.transaction_date",
                        "transactions.transaction_type",
                        "transactions.amount"
                    ]

            elif "orders" in tables:
                if "trades" not in tables:
                    tables.append("trades")
                    conditions.append("orders.trade_id = trades.trade_id")

                if "order_status" not in tables:
                    tables.append("order_status")
                    conditions.append("orders.order_id = order_status.order_id")

                if detailed_request:
                    columns = [
                        "orders.order_id",
                        "orders.order_type",
                        "orders.order_date",
                        "order_status.status",
                        "trades.price"
                    ]
                else:
                    columns = [
                        "orders.order_type",
                        "orders.order_date",
                        "order_status.status"
                    ]

            if not columns:
                if "*" in nl_query:
                    columns = ["*"]
                else:

                    for table in tables:
                        if table == "markets":
                            columns.extend(["name", "location", "operating_hours"])
                        elif table == "brokers":
                            columns.extend(["name"])
                        elif table == "traders":
                            columns.extend(["name", "registration_date"])
                        elif table == "assets":
                            columns.extend(["name", "asset_type"])
                        elif table == "trades":
                            columns.extend(["trade_date", "quantity", "price"])
                        elif table == "accounts":
                            columns.extend(["account_type", "balance"])
                        elif table == "transactions":
                            columns.extend(["transaction_date", "transaction_type", "amount"])
                        elif table == "orders":
                            columns.extend(["order_type", "order_date"])
                        elif table == "order_status":
                            columns.extend(["status"])
                        elif table == "price_history":
                            columns.extend(["price_date", "open_price", "close_price"])

            if "highest" in nl_query or "most expensive" in nl_query:
                if "assets" in tables and "price_history" in tables:
                    order_by = ["price_history.close_price DESC"]
                    self.needs_highest_value_handling = "price_history.close_price"
                elif "trades" in tables:
                    order_by = ["trades.price DESC"]
                    self.needs_highest_value_handling = "trades.price"

            elif "lowest" in nl_query or "cheapest" in nl_query:
                if "assets" in tables and "price_history" in tables:
                    order_by = ["price_history.close_price ASC"]
                    self.needs_lowest_value_handling = "price_history.close_price"
                elif "trades" in tables:
                    order_by = ["trades.price ASC"]
                    self.needs_lowest_value_handling = "trades.price"

            elif "recent" in nl_query or "latest" in nl_query:
                if "trades" in tables:
                    order_by = ["trades.trade_date DESC"]
                elif "transactions" in tables:
                    order_by = ["transactions.transaction_date DESC"]
                elif "orders" in tables:
                    order_by = ["orders.order_date DESC"]

            if "type" in nl_query:
                if "assets" in tables:
                    if "stock" in nl_query:
                        conditions.append("assets.asset_type = 'Stock'")
                    elif "bond" in nl_query:
                        conditions.append("assets.asset_type = 'Bond'")
                    elif "etf" in nl_query:
                        conditions.append("assets.asset_type = 'ETF'")
                    elif "crypto" in nl_query or "cryptocurrency" in nl_query:
                        conditions.append("assets.asset_type = 'Cryptocurrency'")
                elif "accounts" in tables:
                    if "individual" in nl_query:
                        conditions.append("accounts.account_type = 'Individual'")
                    elif "corporate" in nl_query:
                        conditions.append("accounts.account_type = 'Corporate'")
                elif "orders" in tables:
                    if "buy" in nl_query:
                        conditions.append("orders.order_type = 'Buy'")
                    elif "sell" in nl_query:
                        conditions.append("orders.order_type = 'Sell'")

            if "where" in nl_query or "location" in nl_query:
                for location in ["new york", "london", "tokyo", "frankfurt", "hong kong", "sydney"]:
                    if location in nl_query:
                        if "markets" in tables:
                            conditions.append(f"markets.location LIKE '%{location}%'")

            if any(status in nl_query for status in ["complete", "completed", "pending", "cancelled"]):
                if "orders" in tables or "order" in nl_query:
                    if "order_status" not in tables:
                        tables.append("order_status")
                        conditions.append("orders.order_id = order_status.order_id")

                    if "complete" in nl_query or "completed" in nl_query:
                        conditions.append("order_status.status = 'Completed'")
                    elif "pending" in nl_query:
                        conditions.append("order_status.status = 'Pending'")
                    elif "cancelled" in nl_query or "canceled" in nl_query:
                        conditions.append("order_status.status = 'Cancelled'")

            for comparator in ["greater than", "more than", "less than", "at least", "at most"]:
                if comparator in nl_query:
                    pattern = f"{comparator}\\s+(\\d+)"
                    matches = re.search(pattern, nl_query)
                    if matches:
                        value = matches.group(1)
                        if "price" in nl_query and "assets" in tables:
                            if comparator in ["greater than", "more than", "at least"]:
                                conditions.append(f"assets.price > {value}")
                            else:
                                conditions.append(f"assets.price < {value}")
                        elif "balance" in nl_query and "accounts" in tables:
                            if comparator in ["greater than", "more than", "at least"]:
                                conditions.append(f"accounts.balance > {value}")
                            else:
                                conditions.append(f"accounts.balance < {value}")
                        elif "quantity" in nl_query and "trades" in tables:
                            if comparator in ["greater than", "more than", "at least"]:
                                conditions.append(f"trades.quantity > {value}")
                            else:
                                conditions.append(f"trades.quantity < {value}")

            if "top" in nl_query:
                pattern = r"top\s+(\d+)"
                matches = re.search(pattern, nl_query)
                if matches:
                    limit = int(matches.group(1))
                else:
                    limit = 5

            if self.needs_highest_value_handling or self.needs_lowest_value_handling:
                value_field = self.needs_highest_value_handling or self.needs_lowest_value_handling
                is_highest = bool(self.needs_highest_value_handling)

                subquery_table_clause = f"FROM {', '.join(tables)}"
                subquery_condition_clause = ""
                if conditions:
                    subquery_condition_clause = f"WHERE {' AND '.join(conditions)}"

                agg_function = "MAX" if is_highest else "MIN"
                get_extreme_sql = f"SELECT {agg_function}({value_field}) as extreme_value {subquery_table_clause} {subquery_condition_clause}"

                self.logger.info(
                    f"Executing subquery to find {'highest' if is_highest else 'lowest'} value: {get_extreme_sql}")

                extreme_result = self.db_connector.execute_query(get_extreme_sql)

                if extreme_result and isinstance(extreme_result, list) and len(extreme_result) > 0:
                    extreme_value = extreme_result[0].get('extreme_value')
                    if extreme_value is not None:
                        self.logger.info(f"Found {'highest' if is_highest else 'lowest'} value: {extreme_value}")

                        conditions.append(f"{value_field} = {extreme_value}")

                        limit = 100
                        self.logger.info(
                            f"Modified query to return all records with {'highest' if is_highest else 'lowest'} value")

            if len(columns) > 0:
                select_clause = f"{action_type} {', '.join(columns)}"
            else:
                select_clause = f"{action_type} *"

            from_clause = f"FROM {', '.join(tables)}"

            where_clause = ""
            if conditions:
                where_clause = f"WHERE {' AND '.join(conditions)}"

            order_by_clause = ""
            if order_by:
                order_by_clause = f"ORDER BY {', '.join(order_by)}"

            limit_clause = f"LIMIT {limit}"

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            if order_by_clause:
                sql_parts.append(order_by_clause)
            sql_parts.append(limit_clause)

            sql = " ".join(sql_parts)
            self.logger.info(f"Generated SQL query: {sql}")
            return sql

        except Exception as e:
            self.logger.error(f"Error converting natural language to SQL: {e}")
            return None

    def _check_for_name_search(self, nl_query):

        trader_names = self._get_entity_names("traders")
        broker_names = self._get_entity_names("brokers")
        market_names = self._get_entity_names("markets")
        asset_names = self._get_entity_names("assets")

        all_names = []

        for name in trader_names:
            all_names.append((name, "trader", len(name)))
        for name in broker_names:
            all_names.append((name, "broker", len(name)))
        for name in market_names:
            all_names.append((name, "market", len(name)))
        for name in asset_names:
            all_names.append((name, "asset", len(name)))

        all_names.sort(key=lambda x: x[2], reverse=True)

        for name, entity_type, _ in all_names:
            if name.lower() in nl_query:
                return entity_type, name

        return None, None

    def _get_entity_names(self, table_name):
        try:
            cache_attr = f'_cached_{table_name}_names'
            if hasattr(self, cache_attr):
                return getattr(self, cache_attr)

            query = f"SELECT name FROM {table_name}"
            results = self.db_connector.execute_query(query)

            if not results:
                return []

            names = [result['name'] for result in results]
            setattr(self, cache_attr, names)
            return names
        except Exception as e:
            self.logger.error(f"Error fetching {table_name} names: {e}")
            return []
    def _generate_detailed_entity_query(self, entity_type, name_match):
        if entity_type == "trader":
            return f"""
                SELECT 
                    t.trader_id, 
                    t.name, 
                    t.email, 
                    t.phone, 
                    t.registration_date,
                    a.account_id,
                    a.account_type,
                    a.balance,
                    a.creation_date,
                    COUNT(DISTINCT tr.trade_id) as trade_count,
                    SUM(tr.quantity) as total_trade_quantity,
                    SUM(tr.price * tr.quantity) as total_trade_value
                FROM 
                    traders t
                    LEFT JOIN accounts a ON t.trader_id = a.trader_id
                    LEFT JOIN trades tr ON t.trader_id = tr.trader_id
                WHERE 
                    LOWER(t.name) LIKE '%{name_match.lower()}%'
                GROUP BY 
                    t.trader_id, a.account_id
                LIMIT 100
            """

        elif entity_type == "broker":
            return f"""
                SELECT 
                    b.broker_id, 
                    b.name, 
                    b.license_number, 
                    b.contact_email,
                    COUNT(a.asset_id) as asset_count,
                    GROUP_CONCAT(DISTINCT a.asset_type) as asset_types,
                    GROUP_CONCAT(DISTINCT a.name) as assets_managed
                FROM 
                    brokers b
                    LEFT JOIN assets a ON b.broker_id = a.broker_id
                WHERE 
                    LOWER(b.name) LIKE '%{name_match.lower()}%'
                GROUP BY 
                    b.broker_id
                LIMIT 100
            """

        elif entity_type == "market":
            return f"""
                SELECT 
                    m.market_id, 
                    m.name, 
                    m.location, 
                    m.operating_hours,
                    COUNT(t.trade_id) as trade_count,
                    SUM(t.quantity) as total_volume,
                    AVG(t.price) as avg_trade_price,
                    MIN(t.trade_date) as earliest_trade,
                    MAX(t.trade_date) as latest_trade
                FROM 
                    markets m
                    LEFT JOIN trades t ON m.market_id = t.market_id
                WHERE 
                    LOWER(m.name) LIKE '%{name_match.lower()}%'
                GROUP BY 
                    m.market_id
                LIMIT 100
            """

        elif entity_type == "asset":
            return f"""
                SELECT 
                    a.asset_id, 
                    a.name, 
                    a.asset_type,
                    b.name as broker_name,
                    p.price_date,
                    p.open_price,
                    p.close_price,
                    COUNT(t.trade_id) as trade_count,
                    SUM(t.quantity) as total_traded_quantity,
                    AVG(t.price) as avg_trade_price
                FROM 
                    assets a
                    JOIN brokers b ON a.broker_id = b.broker_id
                    LEFT JOIN price_history p ON a.asset_id = p.asset_id
                    LEFT JOIN trades t ON a.asset_id = t.asset_id
                WHERE 
                    LOWER(a.name) LIKE '%{name_match.lower()}%'
                GROUP BY 
                    a.asset_id, p.price_id
                LIMIT 100
            """

        return self._generate_basic_entity_query(entity_type, name_match)

    def _generate_basic_entity_query(self, entity_type, name_match):
        if entity_type == "trader":
            return f"SELECT name, registration_date FROM traders WHERE LOWER(name) LIKE '%{name_match.lower()}%' LIMIT 100"

        elif entity_type == "broker":
            return f"SELECT name FROM brokers WHERE LOWER(name) LIKE '%{name_match.lower()}%' LIMIT 100"

        elif entity_type == "market":
            return f"SELECT name, location, operating_hours FROM markets WHERE LOWER(name) LIKE '%{name_match.lower()}%' LIMIT 100"

        elif entity_type == "asset":
            return f"""
                SELECT 
                    assets.name, 
                    assets.asset_type, 
                    brokers.name AS broker_name,
                    price_history.close_price
                FROM 
                    assets 
                    JOIN brokers ON assets.broker_id = brokers.broker_id
                    LEFT JOIN price_history ON assets.asset_id = price_history.asset_id
                WHERE 
                    LOWER(assets.name) LIKE '%{name_match.lower()}%'
                LIMIT 100
            """

        return f"SELECT * FROM {entity_type}s WHERE LOWER(name) LIKE '%{name_match.lower()}%' LIMIT 100"

    def validate_query(self, query):
        dangerous_patterns = [
            r";\s*--",
            r";\s*DROP",
            r"UNION\s+ALL\s+SELECT",
            r"OR\s+1\s*=\s*1",
            r"OR\s+'1'\s*=\s*'1'",
            r";\s*INSERT",
            r";\s*UPDATE",
            r";\s*DELETE",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                self.logger.warning(f"Potentially dangerous query detected: {query}")
                return False

        return True

    def deduplicate_results(self, results):

        if not results or not isinstance(results, list) or len(results) <= 1:
            return results

        self.logger.info(f"Deduplicating {len(results)} results")

        id_fields = []
        for field in results[0].keys():
            if field.endswith('_id'):
                id_fields.append(field)

        name_fields = [field for field in results[0].keys() if 'name' in field.lower()]
        date_fields = [field for field in results[0].keys() if 'date' in field.lower()]
        location_fields = [field for field in results[0].keys() if field in ['location']]

        exclude_from_comparison = []
        exclude_from_comparison.extend(id_fields)

        significant_fields = []
        significant_fields.extend(name_fields)
        significant_fields.extend(location_fields)

        has_name_fields = bool(name_fields)

        deduplicated = []
        seen_signatures = set()

        for record in results:

            signature_parts = []
            for key, value in sorted(record.items()):
                if key not in exclude_from_comparison:
                    signature_parts.append(f"{key}:{value}")

            record_signature = tuple(signature_parts)

            if has_name_fields and record_signature in seen_signatures:
                existing_names = {r[name_fields[0]] for r in deduplicated if
                                  record_signature == self._get_signature(r, exclude_from_comparison)}
                current_name = record[name_fields[0]]

                if current_name not in existing_names:
                    deduplicated.append(record)
                    self.logger.info(f"Kept record with unique name: {current_name}")
                else:
                    self.logger.info(f"Skipped duplicate with name: {current_name}")
                    continue
            else:
                deduplicated.append(record)
                seen_signatures.add(record_signature)

        removed_count = len(results) - len(deduplicated)
        self.logger.info(f"Deduplication removed {removed_count} records, keeping {len(deduplicated)}")

        return deduplicated

    def _get_signature(self, record, exclude_fields):
        signature_parts = []
        for key, value in sorted(record.items()):
            if key not in exclude_fields:
                signature_parts.append(f"{key}:{value}")
        return tuple(signature_parts)

    def natural_language_to_sql_enhanced(self, nl_query):
        try:
            self.logger.info(f"Processing enhanced natural language query: '{nl_query}'")
            nl_query = nl_query.lower()

            action_type = "SELECT"
            tables = []
            columns = []
            conditions = []
            order_by = []
            limit = 100
            limit_clause = None

            self.needs_highest_value_handling = None
            self.needs_lowest_value_handling = None
            self.needs_middle_value_handling = None

            for entity, table in self.entity_mapping.items():
                if entity in nl_query:
                    tables.append(table)

            if not tables:
                if any(word in nl_query for word in ["market", "exchange"]):
                    tables.append("markets")
                elif any(word in nl_query for word in ["trader", "client", "customer"]):
                    tables.append("traders")
                elif any(word in nl_query for word in ["asset", "stock", "security", "etf", "bond"]):
                    tables.append("assets")
                elif any(word in nl_query for word in ["trade", "trading"]):
                    tables.append("trades")
                elif any(word in nl_query for word in ["order"]):
                    tables.append("orders")
                elif any(word in nl_query for word in ["account", "balance"]):
                    tables.append("accounts")
                elif any(word in nl_query for word in ["price", "value", "cost"]):
                    tables.append("price_history")
                elif any(word in nl_query for word in ["transaction", "payment"]):
                    tables.append("transactions")
                elif any(word in nl_query for word in ["broker", "dealer"]):
                    tables.append("brokers")
                else:
                    tables_info = self.db_connector.get_all_tables()
                    if tables_info:
                        if isinstance(tables_info[0], dict):
                            first_table = list(tables_info[0].values())[0]
                        else:
                            first_table = tables_info[0]
                        tables.append(first_table)

            tables = list(dict.fromkeys(tables))

            if "assets" in tables:
                if "brokers" not in tables:
                    tables.append("brokers")
                    conditions.append("assets.broker_id = brokers.broker_id")

                if any(term in nl_query for term in ["price", "value", "cost", "expensive", "highest", "lowest"]):
                    if "price_history" not in tables:
                        tables.append("price_history")
                        conditions.append("assets.asset_id = price_history.asset_id")

                columns.extend([
                    "assets.asset_id",
                    "assets.name AS asset_name",
                    "assets.asset_type",
                    "brokers.name AS broker_name"
                ])

                if "price_history" in tables:
                    columns.append("price_history.close_price")

            elif "markets" in tables:
                columns.extend([
                    "markets.name AS market_name",
                    "markets.location",
                    "markets.operating_hours"
                ])

            elif "brokers" in tables:
                columns.extend([
                    "brokers.name AS broker_name"
                ])

            elif "traders" in tables:
                columns.extend([
                    "traders.name AS trader_name",
                    "traders.registration_date"
                ])

            elif "trades" in tables:
                if "assets" not in tables:
                    tables.append("assets")
                    conditions.append("trades.asset_id = assets.asset_id")

                if "traders" not in tables:
                    tables.append("traders")
                    conditions.append("trades.trader_id = traders.trader_id")

                if "markets" not in tables:
                    tables.append("markets")
                    conditions.append("trades.market_id = markets.market_id")

                columns.extend([
                    "trades.trade_date",
                    "trades.quantity",
                    "trades.price",
                    "assets.name AS asset_name",
                    "traders.name AS trader_name",
                    "markets.name AS market_name"
                ])

            if not columns:
                columns = ["*"]

            tables, columns, conditions, order_by, comparative_modified, comparative_limit = self.handle_comparative_queries(
                nl_query, tables, columns, conditions, order_by, limit
            )

            select_clause = f"{action_type} {', '.join(columns)}"
            from_clause = f"FROM {', '.join(tables)}"

            where_clause = ""
            if conditions:
                where_clause = f"WHERE {' AND '.join(conditions)}"

            order_by_clause = ""
            if order_by:
                order_by_clause = f"ORDER BY {', '.join(order_by)}"

            if limit_clause is None:
                limit_clause = f"LIMIT {limit}"

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            if order_by_clause:
                sql_parts.append(order_by_clause)
            sql_parts.append(limit_clause)

            sql = " ".join(sql_parts)
            self.logger.info(f"Generated enhanced SQL query: {sql}")
            return sql

        except Exception as e:
            self.logger.error(f"Error converting natural language to enhanced SQL: {e}")
            return None

    def secure_process_query(self, user_query):
        try:
            needs_broker_info = any(term in user_query.lower() for term in ["asset", "stock", "bond", "etf", "crypto"])
            needs_price_info = any(
                term in user_query.lower() for term in ["price", "value", "cost", "expensive", "highest", "lowest"])
            needs_market_info = any(term in user_query.lower() for term in ["market", "exchange", "trading venue"])

            if needs_broker_info:
                sql_query = self.natural_language_to_sql_enhanced(user_query)
            else:
                sql_query = self.natural_language_to_sql(user_query)

            if not sql_query:
                return None

            self.logger.info(f"Generated SQL query: {sql_query}")

            if sql_query.strip().upper().startswith("SELECT"):
                results = self.db_connector.execute_query(sql_query)

                if not results:
                    return {"message": "No results found"}

                processed_results = []
                for row in results:
                    processed_row = {}
                    for key, value in row.items():
                        if self.dev_mode and self._should_encrypt_field(key):
                            processed_row[key] = f"[ENCRYPTED:{value}]"
                        elif not self.dev_mode and self._should_encrypt_field(key):
                            encrypted_value = self.encryption_manager.encrypt_data(value)
                            processed_row[key] = self._make_serializable(encrypted_value)
                        else:
                            processed_row[key] = value
                    processed_results.append(processed_row)

                if self.needs_highest_value_handling or self.needs_lowest_value_handling or self.needs_middle_value_handling:
                    deduplicated_results = self.deduplicate_results(processed_results)
                    return deduplicated_results
                else:
                    return processed_results
            else:
                if self.validate_query(sql_query):
                    return self.db_connector.execute_query(sql_query)
                else:
                    return {"error": "Invalid or unauthorized query"}
        except Exception as e:
            self.logger.error(f"Error processing secure query: {e}")
            return {"error": f"Error processing query: {str(e)}"}
