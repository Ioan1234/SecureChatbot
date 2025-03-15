import logging
import re
import json


class QueryProcessor:
    def __init__(self, db_connector, encryption_manager, sensitive_fields=None):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.encryption_manager = encryption_manager
        self.sensitive_fields = sensitive_fields or [
            "email", "contact_email", "phone", "license_number", "balance"
        ]

        self.dev_mode = True

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

    def process_query(self, nl_query, intent_data):
        """Process queries based on the classified intent"""
        intent = intent_data.get('intent', 'database_query_list')
        confidence = intent_data.get('confidence', 0.0)

        self.logger.info(f"Processing query with intent: {intent}, confidence: {confidence}")

        if confidence < 0.5:
            self.logger.warning(f"Low confidence ({confidence}) for intent: {intent}. Using generic processing.")
            return self._handle_generic_query(nl_query)

        handlers = {
            "database_query_list": self._handle_list_query,
            "database_query_count": self._handle_count_query,
            "database_query_detailed": self._handle_detailed_query,
            "database_query_recent": self._handle_recent_query,
            "database_query_filter": self._handle_filter_query,
            "database_query_sort": self._handle_sort_query,
            "database_query_sort_ascending": self._handle_sort_ascending_query,
            "database_query_sort_descending": self._handle_sort_descending_query,
            "database_query_comparative_highest": self._handle_highest_query,
            "database_query_comparative_lowest": self._handle_lowest_query,
            "database_query_comparative_middle": self._handle_middle_query,
            "database_query_specific_id": self._handle_specific_id_query,
            "database_query_sensitive": self._handle_sensitive_query
        }

        handler = handlers.get(intent, self._handle_generic_query)

        return handler(nl_query)

    def _extract_tables(self, nl_query):
        """Extract table names from the query using entity mapping"""
        tables = []
        nl_query = nl_query.lower()

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

        return list(set(tables))

    def _extract_number_from_query(self, nl_query):
        """Extract a number from the query text"""
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
                return num

        all_numbers = re.findall(r'\b(\d+)\b', nl_query)
        if all_numbers:
            return int(all_numbers[0])

        return None

    def _extract_fields(self, nl_query, tables):
        """Extract relevant fields based on the query and tables"""
        fields = []

        if "*" in nl_query or "all fields" in nl_query.lower():
            return ["*"]

        price_related = any(
            term in nl_query.lower() for term in ["price", "cost", "value", "worth", "expensive", "cheap"])
        date_related = any(term in nl_query.lower() for term in ["date", "time", "when", "recent", "latest", "newest"])
        name_related = any(term in nl_query.lower() for term in ["name", "called", "named"])

        for table in tables:
            if price_related:
                if table == "price_history":
                    fields.extend(["price_history.price_date", "price_history.close_price"])
                elif table == "trades":
                    fields.extend(["trades.trade_date", "trades.price"])
                elif table == "assets":
                    fields.append("assets.name")
                    if "price_history" not in tables:
                        tables.append("price_history")
                        fields.append("price_history.close_price")

            if date_related:
                if table == "trades":
                    fields.append("trades.trade_date")
                elif table == "orders":
                    fields.append("orders.order_date")
                elif table == "transactions":
                    fields.append("transactions.transaction_date")
                elif table == "price_history":
                    fields.append("price_history.price_date")

            if name_related or not fields:
                if table == "traders":
                    fields.extend(["traders.name", "traders.registration_date"])
                elif table == "brokers":
                    fields.append("brokers.name")
                elif table == "assets":
                    fields.extend(["assets.name", "assets.asset_type"])
                elif table == "markets":
                    fields.extend(["markets.name", "markets.location"])

        if not fields:
            for table in tables:
                essential = self.get_essential_fields(table)
                for field in essential:
                    fields.append(f"{table}.{field}")

        return fields

    def _handle_generic_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            self.logger.warning("No tables identified in the query")
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated sort ascending SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_specific_id_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        id_match = re.search(r'(\w+)\s+(\d+)', nl_query)
        if not id_match:
            all_numbers = re.findall(r'\d+', nl_query)
            if all_numbers:
                id_value = all_numbers[0]
                table = tables[0]
            else:
                return self._handle_generic_query(nl_query)
        else:
            entity, id_value = id_match.groups()
            table = self.entity_mapping.get(entity.lower(), tables[0])

        table_fields = self.table_info.get(table, [])
        fields = [f"{table}.{field}" for field in table_fields
                  if not self._should_encrypt_field(field)]

        select_clause = f"SELECT {', '.join(fields)}" if fields else f"SELECT *"
        from_clause = f"FROM {table}"

        id_field = f"{table}_id"
        where_clause = f"WHERE {id_field} = {id_value}"

        sql_parts = [select_clause, from_clause, where_clause]

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated specific ID SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_sensitive_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        all_fields = []
        for table in tables:
            table_fields = self.table_info.get(table, [])
            all_fields.extend([f"{table}.{field}" for field in table_fields])

        select_clause = f"SELECT {', '.join(all_fields)}" if all_fields else "SELECT *"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated sensitive data SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_middle_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_superlative_field(nl_query, tables, "middle")

        limit = self._extract_number_from_query(nl_query) or 10

        if sort_field:

            order_by_clause = f"ORDER BY {sort_field}"

            limit_clause = f"LIMIT {limit} OFFSET (SELECT COUNT(*)/2 - {limit}/2 FROM {', '.join(tables)})"

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            sql_parts.append(order_by_clause)
            sql_parts.append(limit_clause)

            sql = " ".join(sql_parts)
            self.logger.info(f"Generated middle value SQL: {sql}")

            return self._execute_and_process_query(sql)
        else:
            limit_clause = f"LIMIT {limit} OFFSET (SELECT COUNT(*)/2 - {limit}/2 FROM {', '.join(tables)})"

            sql_parts = [select_clause, from_clause]
            if where_clause:
                sql_parts.append(where_clause)
            sql_parts.append(limit_clause)

            sql = " ".join(sql_parts)
            self.logger.info(f"Generated basic middle value SQL: {sql}")

            return self._execute_and_process_query(sql)

    def _handle_highest_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_superlative_field(nl_query, tables, "highest")

        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field} DESC"

        limit = self._extract_number_from_query(nl_query) or 10
        limit_clause = f"LIMIT {limit}"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated highest value SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_lowest_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_superlative_field(nl_query, tables, "lowest")

        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field} ASC"

        limit = self._extract_number_from_query(nl_query) or 10
        limit_clause = f"LIMIT {limit}"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated lowest value SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_sort_descending_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_sort_field(nl_query, tables)

        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field} DESC"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated sort descending SQL: {sql}")

        return self._execute_and_process_query(sql)
        self.logger.info(f"Generated generic SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_list_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            self.logger.warning("No tables identified in the listing query")
            return None

        all_fields = []
        for table in tables:
            essential_fields = self.get_essential_fields(table)
            all_fields.extend([f"{table}.{field}" for field in essential_fields])

        select_clause = f"SELECT {', '.join(all_fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated listing SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_detailed_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        all_fields = []
        for table in tables:
            table_fields = self.table_info.get(table, [])
            all_fields.extend([f"{table}.{field}" for field in table_fields
                               if not self._should_encrypt_field(field)])

        select_clause = f"SELECT {', '.join(all_fields)}" if all_fields else "SELECT *"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated detailed SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_count_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        select_clause = "SELECT COUNT(*) as count"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated count SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_recent_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        date_field = None
        for table in tables:
            if table == "trades":
                date_field = "trades.trade_date"
            elif table == "orders":
                date_field = "orders.order_date"
            elif table == "transactions":
                date_field = "transactions.transaction_date"
            elif table == "price_history":
                date_field = "price_history.price_date"

        if not date_field:
            date_related_tables = ["trades", "orders", "transactions", "price_history"]
            for table in date_related_tables:
                if table in tables:
                    date_field = f"{table}.{table[:-1]}_date"
                    break

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        order_by_clause = ""
        if date_field:
            order_by_clause = f"ORDER BY {date_field} DESC"

        limit_clause = "LIMIT 20"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated recent SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_filter_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        join_conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        join_conditions.append(join_condition)

        filter_conditions = self._extract_filter_conditions(nl_query, tables)

        all_conditions = join_conditions + filter_conditions

        where_clause = ""
        if all_conditions:
            where_clause = f"WHERE {' AND '.join(all_conditions)}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated filter SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_sort_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_sort_field(nl_query, tables)

        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated sort SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_sort_ascending_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        fields = self._extract_fields(nl_query, tables)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {', '.join(tables)}"

        conditions = []
        if len(tables) > 1:
            for i, table1 in enumerate(tables):
                for table2 in tables[i + 1:]:
                    join_condition = self._get_join_condition(table1, table2)
                    if join_condition:
                        conditions.append(join_condition)

        where_clause = ""
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"

        sort_field = self._extract_sort_field(nl_query, tables)

        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field} ASC"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)