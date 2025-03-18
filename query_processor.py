import logging
import re


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
            "trading venues": "markets",
            "exchanges": "markets",

            "trader": "traders",
            "traders": "traders",
            "trading users": "traders",
            "trade accounts": "traders",
            "trader profiles": "traders",
            "customers": "traders",
            "clients": "traders",

            "broker": "brokers",
            "brokers": "brokers",
            "broker accounts": "brokers",
            "broker profiles": "brokers",
            "intermediaries": "brokers",
            "broker companies": "brokers",

            "asset": "assets",
            "assets": "assets",
            "stock": "assets",
            "stocks": "assets",
            "bond": "assets",
            "bonds": "assets",
            "securities": "assets",
            "financial instruments": "assets",
            "investment options": "assets",

            "trade": "trades",
            "trades": "trades",
            "trading": "trades",
            "trading activity": "trades",
            "market activity": "trades",
            "trade records": "trades",
            "deals": "trades",

            "transaction": "transactions",
            "transactions": "transactions",
            "account activity": "transactions",
            "money transfers": "transactions",
            "payments": "transactions",
            "financial transactions": "transactions",

            "account": "accounts",
            "accounts": "accounts",
            "financial accounts": "accounts",
            "trading accounts": "accounts",
            "user accounts": "accounts",

            "order": "orders",
            "orders": "orders",
            "trade orders": "orders",
            "market orders": "orders",
            "order records": "orders",
            "buy orders": "orders",
            "sell orders": "orders",

            "status": "order_status",
            "order status": "order_status",
            "order statuses": "order_status",
            "status of orders": "order_status",
            "order states": "order_status",
            "statuses": "order_status",

            "price": "price_history",
            "price history": "price_history",
            "prices": "price_history",
            "historical prices": "price_history",
            "price records": "price_history",
            "past prices": "price_history",
            "historical pricing": "price_history"
        }

        self.essential_fields = {
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

        self.attribute_mapping = {
            "price": {"tables": ["trades", "price_history"], "fields": ["trades.price", "price_history.close_price"]},
            "cost": {"tables": ["trades", "price_history"], "fields": ["trades.price", "price_history.close_price"]},
            "value": {"tables": ["trades", "price_history"], "fields": ["trades.price", "price_history.close_price"]},
            "date": {"tables": ["trades", "orders", "transactions", "price_history"],
                     "fields": ["trades.trade_date", "orders.order_date", "transactions.transaction_date",
                                "price_history.price_date"]},
            "time": {"tables": ["trades", "orders", "transactions", "price_history"],
                     "fields": ["trades.trade_date", "orders.order_date", "transactions.transaction_date",
                                "price_history.price_date"]},
            "name": {"tables": ["traders", "brokers", "assets", "markets"],
                     "fields": ["traders.name", "brokers.name", "assets.name", "markets.name"]},
            "location": {"tables": ["markets"], "fields": ["markets.location"]},
            "type": {"tables": ["assets", "accounts", "orders", "transactions"],
                     "fields": ["assets.asset_type", "accounts.account_type", "orders.order_type",
                                "transactions.transaction_type"]},
            "quantity": {"tables": ["trades"], "fields": ["trades.quantity"]},
            "amount": {"tables": ["transactions"], "fields": ["transactions.amount"]},
            "volume": {"tables": ["trades"], "fields": ["trades.quantity"]},
            "balance": {"tables": ["accounts"], "fields": ["accounts.balance"]},
            "status": {"tables": ["order_status"], "fields": ["order_status.status"]},

            "transaction_count": {"aggregate": True, "function": "COUNT", "tables": ["transactions"],
                                  "fields": ["transactions.transaction_id"]},
            "trade_count": {"aggregate": True, "function": "COUNT", "tables": ["trades"],
                            "fields": ["trades.trade_id"]},
            "account_count": {"aggregate": True, "function": "COUNT", "tables": ["accounts"],
                              "fields": ["accounts.account_id"]},
            "average_price": {"aggregate": True, "function": "AVG", "tables": ["trades"], "fields": ["trades.price"]},
            "total_amount": {"aggregate": True, "function": "SUM", "tables": ["transactions"],
                             "fields": ["transactions.amount"]},
            "total_quantity": {"aggregate": True, "function": "SUM", "tables": ["trades"],
                               "fields": ["trades.quantity"]}
        }
        self.table_graph = {
            "traders": {"trades": "trader_id", "accounts": "trader_id"},
            "trades": {"traders": "trader_id", "assets": "asset_id", "markets": "market_id", "orders": "trade_id"},
            "assets": {"trades": "asset_id", "brokers": "broker_id", "price_history": "asset_id"},
            "markets": {"trades": "market_id"},
            "accounts": {"traders": "trader_id", "transactions": "account_id"},
            "transactions": {"accounts": "account_id"},
            "orders": {"trades": "trade_id", "order_status": "order_id"},
            "order_status": {"orders": "order_id"},
            "brokers": {"assets": "broker_id"},
            "price_history": {"assets": "asset_id"}
        }
        self._initialize_table_relationships()

    def _extract_sort_field(self, nl_query, tables):
        nl_query = nl_query.lower()

        sort_indicators = ["sort by", "ordered by", "arranged by", "in order of"]
        for indicator in sort_indicators:
            if indicator in nl_query:
                parts = nl_query.split(indicator)
                if len(parts) > 1:
                    field_text = parts[1].strip().split()[0]

                    field_mappings = {
                        "price": "price",
                        "cost": "price",
                        "value": "price",
                        "date": "trade_date",
                        "time": "trade_date",
                        "name": "name",
                        "type": "asset_type",
                        "quantity": "quantity",
                        "amount": "amount",
                        "volume": "quantity",
                        "balance": "balance"
                    }

                    field = field_mappings.get(field_text, field_text)

                    for table in tables:
                        table_fields = self.table_info.get(table, [])
                        for table_field in table_fields:
                            if field in table_field:
                                return f"{table}.{table_field}"

        default_sort_fields = {
            "trades": "trades.trade_date",
            "orders": "orders.order_date",
            "assets": "assets.name",
            "traders": "traders.name",
            "markets": "markets.name",
            "transactions": "transactions.transaction_date",
            "accounts": "accounts.balance",
            "price_history": "price_history.price_date"
        }

        for table in tables:
            if table in default_sort_fields:
                return default_sort_fields[table]

        return None
    def _get_complete_field_set(self, tables):

        fields = []

        for table in tables:
            if table in self.table_info:
                for field in self.table_info[table]:
                    if not self._should_encrypt_field(field):
                        fields.append(f"{table}.{field}")

        return list(set(fields))

    def _get_related_tables(self, primary_table):
        related_tables = []

        for (table1, table2), relationship in self.table_relationships.items():
            if table1 == primary_table and table2 not in related_tables:
                related_tables.append(table2)
            elif table2 == primary_table and table1 not in related_tables:
                related_tables.append(table1)

        return related_tables

    def _extend_tables_for_query(self, tables, nl_query):
        nl_query = nl_query.lower()
        extended_tables = tables.copy()

        if "price" in nl_query and "assets" in extended_tables and "price_history" not in extended_tables:
            extended_tables.append("price_history")

        if "balance" in nl_query and "traders" in extended_tables and "accounts" not in extended_tables:
            extended_tables.append("accounts")

        if "transaction" in nl_query and "traders" in extended_tables:
            if "accounts" not in extended_tables:
                extended_tables.append("accounts")
            if "transactions" not in extended_tables:
                extended_tables.append("transactions")

        if "trade" in nl_query and "date" in nl_query and "trades" not in extended_tables:
            extended_tables.append("trades")

        for table in tables.copy():
            related = self._get_related_tables(table)
            for related_table in related:
                related_words = self._get_related_keywords(related_table)
                if any(word in nl_query for word in related_words) and related_table not in extended_tables:
                    extended_tables.append(related_table)

        return extended_tables

    def _get_related_keywords(self, table_name):

        keywords = [table_name]

        if table_name.endswith('s'):
            keywords.append(table_name[:-1])
        else:
            keywords.append(table_name + 's')

        table_keywords = {
            'traders': ['trader', 'client', 'customer', 'user'],
            'accounts': ['account', 'balance', 'money'],
            'assets': ['asset', 'stock', 'security', 'investment'],
            'trades': ['trade', 'transaction', 'deal'],
            'price_history': ['price', 'cost', 'value'],
            'transactions': ['transaction', 'payment', 'transfer'],
            'markets': ['market', 'exchange'],
            'brokers': ['broker', 'dealer', 'agent'],
            'orders': ['order', 'purchase', 'sale'],
            'order_status': ['status', 'state', 'condition']
        }

        if table_name in table_keywords:
            keywords.extend(table_keywords[table_name])

        return keywords

    def _handle_aggregation_query(self, nl_query, tables, aggregation_table, aggregation_type):
        primary_table = None
        for table in tables:
            if table != aggregation_table:
                primary_table = table
                break

        if not primary_table:
            primary_table = tables[0]

        primary_fields = []
        for field in self.table_info.get(primary_table, []):
            if not self._should_encrypt_field(field):
                primary_fields.append(f"{primary_table}.{field}")

        if aggregation_type == "count":
            agg_field = f"COUNT({aggregation_table}.{aggregation_table[:-1]}_id) as {aggregation_table[:-1]}_count"
        else:
            agg_field = f"{aggregation_type.upper()}({aggregation_table}.amount) as total_amount"

        fields = primary_fields + [agg_field]

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {primary_table}"

        tables_in_query = [primary_table]

        join_clauses = []

        if aggregation_table != primary_table and aggregation_table not in tables_in_query:
            path = self._find_join_path(primary_table, aggregation_table)

            if path:
                for step in path:
                    from_table = step["from"]
                    to_table = step["to"]
                    key = step["key"]

                    if to_table not in tables_in_query:
                        join_clause = f"{to_table} ON {from_table}.{key} = {to_table}.{key}"
                        join_clauses.append(join_clause)
                        tables_in_query.append(to_table)

        for table in tables:
            if table not in tables_in_query:
                best_path = None
                start_table = None

                for joined_table in tables_in_query:
                    path = self._find_join_path(joined_table, table)
                    if path and (best_path is None or len(path) < len(best_path)):
                        best_path = path
                        start_table = joined_table

                if best_path:
                    for step in best_path:
                        from_table = step["from"]
                        to_table = step["to"]
                        key = step["key"]

                        if to_table not in tables_in_query:
                            join_clause = f"{to_table} ON {from_table}.{key} = {to_table}.{key}"
                            join_clauses.append(join_clause)
                            tables_in_query.append(to_table)

        if join_clauses:
            join_clause = " JOIN ".join(join_clauses)
            from_clause = f"{from_clause} JOIN {join_clause}"

        group_by_fields = [f"{primary_table}.{primary_table[:-1]}_id"]
        if f"{primary_table}.name" in primary_fields:
            group_by_fields.append(f"{primary_table}.name")

        group_by_clause = f"GROUP BY {', '.join(group_by_fields)}"

        nl_query = nl_query.lower()
        agg_column = f"{aggregation_table[:-1]}_count"

        if "highest" in nl_query or "most" in nl_query:
            order_by_clause = f"ORDER BY {agg_column} DESC"
        else:
            order_by_clause = f"ORDER BY {agg_column} ASC"

        limit_clause = "LIMIT 10"

        sql = f"{select_clause} {from_clause} {group_by_clause} {order_by_clause} {limit_clause}"

        self.logger.info(f"Generated aggregation SQL: {sql}")
        return self._execute_and_process_query(sql)
    def _process_query_generic(self, nl_query, intent_type=None, sub_intent=None):

        tables = self._extract_tables(nl_query)
        if not tables:
            self.logger.warning("No tables identified in query")
            return None

        extended_tables = self._extend_tables_for_query(tables, nl_query)

        fields = self._get_complete_field_set(extended_tables)

        if "transaction" in nl_query.lower() and "count" in nl_query.lower():
            return self._handle_aggregation_query(nl_query, extended_tables, "transactions", "count")

        if len(fields) == 0:
            self.logger.warning("No fields selected for query")
            return None

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {tables[0]}"

        join_clauses = self._generate_joins_for_tables(extended_tables)
        if join_clauses:
            join_clause = " JOIN ".join(join_clauses)
            from_clause = f"{from_clause} JOIN {join_clause}"

        sort_field = None
        sort_direction = None

        if intent_type == "database_query_sort" or sub_intent in ["database_query_sort_ascending",
                                                                  "database_query_sort_descending"]:
            sort_field = self._extract_sort_field(nl_query, extended_tables)
            if "descending" in nl_query.lower() or sub_intent == "database_query_sort_descending":
                sort_direction = "DESC"
            else:
                sort_direction = "ASC"
        elif intent_type == "database_query_comparative" or sub_intent in ["database_query_comparative_highest",
                                                                           "database_query_comparative_lowest",
                                                                           "database_query_comparative_middle"]:
            if sub_intent == "database_query_comparative_highest" or "highest" in nl_query.lower():
                sort_field = self._extract_superlative_field(nl_query, extended_tables, "highest")
                sort_direction = "DESC"
            elif sub_intent == "database_query_comparative_lowest" or "lowest" in nl_query.lower():
                sort_field = self._extract_superlative_field(nl_query, extended_tables, "lowest")
                sort_direction = "ASC"
            elif sub_intent == "database_query_comparative_middle" or "middle" in nl_query.lower() or "median" in nl_query.lower():
                return self._handle_middle_value_query(nl_query, extended_tables, fields)

        if (intent_type == "database_query_sort" or intent_type == "database_query_comparative") and not sort_field:
            sort_field = self._get_default_sort_field(extended_tables, nl_query)

        order_by_clause = ""
        if sort_field and sort_direction:
            order_by_clause = f"ORDER BY {sort_field} {sort_direction}"

        if intent_type == "database_query_comparative":
            limit = self._extract_number_from_query(nl_query) or 10
        else:
            limit = 100

        limit_clause = f"LIMIT {limit}"

        sql_parts = [select_clause, from_clause]
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated generic SQL: {sql}")

        return self._execute_and_process_query(sql)
    def _find_join_path(self, from_table, to_table, visited=None):
        if visited is None:
            visited = set()

        if from_table == to_table:
            return []

        if from_table in visited:
            return None

        visited.add(from_table)

        if to_table in self.table_graph.get(from_table, {}):
            return [{"from": from_table, "to": to_table, "key": self.table_graph[from_table][to_table]}]

        for neighbor, key in self.table_graph.get(from_table, {}).items():
            path = self._find_join_path(neighbor, to_table, visited.copy())
            if path:
                return [{"from": from_table, "to": neighbor, "key": key}] + path

        return None

    def _generate_joins_for_tables(self, tables):
        if not tables or len(tables) <= 1:
            return []

        join_clauses = []
        already_joined = {tables[0]}

        for target_table in tables[1:]:
            if target_table in already_joined:
                continue

            best_path = None
            start_table = None

            for joined_table in already_joined:
                path = self._find_join_path(joined_table, target_table)
                if path and (best_path is None or len(path) < len(best_path)):
                    best_path = path
                    start_table = joined_table

            if best_path:
                for step in best_path:
                    from_table = step["from"]
                    to_table = step["to"]
                    key = step["key"]

                    if to_table not in already_joined:
                        join_clause = f"{to_table} ON {from_table}.{key} = {to_table}.{key}"
                        join_clauses.append(join_clause)
                        already_joined.add(to_table)

        return join_clauses


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
        from_clause = f"FROM {tables[0]}"

        join_clauses = self._generate_joins_for_tables(tables)
        join_clause = " JOIN ".join(join_clauses)
        if join_clause:
            from_clause = f"{from_clause} JOIN {join_clause}"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause, limit_clause]

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated listing SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_count_query(self, nl_query):
        tables = self._extract_tables(nl_query)

        if not tables:
            return None

        select_clause = "SELECT COUNT(*) as count"
        from_clause = f"FROM {tables[0]}"

        join_clauses = self._generate_joins_for_tables(tables)
        join_clause = " JOIN ".join(join_clauses)
        if join_clause:
            from_clause = f"{from_clause} JOIN {join_clause}"

        sql_parts = [select_clause, from_clause]

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated count SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_highest_query(self, nl_query):
        tables = self._extract_tables(nl_query)
        if not tables:
            return None

        fields = []
        for table in tables:
            for field in self.table_info.get(table, []):
                if not self._should_encrypt_field(field):
                    fields.append(f"{table}.{field}")
        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {tables[0]}"

        join_clauses = self._generate_joins_for_tables(tables)
        join_clause = " JOIN ".join(join_clauses)
        if join_clause:
            from_clause = f"{from_clause} JOIN {join_clause}"

        sort_field = self._extract_sort_field(nl_query, tables)
        order_by_clause = ""
        if sort_field:
            order_by_clause = f"ORDER BY {sort_field} ASC"

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause]
        if order_by_clause:
            sql_parts.append(order_by_clause)
        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated sort ascending SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_middle_query(self, nl_query):
        tables = self._extract_tables(nl_query)
        if not tables:
            return None

        fields = []
        for table in tables:
            for field in self.table_info.get(table, []):
                if not self._should_encrypt_field(field):
                    fields.append(f"{table}.{field}")

        if "price" in nl_query.lower() and "asset" in nl_query.lower():
            if "assets" not in tables:
                tables.append("assets")
            if "price_history" not in tables:
                tables.append("price_history")

        return self._handle_middle_value_query(nl_query, tables, fields)

    def _handle_middle_value_query(self, nl_query, tables, fields):
        sort_field = self._extract_superlative_field(nl_query, tables, "middle")

        if not sort_field:
            sort_field = self._get_default_sort_field(tables, nl_query)

        if not sort_field:
            self.logger.warning("No sort field identified for median query")
            return self._process_query_generic(nl_query)

        select_clause = f"SELECT {', '.join(fields)}"
        from_clause = f"FROM {tables[0]}"

        join_clauses = self._generate_joins_for_tables(tables)
        if join_clauses:
            join_clause = " JOIN ".join(join_clauses)
            from_clause = f"{from_clause} JOIN {join_clause}"

        main_table = tables[0]
        count_sql = f"SELECT COUNT(*) as count FROM {main_table}"
        count_result = self.db_connector.execute_query(count_sql)

        if not count_result or not count_result[0].get('count', 0):
            self.logger.warning(f"Count query returned no results: {count_sql}")
            return None

        total_count = count_result[0]['count']

        requested_limit = self._extract_number_from_query(nl_query)
        limit = requested_limit or 3

        if total_count < limit * 2:
            limit = max(1, total_count // 3)
            middle_offset = max(0, (total_count // 2) - (limit // 2))
        else:
            middle_offset = max(0, (total_count // 2) - (limit // 2))

        order_by_clause = f"ORDER BY {sort_field}"
        limit_clause = f"LIMIT {middle_offset}, {limit}"

        sql_parts = [select_clause, from_clause, order_by_clause, limit_clause]
        sql = " ".join(sql_parts)

        self.logger.info(f"Generated middle value SQL: {sql}")
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
                return self._process_query_generic(nl_query)
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

    def _initialize_table_relationships(self):
        self.table_relationships = {
            ("traders", "accounts"): {
                "join": "traders.trader_id = accounts.trader_id",
                "cardinality": "one-to-many"
            },
            ("accounts", "transactions"): {
                "join": "accounts.account_id = transactions.account_id",
                "cardinality": "one-to-many"
            },
            ("assets", "price_history"): {
                "join": "assets.asset_id = price_history.asset_id",
                "cardinality": "one-to-many"
            },
            ("assets", "trades"): {
                "join": "assets.asset_id = trades.asset_id",
                "cardinality": "one-to-many"
            },
            ("traders", "trades"): {
                "join": "traders.trader_id = trades.trader_id",
                "cardinality": "one-to-many"
            },
            ("orders", "trades"): {
                "join": "orders.trade_id = trades.trade_id",
                "cardinality": "one-to-one"
            },
            ("markets", "trades"): {
                "join": "markets.market_id = trades.market_id",
                "cardinality": "one-to-many"
            },
            ("orders", "order_status"): {
                "join": "orders.order_id = order_status.order_id",
                "cardinality": "one-to-many"
            },
            ("brokers", "assets"): {
                "join": "brokers.broker_id = assets.broker_id",
                "cardinality": "one-to-many"
            }
        }

        reverse_relationships = {}
        for (table1, table2), relationship in self.table_relationships.items():
            join = relationship["join"]
            cardinality = relationship["cardinality"]
            if cardinality == "one-to-many":
                reverse_cardinality = "many-to-one"
            elif cardinality == "many-to-one":
                reverse_cardinality = "one-to-many"
            else:
                reverse_cardinality = cardinality

            reverse_relationships[(table2, table1)] = {
                "join": join,
                "cardinality": reverse_cardinality
            }

        self.table_relationships.update(reverse_relationships)

        self.indirect_relationships = {
            ("traders", "transactions"): {
                "path": [("traders", "accounts"), ("accounts", "transactions")],
                "description": "Traders make transactions through accounts"
            },
            ("brokers", "trades"): {
                "path": [("brokers", "assets"), ("assets", "trades")],
                "description": "Brokers manage assets that are traded"
            }
        }

    def get_essential_fields(self, table_name):
        return self.essential_fields.get(table_name, ["name"])

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

    def process_query(self, nl_query, intent_data):
        intent = intent_data.get('intent', 'database_query_list')
        confidence = intent_data.get('confidence', 0.0)
        sub_intent = intent_data.get('sub_intent')

        self.logger.info(f"Processing query with intent: {intent}, confidence: {confidence}")

        if confidence >= 0.7:
            return self._process_query_generic(nl_query, intent, sub_intent)

        tables = self._extract_tables(nl_query)
        if not tables:
            self.logger.warning("No tables identified in the query")
            return None

        if "count" in nl_query.lower() and any(term in nl_query.lower() for term in ["how many", "number of", "total"]):
            return self._handle_count_query(nl_query)
        elif "id" in nl_query.lower() and any(re.search(r'\b\d+\b', part) for part in nl_query.split()):
            return self._handle_specific_id_query(nl_query)

        return self._handle_list_query(nl_query)

    def _extract_tables(self, nl_query):

        tables = []
        nl_query = nl_query.lower()

        multi_word_entities = sorted([entity for entity in self.entity_mapping if ' ' in entity],
                                     key=len, reverse=True)

        for entity in multi_word_entities:
            if entity in nl_query:
                tables.append(self.entity_mapping[entity])

        for entity, table in self.entity_mapping.items():
            if ' ' not in entity:
                if re.search(r'\b' + re.escape(entity) + r'\b', nl_query):
                    tables.append(table)
        if not tables:
            if any(re.search(r'\b' + word + r'\b', nl_query) for word in ["market", "exchange"]):
                tables.append("markets")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["trader", "client", "customer"]):
                tables.append("traders")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in
                     ["asset", "stock", "security", "etf", "bond"]):
                tables.append("assets")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["trade", "trading"]):
                tables.append("trades")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["order"]):
                tables.append("orders")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["account", "balance"]):
                tables.append("accounts")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["price", "value", "cost"]):
                tables.append("price_history")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["transaction", "payment"]):
                tables.append("transactions")
            elif any(re.search(r'\b' + word + r'\b', nl_query) for word in ["broker", "dealer"]):
                tables.append("brokers")

        return list(set(tables))

    def _extract_fields(self, nl_query, tables):
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

    def _get_default_sort_field(self, tables, nl_query):
        nl_query = nl_query.lower()

        if "price" in nl_query and "price_history" in tables:
            return "price_history.close_price"
        elif "price" in nl_query and "trades" in tables:
            return "trades.price"
        elif "date" in nl_query and "trades" in tables:
            return "trades.trade_date"
        elif "date" in nl_query and "orders" in tables:
            return "orders.order_date"
        elif "date" in nl_query and "transactions" in tables:
            return "transactions.transaction_date"
        elif "balance" in nl_query and "accounts" in tables:
            return "accounts.balance"

        default_sort_fields = {
            "trades": "trades.trade_date",
            "orders": "orders.order_date",
            "assets": "assets.name",
            "traders": "traders.name",
            "markets": "markets.name",
            "transactions": "transactions.transaction_date",
            "accounts": "accounts.balance",
            "price_history": "price_history.price_date",
            "brokers": "brokers.name",
            "order_status": "order_status.status_date"
        }

        if tables and tables[0] in default_sort_fields:
            return default_sort_fields[tables[0]]

        for table in tables:
            if table in default_sort_fields:
                return default_sort_fields[table]

        if tables:
            return f"{tables[0]}.{tables[0][:-1]}_id"

        return None

    def _extract_superlative_field(self, nl_query, tables, superlative_type):

        nl_query = nl_query.lower()

        superlative_indicators = {
            "highest": ["highest", "most", "maximum", "largest", "greatest", "biggest", "top"],
            "lowest": ["lowest", "least", "minimum", "smallest", "least", "bottom"],
            "middle": ["middle", "median", "average", "mid-range", "center"]
        }

        indicators = superlative_indicators.get(superlative_type, [])

        for indicator in indicators:
            if indicator in nl_query:
                parts = nl_query.split(indicator)
                if len(parts) > 1:
                    field_text = parts[1].strip().split()[0]

                    field_mappings = {
                        "price": "price",
                        "cost": "price",
                        "value": "price",
                        "date": "trade_date",
                        "time": "trade_date",
                        "amount": "amount",
                        "quantity": "quantity",
                        "volume": "quantity",
                        "balance": "balance"
                    }

                    field = field_mappings.get(field_text)

                    if field:
                        for table in tables:
                            table_fields = self.table_info.get(table, [])
                            for table_field in table_fields:
                                if field in table_field:
                                    return f"{table}.{table_field}"

        default_fields = {
            "trades": "trades.price",
            "assets": "assets.price",
            "accounts": "accounts.balance",
            "transactions": "transactions.amount",
            "price_history": "price_history.close_price"
        }

        for table in tables:
            if table in default_fields:
                return default_fields[table]

        return None

    def _extract_number_from_query(self, nl_query):
        number_patterns = [
            r'top\s+(\d+)',
            r'first\s+(\d+)',
            r'(\d+)\s+results',
            r'limit\s+(\d+)',
            r'limit\s+to\s+(\d+)'
        ]

        for pattern in number_patterns:
            match = re.search(pattern, nl_query.lower())
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    pass

        all_numbers = re.findall(r'\b\d+\b', nl_query)
        for num in all_numbers:
            try:
                value = int(num)
                if 1 <= value <= 1000:
                    return value
            except ValueError:
                pass

        return None

    def _execute_and_process_query(self, sql):
        try:
            result = self.db_connector.execute_query(sql)

            if not result:
                self.logger.info("No results returned from database")
                return []

            processed_result = []
            for item in result:
                processed_item = {}
                for key, value in item.items():
                    if self._should_encrypt_field(key):
                        processed_item[key] = f"[ENCRYPTED: {key}]"
                    else:
                        processed_item[key] = value
                processed_result.append(processed_item)

            return processed_result
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return None

    def secure_process_query(self, nl_query):
        try:
            self.logger.info(f"Direct query processing for: {nl_query}")
            tables = self._extract_tables(nl_query)

            if not tables:
                self.logger.warning("No tables identified in the query")
                return None

            fields = self._extract_fields(nl_query, tables)

            select_clause = f"SELECT {', '.join(fields)}"
            from_clause = f"FROM {tables[0]}"

            join_clauses = self._generate_joins_for_tables(tables)
            join_clause = " JOIN ".join(join_clauses)
            if join_clause:
                from_clause = f"{from_clause} JOIN {join_clause}"

            limit_clause = "LIMIT 100"

            sql_parts = [select_clause, from_clause, limit_clause]

            sql = " ".join(sql_parts)
            self.logger.info(f"Generated SQL from natural language: {sql}")

            return self._execute_and_process_query(sql)
        except Exception as e:
            self.logger.error(f"Error in secure_process_query: {e}")
            return None


    def _extract_entity_name(self, nl_query):
        original_query = nl_query
        nl_query = nl_query.lower()

        entity_type_mappings = {
            "broker": "brokers",
            "trader": "traders",
            "asset": "assets",
            "stock": "assets",
            "bond": "assets",
            "etf": "assets",
            "market": "markets",
            "exchange": "markets",
            "order": "orders",
            "transaction": "transactions",
            "account": "accounts"
        }

        entity_extraction_patterns = [
            r'(?:about|on|for)\s+((?:[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)+)',

            r'(?:details\s+(?:of|about|for)|information\s+(?:on|about))\s+((?:[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)+)',

            r'(?:show\s+me|tell\s+me\s+about)\s+((?:[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)+)',

            r'(?:find|lookup|get)\s+(?:details\s+(?:about|of|for)|information\s+(?:about|on))?\s+((?:[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)+)',

            r'(?:give\s+me|what\s+is|who\s+is)\s+(?:all\s+information\s+about|details\s+about|information\s+about)?\s+((?:[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)+)'
        ]

        entity_candidates = []

        for pattern in entity_extraction_patterns:
            matches = re.search(pattern, nl_query)
            if matches:
                raw_entity = matches.group(1).strip()

                cleanup_words = ['details', 'info', 'information', 'please', 'the']
                for word in cleanup_words:
                    if raw_entity.endswith(' ' + word):
                        raw_entity = raw_entity.replace(' ' + word, '')
                    if raw_entity.startswith(word + ' '):
                        raw_entity = raw_entity.replace(word + ' ', '')

                for entity_type in entity_type_mappings.keys():
                    if raw_entity.startswith(entity_type + ' '):
                        candidate = raw_entity[len(entity_type) + 1:].strip()
                        entity_candidates.append((candidate, entity_type_mappings[entity_type]))

                entity_candidates.append((raw_entity, None))

        if entity_candidates:
            entity_candidates.sort(key=lambda x: len(x[0]), reverse=True)

            for candidate, table_hint in entity_candidates:
                if table_hint:
                    start_pos = nl_query.find(candidate.lower())
                    if start_pos != -1:
                        extracted_name = original_query[start_pos:start_pos + len(candidate)]

                        result = self._check_entity_exists(extracted_name, table_hint)
                        if result:
                            self.logger.info(f"Found entity '{extracted_name}' in table '{table_hint}'")
                            return extracted_name, table_hint

                possible_tables = ['brokers', 'traders', 'assets', 'markets', 'accounts', 'orders', 'transactions']

                for entity_type, table in entity_type_mappings.items():
                    if entity_type in nl_query:
                        if table in possible_tables:
                            possible_tables.remove(table)
                            possible_tables.insert(0, table)

                start_pos = nl_query.find(candidate.lower())
                if start_pos != -1:
                    extracted_name = original_query[start_pos:start_pos + len(candidate)]

                    for table in possible_tables:
                        result = self._check_entity_exists(extracted_name, table)
                        if result:
                            self.logger.info(f"Found entity '{extracted_name}' in table '{table}'")
                            return extracted_name, table

        self.logger.warning(f"Could not extract entity name from query: {nl_query}")
        return None, None

    def _check_entity_exists(self, entity_name, table_name):
        if not entity_name or not table_name:
            return False

        try:
            sql = f"SELECT * FROM {table_name} WHERE LOWER(name) = LOWER(%s) LIMIT 1"
            params = (entity_name,)
            result = self.db_connector.execute_query(sql, params)

            if result and len(result) > 0:
                return True

            sql = f"SELECT * FROM {table_name} WHERE LOWER(name) LIKE LOWER(%s) LIMIT 1"
            params = (f"%{entity_name}%",)
            result = self.db_connector.execute_query(sql, params)

            return result and len(result) > 0

        except Exception as e:
            self.logger.error(f"Error checking if entity exists: {e}")
            return False

    def _get_entity_by_name(self, entity_name, table_name):
        if not entity_name or not table_name:
            return None

        entity_name_lower = entity_name.lower()
        name_field = "name"

        sql = f"SELECT * FROM {table_name} WHERE LOWER({name_field}) LIKE %s"
        params = (f"%{entity_name_lower}%",)

        self.logger.info(f"Executing entity query: {sql} with params {params}")
        result = self.db_connector.execute_query(sql, params)

        if not result:
            sql = f"SELECT * FROM {table_name} WHERE LOWER({name_field}) LIKE %s"
            params = (f"%{entity_name_lower.split()[0]}%",)
            self.logger.info(f"Trying more general entity query: {sql} with params {params}")
            result = self.db_connector.execute_query(sql, params)

        return result

    def _get_related_entities(self, entity_id, table_name):
        if not entity_id or not table_name:
            self.logger.warning("Missing entity_id or table_name for related entity lookup")
            return {}

        self.logger.info(f"Getting related entities for {table_name} with ID {entity_id}")
        related_data = {}

        id_field = f"{table_name[:-1]}_id"

        for related_table in self.table_info.keys():
            if related_table != table_name:
                if id_field in self.table_info.get(related_table, []):
                    query = f"SELECT * FROM {related_table} WHERE {id_field} = {entity_id} LIMIT 10"
                    self.logger.info(f"Checking for references in {related_table}: {query}")

                    try:
                        result = self.db_connector.execute_query(query)
                        if result and len(result) > 0:
                            related_data[related_table] = result
                            self.logger.info(f"Found {len(result)} related records in {related_table}")
                    except Exception as e:
                        self.logger.error(f"Error checking for references in {related_table}: {e}")

        entity_fields = self.table_info.get(table_name, [])
        for field in entity_fields:
            if field.endswith('_id') and field != id_field:
                referenced_table = field[:-3] + 's'

                try:
                    ref_id_query = f"SELECT {field} FROM {table_name} WHERE {id_field} = {entity_id}"
                    ref_id_result = self.db_connector.execute_query(ref_id_query)

                    if ref_id_result and len(ref_id_result) > 0 and ref_id_result[0].get(field):
                        referenced_id = ref_id_result[0][field]

                        ref_query = f"SELECT * FROM {referenced_table} WHERE {referenced_table[:-1]}_id = {referenced_id}"
                        self.logger.info(f"Looking up reference: {ref_query}")

                        ref_result = self.db_connector.execute_query(ref_query)
                        if ref_result and len(ref_result) > 0:
                            related_data[referenced_table[:-1]] = ref_result
                            self.logger.info(f"Found referenced {referenced_table} record")
                except Exception as e:
                    self.logger.error(f"Error retrieving reference for {field}: {e}")

        return related_data

    def process_entity_query(self, nl_query):
        entity_name, table_name = self._extract_entity_name(nl_query)

        self.logger.info(f"Extracted entity: '{entity_name}' from table: '{table_name}'")

        if not entity_name:
            self.logger.warning(f"Could not extract entity name from query: {nl_query}")
            return None

        if entity_name and not table_name:
            possible_tables = ['brokers', 'traders', 'assets', 'markets', 'accounts', 'orders', 'transactions']
            for table in possible_tables:
                entity_info = self._get_entity_by_name(entity_name, table)
                if entity_info and len(entity_info) > 0:
                    table_name = table
                    self.logger.info(f"Found entity '{entity_name}' in table '{table_name}'")
                    break

            if not table_name:
                self.logger.warning(f"Could not determine table for entity: {entity_name}")
                return None

        entity_info = self._get_entity_by_name(entity_name, table_name)

        if not entity_info or len(entity_info) == 0:
            self.logger.warning(f"No entity found for {entity_name} in {table_name}")

            alternative_tables = [t for t in ['brokers', 'traders', 'assets', 'markets', 'accounts'] if t != table_name]

            for alt_table in alternative_tables:
                self.logger.info(f"Trying alternative table: {alt_table}")
                alt_info = self._get_entity_by_name(entity_name, alt_table)
                if alt_info and len(alt_info) > 0:
                    self.logger.info(f"Found entity in alternative table: {alt_table}")
                    entity_info = alt_info
                    table_name = alt_table
                    break

        if not entity_info or len(entity_info) == 0:
            self.logger.warning(f"No entity found for {entity_name} after all fallbacks")
            return None

        id_field = f"{table_name[:-1]}_id"
        entity_id = entity_info[0].get(id_field)

        if not entity_id:
            self.logger.warning(f"No ID field {id_field} found in entity")
            return {
                "entity_type": table_name,
                "entity_info": entity_info,
                "related_info": {}
            }

        related_info = self._get_related_entities(entity_id, table_name)

        result = {
            "entity_type": table_name,
            "entity_info": entity_info,
            "related_info": related_info
        }

        return self._process_entity_result(result, nl_query)

    def _process_entity_result(self, result, nl_query):
        if not result:
            return None

        processed_result = {
            "entity_type": result["entity_type"],
            "entity_info": []
        }

        for item in result["entity_info"]:
            processed_item = {}
            for key, value in item.items():
                if self._should_encrypt_field(key):
                    processed_item[key] = f"[ENCRYPTED: {key}]"
                else:
                    processed_item[key] = value
            processed_result["entity_info"].append(processed_item)

        processed_related = {}
        for relation_name, relation_items in result["related_info"].items():
            processed_items = []
            for item in relation_items:
                processed_item = {}
                for key, value in item.items():
                    if self._should_encrypt_field(key):
                        processed_item[key] = f"[ENCRYPTED: {key}]"
                    else:
                        processed_item[key] = value
                processed_items.append(processed_item)
            processed_related[relation_name] = processed_items

        processed_result["related_info"] = processed_related
        return processed_result