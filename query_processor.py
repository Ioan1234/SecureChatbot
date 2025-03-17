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

        # Initialize table information
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

        # Entity mapping (for natural language to table name mapping)
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

        # Initialize essential fields for each table
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

        # Initialize attribute mapping
        self.attribute_mapping = {
            # General attributes
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

            # Special aggregate attributes
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

        # Initialize table relationships
        self._initialize_table_relationships()

    def _handle_generic_query(self, nl_query, semantics=None, sub_intent=None):
        """
        Handle generic queries without specific intent classification
        """
        # Try to use the semantics-based query builder first
        if semantics:
            sql = self._build_sql_query(semantics)
            if sql:
                return self._execute_and_process_query(sql)

        # If semantics-based query fails, fall back to the old method
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
        self.logger.info(f"Generated generic SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_list_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle list queries with the new semantic approach"""
        # Try the semantic builder first
        if semantics:
            sql = self._build_sql_query(semantics, "database_query_list")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to the legacy approach
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

    def _handle_detailed_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle detailed query with new semantic approach"""
        # Try semantic builder first
        if semantics:
            # Modify semantics to include more fields
            semantics["detailed"] = True
            sql = self._build_sql_query(semantics, "database_query_detailed")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_count_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle count queries with the new semantic approach"""
        # Try semantic builder first
        if semantics:
            # Force aggregation to count
            semantics["aggregation"] = "count"
            sql = self._build_sql_query(semantics, "database_query_count")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_recent_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle recent queries with the new semantic approach"""
        # Try semantic builder first
        if semantics:
            # Modify semantics to sort by date desc
            semantics["operation"] = "sort_desc"
            semantics["attribute"] = "date"
            sql = self._build_sql_query(semantics, "database_query_recent")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_filter_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle filter queries with the new semantic approach"""
        # Try semantic builder first
        if semantics:
            sql = self._build_sql_query(semantics, "database_query_filter")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_sort_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle sort queries with the new semantic approach"""
        # Use the semantics-based builder
        sql = self._build_sql_query(semantics, "database_query_sort", sub_intent)
        if sql:
            return self._execute_and_process_query(sql)

        # If that fails, use the appropriate legacy handler based on sub-intent
        if sub_intent == "database_query_sort_ascending":
            return self._handle_sort_ascending_query(nl_query)
        elif sub_intent == "database_query_sort_descending":
            return self._handle_sort_descending_query(nl_query)

        # Fall back to legacy approach
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

    def _handle_sort_ascending_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle ascending sort queries"""
        # Try semantic builder first
        if semantics:
            semantics["operation"] = "sort_asc"
            sql = self._build_sql_query(semantics, "database_query_sort", "database_query_sort_ascending")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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
        self.logger.info(f"Generated sort ascending SQL: {sql}")

        return self._execute_and_process_query(sql)

    def _handle_sort_descending_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle descending sort queries"""
        # Try semantic builder first
        if semantics:
            semantics["operation"] = "sort_desc"
            sql = self._build_sql_query(semantics, "database_query_sort", "database_query_sort_descending")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_comparative_query(self, nl_query, semantics=None, sub_intent=None):
        """
        Handle comparative queries (highest, lowest, middle)
        """
        # Use the semantics-based builder
        sql = self._build_sql_query(semantics, "database_query_comparative", sub_intent)
        if sql:
            return self._execute_and_process_query(sql)

        # If that fails, use the appropriate legacy handler based on sub-intent
        if sub_intent == "database_query_comparative_highest":
            return self._handle_highest_query(nl_query)
        elif sub_intent == "database_query_comparative_lowest":
            return self._handle_lowest_query(nl_query)
        elif sub_intent == "database_query_comparative_middle":
            return self._handle_middle_query(nl_query)

        # Default to highest if no sub-intent specified
        return self._handle_highest_query(nl_query)

    def _handle_highest_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle highest queries"""
        # Try semantic builder first
        if semantics:
            semantics["operation"] = "max"
            sql = self._build_sql_query(semantics, "database_query_comparative", "database_query_comparative_highest")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_lowest_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle lowest queries"""
        # Try semantic builder first
        if semantics:
            semantics["operation"] = "min"
            sql = self._build_sql_query(semantics, "database_query_comparative", "database_query_comparative_lowest")
            if sql:
                return self._execute_and_process_query(sql)

        # Special case for transaction count
        nl_query_lower = nl_query.lower()
        if "transaction count" in nl_query_lower and "trader" in nl_query_lower:
            sql = """
            SELECT t.name, t.registration_date, COUNT(tr.transaction_id) as transaction_count 
            FROM traders t
            JOIN accounts a ON t.trader_id = a.trader_id
            LEFT JOIN transactions tr ON a.account_id = tr.account_id
            GROUP BY t.trader_id
            ORDER BY transaction_count ASC
            LIMIT 10
            """
            self.logger.info(f"Generated specialized transaction count SQL: {sql}")
            return self._execute_and_process_query(sql)

        # Special case for account balance
        if "account balance" in nl_query_lower and "trader" in nl_query_lower:
            sql = """
            SELECT t.name, t.registration_date, a.balance 
            FROM traders t
            JOIN accounts a ON t.trader_id = a.trader_id
            ORDER BY a.balance ASC
            LIMIT 10
            """
            self.logger.info(f"Generated specialized account balance SQL: {sql}")
            return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_middle_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle middle/median queries"""
        # Try semantic builder first
        if semantics:
            semantics["operation"] = "avg"
            sql = self._build_sql_query(semantics, "database_query_comparative", "database_query_comparative_middle")
            if sql:
                return self._execute_and_process_query(sql)

        # Special case for median price
        nl_query_lower = nl_query.lower()
        if "median price" in nl_query_lower and "asset" in nl_query_lower:
            sql = """
            SELECT a.name, a.asset_type, p.close_price
            FROM assets a
            JOIN price_history p ON a.asset_id = p.asset_id
            ORDER BY p.close_price
            LIMIT 10 OFFSET (
                SELECT COUNT(*)/2 - 5 FROM price_history
            )
            """
            self.logger.info(f"Generated specialized median price SQL: {sql}")
            return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_specific_id_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle queries for specific IDs with the new semantic approach"""
        # Try semantic builder first
        if semantics:
            sql = self._build_sql_query(semantics, "database_query_specific_id")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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

    def _handle_sensitive_query(self, nl_query, semantics=None, sub_intent=None):
        """Handle queries for sensitive data with the new semantic approach"""
        # Try semantic builder first
        if semantics:
            semantics["sensitive"] = True
            sql = self._build_sql_query(semantics, "database_query_sensitive")
            if sql:
                return self._execute_and_process_query(sql)

        # Fall back to legacy approach
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
    def _initialize_table_relationships(self):
        """Initialize the relationships between tables for proper JOIN handling"""
        self.table_relationships = {
            ("traders", "accounts"): {
                "join": "traders.trader_id = accounts.trader_id",
                "cardinality": "one-to-many"  # One trader has many accounts
            },
            ("accounts", "transactions"): {
                "join": "accounts.account_id = transactions.account_id",
                "cardinality": "one-to-many"  # One account has many transactions
            },
            ("assets", "price_history"): {
                "join": "assets.asset_id = price_history.asset_id",
                "cardinality": "one-to-many"  # One asset has many price points
            },
            ("assets", "trades"): {
                "join": "assets.asset_id = trades.asset_id",
                "cardinality": "one-to-many"  # One asset appears in many trades
            },
            ("traders", "trades"): {
                "join": "traders.trader_id = trades.trader_id",
                "cardinality": "one-to-many"  # One trader makes many trades
            },
            ("orders", "trades"): {
                "join": "orders.trade_id = trades.trade_id",
                "cardinality": "one-to-one"  # One order corresponds to one trade
            },
            ("markets", "trades"): {
                "join": "markets.market_id = trades.market_id",
                "cardinality": "one-to-many"  # One market has many trades
            },
            ("orders", "order_status"): {
                "join": "orders.order_id = order_status.order_id",
                "cardinality": "one-to-many"  # One order has many status updates
            },
            ("brokers", "assets"): {
                "join": "brokers.broker_id = assets.broker_id",
                "cardinality": "one-to-many"  # One broker manages many assets
            }
        }

        # Add reverse relationships automatically
        reverse_relationships = {}
        for (table1, table2), relationship in self.table_relationships.items():
            join = relationship["join"]
            # Flip cardinality for reverse relationship
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

        # Special indirect relationships (multi-hop joins)
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
        """Get essential fields for a table"""
        return self.essential_fields.get(table_name, ["name"])

    def _should_encrypt_field(self, field_name):
        """Determine if a field should be encrypted"""
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
        """Make a value serializable for output"""
        if hasattr(value, '__class__') and value.__class__.__name__ == 'CKKSVector':
            return {
                "type": "encrypted",
                "value": "[ENCRYPTED]"
            }
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        return value

    def process_query(self, nl_query, intent_data):
        """
        Process a natural language query based on intent classification
        """
        intent = intent_data.get('intent', 'database_query_list')
        sub_intent = intent_data.get('sub_intent', None)
        confidence = intent_data.get('confidence', 0.0)

        self.logger.info(f"Processing query with intent: {intent}, sub-intent: {sub_intent}, confidence: {confidence}")

        # Use semantic analysis for SQL generation
        semantics = self._analyze_query_semantics(nl_query)
        self.logger.info(f"Query semantics: {semantics}")

        # If confidence is too low, use generic processing
        if confidence < 0.5:
            self.logger.warning(f"Low confidence ({confidence}) for intent: {intent}. Using generic processing.")
            return self._handle_generic_query(nl_query)

        # Mapping of intents to handlers
        handlers = {
            "database_query_list": self._handle_list_query,
            "database_query_count": self._handle_count_query,
            "database_query_detailed": self._handle_detailed_query,
            "database_query_recent": self._handle_recent_query,
            "database_query_filter": self._handle_filter_query,
            "database_query_sort": self._handle_sort_query,
            "database_query_specific_id": self._handle_specific_id_query,
            "database_query_sensitive": self._handle_sensitive_query,
            "database_query_comparative": self._handle_comparative_query
        }

        # Get the appropriate handler (or default to generic)
        handler = handlers.get(intent, self._handle_generic_query)

        # Pass both the query and semantics to the handler
        return handler(nl_query, semantics, sub_intent)

    def _analyze_query_semantics(self, nl_query):
        """
        Analyze query semantics to understand what the user is asking for
        """
        nl_query_lower = nl_query.lower()

        semantics = {
            "entity_type": self._extract_primary_entity(nl_query),
            "attribute": self._extract_attribute(nl_query),
            "aggregation": None,
            "operation": None,
            "constraints": []
        }

        # Detect operations
        if any(word in nl_query_lower for word in ["highest", "max", "maximum", "greatest", "largest"]):
            semantics["operation"] = "max"
        elif any(word in nl_query_lower for word in ["lowest", "min", "minimum", "smallest", "least"]):
            semantics["operation"] = "min"
        elif any(word in nl_query_lower for word in ["average", "mean", "median", "middle", "mid"]):
            semantics["operation"] = "avg"
        elif any(word in nl_query_lower for word in ["sort", "order", "arrange"]):
            if any(word in nl_query_lower for word in ["descending", "desc", "high to low"]):
                semantics["operation"] = "sort_desc"
            else:
                semantics["operation"] = "sort_asc"

        # Detect aggregations
        if "count" in nl_query_lower or "number of" in nl_query_lower:
            semantics["aggregation"] = "count"
            if "transaction" in nl_query_lower:
                semantics["attribute"] = "transaction_count"
            elif "trade" in nl_query_lower:
                semantics["attribute"] = "trade_count"

        # Detect constraints
        constraint_patterns = [
            (r'where\s+(\w+)\s+is\s+(\w+)', "{} = '{}'"),
            (r'with\s+(\w+)\s+(\w+)', "{} = '{}'"),
        ]

        for pattern, template in constraint_patterns:
            matches = re.findall(pattern, nl_query_lower)
            for match in matches:
                field, value = match
                semantics["constraints"].append((field, value))

        return semantics

    def _extract_primary_entity(self, nl_query):
        """Extract the main entity type from the query"""
        nl_query_lower = nl_query.lower()

        # Try multi-word entity variations first (to avoid partial matches)
        multi_word_entities = sorted([entity for entity in self.entity_mapping if ' ' in entity],
                                     key=len, reverse=True)

        for entity in multi_word_entities:
            if entity in nl_query_lower:
                return self.entity_mapping[entity]

        # Try single-word entity variations
        for entity, table in self.entity_mapping.items():
            if ' ' not in entity and re.search(r'\b' + re.escape(entity) + r'\b', nl_query_lower):
                return table

        # Default fallback detection
        if any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["market", "exchange"]):
            return "markets"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["trader", "client", "customer"]):
            return "traders"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in
                 ["asset", "stock", "security", "etf", "bond"]):
            return "assets"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["trade", "trading"]):
            return "trades"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["order"]):
            return "orders"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["account", "balance"]):
            return "accounts"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["price", "value", "cost"]):
            return "price_history"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["transaction", "payment"]):
            return "transactions"
        elif any(re.search(r'\b' + word + r'\b', nl_query_lower) for word in ["broker", "dealer"]):
            return "brokers"

        return None

    def _extract_attribute(self, nl_query):
        """Extract the attribute of interest from the query"""
        nl_query_lower = nl_query.lower()

        # Common attribute patterns
        attribute_patterns = {
            r'by\s+(\w+)': 1,
            r'with\s+(?:highest|lowest|median)\s+(\w+)': 1,
            r'(?:highest|lowest|median)\s+(\w+)': 1,
            r'sorted\s+by\s+(\w+)': 1,
            r'ordered\s+by\s+(\w+)': 1,
            r'count\s+(?:of|by)\s+(\w+)': 1
        }

        for pattern, group in attribute_patterns.items():
            match = re.search(pattern, nl_query_lower)
            if match:
                attribute = match.group(group)
                mapped_attribute = self._map_attribute_to_field(attribute)
                if mapped_attribute:
                    return mapped_attribute

        # Direct attribute detection
        for attribute in self.attribute_mapping:
            if attribute in nl_query_lower:
                return attribute

        # Specialized attribute detection for common queries
        if "account balance" in nl_query_lower:
            return "balance"
        if "transaction count" in nl_query_lower:
            return "transaction_count"
        if "trade count" in nl_query_lower:
            return "trade_count"

        return None

    def _map_attribute_to_field(self, attribute):
        """Map a natural language attribute to a standardized attribute name"""
        attribute_map = {
            "price": "price",
            "cost": "price",
            "value": "price",
            "amount": "amount",
            "balance": "balance",
            "date": "date",
            "time": "date",
            "transaction": "transaction_count",
            "trade": "trade_count",
            "quantity": "quantity",
            "volume": "quantity",
            "type": "type",
            "name": "name",
            "location": "location",
            "status": "status"
        }

        return attribute_map.get(attribute, attribute)

    def _map_constraint_field_to_column(self, field, tables):
        """Map a constraint field from natural language to a database column"""
        # Try direct mapping first
        direct_mappings = {
            "type": {
                "assets": "assets.asset_type",
                "accounts": "accounts.account_type",
                "orders": "orders.order_type",
                "transactions": "transactions.transaction_type"
            },
            "name": {
                "traders": "traders.name",
                "brokers": "brokers.name",
                "assets": "assets.name",
                "markets": "markets.name"
            },
            "price": {
                "trades": "trades.price",
                "price_history": "price_history.close_price"
            },
            "status": {
                "order_status": "order_status.status"
            },
            "date": {
                "trades": "trades.trade_date",
                "orders": "orders.order_date",
                "transactions": "transactions.transaction_date",
                "price_history": "price_history.price_date"
            }
        }

        # Check for direct mappings
        if field in direct_mappings:
            for table in tables:
                if table in direct_mappings[field]:
                    return direct_mappings[field][table]

        # Check for field in format "table.column"
        if "." in field:
            table, column = field.split(".")
            if table in tables:
                return f"{table}.{column}"

        # Try to find the field in any of the tables
        for table in tables:
            if table in self.table_info:
                for column in self.table_info[table]:
                    if column == field or field in column:
                        return f"{table}.{column}"

        return None

    def _build_join_path(self, table1, table2, visited=None):
        """Find a path of JOINs between two tables that aren't directly related"""
        if visited is None:
            visited = set()

        visited.add(table1)

        # Direct relationship
        if (table1, table2) in self.table_relationships:
            return [(table1, table2)]

        # Check for predefined indirect relationship
        if (table1, table2) in self.indirect_relationships:
            return self.indirect_relationships[(table1, table2)]["path"]

        # Try to find a path through other tables
        for (t1, t2) in self.table_relationships:
            if t1 == table1 and t2 not in visited:
                path = self._build_join_path(t2, table2, visited.copy())
                if path:
                    return [(table1, t2)] + path

        return None

    def _build_sql_query(self, semantics, intent=None, sub_intent=None):
        """
        Build an SQL query based on semantic analysis
        """
        entity_type = semantics["entity_type"]
        attribute = semantics["attribute"]
        operation = semantics["operation"]

        if not entity_type:
            self.logger.warning("No entity type identified. Using generic query.")
            return None

        # Determine required tables
        tables = []

        # Start with the primary entity table
        tables.append(entity_type)

        # Add tables required by the attribute
        if attribute and attribute in self.attribute_mapping:
            attr_info = self.attribute_mapping[attribute]
            for table in attr_info.get("tables", []):
                if table != entity_type and table not in tables:
                    tables.append(table)

        # Build SELECT clause
        select_parts = []

        # Add identification fields from primary entity
        if entity_type in self.essential_fields:
            for field in self.essential_fields[entity_type][:2]:  # Take first two essential fields
                select_parts.append(f"{entity_type}.{field}")

        # Add attribute fields
        if attribute and attribute in self.attribute_mapping:
            attr_info = self.attribute_mapping[attribute]

            # Handle aggregation
            if attr_info.get("aggregate", False):
                agg_function = attr_info.get("function", "COUNT")
                field = attr_info.get("fields", ["*"])[0]
                select_parts.append(f"{agg_function}({field}) as {attribute}")
            else:
                # Add the first relevant field for this attribute
                for field in attr_info.get("fields", []):
                    table_name = field.split('.')[0]
                    if table_name in tables:
                        select_parts.append(field)
                        break

        # If no fields were added, use essential fields from all tables
        if not select_parts:
            for table in tables:
                if table in self.essential_fields:
                    for field in self.essential_fields[table][:2]:
                        select_parts.append(f"{table}.{field}")

        select_clause = f"SELECT {', '.join(select_parts)}"

        if len(tables) == 1:
            from_clause = f"FROM {tables[0]}"
        else:
            # Build FROM clause with explicit JOINs
            main_table = tables[0]
            from_clause = f"FROM {main_table}"

            # Add JOIN clauses for other tables
            for i in range(1, len(tables)):
                secondary_table = tables[i]
                join_condition = self._get_join_condition(main_table, secondary_table)
                if join_condition:
                    from_clause += f" JOIN {secondary_table} ON {join_condition}"
                else:
                    # Try to find an indirect join path
                    join_path = self._build_join_path(main_table, secondary_table)
                    if join_path:
                        # Add intermediate tables if needed
                        for path_table1, path_table2 in join_path:
                            if path_table1 != main_table and path_table1 not in tables[:i]:
                                from_clause += f" JOIN {path_table1} ON {self._get_join_condition(main_table, path_table1)}"
                            if path_table2 not in tables[:i]:
                                from_clause += f" JOIN {path_table2} ON {self._get_join_condition(path_table1, path_table2)}"

        # Build JOIN clauses
        join_clauses = []

        # Add necessary joins between tables
        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                table1, table2 = tables[i], tables[j]

                # Direct relationship
                if (table1, table2) in self.table_relationships:
                    join_info = self.table_relationships[(table1, table2)]
                    join_condition = join_info["join"]
                    join_clauses.append(join_condition)
                else:
                    # Try to find an indirect join path
                    join_path = self._build_join_path(table1, table2)
                    if join_path:
                        for (path_table1, path_table2) in join_path:
                            join_condition = self.table_relationships[(path_table1, path_table2)]["join"]
                            if join_condition not in join_clauses:
                                join_clauses.append(join_condition)

        # Build WHERE clause with constraints
        where_conditions = []

        for field, value in semantics.get("constraints", []):
            # Map the field to a database column if possible
            mapped_field = self._map_constraint_field_to_column(field, tables)
            if mapped_field:
                where_conditions.append(f"{mapped_field} = '{value}'")

        # Build GROUP BY clause if needed
        group_by_clause = ""
        if attribute and attribute in self.attribute_mapping:
            attr_info = self.attribute_mapping[attribute]
            if attr_info.get("aggregate", False):
                # Group by the primary entity's ID for accurate aggregation
                group_by_clause = f"GROUP BY {entity_type}.{entity_type[:-1]}_id"

        # Build ORDER BY clause based on intent and operation
        order_by_clause = ""

        if operation in ["max", "sort_desc"] or sub_intent in ["database_query_comparative_highest",
                                                               "database_query_sort_descending"]:
            direction = "DESC"
        elif operation in ["min", "sort_asc"] or sub_intent in ["database_query_comparative_lowest",
                                                                "database_query_sort_ascending"]:
            direction = "ASC"
        else:
            direction = ""

        if direction and attribute:
            attr_info = self.attribute_mapping.get(attribute, {})

            if attr_info.get("aggregate", False):
                order_by_clause = f"ORDER BY {attribute} {direction}"
            else:
                fields = attr_info.get("fields", [])
                if fields:
                    for field in fields:
                        table_name = field.split('.')[0]
                        if table_name in tables:
                            order_by_clause = f"ORDER BY {field} {direction}"
                            break

        # Build LIMIT and OFFSET clauses
        limit_clause = "LIMIT 10"
        offset_clause = ""

        # For "middle" queries, use OFFSET to get middle values
        if sub_intent == "database_query_comparative_middle" or operation == "avg":
            # Get the count and calculate middle offset
            if tables:
                main_table = tables[0]
                offset_clause = f"OFFSET (SELECT COUNT(*)/2 - 5 FROM {main_table})"

        # Build the final SQL query
        sql_parts = [select_clause, from_clause]

        # Build WHERE with JOINs and constraints
        if join_clauses or where_conditions:
            combined_conditions = join_clauses + where_conditions
            sql_parts.append(f"WHERE {' AND '.join(combined_conditions)}")

        if group_by_clause:
            sql_parts.append(group_by_clause)

        if order_by_clause:
            sql_parts.append(order_by_clause)

        if limit_clause:
            sql_parts.append(limit_clause)

        if offset_clause:
            sql_parts.append(offset_clause)

        sql = " ".join(sql_parts)

        self.logger.info(f"Generated SQL query: {sql}")
        return sql

    def _extract_tables(self, nl_query):
        """
        Extract mentioned tables from natural language query
        """
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

    def _extract_filter_conditions(self, nl_query, tables):
        """Extract filter conditions from a query"""
        nl_query = nl_query.lower()
        conditions = []

        equality_patterns = [
            (r"where (\w+) is (\w+)", "{}.{} = '{}'"),
            (r"with (\w+) (\w+)", "{}.{} = '{}'"),
            (r"(\w+) equal to (\w+)", "{}.{} = '{}'"),
            (r"(\w+) equals (\w+)", "{}.{} = '{}'"),
            (r"(\w+) = (\w+)", "{}.{} = '{}'")
        ]

        for pattern, template in equality_patterns:
            matches = re.findall(pattern, nl_query)
            for match in matches:
                field, value = match

                for table in tables:
                    table_fields = self.table_info.get(table, [])
                    for table_field in table_fields:
                        if field in table_field:
                            conditions.append(template.format(table, table_field, value))
                            break

        asset_types = ["stock", "bond", "etf", "option", "future", "forex"]
        for asset_type in asset_types:
            if asset_type in nl_query and "assets" in tables:
                conditions.append(f"assets.asset_type = '{asset_type}'")

        if "orders" in tables:
            status_terms = {
                "completed": "completed",
                "pending": "pending",
                "cancelled": "cancelled",
                "open": "open",
                "closed": "closed"
            }

            for term, status in status_terms.items():
                if term in nl_query:
                    conditions.append(f"order_status.status = '{status}'")

        return conditions

    def _extract_sort_field(self, nl_query, tables):
        """Extract the field to sort by from the query"""
        nl_query = nl_query.lower()

        # Direct pattern matching for common patterns
        if "price" in nl_query and "descending" in nl_query:
            if "price_history" in tables:
                return "price_history.close_price"
            elif "assets" in tables and "price_history" not in tables:
                return "assets.price"
            elif "trades" in tables:
                return "trades.price"

        if "date" in nl_query and "ascending" in nl_query:
            if "trades" in tables:
                return "trades.trade_date"
            elif "orders" in tables:
                return "orders.order_date"

        # Continue with original logic
        sort_indicators = ["sort by", "ordered by", "arranged by", "in order of"]
        sort_field = None

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
                        "volume": "quantity"
                    }

                    field = field_mappings.get(field_text)

                    if field:
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

    def _extract_superlative_field(self, nl_query, tables, superlative_type):
        """
        Extract the field to sort/compare by for superlative queries
        """
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
        """Extract a numeric value from a query (for limits, etc.)"""
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

        # Look for standalone numbers
        all_numbers = re.findall(r'\b\d+\b', nl_query)
        for num in all_numbers:
            try:
                value = int(num)
                if 1 <= value <= 1000:  # Reasonable limit range
                    return value
            except ValueError:
                pass

        return None

    def _get_join_condition(self, table1, table2):
        """Get join condition between two tables"""
        join_mappings = {
            ("traders", "trades"): "traders.trader_id = trades.trader_id",
            ("trades", "traders"): "trades.trader_id = traders.trader_id",

            ("assets", "trades"): "assets.asset_id = trades.asset_id",
            ("trades", "assets"): "trades.asset_id = assets.asset_id",

            ("markets", "trades"): "markets.market_id = trades.market_id",
            ("trades", "markets"): "trades.market_id = markets.market_id",

            ("brokers", "assets"): "brokers.broker_id = assets.broker_id",
            ("assets", "brokers"): "assets.broker_id = brokers.broker_id",

            ("traders", "accounts"): "traders.trader_id = accounts.trader_id",
            ("accounts", "traders"): "accounts.trader_id = traders.trader_id",

            ("accounts", "transactions"): "accounts.account_id = transactions.account_id",
            ("transactions", "accounts"): "transactions.account_id = accounts.account_id",

            ("trades", "orders"): "trades.trade_id = orders.trade_id",
            ("orders", "trades"): "orders.trade_id = trades.trade_id",

            ("orders", "order_status"): "orders.order_id = order_status.order_id",
            ("order_status", "orders"): "order_status.order_id = orders.order_id",

            ("assets", "price_history"): "assets.asset_id = price_history.asset_id",
            ("price_history", "assets"): "price_history.asset_id = assets.asset_id",

            # Add these for better join support
            ("traders",
             "transactions"): "traders.trader_id = accounts.trader_id AND accounts.account_id = transactions.account_id",
            ("transactions",
             "traders"): "transactions.account_id = accounts.account_id AND accounts.trader_id = traders.trader_id",
        }

        table_pair = (table1, table2)
        return join_mappings.get(table_pair)

    def _execute_and_process_query(self, sql):
        """Execute the SQL query and process the results"""
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
        """
        Process a query without relying on intent classification
        This is used as a fallback when intent classification fails
        """
        try:
            self.logger.info(f"Direct query processing for: {nl_query}")

            # Try the new semantic analysis approach first
            semantics = self._analyze_query_semantics(nl_query)
            sql = self._build_sql_query(semantics)

            if sql:
                self.logger.info(f"Generated SQL using semantic analysis: {sql}")
                return self._execute_and_process_query(sql)

            # If the semantic analysis fails, fall back to the old method
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
            self.logger.info(f"Generated SQL from natural language: {sql}")

            return self._execute_and_process_query(sql)
        except Exception as e:
            self.logger.error(f"Error in secure_process_query: {e}")
            return None

    def natural_language_to_sql(self, nl_query):
        """Convert natural language to SQL without executing it"""
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
        self.logger.info(f"Generated SQL from natural language: {sql}")

        return sql