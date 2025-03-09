
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
        self.sensitive_fields = sensitive_fields or []

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

    def natural_language_to_sql(self, nl_query):
        try:
            nl_query = nl_query.lower()

            action_type = "SELECT"
            tables = []
            columns = ["*"]
            conditions = []
            order_by = []
            limit = 100

            for question_word, sql_action in self.question_patterns.items():
                if nl_query.startswith(question_word) or question_word in nl_query:
                    action_type = sql_action
                    break

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

            if "available" in nl_query:
                columns = ["*"]

            if any(status in nl_query for status in ["complete", "completed", "pending", "cancelled"]):
                if "orders" in tables or "order" in nl_query:
                    tables = ["orders", "order_status"]
                    columns = ["orders.*", "order_status.status"]
                    conditions.append("orders.order_id = order_status.order_id")

                    if "complete" in nl_query or "completed" in nl_query:
                        conditions.append("order_status.status = 'Completed'")
                    elif "pending" in nl_query:
                        conditions.append("order_status.status = 'Pending'")
                    elif "cancelled" in nl_query or "canceled" in nl_query:
                        conditions.append("order_status.status = 'Cancelled'")

            if "where" in nl_query or "location" in nl_query:
                for location in ["new york", "london", "tokyo", "frankfurt", "hong kong", "sydney"]:
                    if location in nl_query:
                        if "markets" in tables:
                            conditions.append(f"markets.location LIKE '%{location}%'")

            if any(term in nl_query for term in ["recent", "latest", "newest", "last"]):
                if "trades" in tables:
                    order_by.append("trades.trade_date DESC")
                elif "transactions" in tables:
                    order_by.append("transactions.transaction_date DESC")
                elif "orders" in tables:
                    order_by.append("orders.order_date DESC")

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

            select_clause = action_type
            if select_clause == "SELECT" and columns:
                select_clause = f"SELECT {', '.join(columns)}"

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
            return sql

        except Exception as e:
            self.logger.error(f"Error converting natural language to SQL: {e}")
            return None

    def _make_serializable(self, value):
        if hasattr(value, '__class__') and value.__class__.__name__ == 'CKKSVector':
            return {
                "type": "encrypted",
                "value": "[ENCRYPTED]"
            }
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        return value

    def secure_process_query(self, user_query):
        try:
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

                return processed_results
            else:
                if self.validate_query(sql_query):
                    return self.db_connector.execute_query(sql_query)
                else:
                    return {"error": "Invalid or unauthorized query"}
        except Exception as e:
            self.logger.error(f"Error processing secure query: {e}")
            return {"error": f"Error processing query: {str(e)}"}

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