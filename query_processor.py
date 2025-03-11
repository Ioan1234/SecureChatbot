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

        # Attributes for handling special query types
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

    def extract_number_from_query(self, nl_query):
        """Extract a number that represents how many items to return from a query."""
        self.logger.info(f"Attempting to extract number from: '{nl_query}'")

        # Dictionary to convert word numbers to digits
        word_to_number = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
            'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14, 'fifteen': 15,
            'sixteen': 16, 'seventeen': 17, 'eighteen': 18, 'nineteen': 19, 'twenty': 20,
            'thirty': 30, 'forty': 40, 'fifty': 50
        }

        # First try to find word numbers (like "two", "three", etc.)
        for word, num in word_to_number.items():
            pattern = rf'\b{word}\b'
            if re.search(pattern, nl_query.lower()):
                self.logger.info(f"Found word number: {word} ({num})")
                return num

        # If no word number found, try to find digit numbers
        all_numbers = re.findall(r'\b(\d+)\b', nl_query)
        self.logger.info(f"Found digit numbers in query: {all_numbers}")

        if all_numbers:
            # Use the first number found as a simple approach
            first_num = int(all_numbers[0])
            self.logger.info(f"Using first digit number found: {first_num}")
            return first_num

        return None

    def handle_comparative_queries(self, nl_query, tables, columns, conditions, order_by, limit=100):
        """
        Handle comparative queries like highest, lowest, median,
        sorting (ascending/descending), and middle values.
        """
        nl_query = nl_query.lower()

        # Track whether we need to modify the query
        modified = False

        # Define comparison patterns
        comparatives = {
            "highest": (
            "DESC", ["high", "highest", "maximum", "most expensive", "top", "largest", "greatest", "biggest"]),
            "lowest": ("ASC", ["low", "lowest", "minimum", "cheapest", "bottom", "smallest", "least"]),
            "median": ("MEDIAN", ["median", "middle", "mid", "center", "mid values", "middle values"]),
            "ascending": ("ASC", ["ascending", "increasing", "going up", "from low to high", "smallest to largest"]),
            "descending": (
            "DESC", ["descending", "decreasing", "going down", "from high to low", "largest to smallest"])
        }

        # Check for price-related terms
        price_related = any(term in nl_query for term in ["price", "cost", "value", "worth", "expensive", "cheap"])

        # Check for sorting-related terms
        sort_related = any(term in nl_query for term in ["sort", "order", "arrange", "rank"])

        # Check which comparison is requested
        requested_comparison = None
        for comp_type, (sort_order, terms) in comparatives.items():
            if any(term in nl_query for term in terms):
                requested_comparison = comp_type
                self.logger.info(f"Detected comparative query type: {requested_comparison}")
                break

        if requested_comparison:
            # Handle different comparison types

            # Simple ascending/descending sorting (not highest/lowest)
            if requested_comparison in ["ascending", "descending"]:
                if "price_history" in tables or price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                    sort_direction = "ASC" if requested_comparison == "ascending" else "DESC"
                    order_by = [f"price_history.close_price {sort_direction}"]
                    self.logger.info(f"Sorting by price in {sort_direction} order")
                    modified = True

                # Handle other tables based on appropriate fields
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

            # Highest/Lowest handling
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

                # Handle markets table separately
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

                # Generic fallback for price-related comparisons
                elif price_related:
                    if "price_history" not in tables:
                        tables.append("price_history")

                        # Try to join with other tables if present
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

            # Handle median/middle value queries
            elif requested_comparison == "median":
                self.logger.info("Processing median/middle value query")
                # SQLite doesn't support MEDIAN directly, so we need special handling
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

                # Handle other tables for middle value
                elif "assets" in tables:
                    order_by = ["assets.asset_id ASC"]
                    self.needs_middle_value_handling = "assets.asset_id"
                    modified = True
                elif "markets" in tables:
                    order_by = ["markets.market_id ASC"]
                    self.needs_middle_value_handling = "markets.market_id"
                    modified = True

        # If no specific comparative is found but user wants sorting
        elif sort_related:
            # Default to ascending order if not specified
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

            action_type = "SELECT"
            tables = []
            columns = ["*"]
            conditions = []
            order_by = []
            limit = 100
            limit_clause = None  # Initialize limit_clause as None

            # Reset the special query handling flags
            self.needs_highest_value_handling = None
            self.needs_lowest_value_handling = None
            self.needs_middle_value_handling = None

            # Check for numeric limits (word numbers or digits)
            requested_limit = self.extract_number_from_query(nl_query)
            if requested_limit is not None and requested_limit > 0:
                limit = requested_limit
                self.logger.info(f"Setting limit to {limit} based on extracted number")

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

            # Handle comparative queries (highest, lowest, median)
            tables, columns, conditions, order_by, comparative_modified, comparative_limit = self.handle_comparative_queries(
                nl_query, tables, columns, conditions, order_by, limit
            )

            # Update the limit if a comparative query was detected
            if comparative_modified:
                limit = comparative_limit
                self.logger.info(f"Comparative query detected. Setting limit to {limit}")

            # Handle special cases for highest/lowest values to ensure all matching records are returned
            if self.needs_highest_value_handling or self.needs_lowest_value_handling:
                value_field = self.needs_highest_value_handling or self.needs_lowest_value_handling
                is_highest = bool(self.needs_highest_value_handling)

                # Create a separate SQL query to find the extreme value first
                subquery_table_clause = f"FROM {', '.join(tables)}"
                subquery_condition_clause = ""
                if conditions:
                    subquery_condition_clause = f"WHERE {' AND '.join(conditions)}"

                # Use MAX or MIN based on whether we're looking for highest or lowest
                agg_function = "MAX" if is_highest else "MIN"
                get_extreme_sql = f"SELECT {agg_function}({value_field}) as extreme_value {subquery_table_clause} {subquery_condition_clause}"

                self.logger.info(
                    f"Executing subquery to find {'highest' if is_highest else 'lowest'} value: {get_extreme_sql}")

                # Execute the subquery to get the extreme value
                extreme_result = self.db_connector.execute_query(get_extreme_sql)

                if extreme_result and isinstance(extreme_result, list) and len(extreme_result) > 0:
                    extreme_value = extreme_result[0].get('extreme_value')
                    if extreme_value is not None:
                        self.logger.info(f"Found {'highest' if is_highest else 'lowest'} value: {extreme_value}")

                        # Add a condition to only include records with this extreme value
                        conditions.append(f"{value_field} = {extreme_value}")

                        # We no longer need to limit to one result since we're getting all matches
                        limit = 100
                        self.logger.info(
                            f"Modified query to return all records with {'highest' if is_highest else 'lowest'} value")

            # Handle special cases for middle/median values
            if self.needs_middle_value_handling:
                value_field = self.needs_middle_value_handling

                # First, create a query to get all sorted values
                subquery_table_clause = f"FROM {', '.join(tables)}"
                subquery_condition_clause = ""
                if conditions:
                    subquery_condition_clause = f"WHERE {' AND '.join(conditions)}"

                # First get the total count
                count_sql = f"SELECT COUNT(*) as total_count {subquery_table_clause} {subquery_condition_clause}"
                self.logger.info(f"Executing count query for middle value: {count_sql}")

                # Execute the count query
                count_result = self.db_connector.execute_query(count_sql)

                if count_result and isinstance(count_result, list) and len(count_result) > 0:
                    total_count = count_result[0].get('total_count', 0)
                    if total_count > 0:
                        self.logger.info(f"Found {total_count} total records for middle value calculation")

                        # Calculate the middle position
                        middle_position = total_count // 2

                        # Now get the middle value using OFFSET
                        middle_value_sql = f"SELECT {value_field} as middle_value {subquery_table_clause} {subquery_condition_clause} ORDER BY {value_field} ASC LIMIT 1 OFFSET {middle_position}"
                        self.logger.info(f"Executing query to find middle value: {middle_value_sql}")

                        # Execute the middle value query
                        middle_value_result = self.db_connector.execute_query(middle_value_sql)

                        if middle_value_result and isinstance(middle_value_result, list) and len(
                                middle_value_result) > 0:
                            middle_value = middle_value_result[0].get('middle_value')
                            if middle_value is not None:
                                self.logger.info(f"Found middle value: {middle_value}")

                                # Add a condition to only include records with this middle value
                                conditions.append(f"{value_field} = {middle_value}")

                                # We no longer need to limit to one result since we're getting all matches
                                limit = 100
                                self.logger.info(f"Modified query to return all records with middle value")
                            else:
                                self.logger.warning("Could not extract middle value from result")
                        else:
                            self.logger.warning("Middle value query returned no results")
                    else:
                        self.logger.warning("No records found for middle value calculation")
                else:
                    self.logger.warning("Failed to execute count query for middle value")

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

            # Use the limit_clause if it was already set, otherwise generate a normal one
            if limit_clause is None:
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

    def deduplicate_results(self, results):
        """
        Deduplicate results while preserving records with different significant attributes.
        """
        if not results or not isinstance(results, list) or len(results) <= 1:
            return results

        self.logger.info(f"Deduplicating {len(results)} results")

        # Determine the primary keys based on the tables in the results
        id_fields = []
        for field in results[0].keys():
            if field.endswith('_id'):
                id_fields.append(field)

        # Identify key fields that might make records unique
        name_fields = [field for field in results[0].keys() if 'name' in field.lower()]
        date_fields = [field for field in results[0].keys() if 'date' in field.lower()]
        location_fields = [field for field in results[0].keys() if field in ['location']]

        # Fields that we'll exclude when checking for duplicates
        exclude_from_comparison = []
        # Always exclude primary ID fields from comparison to avoid considering
        # records with different IDs as duplicates
        exclude_from_comparison.extend(id_fields)

        # Fields that we'll use to detect differences that make records unique
        significant_fields = []
        significant_fields.extend(name_fields)
        significant_fields.extend(location_fields)

        # Check if we have name fields to distinguish records
        has_name_fields = bool(name_fields)

        deduplicated = []
        seen_signatures = set()

        for record in results:
            # Create a signature for the record, excluding ID fields
            # but including name fields to ensure records with different names
            # are preserved as distinct
            signature_parts = []
            for key, value in sorted(record.items()):
                if key not in exclude_from_comparison:
                    signature_parts.append(f"{key}:{value}")

            record_signature = tuple(signature_parts)

            # If we have a name field but there's no difference in data values,
            # check if this record's name is already represented
            if has_name_fields and record_signature in seen_signatures:
                # Check if this record has a different name than existing records
                # If it does, we'll add it anyway
                existing_names = {r[name_fields[0]] for r in deduplicated if
                                  record_signature == self._get_signature(r, exclude_from_comparison)}
                current_name = record[name_fields[0]]

                if current_name not in existing_names:
                    # This record has a different name, include it
                    deduplicated.append(record)
                    self.logger.info(f"Kept record with unique name: {current_name}")
                else:
                    # This is a true duplicate with same name, skip it
                    self.logger.info(f"Skipped duplicate with name: {current_name}")
                    continue
            else:
                # First time seeing this data signature, include the record
                deduplicated.append(record)
                seen_signatures.add(record_signature)

        removed_count = len(results) - len(deduplicated)
        self.logger.info(f"Deduplication removed {removed_count} records, keeping {len(deduplicated)}")

        return deduplicated

    def _get_signature(self, record, exclude_fields):
        """Helper method to get a record's signature for deduplication comparison"""
        signature_parts = []
        for key, value in sorted(record.items()):
            if key not in exclude_fields:
                signature_parts.append(f"{key}:{value}")
        return tuple(signature_parts)

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

                # Process the results to handle sensitive fields
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

                # Apply deduplication if needed - only for comparative queries
                if self.needs_highest_value_handling or self.needs_lowest_value_handling or self.needs_middle_value_handling:
                    # We want to deduplicate the results while preserving records with different names
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

    def _make_serializable(self, value):
        if hasattr(value, '__class__') and value.__class__.__name__ == 'CKKSVector':
            return {
                "type": "encrypted",
                "value": "[ENCRYPTED]"
            }
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        return value

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