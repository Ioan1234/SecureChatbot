import logging
import re
import datetime


class QueryProcessor:
    def __init__(self, db_connector, encryption_manager, sensitive_fields=None):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.encryption_manager = encryption_manager
        self.sensitive_fields = sensitive_fields or [
            "email", "contact_email", "phone", "license_number", "balance"
        ]

        self.schema = {
            "traders": ["trader_id", "name", "email", "phone", "registration_date"],
            "brokers": ["broker_id", "name", "license_number", "contact_email"],
            "assets": ["asset_id", "name", "asset_type", "broker_id"],
            "markets": ["market_id", "name", "location", "opening_time", "closing_time"],
            "trades": ["trade_id", "trader_id", "asset_id", "market_id", "trade_date", "quantity", "price"],
            "accounts": ["account_id", "trader_id", "balance", "account_type", "creation_date"],
            "transactions": ["transaction_id", "account_id", "transaction_date", "transaction_type", "amount"],
            "orders": ["order_id", "trade_id", "order_type", "order_date"],
            "order_status": ["status_id", "order_id", "status", "status_date"],
            "price_history": ["price_id", "asset_id", "price_date", "open_price", "close_price"]
        }

        self.relationships = {
            ("traders", "trades"): ("trader_id", "trader_id"),
            ("traders", "accounts"): ("trader_id", "trader_id"),
            ("assets", "trades"): ("asset_id", "asset_id"),
            ("assets", "price_history"): ("asset_id", "asset_id"),
            ("markets", "trades"): ("market_id", "market_id"),
            ("brokers", "assets"): ("broker_id", "broker_id"),
            ("accounts", "transactions"): ("account_id", "account_id"),
            ("trades", "orders"): ("trade_id", "trade_id"),
            ("orders", "order_status"): ("order_id", "order_id")
        }

        self.filter_patterns = {
            "equal": "{field} = {value}",
            "not_equal": "{field} != {value}",
            "greater": "{field} > {value}",
            "less": "{field} < {value}",
            "contains": "{field} LIKE '%{value}%'",
            "starts_with": "{field} LIKE '{value}%'",
            "ends_with": "{field} LIKE '%{value}'",
            "between": "{field} BETWEEN {value1} AND {value2}",
            "in": "{field} IN ({values})"
        }

    def process_query(self, nl_query, intent_data=None):
        self.logger.info(f"Processing query with intent: {intent_data}")

        intent = intent_data.get('intent') if intent_data else None

        if "trader" in nl_query.lower() and "balance" in nl_query.lower():
            sort_order = "DESC"
            if "lowest" in nl_query.lower():
                sort_order = "ASC"

            sql = f"""
            SELECT 
                traders.trader_id,
                traders.name, 
                traders.registration_date,
                accounts.account_id,
                accounts.account_type,
                accounts.creation_date,
                accounts.balance
            FROM 
                traders
            JOIN 
                accounts ON traders.trader_id = accounts.trader_id
            ORDER BY 
                accounts.balance {sort_order}
            LIMIT 10
            """

            self.logger.info(f"Generated focused trader balance SQL: {sql}")
            result = self._execute_and_process_query(sql)
            return result

        if intent in ['database_query_date_range', 'database_query_recent']:
            return self._handle_date_range_query(nl_query, intent_data)

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

        where_clause = self._generate_where_clause(nl_query, tables[0])

        order_clause = self._generate_order_clause(nl_query, tables[0])

        limit_clause = "LIMIT 100"

        sql_parts = [select_clause, from_clause, limit_clause]
        sql = " ".join(sql_parts)

        self.logger.info(f"Generated SQL: {sql}")
        return self._execute_and_process_query(sql)

    def _generate_where_clause(self, query, table):
        query_lower = query.lower()
        where_conditions = []

        over_pattern = r'(with|having)\s+(\w+)\s+(over|above|greater than|more than)\s+(\d+[,\d]*(?:\.\d+)?)'
        for match in re.finditer(over_pattern, query_lower):
            field = match.group(2)
            value = match.group(4).replace(',', '')

            table_fields = self.schema.get(table, [])
            field_match = None
            for tf in table_fields:
                if field in tf or field.replace(' ', '_') == tf:
                    field_match = tf
                    break

            if field_match:
                where_conditions.append(f"{table}.{field_match} > {value}")

        under_pattern = r'(with|having)\s+(\w+)\s+(under|below|less than|lower than)\s+(\d+[,\d]*(?:\.\d+)?)'
        for match in re.finditer(under_pattern, query_lower):
            field = match.group(2)
            value = match.group(4).replace(',', '')

            table_fields = self.schema.get(table, [])
            field_match = None
            for tf in table_fields:
                if field in tf or field.replace(' ', '_') == tf:
                    field_match = tf
                    break

            if field_match:
                where_conditions.append(f"{table}.{field_match} < {value}")

        if where_conditions:
            return "WHERE " + " AND ".join(where_conditions)

        return ""

    def _generate_order_clause(self, query, table):
        query_lower = query.lower()

        if "sort by" in query_lower or "order by" in query_lower:
            sort_pattern = r'(sort|order)\s+by\s+(\w+)'
            match = re.search(sort_pattern, query_lower)
            if match:
                field = match.group(2)

                table_fields = self.schema.get(table, [])
                field_match = None
                for tf in table_fields:
                    if field in tf or field.replace(' ', '_') == tf:
                        field_match = tf
                        break

                if field_match:
                    direction = "DESC" if "desc" in query_lower or "highest" in query_lower else "ASC"
                    return f"ORDER BY {table}.{field_match} {direction}"

        date_column = self._get_date_column(table)
        if date_column:
            direction = "DESC" if "recent" in query_lower else "DESC"
            return f"ORDER BY {date_column} {direction}"

        id_column = f"{table}.{table[:-1]}_id" if table.endswith('s') else f"{table}.{table}_id"
        return f"ORDER BY {id_column} DESC"

    def _extract_fields(self, query, tables):
        if not tables:
            return ["*"]

        query_lower = query.lower()
        selected_fields = []

        for table in tables:
            if table in self.schema:
                table_fields = self.schema[table]

                for field in table_fields:
                    field_name = field.replace('_', ' ')
                    if field_name in query_lower or field in query_lower:
                        selected_fields.append(f"{table}.{field}")

        if not selected_fields:
            for table in tables:
                if table in self.schema:
                    for field in self.schema[table]:
                        if field.endswith('_id') or field == 'name':
                            selected_fields.append(f"{table}.{field}")

                        if table == 'assets' and field in ['asset_type', 'price']:
                            selected_fields.append(f"{table}.{field}")
                        elif table == 'accounts' and field == 'balance':
                            selected_fields.append(f"{table}.{field}")
                        elif table == 'trades' and field in ['quantity', 'price', 'trade_date']:
                            selected_fields.append(f"{table}.{field}")

        if not selected_fields:
            return ["*"]

        return selected_fields

    def _generate_joins_for_tables(self, tables):
        if len(tables) <= 1:
            return []

        join_clauses = []
        primary_table = tables[0]

        for secondary_table in tables[1:]:
            if (primary_table, secondary_table) in self.relationships:
                from_key, to_key = self.relationships[(primary_table, secondary_table)]
                join_clauses.append(f"{secondary_table} ON {primary_table}.{from_key} = {secondary_table}.{to_key}")

            elif (secondary_table, primary_table) in self.relationships:
                to_key, from_key = self.relationships[(secondary_table, primary_table)]
                join_clauses.append(f"{secondary_table} ON {primary_table}.{from_key} = {secondary_table}.{to_key}")

            else:
                for intermediate_table in self.schema.keys():
                    if intermediate_table == primary_table or intermediate_table == secondary_table:
                        continue

                    if ((primary_table, intermediate_table) in self.relationships and
                            (intermediate_table, secondary_table) in self.relationships):

                        p_from_key, i_to_key = self.relationships[(primary_table, intermediate_table)]
                        join_clauses.append(
                            f"{intermediate_table} ON {primary_table}.{p_from_key} = {intermediate_table}.{i_to_key}")

                        i_from_key, s_to_key = self.relationships[(intermediate_table, secondary_table)]
                        join_clauses.append(
                            f"{secondary_table} ON {intermediate_table}.{i_from_key} = {secondary_table}.{s_to_key}")

                        break

                    elif ((intermediate_table, primary_table) in self.relationships and
                          (intermediate_table, secondary_table) in self.relationships):

                        i_from_key, p_to_key = self.relationships[(intermediate_table, primary_table)]
                        join_clauses.append(
                            f"{intermediate_table} ON {primary_table}.{p_to_key} = {intermediate_table}.{i_from_key}")

                        i_from_key, s_to_key = self.relationships[(intermediate_table, secondary_table)]
                        join_clauses.append(
                            f"{secondary_table} ON {intermediate_table}.{i_from_key} = {secondary_table}.{s_to_key}")

                        break

                if len(join_clauses) == 0:
                    self.logger.warning(
                        f"No direct relationship found between {primary_table} and {secondary_table}. Using fallback join.")
                    join_clauses.append(
                        f"{secondary_table} ON {primary_table}.{primary_table[:-1]}_id = {secondary_table}.{primary_table[:-1]}_id")

        return join_clauses


    def _handle_date_range_query(self, query, intent_data=None):

        query_lower = query.lower()
        tables = self._extract_tables(query)

        if not tables:
            return None

        primary_table = tables[0]
        date_column = self._get_date_column(primary_table)

        if not date_column:
            self.logger.warning(f"No date column found for table {primary_table}")
            return None

        date_range = self._extract_date_range(query_lower)
        if not date_range:
            self.logger.warning("Could not determine date range from query")
            return self.process_query(query, intent_data)

        start_date, end_date = date_range

        sql = f"""
        SELECT * 
        FROM {primary_table}
        WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY {date_column} DESC
        LIMIT 100
        """

        self.logger.info(f"Generated date range SQL: {sql}")
        result = self._execute_and_process_query(sql)
        return result

    def _get_date_column(self, table):
        date_column_mapping = {
            "trades": "trade_date",
            "orders": "order_date",
            "transactions": "transaction_date",
            "accounts": "creation_date",
            "traders": "registration_date",
            "price_history": "price_date",
            "order_status": "status_date"
        }

        return f"{table}.{date_column_mapping.get(table, '')}" if table in date_column_mapping else None

    def _extract_date_range(self, query):

        from datetime import datetime, timedelta
        import re
        import calendar

        today = datetime.now()

        specific_date_pattern = r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})'
        specific_date_match = re.search(specific_date_pattern, query.lower())
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
            return specific_date.strftime('%Y-%m-%d'), specific_date.strftime('%Y-%m-%d')

        month_rel_year_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(last|this|next)\s+year'
        month_rel_year_match = re.search(month_rel_year_pattern, query.lower())
        if month_rel_year_match:
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
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        month_year_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})'
        month_year_match = re.search(month_year_pattern, query.lower())
        if month_year_match:
            month_name = month_year_match.group(1).lower()
            year = int(month_year_match.group(2))

            months = {
                "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
            }
            month = months[month_name]

            days_in_month = calendar.monthrange(year, month)[1]

            start_date = datetime(year, month, 1)
            end_date = datetime(year, month, days_in_month)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        year_pattern = r'from\s+(20\d{2})\b'
        year_match = re.search(year_pattern, query)
        if year_match:
            year = int(year_match.group(1))
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        alt_year_pattern = r'\b(20\d{2})\b.*?(trades|transactions|orders)'
        alt_year_match = re.search(alt_year_pattern, query)
        if alt_year_match:
            year = int(alt_year_match.group(1))
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        last_pattern = r'last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)'
        match = re.search(last_pattern, query)
        if match:
            num = int(match.group(1))
            unit = match.group(2).lower()

            if unit in ['day', 'days']:
                start_date = today - timedelta(days=num)
            elif unit in ['week', 'weeks']:
                start_date = today - timedelta(weeks=num)
            elif unit in ['month', 'months']:
                start_date = today - timedelta(days=num * 30)  # Approximate
            elif unit in ['year', 'years']:
                start_date = today - timedelta(days=num * 365)  # Approximate

            return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

        if "last year" in query:
            start_date = datetime(today.year - 1, 1, 1)
            end_date = datetime(today.year - 1, 12, 31)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        if "last month" in query:
            month = today.month - 1
            year = today.year
            if month == 0:
                month = 12
                year -= 1

            days_in_month = calendar.monthrange(year, month)[1]

            start_date = datetime(year, month, 1)
            end_date = datetime(year, month, days_in_month)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        if "last week" in query:
            start_date = today - timedelta(days=today.weekday() + 7)
            end_date = start_date + timedelta(days=6)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        if "this year" in query:
            start_date = datetime(today.year, 1, 1)
            end_date = datetime(today.year, 12, 31)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        if "this month" in query:
            days_in_month = calendar.monthrange(today.year, today.month)[1]

            start_date = datetime(today.year, today.month, 1)
            end_date = datetime(today.year, today.month, days_in_month)
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        if any(word in query for word in ["recent", "latest", "newest", "current"]):
            start_date = today - timedelta(days=30)
            return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

        months = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
        }

        for month_name, month_num in months.items():
            if month_name in query:
                year_pattern = r'\b(20\d{2})\b'
                year_match = re.search(year_pattern, query)
                year = int(year_match.group(1)) if year_match else today.year

                days_in_month = calendar.monthrange(year, month_num)[1]

                start_date = datetime(year, month_num, 1)
                end_date = datetime(year, month_num, days_in_month)
                return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        start_date = today - timedelta(days=30)
        return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

    def _extract_tables(self, query):
        query_lower = query.lower()

        table_indicators = {
            "traders": ["trader", "traders"],
            "assets": ["asset", "assets", "etf", "stock"],
            "markets": ["market", "markets"],
            "trades": ["trade", "trades"],
            "accounts": ["account", "accounts", "balance"],
            "transactions": ["transaction", "transactions", "transfer", "deposit"],
            "orders": ["order", "orders"],
            "brokers": ["broker", "brokers"],
            "price_history": ["price history", "historical price"]
        }

        for table, indicators in table_indicators.items():
            if any(indicator in query_lower for indicator in indicators):
                return [table]

        return ["traders"]

    def _analyze_query(self, query):
        query_lower = query.lower()
        components = {
            "tables": [],
            "fields": [],
            "filters": [],
            "sort_field": None,
            "sort_order": "ASC",
            "limit": 50,
            "offset": 0,
            "aggregate_function": None,
            "group_by": [],
            "is_list_query": False,
            "is_count_query": False,
            "is_aggregate_query": False,
            "is_specific_entity": False,
            "entity_name": None
        }

        for table in self.schema.keys():
            singular = table[:-1] if table.endswith('s') else table
            if table in query_lower or singular in query_lower:
                components["tables"].append(table)

        entity_patterns = [
            r'about\s+([A-Za-z0-9\s]+)',
            r'details\s+(?:for|of|about)\s+([A-Za-z0-9\s]+)',
            r'show\s+(?:me\s+)?([A-Za-z0-9\s]+)(?:\s+details)?',
            r'tell\s+me\s+about\s+([A-Za-z0-9\s]+)'
        ]

        for pattern in entity_patterns:
            match = re.search(pattern, query_lower)
            if match:
                entity_name = match.group(1).strip()
                skip_words = ["all", "the", "details", "info", "information"]
                if entity_name not in skip_words and len(entity_name.split()) <= 3:
                    components["is_specific_entity"] = True
                    components["entity_name"] = entity_name
                    break

        list_indicators = ["list", "show", "get", "find", "display", "all"]
        if any(indicator in query_lower for indicator in list_indicators) and not components["is_specific_entity"]:
            components["is_list_query"] = True

        count_indicators = ["count", "how many", "number of", "total"]
        if any(indicator in query_lower for indicator in count_indicators):
            components["is_count_query"] = True

        agg_functions = {
            "average": "AVG", "avg": "AVG", "mean": "AVG",
            "sum": "SUM", "total": "SUM",
            "minimum": "MIN", "min": "MIN", "lowest": "MIN",
            "maximum": "MAX", "max": "MAX", "highest": "MAX"
        }

        for indicator, function in agg_functions.items():
            if indicator in query_lower:
                components["is_aggregate_query"] = True
                components["aggregate_function"] = function
                break

        sort_indicators = ["sort by", "order by", "sorted by", "arranged by"]
        for indicator in sort_indicators:
            if indicator in query_lower:
                parts = query_lower.split(indicator)
                if len(parts) > 1:
                    sort_part = parts[1].strip().split()
                    if sort_part:
                        field = sort_part[0]
                        components["sort_field"] = field

                        if "desc" in query_lower or "descending" in query_lower:
                            components["sort_order"] = "DESC"
                        break

        for table in components["tables"]:
            table_fields = self.schema.get(table, [])
            for field in table_fields:
                field_name = field.replace('_', ' ')
                if field_name in query_lower or field in query_lower:
                    components["fields"].append(f"{table}.{field}")

        if "etf" in query_lower and "assets" in components["tables"]:
            components["filters"].append(("assets.asset_type", "=", "ETF"))
        elif "stock" in query_lower and "assets" in components["tables"]:
            components["filters"].append(("assets.asset_type", "=", "STOCK"))
        elif "bond" in query_lower and "assets" in components["tables"]:
            components["filters"].append(("assets.asset_type", "=", "BOND"))

        if "completed" in query_lower and "orders" in components["tables"]:
            components["filters"].append(("order_status.status", "=", "COMPLETED"))
        elif "pending" in query_lower and "orders" in components["tables"]:
            components["filters"].append(("order_status.status", "=", "PENDING"))

        if "balance" in query_lower and "accounts" in components["tables"]:
            if "highest" in query_lower:
                components["sort_field"] = "accounts.balance"
                components["sort_order"] = "DESC"
            elif "lowest" in query_lower:
                components["sort_field"] = "accounts.balance"
                components["sort_order"] = "ASC"

        if any(term in query_lower for term in ["price", "cost", "value"]):
            if "assets" in components["tables"] and "price_history" not in components["tables"]:
                components["tables"].append("price_history")
            if "highest" in query_lower:
                components["sort_field"] = "price_history.close_price"
                components["sort_order"] = "DESC"
            elif "lowest" in query_lower:
                components["sort_field"] = "price_history.close_price"
                components["sort_order"] = "ASC"

        if not components["tables"]:
            if "traders" in query_lower or "trader" in query_lower:
                components["tables"].append("traders")
            elif "assets" in query_lower or "asset" in query_lower:
                components["tables"].append("assets")
            elif "markets" in query_lower or "market" in query_lower:
                components["tables"].append("markets")
            elif "trades" in query_lower or "trade" in query_lower:
                components["tables"].append("trades")
            elif "orders" in query_lower or "order" in query_lower:
                components["tables"].append("orders")
            elif "brokers" in query_lower or "broker" in query_lower:
                components["tables"].append("brokers")

        limit_match = re.search(r'(top|first|limit)\s+(\d+)', query_lower)
        if limit_match:
            try:
                components["limit"] = int(limit_match.group(2))
            except ValueError:
                pass

        return components

    def _build_sql_query(self, components):
        if not components["tables"]:
            self.logger.warning("No tables identified in query")
            return None

        if components["is_count_query"]:
            select_clause = "SELECT COUNT(*) as count"
        elif components["is_aggregate_query"] and components["aggregate_function"]:
            agg_field = None
            if "price" in components["fields"] or "close_price" in ' '.join(components["fields"]):
                agg_field = "price_history.close_price"
            elif "balance" in components["fields"]:
                agg_field = "accounts.balance"
            elif "amount" in components["fields"]:
                agg_field = "transactions.amount"
            elif "quantity" in components["fields"]:
                agg_field = "trades.quantity"
            else:
                table = components["tables"][0]
                numeric_fields = ["price", "balance", "amount", "quantity"]
                for field in self.schema.get(table, []):
                    if any(nf in field for nf in numeric_fields):
                        agg_field = f"{table}.{field}"
                        break

            if not agg_field:
                self.logger.warning("Could not determine field for aggregation")
                select_clause = "SELECT *"
            else:
                select_clause = f"SELECT {components['aggregate_function']}({agg_field}) as result"

                if components["group_by"]:
                    group_fields = []
                    for field in components["group_by"]:
                        group_fields.append(field)
                    select_clause += f", {', '.join(group_fields)}"
        else:
            if components["fields"]:
                select_clause = f"SELECT {', '.join(components['fields'])}"
            else:
                essential_fields = []
                for table in components["tables"]:
                    table_fields = self.schema.get(table, [])
                    for field in table_fields:
                        if field == f"{table[:-1]}_id" or field == "name" or field == "balance" or field == "price":
                            essential_fields.append(f"{table}.{field}")

                if essential_fields:
                    select_clause = f"SELECT {', '.join(essential_fields)}"
                else:
                    select_clause = "SELECT *"

        primary_table = components["tables"][0]
        from_clause = f"FROM {primary_table}"

        for table in components["tables"][1:]:
            join_path = self._find_join_path(primary_table, table)
            if join_path:
                for step in join_path:
                    from_table, to_table, from_key, to_key = step
                    from_clause += f" JOIN {to_table} ON {from_table}.{from_key} = {to_table}.{to_key}"
            else:
                self.logger.warning(f"Could not find join path from {primary_table} to {table}")
        where_clauses = []
        for field, operator, value in components["filters"]:
            if isinstance(value, str) and not value.isdigit():
                value = f"'{value}'"
            where_clauses.append(f"{field} {operator} {value}")

        where_clause = ""
        if where_clauses:
            where_clause = f"WHERE {' AND '.join(where_clauses)}"

        group_by_clause = ""
        if components["is_aggregate_query"] and components["group_by"]:
            group_by_clause = f"GROUP BY {', '.join(components['group_by'])}"

        order_by_clause = ""
        if components["sort_field"]:
            order_by_clause = f"ORDER BY {components['sort_field']} {components['sort_order']}"

        limit_clause = f"LIMIT {components['limit']}"
        if components["offset"] > 0:
            limit_clause += f" OFFSET {components['offset']}"

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)

        sql_parts.append(limit_clause)

        sql = " ".join(sql_parts)
        self.logger.info(f"Generated SQL: {sql}")

        return sql

    def _find_join_path(self, from_table, to_table, visited=None):
        if visited is None:
            visited = set()

        if from_table == to_table:
            return []

        visited.add(from_table)

        if (from_table, to_table) in self.relationships:
            from_key, to_key = self.relationships[(from_table, to_table)]
            return [(from_table, to_table, from_key, to_key)]

        if (to_table, from_table) in self.relationships:
            to_key, from_key = self.relationships[(to_table, from_table)]
            return [(from_table, to_table, from_key, to_key)]

        for next_table in self.schema.keys():
            if next_table in visited:
                continue

            if (from_table, next_table) in self.relationships:
                from_key, next_key = self.relationships[(from_table, next_table)]
                rest_path = self._find_join_path(next_table, to_table, visited.copy())
                if rest_path is not None:
                    return [(from_table, next_table, from_key, next_key)] + rest_path

            if (next_table, from_table) in self.relationships:
                next_key, from_key = self.relationships[(next_table, from_table)]
                rest_path = self._find_join_path(next_table, to_table, visited.copy())
                if rest_path is not None:
                    return [(from_table, next_table, from_key, next_key)] + rest_path

        return None

    def _handle_list_query(self, query_components):
        sql = self._build_sql_query(query_components)
        if not sql:
            return None

        result = self._execute_and_process_query(sql)
        return result

    def _handle_count_query(self, query_components):
        sql = self._build_sql_query(query_components)
        if not sql:
            return None

        result = self._execute_and_process_query(sql)
        return result

    def _handle_aggregate_query(self, query_components):
        sql = self._build_sql_query(query_components)
        if not sql:
            return None

        result = self._execute_and_process_query(sql)
        return result

    def _handle_entity_query(self, query_components):
        entity_name = query_components["entity_name"]
        if not entity_name:
            return None

        tables_to_check = query_components["tables"] if query_components["tables"] else self.schema.keys()

        for table in tables_to_check:
            if "name" not in self.schema.get(table, []):
                continue

            sql = f"SELECT * FROM {table} WHERE name = '{entity_name}' LIMIT 1"
            result = self._execute_and_process_query(sql)

            if result and len(result) > 0:
                entity_id = result[0].get(f"{table[:-1]}_id")
                if entity_id:
                    related_info = self._get_related_entity_data(table, entity_id)
                    return {
                        "entity_type": table,
                        "entity_info": result,
                        "related_info": related_info
                    }

            sql = f"SELECT * FROM {table} WHERE name LIKE '%{entity_name}%' LIMIT 1"
            result = self._execute_and_process_query(sql)

            if result and len(result) > 0:
                entity_id = result[0].get(f"{table[:-1]}_id")
                if entity_id:
                    related_info = self._get_related_entity_data(table, entity_id)
                    return {
                        "entity_type": table,
                        "entity_info": result,
                        "related_info": related_info
                    }

        return None

    def _get_related_entity_data(self, entity_table, entity_id):
        related_data = {}
        id_field = f"{entity_table[:-1]}_id"

        for (table1, table2), (key1, key2) in self.relationships.items():
            if table1 == entity_table and key1 == id_field:
                sql = f"SELECT * FROM {table2} WHERE {key2} = {entity_id} LIMIT 10"
                result = self._execute_and_process_query(sql)

                if result and len(result) > 0:
                    related_data[table2] = result

            elif table2 == entity_table and key2 == id_field:
                sql = f"SELECT * FROM {table1} WHERE {key1} = {entity_id} LIMIT 10"
                result = self._execute_and_process_query(sql)

                if result and len(result) > 0:
                    related_data[table1] = result

        return related_data

    def _handle_search_query(self, query_components):
        sql = self._build_sql_query(query_components)
        if not sql:
            return None

        result = self._execute_and_process_query(sql)
        return result

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

    def _should_encrypt_field(self, field_name):
        if field_name in self.sensitive_fields:
            return True

        sensitive_patterns = [
            "password", "pwd", "secret", "token", "key",
            "ssn", "social_security", "tax_id",
            "credit_card", "card_number", "cvv", "ccv",
            "license_number", "license_id"
        ]

        sensitive_email_fields = ["email", "contact_email"]
        if field_name in sensitive_email_fields:
            return True

        if "phone" in field_name:
            return True

        return any(pattern in field_name.lower() for pattern in sensitive_patterns)