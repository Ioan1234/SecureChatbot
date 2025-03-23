import logging
import json
import random
import os
from collections import defaultdict


class DatabaseQueryGenerator:

    def __init__(self, db_connector, output_path="training/generated_training_data.json", sample_size=50):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.output_path = output_path
        self.sample_size = sample_size

        self.question_templates = {
            "list_all": [
                "Show me all {entities}",
                "List all {entities}",
                "Display all {entities}",
                "What {entities} do we have",
                "Show {entities} information"
            ],
            "filter_by": [
                "Show me {entities} where {attribute} is {value}",
                "List {entities} with {attribute} equal to {value}",
                "Find {entities} that have {attribute} {value}",
                "Which {entities} have {attribute} {value}",
                "Display {entities} filtered by {attribute} {value}"
            ],
            "count": [
                "How many {entities} do we have",
                "Count the number of {entities}",
                "What is the total count of {entities}",
                "Give me the count of {entities}"
            ],
            "sort_by": [
                "Show me {entities} sorted by {attribute}",
                "List {entities} ordered by {attribute}",
                "Display {entities} in {attribute} order"
            ],
            "recent": [
                "Show me recent {entities}",
                "What are the latest {entities}",
                "Display the most recent {entities}",
                "List new {entities}"
            ],
            "specific_id": [
                "Show me {entity} with id {id}",
                "Find {entity} number {id}",
                "Get details for {entity} {id}",
                "Display information about {entity} {id}"
            ],
            "detailed": [
                "Show me all details about {entities}",
                "Give me complete information on {entities}",
                "Show full records for {entities}",
                "I want to see all data for {entities}"
            ],
            "comparative_highest": [
                "Show me {entities} with highest {attribute}",
                "Which {entities} have the highest {attribute}",
                "Find {entities} with maximum {attribute}",
                "List the most expensive {entities}",
                "What are the top {entities} by {attribute}",
                "Show me the {entities} with greatest {attribute}",
                "Display {entities} with the largest {attribute}"
            ],
            "comparative_lowest": [
                "Show me {entities} with lowest {attribute}",
                "Which {entities} have the lowest {attribute}",
                "Find {entities} with minimum {attribute}",
                "List the cheapest {entities}",
                "What are the bottom {entities} by {attribute}",
                "Show me the {entities} with least {attribute}",
                "Display {entities} with the smallest {attribute}"
            ],
            "comparative_middle": [
                "Show me {entities} with median {attribute}",
                "Which {entities} have the middle {attribute}",
                "Find {entities} with average {attribute}",
                "List {entities} with mid-range {attribute}",
                "What are the middle-valued {entities} by {attribute}"
            ],
            "sort_ascending": [
                "Sort {entities} by {attribute} in ascending order",
                "Show {entities} from low to high by {attribute}",
                "List {entities} in increasing order of {attribute}",
                "Display {entities} with {attribute} from smallest to largest"
            ],
            "sort_descending": [
                "Sort {entities} by {attribute} in descending order",
                "Show {entities} from high to low by {attribute}",
                "List {entities} in decreasing order of {attribute}",
                "Display {entities} with {attribute} from largest to smallest"
            ],
            "pagination": [
                "Show me the first 10 {entities}",
                "Display page 2 of {entities}",
                "List {entities} page 3",
                "Show me {entities} 20 to 30",
                "Get the next 50 {entities}"
            ],
            "date_range": [
                "Show me {entities} between {start_date} and {end_date}",
                "Find {entities} from {start_date} to {end_date}",
                "List {entities} in date range {start_date} - {end_date}",
                "Display {entities} created between {start_date} and {end_date}"
            ],
            "aggregation": [
                "Calculate the total {attribute} of all {entities}",
                "What is the average {attribute} for {entities}",
                "Show me the sum of {attribute} across all {entities}",
                "Calculate mean {attribute} of {entities}",
                "Find the maximum {attribute} among all {entities}",
                "What's the minimum {attribute} for {entities}"
            ],
            "group_by": [
                "Group {entities} by {attribute}",
                "Show {entities} counts grouped by {attribute}",
                "Display {entities} statistics by {attribute}",
                "List {entities} summarized by {attribute}"
            ],
            "multi_table": [
                "Show {entities1} with their related {entities2}",
                "List {entities1} and their corresponding {entities2}",
                "Display {entities1} along with {entities2} information",
                "Show me {entities1} joined with {entities2}"
            ],
            "complex_filter": [
                "Find {entities} where {attribute1} is {value1}",
                "Show {entities} with {attribute1} greater than {value1}",
                "List {entities} with {attribute1} less than {value1}",
                "Display {entities} where {attribute1} contains {value1}",
                "Find {entities} with {attribute1} not equal to {value1}"
            ]
        }

        self.entity_variations = {
            "markets": ["markets", "stock exchanges", "trading venues", "exchanges"],
            "traders": ["traders", "trading users", "trade accounts", "trader profiles"],
            "brokers": ["brokers", "broker accounts", "broker profiles", "broker companies"],
            "assets": ["assets", "securities", "financial instruments", "investment options", "stocks and bonds"],
            "trades": ["trades", "transactions", "trading activity", "market activity", "trade records"],
            "accounts": ["accounts", "financial accounts", "trading accounts", "user accounts"],
            "transactions": ["transactions", "financial transactions", "money transfers", "account activity"],
            "orders": ["orders", "trade orders", "market orders", "order records"],
            "order_status": ["order statuses", "order states", "status of orders"],
            "price_history": ["price history", "historical prices", "price records", "past prices"]
        }

        self.entity_singular = {
            "markets": "market",
            "traders": "trader",
            "brokers": "broker",
            "assets": "asset",
            "trades": "trade",
            "accounts": "account",
            "transactions": "transaction",
            "orders": "order",
            "order_status": "order status",
            "price_history": "price record"
        }

        self.essential_fields = {
            "markets": ["name", "location"],
            "brokers": ["name", "license_number"],
            "traders": ["name", "registration_date", "email"],
            "assets": ["name", "asset_type"],
            "trades": ["trade_date", "quantity", "price"],
            "accounts": ["account_type", "balance", "creation_date"],
            "transactions": ["transaction_date", "transaction_type", "amount"],
            "orders": ["order_type", "order_date"],
            "order_status": ["status", "status_date"],
            "price_history": ["price_date", "open_price", "close_price"]
        }

        self.comparative_attributes = {
            "markets": ["trade_volume", "number_of_assets"],
            "brokers": ["client_count", "total_assets"],
            "traders": ["transaction_count", "account_balance", "total_trades"],
            "assets": ["price", "volume", "market_cap", "value"],
            "trades": ["price", "quantity", "value", "amount"],
            "accounts": ["balance", "transaction_count", "age"],
            "transactions": ["amount", "value"],
            "orders": ["quantity", "value", "price"],
            "price_history": ["open_price", "close_price", "volume", "price_change"]
        }

        self.date_fields = {
            "traders": ["registration_date"],
            "trades": ["trade_date"],
            "accounts": ["creation_date"],
            "transactions": ["transaction_date"],
            "orders": ["order_date"],
            "order_status": ["status_date"],
            "price_history": ["price_date"]
        }

        self.table_relationships = {
            "traders": ["accounts", "trades"],
            "brokers": ["assets"],
            "assets": ["trades", "price_history"],
            "markets": ["trades"],
            "trades": ["orders"],
            "accounts": ["transactions"],
            "orders": ["order_status"]
        }

        self.sensitive_fields = ["email", "phone", "license_number", "balance"]

        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(script_dir)
            config_path = os.path.join(parent_dir, "config.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    if "security" in config and "sensitive_fields" in config["security"]:
                        self.sensitive_fields = config["security"]["sensitive_fields"]
        except Exception as e:
            self.logger.warning(f"Could not load sensitive fields from config: {e}")

    def get_table_metadata(self):
        tables_metadata = {}

        try:
            tables = self.db_connector.execute_query("SHOW TABLES")
            if not tables:
                self.logger.warning("No tables found in database")
                return {}

            table_names = []
            for table_result in tables:
                if isinstance(table_result, dict):
                    for key in table_result:
                        table_value = table_result[key]
                        if table_value:
                            table_names.append(table_value)
                        break
                else:
                    if table_result:
                        table_names.append(table_result)

            self.db_connector.handle_unread_results()

            for table_name in table_names:
                if not table_name:
                    continue

                self.logger.info(f"Processing table: {table_name}")

                schema_query = f"DESCRIBE {table_name}"
                schema = self.db_connector.execute_query(schema_query)
                self.db_connector.handle_unread_results()

                if not schema:
                    self.logger.warning(f"Could not get schema for table: {table_name}")
                    continue

                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_result = self.db_connector.execute_query(count_query)
                self.db_connector.handle_unread_results()

                row_count = 0
                if count_result and isinstance(count_result[0], dict) and 'count' in count_result[0]:
                    row_count = count_result[0]['count']

                limit = min(self.sample_size, row_count) if row_count > 0 else self.sample_size

                sample_query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {limit}"
                sample_data = self.db_connector.execute_query(sample_query)
                self.db_connector.handle_unread_results()

                columns = []
                primary_key = None
                foreign_keys = []

                for column_info in schema:
                    if not column_info:
                        continue

                    column_name = column_info.get('Field')
                    data_type = column_info.get('Type')
                    is_primary = column_info.get('Key') == 'PRI'
                    is_foreign = column_info.get('Key') == 'MUL'

                    if column_name:
                        columns.append({
                            'name': column_name,
                            'type': data_type,
                            'is_primary': is_primary,
                            'is_foreign': is_foreign
                        })

                        if is_primary:
                            primary_key = column_name

                        if is_foreign:
                            foreign_keys.append(column_name)

                column_values = {}
                column_stats = {}
                if sample_data:
                    for col_info in columns:
                        col_name = col_info['name']
                        column_values[col_name] = []

                        if 'int' in col_info['type'].lower() or 'decimal' in col_info['type'].lower() or 'float' in \
                                col_info['type'].lower():
                            values = []
                            for row in sample_data:
                                if col_name in row and row[col_name] is not None:
                                    values.append(float(row[col_name]))

                            if values:
                                column_stats[col_name] = {
                                    'min': min(values),
                                    'max': max(values),
                                    'avg': sum(values) / len(values)
                                }

                        for row in sample_data:
                            if col_name in row and row[col_name] is not None and row[col_name] != "":
                                value = str(row[col_name])
                                if col_name.lower() not in [f.lower() for f in self.sensitive_fields]:
                                    if value not in column_values[col_name]:
                                        column_values[col_name].append(value)
                                        if len(column_values[
                                                   col_name]) >= 10:
                                            break

                tables_metadata[table_name] = {
                    'columns': columns,
                    'primary_key': primary_key,
                    'foreign_keys': foreign_keys,
                    'sample_values': column_values,
                    'stats': column_stats,
                    'row_count': row_count
                }

            return tables_metadata

        except Exception as e:
            self.logger.error(f"Error getting table metadata: {e}")
            return {}

    def generate_queries(self):
        queries = []
        labels = []

        try:
            tables_metadata = self.get_table_metadata()

            if not tables_metadata:
                self.logger.warning("No table metadata available. Cannot generate queries.")
                return [], []

            additional_templates = {
                "list_all": [
                    "Can you show me all {entities}?",
                    "I'd like to see all {entities}",
                    "Please display all {entities}",
                    "Give me a list of all {entities}"
                ],
                "count": [
                    "Tell me how many {entities} we have",
                    "I want to know the number of {entities}",
                    "Could you count all {entities} for me?",
                    "What's the total number of {entities}?"
                ]
            }

            for template_type, template_list in additional_templates.items():
                self.question_templates[template_type].extend(template_list)

            date_ranges = [
                ("2023-01-01", "2023-06-30"),
                ("2023-07-01", "2023-12-31"),
                ("2022-01-01", "2022-12-31"),
                ("2021-01-01", "2023-12-31"),
                ("last month", "today"),
                ("last year", "now")
            ]

            for table_name, metadata in tables_metadata.items():
                if table_name not in self.entity_variations and table_name not in self.entity_singular:
                    self.entity_variations[table_name] = [table_name]
                    self.entity_singular[table_name] = table_name[:-1] if table_name.endswith('s') else table_name

                entity_plural = self.entity_variations.get(table_name, [table_name])
                entity_singular = self.entity_singular.get(table_name,
                                                           table_name[:-1] if table_name.endswith('s') else table_name)

                for template in self.question_templates["list_all"]:
                    for entity_name in entity_plural:
                        query = template.format(entities=entity_name)
                        queries.append(query)
                        labels.append("database_query_list")

                for template in self.question_templates["detailed"]:
                    for entity_name in entity_plural:
                        query = template.format(entities=entity_name)
                        queries.append(query)
                        labels.append("database_query_detailed")

                for template in self.question_templates["count"]:
                    for entity_name in entity_plural:
                        query = template.format(entities=entity_name)
                        queries.append(query)
                        labels.append("database_query_count")

                row_count = metadata.get('row_count', 0)
                if row_count > 100:
                    for template in self.question_templates["pagination"]:
                        for entity_name in entity_plural:
                            query = template.format(entities=entity_name)
                            queries.append(query)
                            labels.append("database_query_pagination")

                if not metadata.get('columns'):
                    continue

                for column in metadata['columns']:
                    col_name = column.get('name')
                    if not col_name:
                        continue

                    is_sensitive = col_name.lower() in [f.lower() for f in self.sensitive_fields]

                    if col_name not in ['id', 'created_at', 'updated_at'] and not is_sensitive:
                        for template in self.question_templates["sort_by"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_sort")

                        for template in self.question_templates["sort_ascending"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_sort_ascending")

                        for template in self.question_templates["sort_descending"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_sort_descending")

                    if not is_sensitive and col_name not in ['id', 'created_at', 'updated_at']:
                        for template in self.question_templates["group_by"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_group")

                    numeric_types = ['int', 'float', 'decimal', 'double', 'numeric', 'tinyint', 'smallint', 'bigint']
                    is_numeric = any(num_type in column.get('type', '').lower() for num_type in numeric_types)

                    if is_numeric and not is_sensitive:
                        for template in self.question_templates["aggregation"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_aggregation")

                        for template in self.question_templates["comparative_highest"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_comparative_highest")

                        for template in self.question_templates["comparative_lowest"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_comparative_lowest")

                        for template in self.question_templates["comparative_middle"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    attribute=col_name.replace('_', ' ')
                                )
                                queries.append(query)
                                labels.append("database_query_comparative_middle")

                        if col_name in metadata.get('stats', {}):
                            stats = metadata['stats'][col_name]
                            for template in self.question_templates["complex_filter"]:
                                if '{attribute1}' in template and '{value1}' in template:
                                    for entity_name in entity_plural:
                                        threshold = round(stats['avg'], 2)
                                        query = template.format(
                                            entities=entity_name,
                                            attribute1=col_name.replace('_', ' '),
                                            value1=threshold
                                        )
                                        queries.append(query)
                                        labels.append("database_query_complex_filter")

                if table_name in self.date_fields:
                    date_field = self.date_fields[table_name][0]
                    for start_date, end_date in date_ranges:
                        for template in self.question_templates["date_range"]:
                            for entity_name in entity_plural:
                                query = template.format(
                                    entities=entity_name,
                                    start_date=start_date,
                                    end_date=end_date
                                )
                                queries.append(query)
                                labels.append("database_query_date_range")

                has_date_field = table_name in self.date_fields
                if has_date_field:
                    for template in self.question_templates["recent"]:
                        for entity_name in entity_plural:
                            query = template.format(entities=entity_name)
                            queries.append(query)
                            labels.append("database_query_recent")

                primary_key = metadata.get('primary_key')
                if primary_key and 'sample_values' in metadata and primary_key in metadata['sample_values']:
                    sample_values = metadata['sample_values'].get(primary_key, [])
                    sample_ids = sample_values[:5] if len(sample_values) > 5 else sample_values

                    for id_value in sample_ids:
                        for template in self.question_templates["specific_id"]:
                            query = template.format(
                                entity=entity_singular,
                                id=id_value
                            )
                            queries.append(query)
                            labels.append("database_query_specific_id")

                for column in metadata['columns']:
                    column_name = column.get('name', '')

                    if (not column_name or
                            column.get('is_primary') or
                            column_name in ['created_at', 'updated_at'] or
                            column_name.lower() in [f.lower() for f in self.sensitive_fields]):
                        continue

                    if 'sample_values' in metadata and column_name in metadata['sample_values']:
                        sample_values = metadata['sample_values'].get(column_name, [])
                        value_samples = sample_values[:3] if len(sample_values) > 3 else sample_values

                        for value in value_samples:
                            for template in self.question_templates["filter_by"]:
                                for entity_name in entity_plural:
                                    query = template.format(
                                        entities=entity_name,
                                        attribute=column_name.replace('_', ' '),
                                        value=value
                                    )
                                    queries.append(query)
                                    labels.append("database_query_filter")

                if table_name in self.table_relationships:
                    related_tables = self.table_relationships[table_name]
                    for related_table in related_tables:
                        if related_table in tables_metadata:
                            for template in self.question_templates["multi_table"]:
                                query = template.format(
                                    entities1=self.entity_variations[table_name][0],
                                    entities2=self.entity_variations[related_table][0]
                                )
                                queries.append(query)
                                labels.append("database_query_join")

            finance_queries = [
                ("Show me trades with highest value", "database_query_comparative_highest"),
                ("What is the average trade amount", "database_query_comparative_middle"),
                ("List markets with most trading activity", "database_query_comparative_highest"),
                ("Show me the most active traders", "database_query_comparative_highest"),
                ("Find transactions above $10000", "database_query_filter"),
                ("Which assets have the highest price", "database_query_comparative_highest"),
                ("Show me orders placed today", "database_query_recent"),
                ("Which traders have the largest account balance", "database_query_comparative_highest"),
                ("What is the total value of all trades", "database_query_count"),
                ("Show me the price history of asset 1", "database_query_specific_id"),
                ("Find completed orders with high value", "database_query_filter"),
                ("Which market has the most traders", "database_query_comparative_highest"),
                ("Show me brokers with most clients", "database_query_comparative_highest"),
                ("List all buy orders", "database_query_filter"),
                ("Show all sell orders", "database_query_filter"),
                ("Sort assets by price from highest to lowest", "database_query_sort_descending"),
                ("Order trades by date newest first", "database_query_sort_descending"),
                ("Show me trades executed in the last month", "database_query_date_range"),
                ("What was the average price of assets in Q1 2023", "database_query_aggregation"),
                ("Group transactions by type and show the total value", "database_query_group"),
                ("Compare trading volume across all markets", "database_query_comparative_highest"),
                ("Show assets with price increase in the last quarter", "database_query_complex_filter"),
                ("List the top 5 traders by transaction count", "database_query_comparative_highest"),
                ("Which assets had the most price volatility", "database_query_comparative_highest"),
                ("Show trading activity by market location", "database_query_group"),
                ("Calculate the total commission earned per broker", "database_query_aggregation"),
                ("Show me the distribution of order types", "database_query_group"),
                ("Find trends in trading volume by month", "database_query_aggregation"),
                ("Show me the correlation between account balance and trade frequency", "database_query_complex_filter")
            ]

            sensitive_data_queries = [
                ("Show me broker emails", "database_query_sensitive"),
                ("What are the license numbers for brokers", "database_query_sensitive"),
                ("Show me trader contact information", "database_query_sensitive"),
                ("What's the email address for trader 1", "database_query_sensitive"),
                ("Show me sensitive data for brokers", "database_query_sensitive"),
                ("Display all encrypted fields", "database_query_sensitive"),
                ("Show me broker 1's license number", "database_query_sensitive"),
                ("Export all trader contact details", "database_query_sensitive"),
                ("Give me a list of all phone numbers", "database_query_sensitive"),
                ("Show me accounts with balances over $100,000", "database_query_sensitive")
            ]

            for query, label in sensitive_data_queries:
                queries.append(query)
                labels.append(label)

            for query, label in finance_queries:
                queries.append(query)
                labels.append(label)

            asset_type_queries = [
                ("Show me all stocks", "database_query_asset_type"),
                ("List all stocks", "database_query_asset_type"),
                ("Show stocks assets", "database_query_asset_type"),
                ("Display all stock assets", "database_query_asset_type"),

                ("Show me all bonds", "database_query_asset_type"),
                ("List all bonds", "database_query_asset_type"),
                ("Show bond assets", "database_query_asset_type"),

                ("Show me all ETFs", "database_query_asset_type"),
                ("List all ETFs", "database_query_asset_type"),
                ("Show ETF assets", "database_query_asset_type"),

                ("Show me all futures", "database_query_asset_type"),
                ("List all futures contracts", "database_query_asset_type"),
                ("Show futures assets", "database_query_asset_type"),

                ("Show me all cryptocurrencies", "database_query_asset_type"),
                ("List all crypto assets", "database_query_asset_type"),
                ("Show cryptocurrency assets", "database_query_asset_type")
            ]

            for query, label in asset_type_queries:
                queries.append(query)
                labels.append(label)

            unique_queries = []
            unique_labels = []
            seen_queries = set()

            for query, label in zip(queries, labels):
                query_lower = query.lower()
                if query_lower not in seen_queries:
                    seen_queries.add(query_lower)
                    unique_queries.append(query)
                    unique_labels.append(label)

            self.logger.info(f"Generated {len(unique_queries)} unique queries from database schema")
            return unique_queries, unique_labels

        except Exception as e:
            self.logger.error(f"Error generating queries: {e}")
            return [], []

    def generate_training_data(self):
        try:
            db_queries, db_labels = self.generate_queries()

            if not db_queries:
                self.logger.warning("No queries generated. Using basic conversational intents only.")

            conversational_data = {
                "greeting": [
                    "Hello", "Hi there", "Good morning", "Hey", "Greetings",
                    "Hi chatbot", "Hello there", "Morning", "Good afternoon",
                    "How are you", "Nice to meet you", "Start", "Begin",
                    "Hello bot", "Hi there chatbot", "Howdy", "Hey there",
                    "Just saying hello", "What's up", "Yo"
                ],
                "help": [
                    "Help", "I need help", "What can you do", "How do I use this",
                    "Show me what you can do", "Instructions", "Guide me",
                    "What commands do you understand", "How to use", "Give me examples",
                    "What should I ask", "Give me some sample questions", "What are your features"
                ],
                "goodbye": [
                    "Goodbye", "Bye", "Exit", "Quit", "End", "See you later",
                    "I'm done", "Close", "That's all", "Thanks, bye",
                    "Finish", "Good night", "Cya", "Till next time"
                ]
            }

            for intent, queries in conversational_data.items():
                for query in queries:
                    db_queries.append(query)
                    db_labels.append(intent)

            training_data = {
                "texts": db_queries,
                "labels": db_labels
            }

            output_dir = os.path.dirname(self.output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            with open(self.output_path, 'w') as f:
                json.dump(training_data, f, indent=2)

            self.logger.info(f"Generated {len(db_queries)} training examples and saved to {self.output_path}")

            return training_data

        except Exception as e:
            self.logger.error(f"Error generating training data: {e}")
            return {"texts": [], "labels": []}

    def enrich_existing_training_data(self, existing_path="./generated_training_data.json"):

        try:
            new_data = self.generate_training_data()

            try:
                with open(existing_path, 'r') as f:
                    existing_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                self.logger.warning(f"Could not load existing training data: {e}")
                self.logger.info("Using only generated data")
                return new_data

            combined_texts = existing_data.get("texts", []) + new_data.get("texts", [])
            combined_labels = existing_data.get("labels", []) + new_data.get("labels", [])

            unique_texts = []
            unique_labels = []
            seen_texts = set()

            for text, label in zip(combined_texts, combined_labels):
                text_lower = text.lower()
                if text_lower not in seen_texts:
                    seen_texts.add(text_lower)
                    unique_texts.append(text)
                    unique_labels.append(label)

            combined_data = {
                "texts": unique_texts,
                "labels": unique_labels
            }

            combined_path = self.output_path.replace('.json', '_combined.json')
            output_dir = os.path.dirname(combined_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            with open(combined_path, 'w') as f:
                json.dump(combined_data, f, indent=2)

            self.logger.info(f"Combined training data with {len(unique_texts)} examples saved to {combined_path}")

            return combined_data

        except Exception as e:
            self.logger.error(f"Error enriching training data: {e}")
            return {"texts": [], "labels": []}

    def generate_bulk_test_queries(self, num_queries=100):

        try:
            all_queries, all_labels = self.generate_queries()

            if not all_queries:
                self.logger.warning("No queries generated for testing.")
                return [], []

            queries_by_label = defaultdict(list)
            for query, label in zip(all_queries, all_labels):
                queries_by_label[label].append(query)

            test_queries = []
            test_labels = []

            priority_labels = [
                "database_query_pagination",
                "database_query_aggregation",
                "database_query_group",
                "database_query_join",
                "database_query_sort_descending",
                "database_query_complex_filter",
                "database_query_date_range"
            ]

            for label in priority_labels:
                if label in queries_by_label:
                    sample_size = min(10, len(queries_by_label[label]))
                    samples = random.sample(queries_by_label[label], sample_size)
                    test_queries.extend(samples)
                    test_labels.extend([label] * sample_size)

            remaining_slots = num_queries - len(test_queries)
            if remaining_slots > 0:
                remaining_queries = []
                remaining_labels = []

                for label, queries in queries_by_label.items():
                    if label not in priority_labels:
                        remaining_queries.extend(queries)
                        remaining_labels.extend([label] * len(queries))

                if remaining_queries:
                    indices = random.sample(range(len(remaining_queries)),
                                            min(remaining_slots, len(remaining_queries)))

                    for i in indices:
                        test_queries.append(remaining_queries[i])
                        test_labels.append(remaining_labels[i])

            test_data = {
                "texts": test_queries,
                "labels": test_labels
            }

            test_path = self.output_path.replace('.json', '_test.json')
            with open(test_path, 'w') as f:
                json.dump(test_data, f, indent=2)

            self.logger.info(f"Generated {len(test_queries)} test queries saved to {test_path}")

            return test_queries, test_labels

        except Exception as e:
            self.logger.error(f"Error generating test queries: {e}")
            return [], []

    def generate_large_database_performance_tests(self):

        try:
            tables_metadata = self.get_table_metadata()

            if not tables_metadata:
                self.logger.warning("No table metadata available. Cannot generate performance tests.")
                return []

            performance_tests = []

            pagination_tests = [
                "Show me traders from 1 to 100",
                "Get traders from 101 to 200",
                "List accounts from 500 to 600",
                "Show trades from 700 to 800",
                "Display assets 900 to 1000"
            ]
            performance_tests.extend(pagination_tests)

            sorting_tests = [
                "Sort all trades by price in descending order",
                "Sort all accounts by balance in ascending order",
                "List all assets sorted by name",
                "Show all traders sorted by registration date"
            ]
            performance_tests.extend(sorting_tests)

            aggregation_tests = [
                "Calculate the average trade price across all trades",
                "Find the total transaction amount for all accounts",
                "What is the maximum account balance",
                "Count all trades grouped by market"
            ]
            performance_tests.extend(aggregation_tests)

            join_tests = [
                "Show me traders with their accounts and total balance",
                "List all assets with their complete price history",
                "Display all trades with their corresponding orders",
                "Show all markets with their trading volume"
            ]
            performance_tests.extend(join_tests)

            filter_tests = [
                "Find traders who registered in 2023 and have made more than 10 trades",
                "Show assets with price above average",
                "List accounts with balance over $10000",
                "Find markets with location in 'New York'"
            ]
            performance_tests.extend(filter_tests)

            test_path = self.output_path.replace('.json', '_performance_tests.json')
            with open(test_path, 'w') as f:
                json.dump({"tests": performance_tests}, f, indent=2)

            self.logger.info(f"Generated {len(performance_tests)} performance tests saved to {test_path}")

            return performance_tests

        except Exception as e:
            self.logger.error(f"Error generating performance tests: {e}")
            return []


if __name__ == "__main__":
    from database_connector import DatabaseConnector
    import json

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        config_path = os.path.join(parent_dir, "config.json")
        db_config = json.load(open(config_path))["database"]
        db_connector = DatabaseConnector(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )

        if db_connector.connect():
            generator = DatabaseQueryGenerator(
                db_connector,
                output_path="generated_training_data.json",
                sample_size=500
            )

            training_data = generator.generate_training_data()
            print(f"Generated {len(training_data['texts'])} training examples")

            test_queries, test_labels = generator.generate_bulk_test_queries(100)
            print(f"Generated {len(test_queries)} test queries")

            performance_tests = generator.generate_large_database_performance_tests()
            print(f"Generated {len(performance_tests)} performance tests")

            combined_data = generator.enrich_existing_training_data()
            print(f"Combined training data with {len(combined_data['texts'])} examples")
        else:
            print("Failed to connect to database")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'db_connector' in locals() and db_connector:
            db_connector.disconnect()