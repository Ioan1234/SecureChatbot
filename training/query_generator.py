import logging
import json
import random
import os
from collections import defaultdict


class DatabaseQueryGenerator:

    def __init__(self, db_connector, output_path="training/generated_training_data.json"):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.output_path = output_path

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

                sample_query = f"SELECT * FROM {table_name} LIMIT 10"
                sample_data = self.db_connector.execute_query(sample_query)

                self.db_connector.handle_unread_results()

                columns = []
                primary_key = None

                for column_info in schema:
                    if not column_info:
                        continue

                    column_name = column_info.get('Field')
                    data_type = column_info.get('Type')
                    is_primary = column_info.get('Key') == 'PRI'

                    if column_name:
                        columns.append({
                            'name': column_name,
                            'type': data_type,
                            'is_primary': is_primary
                        })

                        if is_primary:
                            primary_key = column_name

                column_values = {}
                if sample_data:
                    for col_name in columns:
                        col_name = col_name['name']
                        column_values[col_name] = []

                        for row in sample_data:
                            if col_name in row and row[col_name] is not None and row[col_name] != "":
                                value = str(row[col_name])
                                if value not in column_values[col_name]:
                                    column_values[col_name].append(value)

                tables_metadata[table_name] = {
                    'columns': columns,
                    'primary_key': primary_key,
                    'sample_values': column_values
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

                if not metadata.get('columns'):
                    continue

                for column in metadata['columns']:
                    col_name = column.get('name')
                    if col_name and col_name not in ['id', 'created_at', 'updated_at']:
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

                        numeric_types = ['int', 'float', 'decimal', 'double', 'numeric', 'tinyint', 'smallint',
                                         'bigint']
                        is_numeric = any(num_type in column.get('type', '').lower() for num_type in numeric_types)
                        if is_numeric:
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

                has_date_field = any('date' in col.get('name', '').lower() for col in metadata['columns'])
                if has_date_field:
                    for template in self.question_templates["recent"]:
                        for entity_name in entity_plural:
                            query = template.format(entities=entity_name)
                            queries.append(query)
                            labels.append("database_query_recent")

                primary_key = metadata.get('primary_key')
                if primary_key and 'sample_values' in metadata and primary_key in metadata['sample_values']:
                    sample_values = metadata['sample_values'].get(primary_key, [])

                    sample_ids = sample_values[:3] if len(sample_values) > 3 else sample_values

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

                    if not column_name or column.get('is_primary') or column_name in ['created_at', 'updated_at']:
                        continue

                    if 'sample_values' in metadata and column_name in metadata['sample_values']:
                        sample_values = metadata['sample_values'].get(column_name, [])

                        value_samples = sample_values[:2] if len(sample_values) > 2 else sample_values

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

            sensitive_data_queries = [
                ("Show me broker emails", "database_query_sensitive"),
                ("What are the license numbers for brokers", "database_query_sensitive"),
                ("Show me trader contact information", "database_query_sensitive"),
                ("What's the email address for trader 1", "database_query_sensitive"),
                ("Show me sensitive data for brokers", "database_query_sensitive"),
                ("Display all encrypted fields", "database_query_sensitive"),
                ("Show me broker 1's license number", "database_query_sensitive")
            ]

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
                ("Order trades by date newest first", "database_query_sort_descending")
            ]

            for query, label in sensitive_data_queries:
                queries.append(query)
                labels.append(label)

            for query, label in finance_queries:
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
                    "How are you", "Nice to meet you", "Start", "Begin"
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

    def enrich_existing_training_data(self, existing_path="training/training_data.json"):
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


if __name__ == "__main__":
    from database_connector import DatabaseConnector
    import json

    try:
        db_config = json.load(open("config.json"))["database"]
        db_connector = DatabaseConnector(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )

        if db_connector.connect():
            generator = DatabaseQueryGenerator(db_connector)

            training_data = generator.generate_training_data()

            combined_data = generator.enrich_existing_training_data()

            print(f"Generated {len(training_data['texts'])} training examples")
            print(f"Combined training data with {len(combined_data['texts'])} examples")
        else:
            print("Failed to connect to database")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'db_connector' in locals() and db_connector:
            db_connector.disconnect()