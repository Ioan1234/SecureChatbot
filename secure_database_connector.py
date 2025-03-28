import logging
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Any, Optional, Union, Tuple
import json
from database_connector import DatabaseConnector


class SecureDatabaseConnector(DatabaseConnector):

    def __init__(self, host, user, password, database, encryption_manager):

        super().__init__(host, user, password, database)

        self.encryption_manager = encryption_manager

        self.sensitive_fields = {
            "traders": ["email", "phone"],
            "brokers": ["license_number", "contact_email"],
            "accounts": ["balance"],
        }

        self.field_mapping = self._build_field_mapping()

    def is_connected(self):
        try:
            return self.connection and self.connection.is_connected()
        except Error:
            return False

    def _build_field_mapping(self):
        mapping = {}

        for table, fields in self.sensitive_fields.items():
            for field in fields:
                mapping[f"{table}.{field}"] = f"{table}.{field}"
                mapping[field] = f"{table}.{field}"

        return mapping

    def _check_encryption_schema(self):
        try:
            create_metadata_table = """
            CREATE TABLE IF NOT EXISTS encryption_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                field_name VARCHAR(100) NOT NULL,
                is_encrypted BOOLEAN DEFAULT FALSE,
                encryption_type VARCHAR(50),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_field (table_name, field_name)
            )
            """

            self.execute_query(create_metadata_table)

            for table, fields in self.sensitive_fields.items():
                for field in fields:
                    self._ensure_encrypted_column(table, field)

            self.logger.info("Encryption schema check completed")
        except Exception as e:
            self.logger.error(f"Error checking encryption schema: {e}")

    def _ensure_encrypted_column(self, table, field):
        try:
            check_column = f"""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = '{self.database}' 
            AND TABLE_NAME = '{table}' 
            AND COLUMN_NAME = '{field}_encrypted'
            """

            result = self.execute_query(check_column)

            if result and result[0]['COUNT(*)'] == 0:
                add_column = f"""
                ALTER TABLE {table}
                ADD COLUMN {field}_encrypted MEDIUMBLOB
                """

                self.execute_query(add_column)

                insert_metadata = f"""
                INSERT INTO encryption_metadata 
                (table_name, field_name, is_encrypted, encryption_type) 
                VALUES ('{table}', '{field}', TRUE, 'homomorphic')
                ON DUPLICATE KEY UPDATE 
                is_encrypted = TRUE, 
                encryption_type = 'homomorphic',
                last_updated = CURRENT_TIMESTAMP
                """

                self.execute_query(insert_metadata)

                self.logger.info(f"Added encrypted column for {table}.{field}")
        except Exception as e:
            self.logger.error(f"Error ensuring encrypted column for {table}.{field}: {e}")

    def connect(self):
        result = super().connect()
        if result:
            self._check_encryption_schema()
        return result

    def execute_encrypted_query(self, query_type, tables, fields=None, conditions=None, order_by=None, limit=None):

        try:
            if query_type.upper() == "SELECT":
                return self._execute_encrypted_select(tables, fields, conditions, order_by, limit)
            elif query_type.upper() == "INSERT":
                return self._execute_encrypted_insert(tables[0], fields)
            elif query_type.upper() == "UPDATE":
                return self._execute_encrypted_update(tables[0], fields, conditions)
            else:
                self.logger.error(f"Unsupported encrypted query type: {query_type}")
                return None
        except Exception as e:
            self.logger.error(f"Error executing encrypted query: {e}")
            return None

    def _execute_encrypted_select(self, tables, fields=None, conditions=None, order_by=None, limit=None):

        main_table = tables[0]

        encrypted_conditions = []
        regular_conditions = []

        if conditions:
            for condition in conditions:
                field = condition.get("field")
                operation = condition.get("operation")
                value = condition.get("value")

                is_sensitive = False
                for table in tables:
                    if table in self.sensitive_fields and field in self.sensitive_fields[table]:
                        is_sensitive = True
                        break

                if is_sensitive:
                    encrypted_conditions.append({
                        "field": field,
                        "operation": operation,
                        "value": value,
                        "table": main_table
                    })
                else:
                    regular_conditions.append(condition)

        sql_fields = []
        field_mapping = {}

        if not fields:
            table_schema_query = f"DESCRIBE {main_table}"
            table_schema = self.execute_query(table_schema_query)

            fields = [field["Field"] for field in table_schema]

        for field in fields:
            for table in tables:
                if field.endswith('_encrypted'):
                    continue

                if table in self.sensitive_fields and field in self.sensitive_fields[table]:
                    encrypted_field = f"{table}.{field}_encrypted"
                    sql_fields.append(encrypted_field)
                    field_mapping[encrypted_field] = f"{table}.{field}"

                    sql_fields.append(f"NULL as {table}_{field}")
                    field_mapping[f"{table}_{field}"] = f"{table}.{field}"
                else:
                    sql_fields.append(f"{table}.{field}")

        sql_fields = list(dict.fromkeys(sql_fields))

        sql = f"SELECT {', '.join(sql_fields)} FROM {' JOIN '.join(tables)}"

        where_clauses = []
        params = []

        for condition in regular_conditions:
            field = condition.get("field")
            operation = condition.get("operation")
            value = condition.get("value")

            if operation in ["=", ">", "<", ">=", "<=", "<>"]:
                where_clauses.append(f"{field} {operation} %s")
                params.append(value)
            elif operation.upper() == "LIKE":
                where_clauses.append(f"{field} LIKE %s")
                params.append(f"%{value}%")
            elif operation.upper() == "IN":
                placeholders = ", ".join(["%s"] * len(value))
                where_clauses.append(f"{field} IN ({placeholders})")
                params.extend(value)

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql += f" LIMIT {limit}"

        results = self.execute_query(sql, params)

        if not results:
            return []

        processed_results = []

        for row in results:
            processed_row = {}

            for key, value in row.items():
                if key.startswith(tuple(tables)) and key.split('_', 1)[1] in [f for table in tables for f in
                                                                              self.sensitive_fields.get(table, [])]:
                    continue

                if key.endswith("_encrypted"):
                    parts = key.split('.')
                    if len(parts) > 1:
                        table = parts[0]
                        field = parts[1].replace("_encrypted", "")
                    else:
                        field = key.replace("_encrypted", "")
                        table = None

                        for t in tables:
                            if t in self.sensitive_fields and field in self.sensitive_fields[t]:
                                table = t
                                break

                    if table and field:
                        if value is not None:
                            if isinstance(value, (memoryview, bytearray)):
                                encrypted_bytes = bytes(value)
                            else:
                                encrypted_bytes = value

                            decrypted_value = self.encryption_manager.decrypt_value(
                                encrypted_bytes,
                                f"{table}.{field}"
                            )

                            processed_row[field] = decrypted_value
                        else:
                            processed_row[field] = None
                else:
                    processed_row[key] = value

            matches_encrypted_conditions = True

            for condition in encrypted_conditions:
                field = condition.get("field")
                operation = condition.get("operation")
                value = condition.get("value")
                table = condition.get("table")

                decrypted_value = processed_row.get(field)

                if decrypted_value is None:
                    matches_encrypted_conditions = False
                    break

                if operation == "=":
                    result = decrypted_value == value
                elif operation == ">":
                    result = decrypted_value > value
                elif operation == "<":
                    result = decrypted_value < value
                else:
                    self.logger.warning(f"Unsupported operation '{operation}' for field {field}")
                    result = False

                if not result:
                    matches_encrypted_conditions = False
                    break

            if matches_encrypted_conditions:
                processed_results.append(processed_row)

        return processed_results

    def _execute_encrypted_insert(self, table, fields):

        if not fields:
            self.logger.error("No fields provided for encrypted insert")
            return None

        regular_fields = []
        regular_values = []
        encrypted_fields = []
        encrypted_values = []

        for field, value in fields.items():
            if table in self.sensitive_fields and field in self.sensitive_fields[table]:
                encrypted_field = f"{field}_encrypted"
                encrypted_value = self.encryption_manager.encrypt_value(
                    value,
                    f"{table}.{field}"
                )

                if encrypted_value is not None:
                    encrypted_fields.append(encrypted_field)
                    encrypted_values.append(encrypted_value)

                regular_fields.append(field)
                regular_values.append(value)
            else:
                regular_fields.append(field)
                regular_values.append(value)

        all_fields = regular_fields + encrypted_fields
        all_values = regular_values + encrypted_values

        placeholders = ", ".join(["%s"] * len(all_fields))

        sql = f"INSERT INTO {table} ({', '.join(all_fields)}) VALUES ({placeholders})"

        result = self.execute_query(sql, all_values)

        return result

    def _execute_encrypted_update(self, table, fields, conditions=None):
        if not fields:
            self.logger.error("No fields provided for encrypted update")
            return None

        set_clauses = []
        set_values = []

        for field, value in fields.items():
            if table in self.sensitive_fields and field in self.sensitive_fields[table]:
                encrypted_field = f"{field}_encrypted"
                encrypted_value = self.encryption_manager.encrypt_value(
                    value,
                    f"{table}.{field}"
                )

                if encrypted_value is not None:
                    set_clauses.append(f"{encrypted_field} = %s")
                    set_values.append(encrypted_value)

                set_clauses.append(f"{field} = %s")
                set_values.append(value)
            else:
                set_clauses.append(f"{field} = %s")
                set_values.append(value)

        sensitive_conditions = []
        regular_conditions = []

        if conditions:
            for condition in conditions:
                field = condition.get("field")
                operation = condition.get("operation")
                value = condition.get("value")

                if table in self.sensitive_fields and field in self.sensitive_fields[table]:
                    sensitive_conditions.append({
                        "field": field,
                        "operation": operation,
                        "value": value
                    })
                else:
                    regular_conditions.append({
                        "field": field,
                        "operation": operation,
                        "value": value
                    })

        where_clauses = []
        where_params = []

        for condition in regular_conditions:
            field = condition.get("field")
            operation = condition.get("operation")
            value = condition.get("value")

            if operation in ["=", ">", "<", ">=", "<=", "<>"]:
                where_clauses.append(f"{field} {operation} %s")
                where_params.append(value)
            elif operation.upper() == "LIKE":
                where_clauses.append(f"{field} LIKE %s")
                where_params.append(f"%{value}%")
            elif operation.upper() == "IN":
                placeholders = ", ".join(["%s"] * len(value))
                where_clauses.append(f"{field} IN ({placeholders})")
                where_params.extend(value)

        if sensitive_conditions:
            query_fields = ["id"] + [condition["field"] for condition in sensitive_conditions]

            query_fields_with_encrypted = ["id"]
            for condition in sensitive_conditions:
                field = condition["field"]
                query_fields_with_encrypted.append(field)
                query_fields_with_encrypted.append(f"{field}_encrypted")

            fields_str = ", ".join(query_fields_with_encrypted)
            query_sql = f"SELECT {fields_str} FROM {table}"

            if where_clauses:
                query_sql += " WHERE " + " AND ".join(where_clauses)

            query_result = self.execute_query(query_sql, where_params)

            if not query_result:
                self.logger.info("No rows match the regular conditions, no update needed")
                return {"affected_rows": 0}

            matching_ids = []

            for row in query_result:
                match = True

                for condition in sensitive_conditions:
                    field = condition["field"]
                    operation = condition["operation"]
                    target_value = condition["value"]

                    encrypted_field = f"{field}_encrypted"
                    encrypted_value = row.get(encrypted_field)

                    if encrypted_value is None:
                        match = False
                        break

                    if isinstance(encrypted_value, (memoryview, bytearray)):
                        encrypted_bytes = bytes(encrypted_value)
                    else:
                        encrypted_bytes = encrypted_value

                    condition_encrypted = self.encryption_manager.encrypt_value(
                        target_value,
                        f"{table}.{field}"
                    )

                    if operation == "=":
                        result = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "=="
                        )
                    elif operation == ">":
                        result = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, ">"
                        )
                    elif operation == "<":
                        result = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "<"
                        )
                    elif operation == ">=":
                        result_gt = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, ">"
                        )
                        result_eq = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "=="
                        )
                        result = result_gt or result_eq
                    elif operation == "<=":
                        result_lt = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "<"
                        )
                        result_eq = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "=="
                        )
                        result = result_lt or result_eq
                    elif operation == "<>":
                        result_eq = self.encryption_manager.compare_encrypted_values(
                            encrypted_bytes, condition_encrypted, "=="
                        )
                        result = not result_eq
                    else:
                        self.logger.warning(f"Unsupported operation '{operation}' for encrypted field {field}")
                        result = False

                    if not result:
                        match = False
                        break

                if match:
                    matching_ids.append(row["id"])

            if not matching_ids:
                self.logger.info("No rows match the sensitive conditions, no update needed")
                return {"affected_rows": 0}

            where_clauses = [f"id IN ({', '.join(['%s'] * len(matching_ids))})"]
            where_params = matching_ids

        sql = f"UPDATE {table} SET {', '.join(set_clauses)}"

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        params = set_values + where_params

        self.logger.info(f"Executing encrypted update: {sql}")
        result = self.execute_query(sql, params)

        return result

    def insert_with_encryption(self, table, data):

        return self._execute_encrypted_insert(table, data)

    def update_with_encryption(self, table, data, conditions):

        return self._execute_encrypted_update(table, data, conditions)

    def select_with_decryption(self, tables, fields=None, conditions=None, order_by=None, limit=None):

        return self._execute_encrypted_select(tables, fields, conditions, order_by, limit)

    def perform_encrypted_aggregation(self, table, field, operation, conditions=None):

        if table not in self.sensitive_fields or field not in self.sensitive_fields[table]:
            agg_op = operation.upper()
            sql = f"SELECT {agg_op}({field}) AS result FROM {table}"

            where_clauses = []
            params = []

            if conditions:
                for condition in conditions:
                    where_clauses.append(f"{condition['field']} {condition['operation']} %s")
                    params.append(condition['value'])

            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            result = self.execute_query(sql, params)
            return result[0]['result'] if result and 'result' in result[0] else None

        sql = f"SELECT {field}_encrypted FROM {table}"

        where_clauses = []
        params = []

        if conditions:
            for condition in conditions:
                where_clauses.append(f"{condition['field']} {condition['operation']} %s")
                params.append(condition['value'])

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        result = self.execute_query(sql, params)

        if not result:
            return None

        encrypted_values = []

        for row in result:
            encrypted_field = f"{field}_encrypted"
            encrypted_value = row.get(encrypted_field)

            if encrypted_value is not None:
                if isinstance(encrypted_value, (memoryview, bytearray)):
                    encrypted_bytes = bytes(encrypted_value)
                else:
                    encrypted_bytes = encrypted_value

                encrypted_values.append(encrypted_bytes)

        if not encrypted_values:
            return None

        if operation.upper() == "SUM":
            encrypted_result = self.encryption_manager.aggregate_encrypted_values(encrypted_values, "sum")
        elif operation.upper() == "AVG":
            encrypted_result = self.encryption_manager.aggregate_encrypted_values(encrypted_values, "avg")
        else:
            self.logger.warning(f"Operation {operation} not supported for encrypted fields.")
            return None

        if encrypted_result:
            return self.encryption_manager.decrypt_value(encrypted_result, f"{table}.{field}")

        return None

    def get_encryption_status(self):

        try:
            query = """
            SELECT table_name, field_name, is_encrypted, encryption_type, last_updated
            FROM encryption_metadata
            ORDER BY table_name, field_name
            """

            metadata = self.execute_query(query)

            field_stats = {}

            for table, fields in self.sensitive_fields.items():
                for field in fields:
                    encrypted_field = f"{field}_encrypted"

                    check_field = f"""
                    SELECT COUNT(*) as total, 
                           SUM(CASE WHEN {encrypted_field} IS NOT NULL THEN 1 ELSE 0 END) as encrypted
                    FROM {table}
                    """

                    result = self.execute_query(check_field)

                    if result and len(result) > 0:
                        total = result[0].get('total', 0)
                        encrypted = result[0].get('encrypted', 0)

                        field_stats[f"{table}.{field}"] = {
                            "total_records": total,
                            "encrypted_records": encrypted,
                            "encryption_percentage": round(encrypted / total * 100, 2) if total > 0 else 0
                        }

            return {
                "metadata": metadata or [],
                "field_stats": field_stats
            }
        except Exception as e:
            self.logger.error(f"Error getting encryption status: {e}")
            return {
                "error": str(e)
            }