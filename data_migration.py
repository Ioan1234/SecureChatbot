import logging
import argparse
import sys
import time
from tqdm import tqdm

# Import our custom modules
from encryption_manager import HomomorphicEncryptionManager
from secure_database_connector import SecureDatabaseConnector


class DataEncryptionMigrator:
    def __init__(self, config, encryption_manager=None, db_connector=None):
        self.logger = logging.getLogger(__name__)
        self.config = config

        if encryption_manager:
            self.encryption_manager = encryption_manager
        else:
            enc_config = config.get("encryption", {})
            key_size = enc_config.get("key_size", 2048)
            context_params = enc_config.get("context_parameters", {})
            keys_dir = enc_config.get("keys_dir", "encryption_keys")

            self.encryption_manager = HomomorphicEncryptionManager(
                key_size=key_size,
                context_params=context_params,
                keys_dir=keys_dir
            )

        if db_connector:
            self.db_connector = db_connector
        else:
            db_config = config.get("database", {})

            self.db_connector = SecureDatabaseConnector(
                host=db_config.get("host", "localhost"),
                user=db_config.get("user", "root"),
                password=db_config.get("password", ""),
                database=db_config.get("database", "secure_chatbot"),
                encryption_manager=self.encryption_manager
            )

        self.db_connector.connect()

        self.sensitive_fields = {
            "traders": ["email", "phone"],
            "brokers": ["license_number", "contact_email"],
            "accounts": ["balance"]
        }

    def migrate_all_tables(self, batch_size=100):
        stats = {}

        for table, fields in self.sensitive_fields.items():
            table_stats = self.migrate_table(table, fields, batch_size)
            stats[table] = table_stats

        return stats

    def migrate_table(self, table, fields, batch_size=100):
        self.logger.info(f"Starting migration for table {table}...")

        stats = {
            "total_records": 0,
            "processed_records": 0,
            "successful_encryptions": 0,
            "failed_encryptions": 0,
            "elapsed_time": 0
        }

        primary_key_query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = '{self.db_connector.database}' 
        AND TABLE_NAME = '{table}' 
        AND CONSTRAINT_NAME = 'PRIMARY'
        """

        result = self.db_connector.execute_query(primary_key_query)
        if not result or not result[0]["COLUMN_NAME"]:
            self.logger.error(f"Could not determine primary key for table {table}")
            return stats

        primary_key = result[0]["COLUMN_NAME"]

        count_query = f"SELECT COUNT(*) as total FROM {table}"
        result = self.db_connector.execute_query(count_query)
        total_records = result[0]["total"] if result and "total" in result[0] else 0
        stats["total_records"] = total_records

        if total_records == 0:
            self.logger.info(f"No records to migrate in table {table}")
            return stats

        start_time = time.time()

        for offset in tqdm(range(0, total_records, batch_size),
                           desc=f"Migrating {table}",
                           unit="batch"):

            select_query = f"""
            SELECT {primary_key}, {', '.join(fields)}
            FROM {table}
            ORDER BY {primary_key}
            LIMIT {batch_size} OFFSET {offset}
            """

            batch = self.db_connector.execute_query(select_query)
            if not batch:
                continue

            for record in batch:
                primary_key_value = record[primary_key]

                for field in fields:
                    original_value = record.get(field)

                    if original_value is None:
                        continue

                    try:
                        encrypted_value = self.encryption_manager.encrypt_value(
                            original_value,
                            f"{table}.{field}"
                        )

                        if encrypted_value is not None:
                            update_query = f"""
                            UPDATE {table}
                            SET {field}_encrypted = %s
                            WHERE {primary_key} = %s
                            """

                            result = self.db_connector.execute_query(update_query,
                                                                     [encrypted_value, primary_key_value])

                            if result and result.get("affected_rows", 0) > 0:
                                stats["successful_encryptions"] += 1
                            else:
                                stats["failed_encryptions"] += 1
                        else:
                            stats["failed_encryptions"] += 1
                    except Exception as e:
                        self.logger.error(
                            f"Error encrypting {table}.{field} for {primary_key}={primary_key_value}: {e}")
                        stats["failed_encryptions"] += 1

                stats["processed_records"] += 1

        stats["elapsed_time"] = time.time() - start_time

        self.logger.info(f"Migration complete for table {table}. "
                         f"Processed {stats['processed_records']} records in {stats['elapsed_time']:.2f} seconds. "
                         f"Success: {stats['successful_encryptions']}, Failures: {stats['failed_encryptions']}")

        return stats

    def verify_encryption(self, table, limit=10):
        if table not in self.sensitive_fields:
            self.logger.error(f"Table {table} does not have sensitive fields defined")
            return []

        sensitive_fields = self.sensitive_fields[table]

        primary_key_query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = '{self.db_connector.database}' 
        AND TABLE_NAME = '{table}' 
        AND CONSTRAINT_NAME = 'PRIMARY'
        """

        result = self.db_connector.execute_query(primary_key_query)
        if not result or not result[0]["COLUMN_NAME"]:
            self.logger.error(f"Could not determine primary key for table {table}")
            return []

        primary_key = result[0]["COLUMN_NAME"]

        field_list = [primary_key] + sensitive_fields
        encrypted_field_conditions = []

        for field in sensitive_fields:
            field_list.append(f"{field}_encrypted")
            encrypted_field_conditions.append(f"{field}_encrypted IS NOT NULL")

        where_clause = ""
        if encrypted_field_conditions:
            where_clause = "WHERE " + " AND ".join(encrypted_field_conditions)

        select_query = f"""
        SELECT {', '.join(field_list)}
        FROM {table}
        {where_clause}
        LIMIT {limit}
        """

        records = self.db_connector.execute_query(select_query)
        if not records:
            self.logger.warning(f"No encrypted records found in table {table}")
            return []

        verification_results = []
        success_count = 0
        total_count = 0

        for record in records:
            record_result = {
                "record_id": record[primary_key],
                "fields": {}
            }

            for field in sensitive_fields:
                total_count += 1
                original_value = record.get(field)
                encrypted_value = record.get(f"{field}_encrypted")

                if encrypted_value is None:
                    record_result["fields"][field] = {
                        "status": "missing",
                        "original": original_value,
                        "decrypted": None
                    }
                    continue

                if isinstance(encrypted_value, (memoryview, bytearray)):
                    encrypted_bytes = bytes(encrypted_value)
                else:
                    encrypted_bytes = encrypted_value

                try:
                    decrypted_value = self.encryption_manager.decrypt_value(
                        encrypted_bytes,
                        f"{table}.{field}"
                    )

                    if table == "accounts" and field == "balance":
                        self.logger.info(f"ACCOUNT BALANCE: original={original_value} ({type(original_value)}), "
                                         f"decrypted={decrypted_value} ({type(decrypted_value)})")

                        original_float = float(original_value)
                        decrypted_float = float(decrypted_value)

                        if original_float != 0:
                            relative_diff = abs(original_float - decrypted_float) / abs(original_float)
                            is_match = relative_diff < 0.01
                        else:
                            is_match = abs(decrypted_float) < 0.01

                        self.logger.info(f"COMPARISON: original={original_float}, decrypted={decrypted_float}, "
                                         f"relative_diff={relative_diff:.6f}, is_match={is_match}")
                    else:
                        is_match = original_value == decrypted_value

                    record_result["fields"][field] = {
                        "status": "success" if is_match else "mismatch",
                        "original": original_value,
                        "decrypted": decrypted_value
                    }

                    if is_match:
                        success_count += 1

                except Exception as e:
                    record_result["fields"][field] = {
                        "status": "error",
                        "original": original_value,
                        "error": str(e)
                    }

            verification_results.append(record_result)

        self.logger.info(f"Verification results for {table}: {success_count}/{total_count} successful")
        return verification_results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("encryption_migration.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Migrate existing data to use homomorphic encryption")
    parser.add_argument("--config", type=str, default="config.json", help="Path to configuration file")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of records to process in each batch")
    parser.add_argument("--table", type=str, help="Specific table to migrate (default: all tables)")
    parser.add_argument("--verify", action="store_true", help="Verify encryption after migration")
    parser.add_argument("--verify-limit", type=int, default=10, help="Number of records to sample for verification")

    args = parser.parse_args()

    import json
    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return 1

    migrator = DataEncryptionMigrator(config)

    if args.table:
        if args.table not in migrator.sensitive_fields:
            logger.error(f"Table {args.table} does not have sensitive fields defined")
            return 1

        stats = migrator.migrate_table(args.table, migrator.sensitive_fields[args.table], args.batch_size)
        logger.info(f"Migration statistics for {args.table}: {stats}")
    else:
        stats = migrator.migrate_all_tables(args.batch_size)
        logger.info(f"Migration statistics: {stats}")

    if args.verify:
        tables_to_verify = [args.table] if args.table else migrator.sensitive_fields.keys()

        for table in tables_to_verify:
            verification_results = migrator.verify_encryption(table, args.verify_limit)

            if verification_results:
                success_count = sum(
                    1 for r in verification_results for f in r["fields"].values() if f["status"] == "success")
                total_count = sum(len(r["fields"]) for r in verification_results)

                logger.info(f"Verification for {table}: {success_count}/{total_count} successful")

                for result in verification_results:
                    logger.debug(f"Record {result['record_id']}: {result['fields']}")
            else:
                logger.warning(f"No verification results for table {table}")

    return 0


if __name__ == "__main__":
    sys.exit(main())