import json
import logging
import argparse
import sys
import time
from tqdm import tqdm

from encryption_manager import HomomorphicEncryptionManager
from secure_database_connector import SecureDatabaseConnector
import logging
from pathlib import Path
from load_config import load_config
from encryption_manager import HomomorphicEncryptionManager
from secure_database_connector import SecureDatabaseConnector

class DataEncryptionMigrator:
    def __init__(
            self,
            config_path: str = "config.json",
            encryption_manager=None,
            db_connector=None
    ):
        self.logger = logging.getLogger(__name__)
        if isinstance(config_path, dict):
            self.config = config_path
        else:
            self.config = load_config(config_path)

        if encryption_manager is not None:
            self.encryption_manager = encryption_manager
        else:
            enc_cfg = self.config["encryption"]
            keys_dir = Path(__file__).parent / enc_cfg.get("keys_dir", "encryption_keys")
            self.encryption_manager = HomomorphicEncryptionManager(
                key_size=enc_cfg["key_size"],
                context_params=enc_cfg["context_parameters"],
                keys_dir=str(keys_dir)
            )
        self.logger.info("Encryption manager ready")

        if db_connector is not None:
            self.db_connector = db_connector
        else:
            db_cfg = self.config["database"]
            self.db_connector = SecureDatabaseConnector(
                host=db_cfg["host"],
                user=db_cfg["user"],
                password=db_cfg["password"],
                database=db_cfg["database"],
                encryption_manager=self.encryption_manager
            )
        if not self.db_connector.connect():
            self.logger.error("Failed to connect to database")
            raise RuntimeError("DB connection failed")
        self.logger.info("Database connector initialized and connected")

        sec_cfg = self.config.get("security", {}).get("sensitive_fields", {})
        self.sensitive_fields = {}
        for table, fields in sec_cfg.items():
            self.sensitive_fields[table] = list(fields)

        self.logger.info(f"Sensitive fields loaded for tables: {list(self.sensitive_fields)}")
    def get_primary_key(self, table):
        q = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
        """
        res = self.db_connector.execute_query(q, [self.db_connector.database, table])
        return res[0]["COLUMN_NAME"] if res and res[0].get("COLUMN_NAME") else None

    def migrate_numeric_fields(self, table, fields, batch_size=100):
        pk = self.get_primary_key(table)
        if not pk:
            self.logger.error(f"No PK found for {table}")
            return
        count = self.db_connector.execute_query(f"SELECT COUNT(*) as total FROM {table}")[0]['total']
        for offset in range(0, count, batch_size):
            rows = self.db_connector.execute_query(
                f"SELECT {pk}, {', '.join(fields)} FROM {table} "
                f"ORDER BY {pk} LIMIT %s OFFSET %s", (batch_size, offset)
            )
            for r in rows:
                key = r[pk]
                for f in fields:
                    val = r[f]
                    if val is None: continue
                    blob = self.encryption_manager.encrypt_numeric(val)
                    self.db_connector.execute_query(
                        f"UPDATE {table} SET {f}_encrypted = %s WHERE {pk} = %s", (blob, key)
                    )

    def migrate_string_fields(self, table, fields, batch_size=500):
        pk = self.get_primary_key(table)
        if not pk:
            self.logger.error(f"No PK found for {table}")
            return

        for field in fields:
            encrypted_col = f"{field}_encrypted"
            offset = 0

            while True:

                rows = self.db_connector.execute_query(
                    f"SELECT `{pk}`, `{field}` FROM `{table}` "
                    f"ORDER BY `{pk}` LIMIT %s OFFSET %s",
                    (batch_size, offset)
                )
                if not rows:
                    break


                for row in rows:
                    key = row[pk]
                    plaintext = row[field]
                    if plaintext is None or plaintext == "":
                        continue


                    bfv_blob = self.encryption_manager.encrypt_string(plaintext)


                    self.db_connector.execute_query(
                        f"UPDATE `{table}` "
                        f"SET `{encrypted_col}` = %s "
                        f"WHERE `{pk}` = %s",
                        (bfv_blob, key)
                    )

                offset += batch_size
                self.logger.info(
                    f"Migrated batch of {len(rows)} rows into {table}.{encrypted_col}"
                )

            self.logger.info(
                f"Completed BFV migration for {table}.{encrypted_col}"
            )

    def migrate_all_tables(self, batch_size_numeric=100, batch_size_string=500):
        for table, all_fields in self.sensitive_fields.items():
            numeric = []
            strings = []
            for f in all_fields:
                t = self.encryption_manager._get_field_type(f"{table}.{f}")
                if t == 'numeric':
                    numeric.append(f)
                else:
                    strings.append(f)
            if numeric:
                self.logger.info(f"Encrypting numeric fields {numeric} in {table}")
                self.migrate_numeric_fields(table, numeric, batch_size_numeric)
            if strings:
                self.logger.info(f"Migrating string fields {strings} in {table}")
                self.migrate_string_fields(table, strings, batch_size_string)
        self.cleanup_plaintext_columns()

    def cleanup_plaintext_columns(self):
        drops = {
            "traders": ["email", "phone"],
            "brokers": ["contact_email", "license_number"],
            "accounts": ["balance"],
        }
        for table, fields in drops.items():
            cols = ", ".join(f"DROP COLUMN `{f}`" for f in fields)
            sql = f"ALTER TABLE `{table}` {cols}"
            self.db_connector.execute_query(sql)
            self.logger.info(f"Dropped plaintext columns {fields} from {table}")





def main():

    parser = argparse.ArgumentParser(description="Migrate sensitive columns to CKKS (numeric) and BFV (string)")
    parser.add_argument('--config', default='config.json')
    parser.add_argument('--num-batch-num', type=int, default=100)
    parser.add_argument('--num-batch-str', type=int, default=500)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    cfg = json.load(open(args.config))

    migrator = DataEncryptionMigrator(cfg)
    migrator.migrate_all_tables(
        batch_size_numeric=args.num_batch_num,
        batch_size_string=args.num_batch_str
    )

if __name__ == '__main__':
    main()
