import json
import logging
import argparse
import sys
import time
from tqdm import tqdm

from encryption_manager import HomomorphicEncryptionManager
from secure_database_connector import SecureDatabaseConnector


class DataEncryptionMigrator:
    def __init__(self, config, encryption_manager=None, db_connector=None):
        self.logger = logging.getLogger(__name__)
        self.config = config

        if encryption_manager:
            self.encryption_manager = encryption_manager
        else:
            enc_cfg = config.get("encryption", {})
            self.encryption_manager = HomomorphicEncryptionManager(
                key_size=enc_cfg.get("key_size", 2048),
                context_params=enc_cfg.get("context_parameters", {}),
                keys_dir=enc_cfg.get("keys_dir", "encryption_keys")
            )

        if db_connector:
            self.db_connector = db_connector
        else:
            db_cfg = config.get("database", {})
            self.db_connector = SecureDatabaseConnector(
                host=db_cfg.get("host", "localhost"),
                user=db_cfg.get("user", "root"),
                password=db_cfg.get("password", ""),
                database=db_cfg.get("database", "secure_chatbot"),
                encryption_manager=self.encryption_manager
            )
        self.db_connector.connect()

        self.sensitive_fields = {}
        for tf, ftype in self.encryption_manager.sensitive_fields.items():
            table, field = tf.split('.', 1)
            self.sensitive_fields.setdefault(table, []).append(field)

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
                # 1) pull the plaintext column
                rows = self.db_connector.execute_query(
                    f"SELECT `{pk}`, `{field}` FROM `{table}` "
                    f"ORDER BY `{pk}` LIMIT %s OFFSET %s",
                    (batch_size, offset)
                )
                if not rows:
                    break

                # 2) encrypt & write back
                for row in rows:
                    key = row[pk]
                    plaintext = row[field]
                    if plaintext is None or plaintext == "":
                        continue

                    # BFV-encrypt the plaintext string
                    bfv_blob = self.encryption_manager.encrypt_string(plaintext)

                    # update the *_encrypted column
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

    # … after your migrate_string_fields() and migrate_numeric_fields() calls …



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
