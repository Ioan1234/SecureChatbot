import random
from datetime import datetime, timedelta
import string
import argparse
import os
import logging
import time
import gc
import concurrent.futures
import threading

import mysql.connector




import mysql.connector
import logging
from load_config import load_config


config = load_config()
db_cfg = config["database"]


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_asset_ids():
    
    conn = mysql.connector.connect(
        host=db_cfg["host"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"]
    )
    cursor = conn.cursor()
    cursor.execute("SELECT asset_id FROM assets")
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    logger.info(f"Fetched {len(ids)} asset IDs")
    return ids




class ThreadSafeCounter:

    def __init__(self, total):
        self.count = 0
        self.total = total
        self.lock = threading.Lock()
        self.last_logged = 0
        self.start_time = time.time()

    def increment(self, amount=1):
        with self.lock:
            self.count += amount
            if self.count - self.last_logged >= self.total // 10:
                elapsed = time.time() - self.start_time
                rate = self.count / elapsed if elapsed > 0 else 0
                eta = (self.total - self.count) / rate if rate > 0 else 0
                logger.info(
                    f"Progress: {self.count}/{self.total} entries ({self.count / self.total * 100:.1f}%) - {rate:.1f} entries/sec - ETA: {eta:.1f} sec")
                self.last_logged = self.count

    def get_count(self):
        with self.lock:
            return self.count


class MultiThreadedDatabaseGenerator:
    def __init__(self, num_entries=100, output_dir="sql_output", start_ids=None, batch_size=100, num_threads=4):
        self.num_entries = num_entries
        self.output_dir = output_dir
        self.batch_size = min(batch_size, num_entries)
        self.num_threads = min(num_threads, 8)

        os.makedirs(output_dir, exist_ok=True)

        self.start_ids = start_ids or {
            "traders": 1,
            "markets": 1,
            "trades": 1,
            "accounts": 1,
            "transactions": 1,
            "orders": 1,
            "order_status": 1,
            "price_history": 1
        }

        self.used_emails = set()
        self.used_license_numbers = set()
        self.used_market_names = set()

        self.email_lock = threading.Lock()
        self.license_lock = threading.Lock()
        self.market_name_lock = threading.Lock()

        self.asset_ids = load_asset_ids()
        if not self.asset_ids:
            raise RuntimeError("No asset_ids found in database; generate assets first.")

        self._cache_common_values()

    def _get_realistic_market_hours(self, country):
        market_hours = {
            "USA": {
                "Eastern": ("09:30:00", "16:00:00"),
                "Central": ("08:30:00", "15:00:00"),
                "Pacific": ("06:30:00", "13:00:00")
            },
            "UK": {
                "London": ("08:00:00", "16:30:00")
            },
            "Japan": {
                "Tokyo": ("09:00:00", "15:00:00")
            },
            "China": {
                "Shanghai": ("09:30:00", "15:00:00"),
                "Shenzhen": ("09:30:00", "15:00:00")
            },
            "Germany": {
                "Frankfurt": ("09:00:00", "17:30:00")
            },
            "France": {
                "Paris": ("09:00:00", "17:30:00")
            },
            "Canada": {
                "Toronto": ("09:30:00", "16:00:00")
            },
            "Australia": {
                "Sydney": ("10:00:00", "16:00:00")
            },
            "Singapore": {
                "Singapore": ("09:00:00", "17:00:00")
            },
            "India": {
                "Mumbai": ("09:15:00", "15:30:00")
            },
            "Brazil": {
                "Sao Paulo": ("10:00:00", "17:30:00")
            },
            "South Korea": {
                "Seoul": ("09:00:00", "15:30:00")
            }
        }

        if random.random() < 0.1:
            hour_offset = random.choice([-1, -0.5, 0.5, 1])
            minute_offset = random.choice([0, 15, 30, 45])

            if country in market_hours:
                region = random.choice(list(market_hours[country].keys()))
                opening, closing = market_hours[country][region]

                open_h, open_m = map(int, opening.split(':')[:2])
                close_h, close_m = map(int, closing.split(':')[:2])

                open_h += int(hour_offset)
                open_m += minute_offset
                if open_m >= 60:
                    open_h += 1
                    open_m -= 60

                close_h += int(hour_offset)
                close_m += minute_offset
                if close_m >= 60:
                    close_h += 1
                    close_m -= 60

                opening = f"{max(0, min(23, open_h)):02d}:{open_m:02d}:00"
                closing = f"{max(0, min(23, close_h)):02d}:{close_m:02d}:00"

                return opening, closing

        if country not in market_hours:
            return "09:00:00", "16:00:00"

        region = random.choice(list(market_hours[country].keys()))
        return market_hours[country][region]

    def parse_time_to_24hr(self, time_str):
        try:
            if " - " in time_str:
                time_str = time_str.split(" - ")[0]

            time_str = time_str.strip()

            is_pm = "PM" in time_str.upper()
            is_am = "AM" in time_str.upper()

            time_str = time_str.upper().replace("AM", "").replace("PM", "").strip()

            if ":" in time_str:
                hours, minutes = map(int, time_str.split(":"))
            else:
                hours = int(time_str)
                minutes = 0

            if is_pm and hours < 12:
                hours += 12
            elif is_am and hours == 12:
                hours = 0

            return f"{hours:02d}:{minutes:02d}:00"
        except:
            return "09:00:00"

    def _cache_common_values(self):
        logger.info("Caching common values for better performance")

        self.first_names = ["James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph", "Thomas",
                            "Charles",
                            "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
                            "Sarah", "Karen",
                            "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Andrew", "Paul", "Joshua",
                            "Kenneth",
                            "Nancy", "Lisa", "Betty", "Margaret", "Sandra", "Ashley", "Kimberly", "Emily", "Donna",
                            "Michelle",
                            "Emma", "Olivia", "Noah", "Liam", "William", "Sophia", "Isabella", "Mia", "Charlotte",
                            "Amelia"]

        self.last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore",
                           "Taylor",
                           "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia",
                           "Martinez", "Robinson",
                           "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez",
                           "King",
                           "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson",
                           "Carter"]

        self.email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com",
                              "company.com", "business.net", "trade.org", "invest.io", "market.co"]

        self.broker_prefixes = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta",
                                "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho"]

        self.broker_suffixes = ["Securities", "Investments", "Capital", "Partners", "Group", "Finance",
                                "Trade", "Markets", "Advisors", "Global", "Corp", "International"]

        self.asset_types = ["Stock", "Bond", "ETF", "Cryptocurrency", "Commodity", "Futures", "Options"]

        self.stock_prefixes = ["Global", "United", "American", "Euro", "Pacific", "Atlantic", "Alpine",
                               "Asia", "Tech", "Bio", "Eco", "Smart", "Digital", "Quantum", "Cyber"]

        self.stock_suffixes = ["Corp", "Inc", "Ltd", "Group", "Systems", "Networks", "Solutions", "Tech",
                               "Energy", "Healthcare", "Pharma", "Industries", "Communications", "Media"]

        self.account_types = ["Individual", "Corporate", "Joint", "IRA", "Roth IRA", "401k", "Trust"]

        self.transaction_types = ["Deposit", "Withdrawal", "Transfer", "Fee", "Interest", "Dividend", "Rebate"]

        self.order_types = ["Buy", "Sell", "Buy to Cover", "Sell Short", "Limit Buy", "Limit Sell"]

        self.order_statuses = ["Pending", "Completed", "Cancelled", "Rejected", "Expired", "Partially Filled"]

        self.registration_start = datetime(2023, 1, 1)
        self.registration_end = datetime(2024, 12, 31)
        self.date_range_days = (self.registration_end - self.registration_start).days

    def random_date(self, start_date, end_date):
        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        return start_date + timedelta(days=random_days)

    def random_name(self):
        return f"{random.choice(self.first_names)} {random.choice(self.last_names)}"

    def random_email(self, name):
        name_part = name.lower().replace(" ", ".")
        random_suffix = ''.join(random.choices(string.digits, k=2))

        email = f"{name_part}{random_suffix}@{random.choice(self.email_domains)}"

        with self.email_lock:
            attempts = 0
            while email in self.used_emails and attempts < 5:
                random_suffix = ''.join(random.choices(string.digits, k=2))
                email = f"{name_part}{random_suffix}@{random.choice(self.email_domains)}"
                attempts += 1

            if email in self.used_emails:
                email = f"{name_part}.{int(time.time() * 1000)}@{random.choice(self.email_domains)}"

            self.used_emails.add(email)

        return email

    def random_phone(self):
        return ''.join(random.choices(string.digits, k=10))

    def random_license(self):
        prefix = random.choice(["LIC", "BRK", "TRD", "MKT", "FIN"])
        suffix = ''.join(random.choices(string.digits, k=5))

        license_num = f"{prefix}{suffix}"

        with self.license_lock:
            attempts = 0
            while license_num in self.used_license_numbers and attempts < 5:
                suffix = ''.join(random.choices(string.digits, k=5))
                license_num = f"{prefix}{suffix}"
                attempts += 1

            if license_num in self.used_license_numbers:
                license_num = f"{prefix}{int(time.time() * 1000) % 100000}"

            self.used_license_numbers.add(license_num)

        return license_num

    def random_decimal(self, min_val, max_val, precision=2):
        val = random.uniform(min_val, max_val)
        return round(val, precision)

    def _write_batch(self, file_handle, batch_header, values):
        if values:
            file_handle.write(batch_header)
            file_handle.write(",\n".join(values))
            file_handle.write(";\n\n")
            file_handle.flush()

    def _generate_chunk(self, table_name, start_idx, chunk_size, counter=None):
        workers = {
            "traders": self._generate_traders_chunk,
            "markets": self._generate_markets_chunk_realistic,
            "trades": self._generate_trades_chunk,
            "accounts": self._generate_accounts_chunk,
            "transactions": self._generate_transactions_chunk,
            "orders": self._generate_orders_chunk,
            "order_status": self._generate_order_status_chunk,
            "price_history": self._generate_price_history_chunk
        }

        if table_name in workers:
            return workers[table_name](start_idx, chunk_size, counter)
        else:
            raise ValueError(f"Unknown table name: {table_name}")

    def _generate_traders_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            name = self.random_name()
            email = self.random_email(name)
            phone = self.random_phone()
            registration_date = self.random_date(self.registration_start, self.registration_end).strftime('%Y-%m-%d')

            values.append(f"('{name}', '{email}', '{phone}', '{registration_date}')")

            if counter:
                counter.increment()

        return values


    def _generate_markets_chunk_realistic(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        countries = ["USA", "UK", "Japan", "China", "Germany", "France", "Canada",
                     "Australia", "Singapore", "India", "Brazil", "South Korea"]

        cities = {
            "USA": ["New York", "Chicago", "San Francisco", "Boston", "Los Angeles"],
            "UK": ["London", "Manchester", "Edinburgh"],
            "Japan": ["Tokyo", "Osaka", "Nagoya"],
            "China": ["Shanghai", "Beijing", "Shenzhen"],
            "Germany": ["Frankfurt", "Berlin", "Munich"],
            "France": ["Paris", "Lyon", "Marseille"],
            "Canada": ["Toronto", "Montreal", "Vancouver"],
            "Australia": ["Sydney", "Melbourne", "Perth"],
            "Singapore": ["Singapore"],
            "India": ["Mumbai", "New Delhi", "Bangalore"],
            "Brazil": ["Sao Paulo", "Rio de Janeiro"],
            "South Korea": ["Seoul", "Busan"]
        }

        market_types = ["Stock Exchange", "Options Exchange", "Futures Exchange", "Commodity Exchange"]
        city_mappings = {
            "New York": "USA:Eastern",
            "Chicago": "USA:Central",
            "San Francisco": "USA:Pacific",
            "Los Angeles": "USA:Pacific",
            "London": "UK:London",
            "Tokyo": "Japan:Tokyo",
            "Shanghai": "China:Shanghai",
            "Shenzhen": "China:Shenzhen",
            "Frankfurt": "Germany:Frankfurt",
            "Paris": "France:Paris",
            "Toronto": "Canada:Toronto",
            "Sydney": "Australia:Sydney",
            "Singapore": "Singapore:Singapore",
            "Mumbai": "India:Mumbai",
            "Sao Paulo": "Brazil:Sao Paulo",
            "Seoul": "South Korea:Seoul"
        }

        for i in range(start_idx, end_idx):
            country = random.choice(countries)
            city = random.choice(cities.get(country, ["Capital"]))
            market_type = random.choice(market_types)

            with self.market_name_lock:
                if random.random() < 0.7:
                    name = f"{city} {market_type}"
                else:
                    name = f"{country} {market_type}"

                attempts = 0
                while name in self.used_market_names and attempts < 3:
                    if random.random() < 0.7:
                        name = f"{city} {market_type} {attempts + 1}"
                    else:
                        name = f"{country} {market_type} {attempts + 1}"
                    attempts += 1

                self.used_market_names.add(name)

            if city in city_mappings:
                country_region = city_mappings[city].split(':')
                country_for_hours = country_region[0]
                opening_time, closing_time = self._get_realistic_market_hours(country_for_hours)
            else:
                opening_time, closing_time = self._get_realistic_market_hours(country)

            location = f"{city}, {country}"

            values.append(f"('{name}', '{location}', '{opening_time}', '{closing_time}')")

            if counter:
                counter.increment()

        return values

    def _generate_trades_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            trader_id = random.randint(1, self.start_ids["traders"] + min(i, self.num_entries - 1))
            market_id = random.randint(1, self.start_ids["markets"] + min(i, self.num_entries - 1))

            trade_date = self.random_date(datetime(2023, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')
            quantity = random.randint(1, 1000)
            price = self.random_decimal(10, 1000)

            asset_id = random.choice(self.asset_ids)
            values.append(f"({trader_id}, {asset_id}, {market_id}, '{trade_date}', {quantity}, {price})")

            if counter:
                counter.increment()

        return values

    def _generate_accounts_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            trader_id = random.randint(1, self.start_ids["traders"] + min(i, self.num_entries - 1))
            balance = self.random_decimal(1000, 100000)
            account_type = random.choice(self.account_types)
            creation_date = self.random_date(datetime(2020, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')

            values.append(f"({trader_id}, {balance}, '{account_type}', '{creation_date}')")

            if counter:
                counter.increment()

        return values

    def _generate_transactions_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            account_id = random.randint(1, self.start_ids["accounts"] + min(i, self.num_entries - 1))
            transaction_date = self.random_date(datetime(2020, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')
            transaction_type = random.choice(self.transaction_types)

            if transaction_type in ["Deposit", "Interest", "Dividend", "Rebate"]:
                amount = self.random_decimal(100, 5000)
            else:
                amount = self.random_decimal(10, 2000)

            values.append(f"({account_id}, '{transaction_date}', '{transaction_type}', {amount})")

            if counter:
                counter.increment()

        return values

    def _generate_orders_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            trade_id = random.randint(1, self.start_ids["trades"] + min(i, self.num_entries - 1))
            order_type = random.choice(self.order_types)
            order_date = self.random_date(datetime(2020, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')

            values.append(f"({trade_id}, '{order_type}', '{order_date}')")

            if counter:
                counter.increment()

        return values

    def _generate_order_status_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            order_id = random.randint(1, self.start_ids["orders"] + min(i, self.num_entries - 1))
            status = random.choice(self.order_statuses)
            status_date = self.random_date(datetime(2020, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')

            values.append(f"({order_id}, '{status}', '{status_date}')")

            if counter:
                counter.increment()

        return values

    def _generate_price_history_chunk(self, start_idx, chunk_size, counter=None):
        values = []
        end_idx = start_idx + chunk_size

        for i in range(start_idx, end_idx):
            price_date = self.random_date(datetime(2020, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d')

            base_price = self.random_decimal(10, 1000)
            volatility = random.uniform(0.01, 0.1)

            if random.random() < 0.5:
                open_price = base_price
                close_price = base_price * (1 + volatility)
            else:
                open_price = base_price
                close_price = base_price * (1 - volatility)

            open_price = round(open_price, 2)
            close_price = round(close_price, 2)

            asset_id = random.choice(self.asset_ids)
            values.append(f"({asset_id}, '{price_date}', {open_price}, {close_price})")

            if counter:
                counter.increment()

        return values

    def generate_table_data_multithreaded(self, table_name):
        start_time = time.time()
        logger.info(f"Generating {table_name} data: {self.num_entries} entries using {self.num_threads} threads")

        output_file = os.path.join(self.output_dir, f"{table_name}_inserts.sql")

        headers = {
            "traders": "INSERT INTO traders (name, email, phone, registration_date) VALUES\n",
            "markets": "INSERT INTO markets (name, location, opening_time, closing_time) VALUES\n",
            "trades": "INSERT INTO trades (trader_id, asset_id, market_id, trade_date, quantity, price) VALUES\n",
            "accounts": "INSERT INTO accounts (trader_id, balance, account_type, creation_date) VALUES\n",
            "transactions": "INSERT INTO transactions (account_id, transaction_date, transaction_type, amount) VALUES\n",
            "orders": "INSERT INTO orders (trade_id, order_type, order_date) VALUES\n",
            "order_status": "INSERT INTO order_status (order_id, status, status_date) VALUES\n",
            "price_history": "INSERT INTO price_history (asset_id, price_date, open_price, close_price) VALUES\n"
        }

        with open(output_file, 'w') as f:
            f.write(f"-- {table_name.capitalize()} data generated on {datetime.now()}\n\n")

        counter = ThreadSafeCounter(self.num_entries)

        chunk_size = min(1000, self.num_entries // (self.num_threads * 2) or 1)
        chunks = []

        for start_idx in range(0, self.num_entries, chunk_size):
            end_idx = min(start_idx + chunk_size, self.num_entries)
            actual_chunk_size = end_idx - start_idx
            chunks.append((start_idx, actual_chunk_size))

        logger.info(f"Split work into {len(chunks)} chunks of ~{chunk_size} entries each")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_chunk = {
                executor.submit(self._generate_chunk, table_name, start_idx, size, counter): (start_idx, size)
                for start_idx, size in chunks
            }

            with open(output_file, 'a') as f:
                for i, future in enumerate(concurrent.futures.as_completed(future_to_chunk)):
                    try:
                        values = future.result()
                        start_idx, size = future_to_chunk[future]

                        if values:
                            self._write_batch(f, headers[table_name], values)

                            if i % max(1, len(chunks) // 10) == 0:
                                logger.info(f"  Processed chunk {i + 1}/{len(chunks)} for {table_name}")

                    except Exception as e:
                        logger.error(f"Error processing chunk: {str(e)}")

        elapsed = time.time() - start_time
        logger.info(f"Completed {table_name} data in {elapsed:.2f} seconds")
        return output_file

    def generate_all_data(self, tables_to_generate=None):
        start_time = time.time()

        os.makedirs(self.output_dir, exist_ok=True)

        if tables_to_generate is None:
            tables_to_generate = ["traders", "markets", "trades",
                                  "accounts", "transactions", "orders", "order_status", "price_history"]

        valid_tables = [t for t in tables_to_generate if t in [
            "traders", "markets", "trades",
            "accounts", "transactions", "orders", "order_status", "price_history"
        ]]

        if not valid_tables:
            logger.warning("No valid tables specified for generation")
            return []

        logger.info(
            f"Beginning multi-threaded data generation for {len(valid_tables)} tables: {', '.join(valid_tables)}")
        logger.info(f"Using {self.num_threads} threads to generate {self.num_entries} entries per table")

        generated_files = []

        for table in valid_tables:
            try:
                output_file = self.generate_table_data_multithreaded(table)
                generated_files.append(output_file)

                gc.collect()

            except Exception as e:
                logger.error(f"Error generating {table} data: {str(e)}")

        if generated_files:
            merge_file = os.path.join(self.output_dir, "merge_command.sh")
            total_file = os.path.join(self.output_dir, f"all_inserts_{self.num_entries}.sql")

            with open(merge_file, 'w') as f:
                f.write("#!/bin/bash\n\n")
                f.write(f"# Merge all SQL files into {os.path.basename(total_file)}\n")
                f.write(f"echo '-- Combined inserts generated on {datetime.now()}' > {os.path.basename(total_file)}\n")
                f.write(f"echo '' >> {os.path.basename(total_file)}\n")

                for file_path in generated_files:
                    f.write(f"cat {os.path.basename(file_path)} >> {os.path.basename(total_file)}\n")

                f.write(f"\necho 'All data merged to {os.path.basename(total_file)}'\n")

            os.chmod(merge_file, 0o755)

            elapsed = time.time() - start_time
            logger.info(f"Multi-threaded data generation completed in {elapsed:.2f} seconds")
            logger.info(f"Generated {len(generated_files)} SQL files in {self.output_dir}")
            logger.info(f"To merge all files, run: sh {os.path.basename(merge_file)}")

        return generated_files


def main():
    parser = argparse.ArgumentParser(description="Multi-Threaded Database Generator for bulk SQL inserts")

    parser.add_argument("--num-entries", type=int, default=100,
                        help="Number of entries to generate for each table (default: 100)")
    parser.add_argument("--output-dir", type=str, default="sql_output",
                        help="Output directory for SQL files (default: sql_output)")
    parser.add_argument("--start-id", type=int, default=11,
                        help="Starting ID for all tables (default: 11)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Batch size for SQL inserts (default: 100)")
    parser.add_argument("--threads", type=int, default=4,
                        help="Number of threads to use (default: 4)")

    parser.add_argument("--tables", type=str,
                        help="Comma-separated list of tables to generate (e.g., 'traders,brokers,assets')")

    args = parser.parse_args()

    start_ids = {
        "traders": args.start_id,
        "markets": args.start_id,
        "trades": args.start_id,
        "accounts": args.start_id,
        "transactions": args.start_id,
        "orders": args.start_id,
        "order_status": args.start_id,
        "price_history": args.start_id
    }

    tables_to_generate = None
    if args.tables:
        tables_to_generate = [t.strip() for t in args.tables.split(',')]
        logger.info(f"Will generate data for specific tables: {', '.join(tables_to_generate)}")

    generator = MultiThreadedDatabaseGenerator(
        num_entries=args.num_entries,
        output_dir=args.output_dir,
        start_ids=start_ids,
        batch_size=args.batch_size,
        num_threads=args.threads
    )

    generator.generate_all_data(tables_to_generate)


if __name__ == "__main__":
    main()