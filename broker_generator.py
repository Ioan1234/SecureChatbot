import random
import string
import argparse
import os
import logging
import time
import concurrent.futures
import threading
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("broker_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ThreadSafeCounter:
    def __init__(self, total: int):
        self.count = 0
        self.total = total
        self.lock = threading.Lock()
        self.last_logged = 0
        self.start_time = time.time()

    def increment(self, amount: int = 1):
        with self.lock:
            self.count += amount
            if self.count - self.last_logged >= max(1, self.total // 10):
                elapsed = time.time() - self.start_time
                rate = self.count / elapsed if elapsed > 0 else 0
                eta = (self.total - self.count) / rate if rate > 0 else 0
                logger.info(f"Progress: {self.count}/{self.total} ({self.count/self.total*100:.1f}%) — {rate:.1f} rows/s — ETA {eta:.1f}s")
                self.last_logged = self.count

import random
from datetime import datetime
import argparse
import os


class BrokerGenerator:
    

    def __init__(self, num_entries=100, output_dir="sql_output", start_id=1):
        self.num_entries = num_entries
        self.output_dir = output_dir
        self.start_id = start_id

        os.makedirs(self.output_dir, exist_ok=True)


        self.prefixes = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta",
                         "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho"]
        self.suffixes = ["Securities", "Investments", "Capital", "Partners", "Group", "Finance",
                         "Trade", "Markets", "Advisors", "Global", "Corp", "International"]

    def _random_name(self):
        return f"{random.choice(self.prefixes)} {random.choice(self.suffixes)}"

    def _random_license(self):
        prefix = random.choice(["LIC", "BRK", "TRD", "MKT", "FIN"])
        suffix = ''.join(random.choices('0123456789', k=6))
        return f"{prefix}{suffix}"

    def _random_email(self, name):
        local = name.lower().replace(' ', '')
        return f"{local}@broker.com"

    def generate(self):
        filename = os.path.join(self.output_dir, 'brokers_inserts.sql')
        with open(filename, 'w') as f:
            f.write(f"-- brokers data generated on {datetime.now().isoformat()}\n\n")
            f.write("INSERT INTO brokers (name, license_number, contact_email) VALUES\n")

            entries = []
            for i in range(self.start_id, self.start_id + self.num_entries):
                name = self._random_name()
                lic  = self._random_license()
                email = self._random_email(name)
                entries.append(f"('{name}', '{lic}', '{email}')")

            f.write(",\n".join(entries) + ";\n")

        print(f"Generated {self.num_entries} broker INSERTs in {filename}")
        return filename


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate brokers INSERT SQL")
    parser.add_argument("--num-entries", type=int, default=50,
                        help="Number of brokers to generate (default: 50)")
    parser.add_argument("--output-dir", type=str, default="sql_output",
                        help="Directory for output SQL file")
    parser.add_argument("--start-id", type=int, default=1,
                        help="Starting auto-increment ID placeholder (default: 1)")

    args = parser.parse_args()

    gen = BrokerGenerator(
        num_entries=args.num_entries,
        output_dir=args.output_dir,
        start_id=args.start_id
    )
    gen.generate()
