#query executions for every relevant limit
import time
import os
import statistics
import sys

sys.path.append("C://Users//ioanc//PycharmProjects//ChatBot")

from load_config import load_config
from encryption_manager import HomomorphicEncryptionManager
from secure_database_connector import SecureDatabaseConnector
from query_processor import QueryProcessor

import time
import statistics

def time_query(limit, repetitions=20):
    config = load_config("C://Users//ioanc//PycharmProjects//ChatBot//config.json")

    db_config = config["database"]

    he_manager = HomomorphicEncryptionManager(
        key_size=config["encryption"]["key_size"],
        context_params=config["encryption"]["context_parameters"],
        keys_dir="C://Users//ioanc//PycharmProjects//ChatBot//encryption_keys"
    )

    db_connector = SecureDatabaseConnector(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        encryption_manager=he_manager
    )

    sensitive_fields = config["security"]["sensitive_fields"]

    qp = QueryProcessor(
        db_connector=db_connector,
        encryption_manager=he_manager,
        sensitive_fields=sensitive_fields
    )

    durations = []
    for _ in range(repetitions):
        start = time.perf_counter()
        qp.get_highest_balance_account(limit=limit)
        durations.append(time.perf_counter() - start)

    mean, stdev = statistics.mean(durations), statistics.stdev(durations)
    print(f"LIMIT={limit:3d} → {mean*1000:.1f}±{stdev*1000:.1f} ms")

if __name__ == "__main__":
    for lim in [1, 5, 10, 50, 100]:
        time_query(limit=lim)



