#decrypt benchmarks
import timeit
import statistics
import random
import pickle
import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from encryption_manager import HomomorphicEncryptionManager

he = HomomorphicEncryptionManager()
he.load_encryption_data()


def bench_decrypt_numeric(n=100):
    sample_ct = he.encrypt_numeric(random.random() * 1e6)
    timer = timeit.Timer(
        stmt="he.decrypt_numeric(ct)",
        globals={"he": he, "ct": sample_ct}
    )
    times = timer.repeat(repeat=5, number=n)
    mean_ms = statistics.mean(times) * 1000 / n
    std_ms = statistics.stdev(times) * 1000 / n
    print(f"Numeric Decrypt: {mean_ms:.3f}±{std_ms:.3f} ms per call")
    size = len(pickle.dumps(sample_ct))
    print(f"Numeric Ciphertext size: {size} bytes")


def bench_decrypt_string(n=100):
    sample_ct = he.encrypt_string('Hello, world!')
    timer = timeit.Timer(
        stmt="he.decrypt_string(ct)",
        globals={"he": he, "ct": sample_ct}
    )
    times = timer.repeat(repeat=5, number=n)
    mean_ms = statistics.mean(times) * 1000 / n
    std_ms = statistics.stdev(times) * 1000 / n
    print(f"Text Decrypt: {mean_ms:.3f}±{std_ms:.3f} ms per call")
    size = len(pickle.dumps(sample_ct))
    print(f"Text Ciphertext size: {size} bytes")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type=int, default=100, help="Calls per batch")
    args = parser.parse_args()

    print("=== Running Micro Crypto Benchmarks ===")
    bench_decrypt_numeric(args.n)
    bench_decrypt_string(args.n)
