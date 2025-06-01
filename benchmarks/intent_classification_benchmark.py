#precision/recall on a held-out intent dataset and per-prediction latency
import time
import os, sys
import statistics
from sklearn.metrics import classification_report


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from model.intent_classifier import EnhancedIntentClassifier
def load_testset(path=None):
    if path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(current_dir, "intent_test.csv")

    with open(path, encoding='utf-8') as f:
        import csv
        reader = csv.DictReader(f)
        return [(row["text"], row["intent_label"]) for row in reader]

def bench_classifier():
    clf = EnhancedIntentClassifier()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(base_dir, "../models/intent_classifier")
    clf.load_model(model_path)
    test = load_testset()
    y_true, y_pred, times = [], [], []
    for text, label in test:
        start = time.perf_counter()
        result = clf.classify_intent(text)
        times.append(time.perf_counter() - start)
        y_true.append(label)
        y_pred.append(result['intent'])
    print(classification_report(y_true, y_pred, digits=4))
    mean_ms = statistics.mean(times) * 1000
    std_ms  = statistics.stdev(times) * 1000
    print(f"Per-classify: {mean_ms:.2f}±{std_ms:.2f} ms")



if __name__ == "__main__":
    bench_classifier()
