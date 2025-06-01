#end-to-end testing for the chatbot API
import time
import requests
import pandas as pd
from statistics import mean, median

BASE = "http://localhost:5000/api/chat"

QUERIES = {
    "top_balances":        "Show me traders with the largest account balances",
    "latest_trades":       "What are the latest trades?",
    "latest_transactions": "List the most recent transactions in the market",
    "crypto_assets":       "List cryptocurrency assets",
    "most_active":         "Who is the most active trader?",
    "market_activity":     "Show me trading activity across markets",
    "top_broker":          "Who is the broker with the highest number of assets?",
    "asset_prices":        "Show current asset prices",
    "account_types":       "Show me all account types",
    "busiest_market":      "Which market has the most activity?"
}

results = []
for name, query in QUERIES.items():
    t0 = time.perf_counter()
    r = requests.post(BASE, json={"message": query})
    t1 = time.perf_counter()
    latency_ms = (t1 - t0) * 1000

    ok = r.status_code == 200
    try:
        payload = r.json()
    except ValueError:
        payload = {}
        ok = False

    has_resp = isinstance(payload.get("response"), str)
    data = payload.get("data")
    has_table = isinstance(data, list) and all(isinstance(row, dict) for row in data)

    sanity = True
    if name == "top_balances":
        sanity = "balance" in payload.get("response", "").lower()

    results.append({
        "query_name": name,
        "latency_ms": latency_ms,
        "http_ok": ok,
        "has_response": has_resp,
        "has_table": has_table,
        "sanity_check": sanity,
        "rows_returned": len(data) if isinstance(data, list) else 0
    })

    print(f"\n--- {name} ({latency_ms:.1f} ms) ---")
    print(payload.get("response", "").split("\n")[0], "\n")
    if has_table and data:
        df = pd.DataFrame(data)
        print(df.head(5).to_string(index=False))
    else:
        print("(no table)")

df = pd.DataFrame(results)
print("\n=== Summary ===")
print(f"Total queries: {len(df)}")
print(f"HTTP OK rate:     {df['http_ok'].mean():.0%}")
print(f"Schema pass rate: {((df['has_response'] & df['has_table'])).mean():.0%}")
print(f"Sanity pass rate: {df['sanity_check'].mean():.0%}")
print(f"Latency p50/p95:  {median(df.latency_ms):.1f} ms / {df.latency_ms.quantile(.95):.1f} ms")
print(f"Mean rows:        {df.rows_returned.mean():.1f}")
