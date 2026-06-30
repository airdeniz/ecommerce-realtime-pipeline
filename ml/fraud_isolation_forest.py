"""
Scenario 1 — Fraud / anomaly detection (unsupervised).

Reads order-level features, fits an IsolationForest (no labels), and writes a
per-order anomaly score + flag to lakehouse.ml.fraud_scores. Higher score = more
anomalous. The generator injects ~2% deliberately unusual orders (very large
baskets / quantities) which this should surface.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from common import build_spark, read_table, write_iceberg, FEATURES_NS, ML_NS

FEATURES = [
    "total_amount",
    "item_count",
    "distinct_products",
    "total_quantity",
    "avg_unit_price",
    "max_unit_price",
]


def main():
    spark = build_spark("ml-fraud-isolation-forest")
    df = read_table(spark, f"{FEATURES_NS}.feat_order_features")
    if df.empty:
        print("No order features yet — nothing to score.")
        spark.stop()
        return

    X = df[FEATURES].fillna(0.0).astype(float)
    Xs = StandardScaler().fit_transform(X)

    model = IsolationForest(n_estimators=200, contamination=0.02, random_state=42)
    model.fit(Xs)
    # decision_function: higher = more normal. Negate so higher = more anomalous.
    df["anomaly_score"] = (-model.decision_function(Xs)).astype(float)
    df["is_anomaly"] = (model.predict(Xs) == -1)

    out = df[["order_id", "user_id", "status", "total_amount", "anomaly_score", "is_anomaly"]].copy()
    out["order_id"] = out["order_id"].astype("int64")
    out["user_id"] = out["user_id"].astype("int64")
    out["total_amount"] = out["total_amount"].astype(float)
    out["anomaly_score"] = out["anomaly_score"].astype(float)
    out["is_anomaly"] = out["is_anomaly"].astype(bool)

    write_iceberg(spark, out, f"{ML_NS}.fraud_scores")
    spark.stop()


if __name__ == "__main__":
    main()
