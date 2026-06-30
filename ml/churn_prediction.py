"""
Scenario 4 — Churn prediction (with a documented PROXY label).

The synthetic data has no ground-truth churn label, so we define a proxy:
a customer is labelled churned (1) when their recency (days since last order,
relative to the dataset's latest order) falls in the worst tercile. A
LogisticRegression then predicts churn probability from the OTHER RFM features.

IMPORTANT — leakage avoidance: `recency_days` is the basis of the proxy label,
so it is DELIBERATELY EXCLUDED from the model inputs. The model learns from
frequency / monetary / basket / tenure / category-breadth / cancel-rate instead.
This is a demonstration of the modelling mechanics on synthetic data, not a
production-grade churn model. See ARCHITECTURE.md.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from common import build_spark, read_table, write_iceberg, FEATURES_NS, ML_NS

# Recency is intentionally NOT here (it defines the label — see module docstring).
FEATURES = [
    "frequency",
    "monetary",
    "avg_basket",
    "tenure_days",
    "distinct_categories",
    "cancel_rate",
]

CHURN_QUANTILE = 0.66  # worst tercile of recency = proxy "churned"


def main():
    spark = build_spark("ml-churn-prediction")
    df = read_table(spark, f"{FEATURES_NS}.feat_customer_rfm")
    if len(df) < 10:
        print("Too few customers to model churn.")
        spark.stop()
        return

    # Defensive: ensure recency is float (guards against a DECIMAL feature column
    # leaking in as Python Decimal, which would break pandas quantile arithmetic).
    df["recency_days"] = df["recency_days"].astype(float)
    threshold = df["recency_days"].quantile(CHURN_QUANTILE)
    df["churn_label_proxy"] = (df["recency_days"] > threshold).astype(int)
    if df["churn_label_proxy"].nunique() < 2:
        print("Proxy label collapsed to a single class — skipping.")
        spark.stop()
        return

    X = df[FEATURES].fillna(0.0).astype(float)
    Xs = StandardScaler().fit_transform(X)
    clf = LogisticRegression(max_iter=1000)
    clf.fit(Xs, df["churn_label_proxy"])
    df["churn_probability"] = clf.predict_proba(Xs)[:, 1].astype(float)

    out = df[["user_id", "churn_probability", "churn_label_proxy"] + FEATURES].copy()
    out["user_id"] = out["user_id"].astype("int64")
    out["churn_probability"] = out["churn_probability"].astype(float)
    out["churn_label_proxy"] = out["churn_label_proxy"].astype("int32")
    for c in FEATURES:
        out[c] = out[c].astype(float)

    write_iceberg(spark, out, f"{ML_NS}.churn_predictions")
    spark.stop()


if __name__ == "__main__":
    main()
