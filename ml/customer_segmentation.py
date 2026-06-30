"""
Scenario 3 — Customer segmentation (KMeans).

Clusters customers on their RFM + behavioural features and writes one row per
customer to lakehouse.ml.customer_segments with a cluster id and a descriptive
label. Clusters are labelled by their mean monetary rank (Champions = highest
spend, ... At-Risk = lowest), so the labels are descriptive of the data, not a
hand-tuned business rule.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from common import build_spark, read_table, write_iceberg, FEATURES_NS, ML_NS

FEATURES = [
    "recency_days",
    "frequency",
    "monetary",
    "avg_basket",
    "tenure_days",
    "distinct_categories",
    "cancel_rate",
]

# Labels applied by descending mean-monetary rank of each cluster.
RANK_LABELS = ["Champions", "Loyal", "Promising", "At-Risk"]


def main():
    spark = build_spark("ml-customer-segmentation")
    df = read_table(spark, f"{FEATURES_NS}.feat_customer_rfm")
    if len(df) < 4:
        print("Too few customers to segment.")
        spark.stop()
        return

    X = df[FEATURES].fillna(0.0).astype(float)
    Xs = StandardScaler().fit_transform(X)

    k = min(len(RANK_LABELS), len(df))
    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    df["cluster_id"] = km.fit_predict(Xs).astype(int)

    # Rank clusters by mean monetary (1 = highest spend) and map to a label.
    rank = df.groupby("cluster_id")["monetary"].mean().rank(ascending=False).astype(int)
    df["segment_label"] = df["cluster_id"].map(
        lambda c: RANK_LABELS[int(rank[c]) - 1] if int(rank[c]) - 1 < len(RANK_LABELS) else f"Segment-{c}"
    )

    out = df[["user_id", "cluster_id", "segment_label"] + FEATURES].copy()
    out["user_id"] = out["user_id"].astype("int64")
    out["cluster_id"] = out["cluster_id"].astype("int32")
    for c in FEATURES:
        out[c] = out[c].astype(float)

    write_iceberg(spark, out, f"{ML_NS}.customer_segments")
    spark.stop()


if __name__ == "__main__":
    main()
