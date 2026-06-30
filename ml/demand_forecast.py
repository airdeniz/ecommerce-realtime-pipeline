"""
Scenario 2 — Sales / demand forecasting (Prophet).

Forecasts revenue on two grains:
  - hourly, from lakehouse.ml_features.feat_revenue_hourly (usable within a
    single day of runtime), forecasting the next 24 hours
  - daily, reusing the existing gold.mart_daily_revenue, forecasting 14 days

Both are written to lakehouse.ml.demand_forecast with a `grain` discriminator and
an `is_forecast` flag (false = fitted history, true = future). Prophet emits
yhat plus an 80% confidence band (yhat_lower / yhat_upper).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from prophet import Prophet

from common import build_spark, read_table, write_iceberg, FEATURES_NS, ML_NS


def forecast(df, ds_col, y_col, grain, periods, freq):
    d = df[[ds_col, y_col]].rename(columns={ds_col: "ds", y_col: "y"}).dropna()
    d["ds"] = pd.to_datetime(d["ds"])
    d["y"] = d["y"].astype(float)
    # Prophet needs a few points to fit anything meaningful.
    if len(d) < 3:
        print(f"[{grain}] only {len(d)} points — skipping this grain.")
        return pd.DataFrame()

    model = Prophet(interval_width=0.8)
    model.fit(d)
    future = model.make_future_dataframe(periods=periods, freq=freq)
    fc = model.predict(future)

    res = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    res["grain"] = grain
    res["is_forecast"] = res["ds"] > d["ds"].max()
    return res


def main():
    spark = build_spark("ml-demand-forecast")
    hourly = read_table(spark, f"{FEATURES_NS}.feat_revenue_hourly")
    daily = read_table(spark, "lakehouse.gold.mart_daily_revenue")

    frames = []
    if not hourly.empty:
        frames.append(forecast(hourly, "revenue_hour", "total_revenue", "hourly", 24, "h"))
    if not daily.empty:
        frames.append(forecast(daily, "order_date", "total_revenue", "daily", 14, "D"))
    frames = [f for f in frames if not f.empty]

    if not frames:
        print("Not enough history to forecast yet.")
        spark.stop()
        return

    out = pd.concat(frames, ignore_index=True)
    out["ds"] = pd.to_datetime(out["ds"])
    for c in ["yhat", "yhat_lower", "yhat_upper"]:
        out[c] = out[c].astype(float)
    out["is_forecast"] = out["is_forecast"].astype(bool)

    write_iceberg(
        spark,
        out[["grain", "ds", "yhat", "yhat_lower", "yhat_upper", "is_forecast"]],
        f"{ML_NS}.demand_forecast",
    )
    spark.stop()


if __name__ == "__main__":
    main()
