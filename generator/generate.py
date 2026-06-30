import os
import sys
sys.stdout.reconfigure(line_buffering=True)
import time
import random
import psycopg2

DSN = os.getenv("PG_DSN", "host=localhost port=5433 dbname=ecommerce user=postgres password=postgres")

# Behavioural-simulation parameters. A uniform-random generator produces data
# with no structure, which makes the downstream ML models meaningless. These
# knobs inject the three kinds of signal the ML layer looks for:
#   - skewed purchase frequency  -> a few heavy buyers, many light ones
#     (gives customer segmentation real clusters)
#   - some customers go "quiet"  -> their recency grows over time
#     (gives the churn model something to separate)
#   - occasional anomalous orders -> unusually large baskets / quantities
#     (gives the anomaly model outliers to flag)
ANOMALY_RATE = 0.02        # fraction of orders that are deliberately anomalous
CHURN_FRACTION = 0.25      # fraction of users who stop ordering after their window
ACTIVE_WINDOW_SEC = 1800   # churned users only order in the first 30 min of runtime


def build_profiles(user_ids):
    """Assign each user a stable behavioural profile for this run."""
    profiles = {}
    for uid in user_ids:
        profiles[uid] = {
            # Pareto-distributed weight: most users are light buyers, a few are
            # very heavy. Used to bias the random user choice below.
            "weight": random.paretovariate(1.5),
            # A quarter of users churn: they only buy during the active window.
            "churned": random.random() < CHURN_FRACTION,
            # Typical basket size for this user (drives per-user variation).
            "basket_bias": random.randint(1, 4),
        }
    return profiles


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()

    profiles = build_profiles(user_ids)
    start = time.time()

    print(f"Order generator started ({len(user_ids)} users, {len(products)} products). "
          "Press Ctrl+C to stop.")
    while True:
        elapsed = time.time() - start

        # Eligible users: churned users drop out once the active window passes,
        # so their last-order timestamp ages and they look "churned" downstream.
        eligible = [
            u for u in user_ids
            if not (profiles[u]["churned"] and elapsed > ACTIVE_WINDOW_SEC)
        ]
        weights = [profiles[u]["weight"] for u in eligible]
        user_id = random.choices(eligible, weights=weights, k=1)[0]
        prof = profiles[user_id]

        # ~2% of orders are anomalous: many distinct products and/or very high
        # quantities, well outside the normal distribution -> the anomaly model
        # should flag these.
        is_anomaly = random.random() < ANOMALY_RATE
        if is_anomaly:
            k = min(len(products), random.randint(5, 10))
            qty_max = 50
        else:
            # Basket size centred on the user's bias, clamped to a sane range.
            k = int(round(random.gauss(prof["basket_bias"], 1)))
            k = max(1, min(len(products), k))
            qty_max = 4

        chosen = random.sample(products, k=k)

        total = 0
        items = []
        for product_id, price in chosen:
            qty = random.randint(1, qty_max)
            total += float(price) * qty
            items.append((product_id, qty, float(price)))

        cur.execute(
            "INSERT INTO orders (user_id, status, total_amount) VALUES (%s, 'CREATED', %s) RETURNING order_id",
            (user_id, round(total, 2)),
        )
        order_id = cur.fetchone()[0]

        for product_id, qty, price in items:
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                (order_id, product_id, qty, price),
            )
            cur.execute(
                "UPDATE inventory SET stock_qty = stock_qty - %s WHERE product_id = %s",
                (qty, product_id),
            )

        roll = random.random()
        if roll < 0.70:
            cur.execute("UPDATE orders SET status = 'PAID' WHERE order_id = %s", (order_id,))
        elif roll < 0.85:
            cur.execute("UPDATE orders SET status = 'CANCELLED' WHERE order_id = %s", (order_id,))

        flag = " [ANOMALY]" if is_anomaly else ""
        print(f"Order {order_id} | user {user_id} | amount {round(total,2)} | {len(items)} items{flag}")

        # Occasionally (~5%) we delete an old CANCELLED order. In real life
        # cancelled orders may be cleaned out of the OLTP after a while.
        # This lets CDC delete (op='d') events flow through the pipeline;
        # downstream they are captured as is_deleted=true (soft delete).
        if random.random() < 0.05:
            cur.execute(
                "SELECT order_id FROM orders WHERE status = 'CANCELLED' ORDER BY random() LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                to_delete = row[0]
                cur.execute("DELETE FROM order_items WHERE order_id = %s", (to_delete,))
                cur.execute("DELETE FROM orders WHERE order_id = %s", (to_delete,))
                print(f"  -> Order {to_delete} deleted (cancellation cleanup)")

        time.sleep(random.uniform(2.0, 5.0))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
