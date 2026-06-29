import os
import sys
sys.stdout.reconfigure(line_buffering=True)
import time
import random
import psycopg2

DSN = os.getenv("PG_DSN", "host=localhost port=5433 dbname=ecommerce user=postgres password=postgres")

def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = cur.fetchall()

    print("Siparis uretici basladi. Durdurmak icin Ctrl+C.")
    while True:
        user_id = random.choice(user_ids)
        chosen = random.sample(products, k=random.randint(1, 3))

        total = 0
        items = []
        for product_id, price in chosen:
            qty = random.randint(1, 4)
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

        print(f"Siparis {order_id} | kullanici {user_id} | tutar {round(total,2)} | {len(items)} kalem")

        # Ara sira (~%5) eski bir CANCELLED siparisi siliyoruz. Gercek hayatta
        # iptal edilen siparisler bir sure sonra OLTP'den temizlenebilir.
        # Bu, CDC delete (op='d') olaylarinin pipeline boyunca akmasini saglar;
        # downstream'de is_deleted=true olarak yakalanir (soft delete).
        if random.random() < 0.05:
            cur.execute(
                "SELECT order_id FROM orders WHERE status = 'CANCELLED' ORDER BY random() LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                silinecek = row[0]
                cur.execute("DELETE FROM order_items WHERE order_id = %s", (silinecek,))
                cur.execute("DELETE FROM orders WHERE order_id = %s", (silinecek,))
                print(f"  -> Siparis {silinecek} silindi (iptal temizligi)")

        time.sleep(random.uniform(5.0, 7.0))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDurduruldu.")