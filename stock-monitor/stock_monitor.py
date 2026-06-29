"""
Stok Izleme Servisi (Stock Monitoring Service)
================================================

Bu servis bir Kafka CONSUMER'dir. Mevcut pipeline'a hicbir sekilde dokunmaz;
Postgres, Debezium, Kafka veya PySpark'ta degisiklik gerektirmez. Sadece
zaten akmakta olan `ecom.public.inventory` topic'ine yeni bir consumer group
ile baglanir.

ONEMLI KAVRAMSAL AYRIM:
-----------------------
Stok KONTROLU ve DUSURME islemi uygulama (OLTP) tarafinin isidir:
  - Musteri "Siparis Ver" der
  - Backend stok yeterli mi diye kontrol eder (stock_qty >= istenen mi?)
  - Yeterliyse siparis olusturur + stok duser (UPDATE inventory ...)
  - Yetmezse "stok yok" hatasi doner
Bu tamamen transaction-time'da, milisaniyeler icinde gerceklesir. CDC'nin
bundan haberi yoktur.

Bu servis stok YONETMEZ; stok degisikliklerini IZLER. Karar zaten alinmistir,
stok zaten dusmustur. Bu servis "birileri stokla ne yapti, bunu kimin bilmesi
lazim" sorusunun cevabidir:

  1. UYARI / MONITORING — stok kritik esige duserse satin alma ekibine bildirim
     (tedarikciye yeni siparis acmalari icin). Uygulama bunu yapmaz; onun isi
     siparis almak, tedarik planlamasi degil.

  2. ANALITIK — burn rate analizi: bir urun gunde kac adet eriyor, ne zaman
     tukenecek? Bu gecmis OLTP'de yoktur (sadece guncel stok vardir); CDC
     event akisinda vardir.

  3. SENKRONIZASYON — marketplace entegrasyonu, depo yonetim sistemi,
     tedarikci portali gibi diger sistemlere stok degisikliklerini aktarmak.
     Hepsi ayri ayri OLTP'ye baglanmak yerine bu topic'ten okur.

Bu basit ornek (1) numarali kullanim durumunu gosterir: dusuk stok uyarisi.
"""

import os
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INVENTORY_TOPIC = os.environ.get("INVENTORY_TOPIC", "ecom.public.inventory")
# Stok bu esigin altina dustugunde uyari uretilir
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "10"))

# Ayni urun icin tekrar tekrar uyari uretmemek icin basit bir hafiza.
# (Prod'da bu Redis/DB'de tutulur; burada bellek ici yeterli.)
already_alerted = set()


def handle_inventory_event(payload):
    """Debezium envelope'undan stok degisikligini cikar ve degerlendir."""
    after = payload.get("after")
    if after is None:
        # Silme (op=d) — urun envanterden cikti, ilgilenmiyoruz
        return

    product_id = after.get("product_id")
    stock_qty = after.get("stock_qty")

    if product_id is None or stock_qty is None:
        return

    if stock_qty < LOW_STOCK_THRESHOLD:
        if product_id not in already_alerted:
            # Gercek hayatta burada Slack webhook / email / PagerDuty cagrilirdi:
            #   requests.post(SLACK_WEBHOOK, json={"text": ...})
            print(
                f"[UYARI] Stok kritik seviyede! "
                f"product_id={product_id} stock_qty={stock_qty} "
                f"(esik={LOW_STOCK_THRESHOLD}) -> satin alma ekibine bildir"
            )
            already_alerted.add(product_id)
    else:
        # Stok tekrar normale dondu (yeniden stoklandi) -> uyari hafizasini temizle
        already_alerted.discard(product_id)


def main():
    print(f"Stok izleme servisi basladi.")
    print(f"  Topic            : {INVENTORY_TOPIC}")
    print(f"  Dusuk stok esigi : {LOW_STOCK_THRESHOLD}")
    print(f"  Consumer group   : stock-monitor-service")

    consumer = KafkaConsumer(
        INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # PySpark'tan BAGIMSIZ consumer group -> ayni topic'i bagimsiz okuruz
        group_id="stock-monitor-service",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    for message in consumer:
        if message.value is None:
            continue
        payload = message.value.get("payload")
        if payload is None:
            continue
        handle_inventory_event(payload)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDurduruldu.")
