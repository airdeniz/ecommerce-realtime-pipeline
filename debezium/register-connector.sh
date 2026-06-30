#!/bin/bash
set -euo pipefail

echo "Debezium connector kaydediliyor..."

# Connect REST API'sinin GERCEKTEN hazir olmasini bekle. Sadece baglanti
# kurulmasi yetmez (port acik ama servis hazir olmayabilir); /connectors
# ucundan 200 donmesini bekliyoruz. Aksi halde POST 404 yer ve connector
# sessizce kaydedilmemis olur (Apple Silicon gibi yavas ortamlarda yasandi).
until [ "$(curl -s -o /dev/null -w '%{http_code}' http://connect:8083/connectors)" = "200" ]; do
  echo "Connect servisi hazir degil, bekleniyor..."
  sleep 5
done

# Connector'i kaydet ve HTTP kodunu kontrol et (201 created / 200 ok).
# 409 = zaten var (idempotent, sorun degil). Diger kodlar HATA -> cik.
code=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
  -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://connect:8083/connectors \
  -d @/register-postgres.json)

if [ "$code" = "201" ] || [ "$code" = "200" ]; then
  echo "Connector kaydedildi (HTTP $code)."
elif [ "$code" = "409" ]; then
  echo "Connector zaten kayitli (HTTP 409) - sorun yok."
else
  echo "HATA: connector kaydedilemedi (HTTP $code). Yanit:"
  cat /tmp/resp.json
  exit 1
fi
