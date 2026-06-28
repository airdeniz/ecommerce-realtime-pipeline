#!/bin/bash

echo "Debezium connector kaydediliyor..."

until curl -s http://connect:8083/connectors > /dev/null; do
  echo "Connect servisi hazır değil, bekleniyor..."
  sleep 5
done

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://connect:8083/connectors \
  -d @/register-postgres.json

echo "Connector kaydedildi."