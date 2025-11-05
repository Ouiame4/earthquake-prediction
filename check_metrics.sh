#!/bin/bash

echo "=========================================="
echo "🔍 VÉRIFICATION DES MÉTRIQUES KAFKA"
echo "=========================================="

echo ""
echo "1️⃣ Vérification JMX Exporter (port 5556)..."
if curl -s http://localhost:5556/metrics | grep -q "kafka_"; then
    echo "✅ JMX Exporter fonctionne"
    echo "   Métriques disponibles:"
    curl -s http://localhost:5556/metrics | grep "kafka_topic_messages_in_total" | head -3
else
    echo "❌ JMX Exporter ne répond pas"
fi

echo ""
echo "2️⃣ Vérification Prometheus targets..."
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep -A5 "job.*kafka"

echo ""
echo "3️⃣ Test requête Prometheus pour topic raw..."
QUERY='kafka_topic_messages_in_total{topic="data.raw.earthquakes"}'
curl -s "http://localhost:9090/api/v1/query?query=${QUERY}" | python3 -m json.tool

echo ""
echo "4️⃣ Liste des métriques Kafka disponibles..."
curl -s http://localhost:9090/api/v1/label/__name__/values | python3 -m json.tool | grep kafka_topic

echo ""
echo "=========================================="
echo "📊 RÉSUMÉ"
echo "=========================================="
echo "JMX Exporter : http://localhost:5556/metrics"
echo "Prometheus   : http://localhost:9090"
echo "Grafana      : http://localhost:3000 (admin/admin)"
echo "=========================================="