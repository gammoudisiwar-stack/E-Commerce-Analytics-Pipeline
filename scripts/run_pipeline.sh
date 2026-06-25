#!/usr/bin/env bash
# =============================================================
#  Lancement rapide du pipeline Streaming (Étapes 6 & 7)
#  Démarre le simulateur en arrière-plan puis Spark Streaming.
# =============================================================
set -euo pipefail

CSV="${1:-data/ecommerce_sales_34500.csv}"
HOST="${2:-localhost}"
PORT="${3:-9999}"

echo "==> Démarrage du simulateur de flux ($CSV -> $HOST:$PORT)"
python streaming/simulateur.py "$CSV" "$HOST" "$PORT" 0.05 &
SIM_PID=$!

# Laisser le temps au simulateur d'ouvrir le socket
sleep 2

cleanup() {
    echo "==> Arrêt du simulateur (PID $SIM_PID)"
    kill "$SIM_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Démarrage de Spark Streaming (Top 5)"
spark-submit streaming/spark_streaming_top5.py "$HOST" "$PORT"
