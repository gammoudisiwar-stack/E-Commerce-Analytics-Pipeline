#!/usr/bin/env python3
"""
Étape 7 — Traitement Streaming avec Spark Streaming
===================================================

Lit le flux de transactions émis par `simulateur.py` (socket TCP localhost:9999)
et calcule le **Top 5 des produits les plus vendus** (par quantité) sur une
**fenêtre glissante** de 30 secondes, mise à jour toutes les 10 secondes.

Pipeline
--------
    socket --> map (product_id, quantity)
           --> reduceByKeyAndWindow (somme sur fenêtre 30s, glissement 10s)
           --> transform (tri décroissant)
           --> Top 5 affiché à chaque intervalle

Usage
-----
    spark-submit streaming/spark_streaming_top5.py [host] [port]

Lancement complet (2 terminaux)
-------------------------------
    # Terminal 1 — démarrer d'abord le simulateur
    python streaming/simulateur.py data/ecommerce_sales_34500.csv

    # Terminal 2 — démarrer l'analyse temps réel
    spark-submit streaming/spark_streaming_top5.py localhost 9999
"""
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Paramètres de la fenêtre glissante
WINDOW_DURATION = 30   # taille de la fenêtre (secondes)
SLIDE_DURATION = 10    # intervalle de glissement (secondes)
BATCH_INTERVAL = 5     # micro-batch (secondes)
TOP_N = 5


def parse_line(line):
    """Extrait (product_id, quantity) d'une ligne CSV. None si invalide."""
    fields = line.split(",")
    if len(fields) < 7:
        return None
    try:
        product_id = fields[2].strip()
        quantity = int(fields[6].strip())
        if not product_id:
            return None
        return (product_id, quantity)
    except (ValueError, IndexError):
        return None


def print_top(rdd):
    """Affiche le Top N des produits pour la fenêtre courante."""
    import datetime
    now = datetime.datetime.now().strftime("%H:%M:%S")
    top = rdd.take(TOP_N)
    print("\n" + "=" * 45)
    print(f"  TOP {TOP_N} PRODUITS — fenêtre {WINDOW_DURATION}s  [{now}]")
    print("=" * 45)
    if not top:
        print("  (aucune donnée dans la fenêtre)")
    else:
        print(f"  {'rang':<5}{'product_id':<14}{'quantite':>10}")
        print("  " + "-" * 29)
        for rang, (pid, qty) in enumerate(top, start=1):
            print(f"  {rang:<5}{pid:<14}{qty:>10}")
    print("=" * 45)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999

    sc = SparkContext(appName="EcommerceTop5Streaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    # Checkpoint requis pour les opérations à fenêtre avec inverse-reduce
    ssc.checkpoint("checkpoint_top5")

    # Source : socket TCP alimenté par simulateur.py
    lines = ssc.socketTextStream(host, port)

    # (product_id, quantity)
    pairs = (
        lines
        .map(parse_line)
        .filter(lambda x: x is not None)
    )

    # Agrégation des quantités sur une fenêtre glissante (30s / glissement 10s)
    windowed_counts = pairs.reduceByKeyAndWindow(
        func=lambda a, b: a + b,        # ajoute les nouveaux batches
        invFunc=lambda a, b: a - b,     # retire les batches sortis de la fenêtre
        windowDuration=WINDOW_DURATION,
        slideDuration=SLIDE_DURATION,
    )

    # transform : trier par quantité décroissante puis afficher le Top 5
    sorted_counts = windowed_counts.transform(
        lambda rdd: rdd.filter(lambda x: x[1] > 0).sortBy(lambda x: -x[1])
    )

    sorted_counts.foreachRDD(print_top)

    print(f"[streaming] Connexion au flux {host}:{port} ...")
    print(f"[streaming] Fenêtre={WINDOW_DURATION}s, glissement={SLIDE_DURATION}s, batch={BATCH_INTERVAL}s")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
