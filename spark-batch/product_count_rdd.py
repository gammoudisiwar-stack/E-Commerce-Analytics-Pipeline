#!/usr/bin/env python3
"""
Étape 5 — Ré-écriture du ProductCount avec Spark (API RDD)
==========================================================

Le même calcul que le job MapReduce « ProductCount » en quelques lignes,
en utilisant les RDD (Resilient Distributed Datasets) et les
transformations `map` / `reduceByKey`.

Usage
-----
    spark-submit spark-batch/product_count_rdd.py data/ecommerce_sales_34500.csv

Ou dans le conteneur Spark :
    spark-submit /project/spark-batch/product_count_rdd.py \
        /project/data/ecommerce_sales_34500.csv
"""
import sys
from pyspark import SparkContext


def parse_line(line):
    """Extrait (product_id, quantity) d'une ligne CSV. None si invalide/en-tête."""
    fields = line.split(",")
    if len(fields) < 7 or fields[0] == "order_id":
        return None
    try:
        product_id = fields[2].strip()
        quantity = int(fields[6].strip())
        return (product_id, quantity)
    except (ValueError, IndexError):
        return None


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/ecommerce_sales_34500.csv"

    sc = SparkContext(appName="ProductCount-RDD")
    sc.setLogLevel("WARN")

    lines = sc.textFile(input_path)

    # map -> filtre des lignes invalides -> reduceByKey (somme des quantités)
    product_counts = (
        lines
        .map(parse_line)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
    )

    # Top 10 des produits les plus vendus (tri décroissant par quantité)
    top10 = product_counts.takeOrdered(10, key=lambda x: -x[1])

    print("\n===== TOP 10 PRODUITS LES PLUS VENDUS (RDD) =====")
    print(f"{'product_id':<12} {'quantite_totale':>15}")
    print("-" * 30)
    for product_id, total in top10:
        print(f"{product_id:<12} {total:>15}")

    print(f"\nNombre total de produits distincts : {product_counts.count()}")

    # Sauvegarde optionnelle du résultat complet
    # product_counts.saveAsTextFile("output/spark_product_count")

    sc.stop()


if __name__ == "__main__":
    main()
