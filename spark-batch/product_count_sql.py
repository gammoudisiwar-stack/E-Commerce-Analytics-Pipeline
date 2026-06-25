#!/usr/bin/env python3
"""
Étape 5 (Exercice) — ProductCount & analyses avec Spark SQL / DataFrame
=======================================================================

Refait le même calcul avec un DataFrame et des requêtes SQL-like,
puis ajoute quelques analyses « tendances historiques » :
  - Top produits par quantité
  - Chiffre d'affaires par catégorie
  - Périodes de forte activité (par mois)

Usage
-----
    spark-submit spark-batch/product_count_sql.py data/ecommerce_sales_34500.csv
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/ecommerce_sales_34500.csv"

    spark = (
        SparkSession.builder
        .appName("ProductCount-SQL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Lecture du CSV avec en-tête et inférence de schéma
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    df.createOrReplaceTempView("ventes")

    print("\n===== SCHÉMA =====")
    df.printSchema()
    print(f"Nombre de transactions : {df.count():,}")

    # ---- 1) Top 10 produits par quantité vendue (Spark SQL) ----
    print("\n===== TOP 10 PRODUITS (Spark SQL) =====")
    spark.sql("""
        SELECT product_id, SUM(quantity) AS quantite_totale
        FROM ventes
        GROUP BY product_id
        ORDER BY quantite_totale DESC
        LIMIT 10
    """).show(truncate=False)

    # ---- 2) Chiffre d'affaires par catégorie (API DataFrame) ----
    print("\n===== CHIFFRE D'AFFAIRES PAR CATÉGORIE =====")
    (
        df.groupBy("category")
        .agg(F.round(F.sum("total_amount"), 2).alias("chiffre_affaires"))
        .orderBy(F.desc("chiffre_affaires"))
        .show(truncate=False)
    )

    # ---- 3) Périodes de forte activité (par mois) ----
    print("\n===== ACTIVITÉ PAR MOIS (nombre de commandes) =====")
    (
        df.withColumn("mois", F.date_format(F.col("order_date"), "yyyy-MM"))
        .groupBy("mois")
        .agg(
            F.count("*").alias("nb_commandes"),
            F.round(F.sum("total_amount"), 2).alias("chiffre_affaires"),
        )
        .orderBy(F.desc("nb_commandes"))
        .show(12, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
