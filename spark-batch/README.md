# Étape 5 — Introduction à Spark (Batch)

Ré-écriture du **ProductCount** avec Spark, en trois variantes équivalentes.

| Fichier | API | Lancement |
|---------|-----|-----------|
| [product_count_rdd.py](product_count_rdd.py) | RDD (`map` / `reduceByKey`) | `spark-submit product_count_rdd.py <csv>` |
| [product_count_sql.py](product_count_sql.py) | DataFrame + Spark SQL | `spark-submit product_count_sql.py <csv>` |
| [ProductCount.scala](ProductCount.scala) | RDD + Spark SQL (Scala) | `spark-shell -i ProductCount.scala` |

## ▶️ Exécution (PySpark)

```bash
spark-submit spark-batch/product_count_rdd.py data/ecommerce_sales_34500.csv
spark-submit spark-batch/product_count_sql.py data/ecommerce_sales_34500.csv
```

## ▶️ Exécution dans le conteneur Spark (Docker)

```bash
docker exec -it spark-master bash
spark-submit /project/spark-batch/product_count_rdd.py \
    /project/data/ecommerce_sales_34500.csv
```

## ▶️ Exécution interactive (spark-shell / Scala)

```bash
docker exec -it spark-master bash
spark-shell -i /project/spark-batch/ProductCount.scala
```

## 💡 Comparaison avec MapReduce

Le même résultat que le job MapReduce (≈ 100 lignes de Java) tient en
**3 transformations Spark** :

```python
lines.map(parse_line).filter(...).reduceByKey(lambda a, b: a + b)
```

C'est l'intérêt majeur de Spark : un modèle de programmation plus expressif
et une exécution en mémoire bien plus rapide pour les traitements itératifs.
