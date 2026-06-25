# 📝 NOTES — E-Commerce Analytics Pipeline (Projet Big Data)

**Auteur :** Siwar Gammoudi
**Dépôt GitHub :** https://github.com/gammoudisiwar-stack/E-Commerce-Analytics-Pipeline
**Date :** 2026

---

## 1. Résumé du projet

Pipeline Big Data illustrant une **architecture Lambda** :
- **Couche Batch** → analyse historique avec **Hadoop MapReduce**
- **Couche Streaming** → analyse temps réel avec **Spark Streaming**

Source de données : un fichier CSV de transactions e-commerce (17 colonnes).

---

## 2. Structure du projet

```
E-Commerce-Analytics-Pipeline/
├── README.md                  # Présentation générale
├── .gitignore
├── data/
│   ├── generate_data.py       # Générateur (500k–1M lignes)
│   ├── ecommerce_sales_34500.csv  # Dataset fourni (34 500 lignes)
│   └── README.md
├── docker/
│   ├── docker-compose.yml     # Cluster Hadoop (1 master + 2 workers) + Spark
│   ├── hadoop.env
│   └── README.md
├── batch-mapreduce/           # Projet Maven (Java)
│   ├── pom.xml
│   └── src/main/java/com/ecommerce/
│       ├── productcount/      # Job 1 : quantité par produit
│       └── categoryrevenue/   # Job 2 : CA par catégorie
├── spark-batch/               # Spark RDD + SQL + Scala
│   ├── product_count_rdd.py
│   ├── product_count_sql.py
│   └── ProductCount.scala
├── streaming/
│   ├── simulateur.py          # Flux socket TCP (port 9999)
│   └── spark_streaming_top5.py
├── docs/RAPPORT.md            # Rapport complet
└── scripts/run_pipeline.sh
```

---

## 3. Les 8 étapes réalisées

| # | Étape | Fichiers |
|---|-------|----------|
| 1 | Environnement Hadoop + Spark (Docker) | `docker/` |
| 2 | Génération du jeu de données | `data/generate_data.py` |
| 3 | Batch MapReduce — ProductCount | `batch-mapreduce/.../productcount/` |
| 4 | Batch MapReduce — CategoryRevenue | `batch-mapreduce/.../categoryrevenue/` |
| 5 | Spark Batch (RDD + SQL) | `spark-batch/` |
| 6 | Simulateur de flux | `streaming/simulateur.py` |
| 7 | Spark Streaming — Top 5 produits | `streaming/spark_streaming_top5.py` |
| 8 | Rapport | `docs/RAPPORT.md` |

---

## 4. Schéma des données (17 colonnes)

```
order_id, customer_id, product_id, category, price, discount, quantity,
payment_method, order_date, delivery_time_days, region, returned,
total_amount, shipping_cost, profit_margin, customer_age, customer_gender
```

Colonnes clés utilisées :
- ProductCount : `product_id` (col 2) + `quantity` (col 6)
- CategoryRevenue : `category` (col 3) + `total_amount` (col 12)

---

## 5. Commandes principales

### Générer un gros dataset
```bash
python data/generate_data.py --rows 1000000 --output data/ecommerce_sales_1M.csv
```

### Démarrer le cluster Docker
```bash
cd docker
docker compose up -d
```
Interfaces Web :
- HDFS NameNode → http://localhost:9870
- YARN → http://localhost:8088
- Spark Master → http://localhost:8080

### Tester HDFS
```bash
docker exec -it hadoop-master bash
hdfs dfs -mkdir -p /user/root/input
hdfs dfs -put /project/data/ecommerce_sales_34500.csv /user/root/input/
hdfs dfs -ls /user/root/input
```

### Batch MapReduce
```bash
cd batch-mapreduce
mvn clean package
docker cp target/ecommerce-batch-1.0.jar hadoop-master:/tmp/
docker exec -it hadoop-master bash
# Job 1
hadoop jar /tmp/ecommerce-batch-1.0.jar \
    com.ecommerce.productcount.ProductCountDriver \
    /user/root/input/ecommerce_sales_34500.csv /user/root/output/product_count
# Job 2
hadoop jar /tmp/ecommerce-batch-1.0.jar \
    com.ecommerce.categoryrevenue.CategoryRevenueDriver \
    /user/root/input/ecommerce_sales_34500.csv /user/root/output/category_revenue
# Lire les résultats
hdfs dfs -cat /user/root/output/product_count/part-r-00000 | head
```

### Spark Batch
```bash
spark-submit spark-batch/product_count_rdd.py data/ecommerce_sales_34500.csv
spark-submit spark-batch/product_count_sql.py data/ecommerce_sales_34500.csv
```

### Streaming (2 terminaux, dans l'ordre)
```bash
# Terminal 1 — simulateur (à lancer EN PREMIER)
python streaming/simulateur.py data/ecommerce_sales_34500.csv localhost 9999 0.05
# Terminal 2 — analyse temps réel
spark-submit streaming/spark_streaming_top5.py localhost 9999
```

---

## 6. Paramètres Spark Streaming

```python
WINDOW_DURATION = 30   # fenêtre glissante (s)
SLIDE_DURATION  = 10   # glissement (s)
BATCH_INTERVAL  = 5    # micro-batch (s)
TOP_N           = 5    # Top 5 produits
```

Pipeline : `socket → map(product_id, quantity) → reduceByKeyAndWindow → tri → Top 5`

---

## 7. Git / GitHub

```bash
# Le projet est déjà poussé sur :
# https://github.com/gammoudisiwar-stack/E-Commerce-Analytics-Pipeline

# Pour les futures modifications :
git add .
git commit -m "mon message"
git push
```

Branche `main` → suit `origin/main`.

---

## 8. Améliorations possibles (mentionnées dans le rapport)

- Remplacer le socket TCP par **Apache Kafka**
- Migrer vers **Spark Structured Streaming**
- Persister les résultats (HBase / Cassandra / PostgreSQL)
- Tableau de bord (Grafana / Superset)
- Détection d'anomalies automatique (seuils ou ML)

---

## 9. Technologies utilisées

- Hadoop 3.2 (HDFS + YARN + MapReduce)
- Apache Spark 3.x (RDD, DataFrame, Spark SQL, Spark Streaming)
- Docker / Docker Compose
- Java 8 + Maven
- Python 3.8+
