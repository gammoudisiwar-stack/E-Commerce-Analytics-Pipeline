# E-Commerce Analytics Pipeline

> Analyse des ventes en **temps réel** et **historique** avec **Hadoop MapReduce** (Batch) et **Apache Spark Streaming** (Stream).

Projet Big Data complet illustrant une **architecture Lambda** : une couche *Batch* (Hadoop/MapReduce) pour l'analyse historique et une couche *Speed/Streaming* (Spark Streaming) pour l'analyse temps réel d'un flux de transactions e-commerce.

---

## 🎯 Objectifs

| Couche | Question métier | Technologie |
|--------|-----------------|-------------|
| **Batch** | Quels sont les produits les plus vendus ? | Hadoop MapReduce |
| **Batch** | Quel est le chiffre d'affaires par catégorie ? | Hadoop MapReduce |
| **Batch** | Refaire le ProductCount en quelques lignes | Spark RDD / Spark SQL |
| **Streaming** | Top 5 des produits sur une fenêtre glissante de 30s | Spark Streaming (socket TCP) |

---

## 🗂️ Structure du projet

```
ecommerce-analytics-pipeline/
├── README.md                      # Ce fichier
├── data/
│   ├── generate_data.py           # Générateur de données synthétiques (500k–1M lignes)
│   └── ecommerce_sales_34500.csv  # Jeu de données fourni (34 500 lignes)
├── docker/
│   ├── docker-compose.yml         # Cluster Hadoop (1 master + 2 workers) + Spark
│   ├── hadoop.env                 # Configuration Hadoop
│   └── README.md                  # Guide de mise en place de l'environnement
├── batch-mapreduce/               # Projet Maven (Étapes 3 & 4)
│   ├── pom.xml
│   └── src/main/java/com/ecommerce/
│       ├── productcount/          # Job 1 : quantité vendue par produit
│       └── categoryrevenue/       # Job 2 : chiffre d'affaires par catégorie
├── spark-batch/                   # Étape 5 : Spark RDD + Spark SQL
│   ├── product_count_rdd.py
│   ├── product_count_sql.py
│   └── ProductCount.scala
├── streaming/                     # Étapes 6 & 7
│   ├── simulateur.py              # Simulateur de flux (socket TCP, port 9999)
│   └── spark_streaming_top5.py    # Top 5 produits sur fenêtre glissante
├── docs/
│   └── RAPPORT.md                 # Rapport de projet (Étape 8)
└── scripts/
    └── run_pipeline.sh            # Lancement rapide du pipeline streaming
```

---

## 📊 Jeu de données

Fichier CSV avec en-tête, **17 colonnes** :

| Colonne | Description |
|---------|-------------|
| `order_id` | Identifiant unique de la transaction |
| `customer_id` | Identifiant du client |
| `product_id` | Identifiant du produit |
| `category` | Catégorie (Electronics, Fashion, Home, Beauty, Grocery...) |
| `price` | Prix unitaire |
| `discount` | Remise appliquée (0–0.3) |
| `quantity` | Quantité achetée |
| `payment_method` | Moyen de paiement |
| `order_date` | Date de la transaction (YYYY-MM-DD) |
| `delivery_time_days` | Délai de livraison (jours) |
| `region` | Région (North, South, East, West) |
| `returned` | Produit retourné (Yes/No) |
| `total_amount` | Montant total payé |
| `shipping_cost` | Frais de port |
| `profit_margin` | Marge bénéficiaire |
| `customer_age` | Âge du client |
| `customer_gender` | Genre du client |

---

## 🚀 Démarrage rapide

### 1. Cloner le dépôt
```bash
git clone https://github.com/gammoudisiwar-stack/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline
```

### 2. (Optionnel) Générer un plus gros jeu de données
```bash
python data/generate_data.py --rows 1000000 --output data/ecommerce_sales_1M.csv
```

### 3. Monter l'environnement Hadoop + Spark
```bash
cd docker
docker compose up -d
```
👉 Voir [docker/README.md](docker/README.md) pour le détail des commandes HDFS.

### 4. Traitement Batch (MapReduce)
```bash
cd batch-mapreduce
mvn clean package
# Copier le JAR dans le master puis lancer le job (voir docker/README.md)
```

### 5. Traitement Streaming
```bash
# Terminal 1 — démarre le simulateur de flux
python streaming/simulateur.py data/ecommerce_sales_34500.csv

# Terminal 2 — démarre l'analyse temps réel
spark-submit streaming/spark_streaming_top5.py localhost 9999
```

---

## 🏗️ Architecture (Lambda)

```
                          ┌─────────────────────────┐
                          │   ecommerce_sales.csv    │
                          └───────────┬─────────────┘
                                      │
                ┌─────────────────────┴────────────────────┐
                │                                           │
        ┌───────▼────────┐                        ┌─────────▼─────────┐
        │  BATCH LAYER   │                        │   SPEED LAYER     │
        │ Hadoop HDFS +  │                        │  simulateur.py    │
        │   MapReduce    │                        │   (socket 9999)   │
        │                │                        │        │          │
        │ • ProductCount │                        │  ┌─────▼───────┐  │
        │ • CategoryRev. │                        │  │Spark Stream │  │
        └───────┬────────┘                        │  │Top5 fenêtre │  │
                │                                  │  │  glissante  │  │
                │                                  └──┴─────┬───────┘  │
                │                                           │          │
        ┌───────▼────────┐                        ┌─────────▼─────────┐
        │ Vues Batch     │                        │   Vues Temps Réel │
        │ part-r-00000   │                        │   Top 5 produits  │
        └────────────────┘                        └───────────────────┘
```

---

## 📋 Plan du projet en 8 étapes

1. ✅ Mise en place de l'environnement (Hadoop & Spark via Docker) — [docker/](docker/)
2. ✅ Génération du jeu de données — [data/generate_data.py](data/generate_data.py)
3. ✅ Batch MapReduce — ProductCount — [batch-mapreduce/](batch-mapreduce/)
4. ✅ Batch MapReduce — Chiffre d'affaires par catégorie — [batch-mapreduce/](batch-mapreduce/)
5. ✅ Introduction à Spark (RDD + Spark SQL) — [spark-batch/](spark-batch/)
6. ✅ Simulateur de flux — [streaming/simulateur.py](streaming/simulateur.py)
7. ✅ Spark Streaming — Top 5 produits — [streaming/spark_streaming_top5.py](streaming/spark_streaming_top5.py)
8. ✅ Rapport — [docs/RAPPORT.md](docs/RAPPORT.md)

---

## 🛠️ Technologies

- **Hadoop 3.2** (HDFS + YARN + MapReduce)
- **Apache Spark 3.x** (RDD, DataFrame, Spark SQL, Spark Streaming)
- **Docker / Docker Compose**
- **Java 8+ / Maven** (jobs MapReduce)
- **Python 3.8+** (génération de données & simulateur de flux)

---

## 👤 Auteur

**Siwar Gammoudi** — Projet Big Data — 2026
GitHub : [@gammoudisiwar-stack](https://github.com/gammoudisiwar-stack)
