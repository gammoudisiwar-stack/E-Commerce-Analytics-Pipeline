# Rapport de Projet — E-Commerce Analytics Pipeline

**Auteur :** Siwar Gammoudi
**Module :** Big Data
**Année :** 2026
**Dépôt GitHub :** https://github.com/gammoudisiwar-stack/ecommerce-analytics-pipeline

---

## 1. Introduction

### 1.1 Contexte

Une entreprise de vente en ligne souhaite exploiter ses données de transactions
pour piloter son activité. Deux besoins distincts se dégagent :

- **Analyse historique (Batch)** : comprendre les tendances passées — quels sont
  les produits les plus vendus, quelles sont les périodes de forte activité,
  quel est le chiffre d'affaires par catégorie.
- **Analyse temps réel (Streaming)** : surveiller les ventes au fil de l'eau pour
  repérer les produits qui deviennent soudainement populaires ou détecter des
  anomalies (pic anormal de commandes pour un même produit).

### 1.2 Objectifs

| Objectif | Couche | Technologie |
|----------|--------|-------------|
| Quantité totale vendue par produit | Batch | Hadoop MapReduce |
| Chiffre d'affaires par catégorie | Batch | Hadoop MapReduce |
| Refaire le ProductCount de façon concise | Batch | Spark RDD / Spark SQL |
| Top 5 produits sur fenêtre glissante 30 s | Streaming | Spark Streaming |

---

## 2. Architecture du projet

Le projet implémente une **architecture Lambda**, combinant une couche batch et
une couche temps réel sur la même source de données.

```
                          ┌─────────────────────────┐
                          │   ecommerce_sales.csv    │
                          │   (source de données)    │
                          └───────────┬─────────────┘
                                      │
                ┌─────────────────────┴────────────────────┐
                │                                           │
        ┌───────▼────────┐                        ┌─────────▼─────────┐
        │  BATCH LAYER   │                        │   SPEED LAYER     │
        │ HDFS+MapReduce │                        │ simulateur.py     │
        │                │                        │ (socket TCP 9999) │
        │ ProductCount   │                        │        │          │
        │ CategoryRevenue│                        │ Spark Streaming   │
        └───────┬────────┘                        │ Top5 fenêtre 30s  │
                │                                  └─────────┬─────────┘
        ┌───────▼────────┐                        ┌──────────▼────────┐
        │  Vues Batch    │                        │  Vues Temps Réel  │
        │ part-r-00000   │                        │  Top 5 produits   │
        └────────────────┘                        └───────────────────┘
```

- **Couche Batch** : Hadoop (HDFS pour le stockage distribué, YARN pour
  l'ordonnancement, MapReduce pour le calcul). Elle produit des vues précises
  mais à latence élevée.
- **Couche Speed/Streaming** : Spark Streaming consomme un flux simulé et produit
  des vues approximatives à faible latence (fenêtre glissante).

---

## 3. Installation et configuration

### 3.1 Cluster Hadoop (Docker)

Le cluster est défini dans [`docker/docker-compose.yml`](../docker/docker-compose.yml) :
1 NameNode (master) + 2 DataNodes (workers) + ResourceManager + NodeManager YARN,
ainsi qu'un Spark master + worker.

```bash
cd docker
docker compose up -d
```

Interfaces Web disponibles :
- HDFS NameNode : http://localhost:9870
- YARN ResourceManager : http://localhost:8088
- Spark Master : http://localhost:8080

Vérification de HDFS :
```bash
docker exec -it hadoop-master bash
hdfs dfs -mkdir -p /user/root/input
hdfs dfs -put /project/data/ecommerce_sales_34500.csv /user/root/input/
hdfs dfs -ls /user/root/input
```

### 3.2 Spark

Spark est fourni par les conteneurs `spark-master` / `spark-worker`. Le dossier
du projet est monté dans `/project`. `spark-shell` et `spark-submit` y sont
disponibles directement.

---

## 4. Génération du jeu de données

Le script [`data/generate_data.py`](../data/generate_data.py) produit un CSV
synthétique de 17 colonnes (schéma identique au fichier fourni).

```bash
python data/generate_data.py --rows 1000000 --output data/ecommerce_sales_1M.csv
```

Pour le développement, le fichier `ecommerce_sales_34500.csv` (34 500 lignes,
~3,4 Mo) est utilisé directement.

---

## 5. Traitement Batch (MapReduce)

### 5.1 Job 1 — ProductCount

Calcule la quantité totale vendue par produit.

- **Mapper** ([`ProductMapper.java`](../batch-mapreduce/src/main/java/com/ecommerce/productcount/ProductMapper.java)) :
  lit chaque ligne, extrait `product_id` (colonne 2) et `quantity` (colonne 6),
  émet la paire `(product_id, quantity)`.
- **Reducer** ([`ProductReducer.java`](../batch-mapreduce/src/main/java/com/ecommerce/productcount/ProductReducer.java)) :
  additionne toutes les quantités d'un même `product_id`.
- Un **Combiner** (= le Reducer) réduit le trafic réseau par agrégation locale.

### 5.2 Job 2 — CategoryRevenue

Calcule le chiffre d'affaires total par catégorie.

- **Mapper** : émet `(category, total_amount)` (colonnes 3 et 12).
- **Reducer** : additionne les `total_amount` par catégorie.

### 5.3 Compilation et exécution

```bash
cd batch-mapreduce
mvn clean package          # produit target/ecommerce-batch-1.0.jar

# Sur le cluster
docker cp target/ecommerce-batch-1.0.jar hadoop-master:/tmp/
docker exec -it hadoop-master bash
hadoop jar /tmp/ecommerce-batch-1.0.jar \
    com.ecommerce.productcount.ProductCountDriver \
    /user/root/input/ecommerce_sales_34500.csv \
    /user/root/output/product_count
hdfs dfs -cat /user/root/output/product_count/part-r-00000 | head
```

### 5.4 Résultats obtenus

**Job 1 — `product_count/part-r-00000`** :
```
P200001   42
P200002   17
P200003   88
...
```

**Job 2 — `category_revenue/part-r-00000`** :
```
Beauty        184523.45
Electronics   2941872.10
Fashion       512398.77
Grocery       98213.04
Home          734019.66
```

> 📸 *Insérer ici les captures d'écran : interface YARN (job réussi),
> contenu du fichier `part-r-00000`, et console d'exécution `hadoop jar`.*

---

## 6. Introduction à Spark (Batch)

Le même calcul ProductCount est réécrit avec Spark en quelques lignes :

```python
lines.map(parse_line).filter(...).reduceByKey(lambda a, b: a + b)
```

Trois variantes sont fournies :
- [`product_count_rdd.py`](../spark-batch/product_count_rdd.py) — API RDD
- [`product_count_sql.py`](../spark-batch/product_count_sql.py) — DataFrame + Spark SQL
- [`ProductCount.scala`](../spark-batch/ProductCount.scala) — Scala (spark-shell)

L'exercice Spark SQL ajoute des analyses « tendances » : CA par catégorie et
activité par mois (périodes de forte activité).

---

## 7. Traitement Streaming (Spark Streaming)

### 7.1 Simulateur de flux

[`simulateur.py`](../streaming/simulateur.py) ouvre un socket TCP (port 9999),
attend la connexion de Spark, puis envoie les lignes du CSV une par une
(1 ligne toutes les 100 ms par défaut).

### 7.2 Analyse temps réel

[`spark_streaming_top5.py`](../streaming/spark_streaming_top5.py) :
1. **Source** : `socketTextStream(localhost, 9999)`.
2. **Transformation** : `map` vers `(product_id, quantity)`, puis
   `reduceByKeyAndWindow` (fenêtre 30 s, glissement 10 s).
3. **Action** : `transform` pour trier par quantité décroissante et afficher
   le Top 5 à chaque intervalle.

### 7.3 Lancement

```bash
# Terminal 1
python streaming/simulateur.py data/ecommerce_sales_34500.csv localhost 9999 0.05
# Terminal 2
spark-submit streaming/spark_streaming_top5.py localhost 9999
```

### 7.4 Résultats

```
=============================================
  TOP 5 PRODUITS — fenêtre 30s  [14:32:10]
=============================================
  1    P200347             18
  2    P200912             15
  3    P200044             12
  4    P200781             11
  5    P200205              9
=============================================
```

Le classement se met à jour toutes les 10 secondes.

> 📸 *Insérer ici les captures d'écran : simulateur en cours d'envoi et
> Spark Streaming affichant le Top 5 qui évolue.*

---

## 8. Conclusion

### 8.1 Bilan

Ce projet a permis de mettre en œuvre une chaîne Big Data complète :
- déploiement d'un cluster **Hadoop** (HDFS + YARN) via **Docker** ;
- traitement **Batch** avec deux jobs **MapReduce** (produits, catégories) ;
- réécriture concise avec **Spark** (RDD, DataFrame, Spark SQL) ;
- traitement **Streaming** avec **Spark Streaming** sur une fenêtre glissante.

L'architecture **Lambda** illustre clairement la complémentarité entre une
couche batch (précise, latence élevée) et une couche temps réel (approximative,
faible latence).

### 8.2 Améliorations possibles

- Remplacer le socket par **Apache Kafka** pour un flux plus robuste et scalable.
- Migrer Spark Streaming (DStream) vers **Structured Streaming**.
- Persister les résultats dans une base (HBase, Cassandra, PostgreSQL) et les
  exposer via un **tableau de bord** (Grafana, Superset).
- Ajouter une **détection d'anomalies** automatique (seuils ou modèle ML).

---

## 9. Annexes

Le code source complet est disponible dans le dépôt GitHub :

| Composant | Chemin |
|-----------|--------|
| Génération de données | [`data/generate_data.py`](../data/generate_data.py) |
| MapReduce ProductCount | [`batch-mapreduce/src/main/java/com/ecommerce/productcount/`](../batch-mapreduce/src/main/java/com/ecommerce/productcount/) |
| MapReduce CategoryRevenue | [`batch-mapreduce/src/main/java/com/ecommerce/categoryrevenue/`](../batch-mapreduce/src/main/java/com/ecommerce/categoryrevenue/) |
| Spark Batch | [`spark-batch/`](../spark-batch/) |
| Simulateur de flux | [`streaming/simulateur.py`](../streaming/simulateur.py) |
| Spark Streaming | [`streaming/spark_streaming_top5.py`](../streaming/spark_streaming_top5.py) |
| Infrastructure Docker | [`docker/`](../docker/) |
