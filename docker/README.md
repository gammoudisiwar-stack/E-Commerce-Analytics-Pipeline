# Étape 1 — Mise en place de l'environnement (Hadoop & Spark)

Ce dossier contient la configuration Docker pour monter un **cluster Hadoop**
(1 master + 2 workers) avec **HDFS**, **YARN** et **Spark**.

## 🚀 Démarrer le cluster

```bash
cd docker
docker compose up -d
```

Vérifier que tous les conteneurs tournent :

```bash
docker compose ps
```

## 🌐 Interfaces Web

| Service | URL | Description |
|---------|-----|-------------|
| HDFS NameNode | http://localhost:9870 | État du système de fichiers distribué |
| YARN ResourceManager | http://localhost:8088 | Suivi des jobs MapReduce/Spark |
| Spark Master | http://localhost:8080 | État du cluster Spark |
| Spark Worker | http://localhost:8081 | État du worker Spark |

## 📁 Tester HDFS (Étape 1 / Chapitre 2)

Entrer dans le conteneur master :

```bash
docker exec -it hadoop-master bash
```

Commandes de base HDFS :

```bash
# Lister la racine HDFS
hdfs dfs -ls /

# Créer le dossier d'entrée
hdfs dfs -mkdir -p /user/root/input

# Copier le jeu de données local vers HDFS
hdfs dfs -put /project/data/ecommerce_sales_34500.csv /user/root/input/

# Vérifier
hdfs dfs -ls /user/root/input
hdfs dfs -cat /user/root/input/ecommerce_sales_34500.csv | head -5

# Voir l'espace utilisé
hdfs dfs -du -h /user/root/input
```

## ⚙️ Exécuter un job MapReduce sur le cluster (Étapes 3 & 4)

Après avoir compilé le JAR (`mvn clean package` dans `batch-mapreduce/`) :

```bash
# 1. Copier le JAR dans le conteneur master
docker cp ../batch-mapreduce/target/ecommerce-batch-1.0.jar hadoop-master:/tmp/

# 2. Entrer dans le master
docker exec -it hadoop-master bash

# 3. Lancer le Job 1 : quantité vendue par produit
hadoop jar /tmp/ecommerce-batch-1.0.jar com.ecommerce.productcount.ProductCountDriver \
    /user/root/input/ecommerce_sales_34500.csv \
    /user/root/output/product_count

# 4. Lancer le Job 2 : chiffre d'affaires par catégorie
hadoop jar /tmp/ecommerce-batch-1.0.jar com.ecommerce.categoryrevenue.CategoryRevenueDriver \
    /user/root/input/ecommerce_sales_34500.csv \
    /user/root/output/category_revenue

# 5. Lire les résultats
hdfs dfs -cat /user/root/output/product_count/part-r-00000 | head -20
hdfs dfs -cat /user/root/output/category_revenue/part-r-00000
```

> ⚠️ HDFS refuse d'écrire si le dossier de sortie existe déjà.
> Supprimer avant de relancer : `hdfs dfs -rm -r /user/root/output/product_count`

## 🛑 Arrêter le cluster

```bash
docker compose down          # arrête les conteneurs
docker compose down -v       # arrête ET supprime les volumes (données HDFS)
```
