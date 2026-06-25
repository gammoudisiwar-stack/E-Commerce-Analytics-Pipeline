# Étapes 3 & 4 — Traitement Batch avec MapReduce

Projet **Maven** contenant deux jobs Hadoop MapReduce.

## 📦 Jobs

| Job | Classe Driver | Résultat |
|-----|---------------|----------|
| **Job 1 — ProductCount** | `com.ecommerce.productcount.ProductCountDriver` | Quantité totale vendue par produit |
| **Job 2 — CategoryRevenue** | `com.ecommerce.categoryrevenue.CategoryRevenueDriver` | Chiffre d'affaires total par catégorie |

## 🔨 Compiler

```bash
cd batch-mapreduce
mvn clean package
```
Produit `target/ecommerce-batch-1.0.jar`.

## ▶️ Exécuter en local (test rapide, sans cluster)

```bash
# Job 1
hadoop jar target/ecommerce-batch-1.0.jar \
    com.ecommerce.productcount.ProductCountDriver \
    ../data/ecommerce_sales_34500.csv \
    output/product_count

# Job 2
hadoop jar target/ecommerce-batch-1.0.jar \
    com.ecommerce.categoryrevenue.CategoryRevenueDriver \
    ../data/ecommerce_sales_34500.csv \
    output/category_revenue
```

> En local (IntelliJ), il est aussi possible de lancer directement la classe
> `ProductCountDriver` avec les arguments
> `data/ecommerce_sales_34500.csv output/product_count`.
> Le résultat se trouve dans `output/product_count/part-r-00000`.

## ☁️ Exécuter sur le cluster Hadoop (Docker)

Voir [../docker/README.md](../docker/README.md) — section « Exécuter un job MapReduce ».

```bash
docker cp target/ecommerce-batch-1.0.jar hadoop-master:/tmp/
docker exec -it hadoop-master bash
hadoop jar /tmp/ecommerce-batch-1.0.jar \
    com.ecommerce.productcount.ProductCountDriver \
    /user/root/input/ecommerce_sales_34500.csv \
    /user/root/output/product_count
hdfs dfs -cat /user/root/output/product_count/part-r-00000 | head
```

## 📤 Exemple de sortie

**Job 1 (product_count/part-r-00000)** — `product_id<TAB>quantité_totale` :
```
P200001   42
P200002   17
P200003   88
...
```

**Job 2 (category_revenue/part-r-00000)** — `category<TAB>chiffre_affaires` :
```
Beauty        184523.45
Electronics   2941872.10
Fashion       512398.77
Grocery       98213.04
Home          734019.66
...
```
