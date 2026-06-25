# Données

## Fichiers

- **`ecommerce_sales_34500.csv`** — Jeu de données fourni (34 500 transactions, ~3,4 Mo).
  Utilisable directement pour tous les traitements Batch et Streaming.
- **`generate_data.py`** — Générateur de données synthétiques pour produire un
  fichier plus volumineux (500k–1M lignes ≈ 50–100 Mo) respectant **le même schéma**.

## Générer un gros fichier

```bash
# 500 000 lignes (~50 Mo)
python data/generate_data.py --rows 500000 --output data/ecommerce_sales_500k.csv

# 1 000 000 lignes (~100 Mo)
python data/generate_data.py --rows 1000000 --output data/ecommerce_sales_1M.csv
```

> Les fichiers générés volumineux sont ignorés par git (voir `.gitignore`)
> pour ne pas alourdir le dépôt. Seul l'échantillon de 34 500 lignes est versionné.

## Schéma (17 colonnes)

```
order_id,customer_id,product_id,category,price,discount,quantity,payment_method,
order_date,delivery_time_days,region,returned,total_amount,shipping_cost,
profit_margin,customer_age,customer_gender
```

Index utilisés par les traitements :

| Traitement | Colonnes utilisées | Indices |
|------------|--------------------|---------|
| ProductCount (MapReduce / Spark) | `product_id`, `quantity` | 2, 6 |
| CategoryRevenue (MapReduce) | `category`, `total_amount` | 3, 12 |
| Streaming Top 5 | `product_id`, `quantity` | 2, 6 |
