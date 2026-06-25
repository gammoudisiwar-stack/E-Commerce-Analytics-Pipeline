#!/usr/bin/env python3
"""
Étape 2 — Génération du jeu de données synthétiques
===================================================

Génère un fichier CSV de transactions e-commerce avec le même schéma
(17 colonnes) que le fichier fourni `ecommerce_sales_34500.csv`.

Pour un projet Big Data « ni petit ni grand » (~50–100 Mo), générer
entre 500 000 et 1 000 000 de lignes.

Usage
-----
    python generate_data.py                                  # 500 000 lignes par défaut
    python generate_data.py --rows 1000000                   # 1 million de lignes
    python generate_data.py --rows 800000 --output ventes.csv
"""
import argparse
import csv
import random
from datetime import datetime, timedelta

# --------------------------------------------------------------------------
# Référentiels (catalogue produit, catégories, etc.)
# --------------------------------------------------------------------------
CATEGORIES = ["Electronics", "Fashion", "Home", "Beauty", "Grocery", "Sports", "Toys"]

# Prix de base typiques par catégorie (min, max)
CATEGORY_PRICE_RANGE = {
    "Electronics": (50.0, 1500.0),
    "Fashion": (5.0, 250.0),
    "Home": (10.0, 500.0),
    "Beauty": (3.0, 120.0),
    "Grocery": (1.0, 80.0),
    "Sports": (8.0, 400.0),
    "Toys": (4.0, 150.0),
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "UPI", "COD"]
REGIONS = ["North", "South", "East", "West"]
GENDERS = ["Male", "Female", "Other"]
DISCOUNTS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30]

HEADER = [
    "order_id", "customer_id", "product_id", "category", "price", "discount",
    "quantity", "payment_method", "order_date", "delivery_time_days", "region",
    "returned", "total_amount", "shipping_cost", "profit_margin",
    "customer_age", "customer_gender",
]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def random_date():
    """Retourne une date aléatoire entre START_DATE et END_DATE."""
    return (START_DATE + timedelta(days=random.randint(0, DATE_RANGE_DAYS))).strftime("%Y-%m-%d")


def generate_rows(n, num_products=5000, num_customers=20000):
    """Générateur paresseux de `n` transactions."""
    for i in range(n):
        order_id = f"O{100000 + i}"
        customer_id = f"C{random.randint(10000, 10000 + num_customers)}"
        product_id = f"P{200000 + random.randint(0, num_products)}"
        category = random.choice(CATEGORIES)

        low, high = CATEGORY_PRICE_RANGE[category]
        price = round(random.uniform(low, high), 2)
        discount = random.choice(DISCOUNTS)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[55, 25, 10, 6, 4])[0]

        gross = price * quantity
        total_amount = round(gross * (1 - discount), 2)
        shipping_cost = round(random.uniform(2.0, 12.0), 2)
        # marge bénéficiaire : entre -10% et +40% du montant total
        profit_margin = round(total_amount * random.uniform(-0.10, 0.40), 2)

        payment_method = random.choice(PAYMENT_METHODS)
        order_date = random_date()
        delivery_time_days = random.randint(1, 10)
        region = random.choice(REGIONS)
        returned = "Yes" if random.random() < 0.05 else "No"
        customer_age = random.randint(18, 70)
        customer_gender = random.choice(GENDERS)

        yield [
            order_id, customer_id, product_id, category, price, discount,
            quantity, payment_method, order_date, delivery_time_days, region,
            returned, total_amount, shipping_cost, profit_margin,
            customer_age, customer_gender,
        ]


def main():
    parser = argparse.ArgumentParser(description="Générateur de données e-commerce synthétiques")
    parser.add_argument("--rows", type=int, default=500_000,
                        help="Nombre de lignes à générer (défaut : 500000)")
    parser.add_argument("--output", type=str, default="data/ecommerce_sales_generated.csv",
                        help="Chemin du fichier CSV de sortie")
    parser.add_argument("--seed", type=int, default=42,
                        help="Graine aléatoire pour la reproductibilité")
    args = parser.parse_args()

    random.seed(args.seed)

    print(f"Génération de {args.rows:,} lignes -> {args.output} ...")
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for idx, row in enumerate(generate_rows(args.rows), start=1):
            writer.writerow(row)
            if idx % 100_000 == 0:
                print(f"  {idx:,} lignes écrites...")

    print(f"✅ Terminé : {args.rows:,} lignes écrites dans {args.output}")


if __name__ == "__main__":
    main()
