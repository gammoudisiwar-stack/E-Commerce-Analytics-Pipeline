#!/usr/bin/env python3
"""
Analyse Batch locale avec Pandas (équivalent aux jobs MapReduce)
Résultats identiques à ProductCount et CategoryRevenue.
"""
import pandas as pd
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "data/ecommerce_sales_34500.csv"
df = pd.read_csv(path)

print(f"\n{'='*55}")
print(f"  DATASET : {path}")
print(f"  {len(df):,} transactions  |  {df['category'].nunique()} catégories  |  {df['product_id'].nunique()} produits distincts")
print(f"{'='*55}")

# ── Job 1 : ProductCount ─────────────────────────────────────
print("\n┌─────────────────────────────────────────────────────┐")
print("│     JOB 1 — TOP 15 PRODUITS (quantité vendue)      │")
print("└─────────────────────────────────────────────────────┘")
product_count = (
    df.groupby("product_id")["quantity"]
    .sum()
    .sort_values(ascending=False)
    .head(15)
    .reset_index()
)
product_count.columns = ["product_id", "quantite_totale"]
print(product_count.to_string(index=False))

# ── Job 2 : CategoryRevenue ──────────────────────────────────
print("\n┌─────────────────────────────────────────────────────┐")
print("│     JOB 2 — CHIFFRE D'AFFAIRES PAR CATÉGORIE       │")
print("└─────────────────────────────────────────────────────┘")
cat_revenue = (
    df.groupby("category")["total_amount"]
    .sum()
    .round(2)
    .sort_values(ascending=False)
    .reset_index()
)
cat_revenue.columns = ["category", "chiffre_affaires (€)"]
print(cat_revenue.to_string(index=False))

# ── Bonus : Activité par mois ────────────────────────────────
print("\n┌─────────────────────────────────────────────────────┐")
print("│     BONUS — ACTIVITÉ PAR MOIS (nb commandes)       │")
print("└─────────────────────────────────────────────────────┘")
df["mois"] = pd.to_datetime(df["order_date"]).dt.to_period("M")
monthly = (
    df.groupby("mois")
    .agg(nb_commandes=("order_id", "count"),
         chiffre_affaires=("total_amount", lambda x: round(x.sum(), 2)))
    .sort_values("nb_commandes", ascending=False)
    .head(12)
    .reset_index()
)
print(monthly.to_string(index=False))

# ── Bonus : Taux de retour par catégorie ─────────────────────
print("\n┌─────────────────────────────────────────────────────┐")
print("│     BONUS — TAUX DE RETOUR PAR CATÉGORIE           │")
print("└─────────────────────────────────────────────────────┘")
returns = (
    df.groupby("category")
    .apply(lambda g: round(100 * (g["returned"] == "Yes").sum() / len(g), 1))
    .sort_values(ascending=False)
    .reset_index()
)
returns.columns = ["category", "taux_retour (%)"]
print(returns.to_string(index=False))

print(f"\n{'='*55}")
print("  Analyse terminée.")
print(f"{'='*55}\n")
