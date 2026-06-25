# Étapes 6 & 7 — Traitement Streaming

Pipeline temps réel : un **simulateur** rejoue le fichier CSV sur un socket TCP,
et **Spark Streaming** calcule le **Top 5 des produits** sur une fenêtre glissante.

## 📁 Fichiers

| Fichier | Rôle |
|---------|------|
| [simulateur.py](simulateur.py) | Émet les lignes du CSV sur `localhost:9999` (1 ligne / 100 ms) |
| [spark_streaming_top5.py](spark_streaming_top5.py) | Consomme le flux et affiche le Top 5 (fenêtre 30 s, glissement 10 s) |

## ▶️ Lancement (2 terminaux, dans l'ordre)

**Terminal 1 — démarrer le simulateur (il attend une connexion) :**
```bash
python streaming/simulateur.py data/ecommerce_sales_34500.csv localhost 9999 0.05
```

**Terminal 2 — démarrer l'analyse Spark Streaming :**
```bash
spark-submit streaming/spark_streaming_top5.py localhost 9999
```

> ⚠️ Toujours lancer **le simulateur en premier** : il joue le rôle de serveur
> socket, Spark Streaming est le client qui s'y connecte.

## 🔧 Paramètres de la fenêtre

Dans [spark_streaming_top5.py](spark_streaming_top5.py) :

```python
WINDOW_DURATION = 30   # taille de la fenêtre (s)
SLIDE_DURATION  = 10   # intervalle de glissement (s)
BATCH_INTERVAL  = 5    # micro-batch (s)
TOP_N           = 5
```

## 📤 Exemple de sortie

```
=============================================
  TOP 5 PRODUITS — fenêtre 30s  [14:32:10]
=============================================
  rang product_id      quantite
  -----------------------------
  1    P200347             18
  2    P200912             15
  3    P200044             12
  4    P200781             11
  5    P200205              9
=============================================
```

Le classement se met à jour toutes les 10 secondes au fur et à mesure que
de nouvelles transactions arrivent et que les anciennes sortent de la fenêtre.

## 🧩 Détection d'anomalies (piste d'amélioration)

Un produit dont la quantité dans la fenêtre dépasse soudainement un seuil
peut être signalé comme « anomalie » (pic de commandes). Il suffit d'ajouter
un filtre dans `print_top`, par exemple :

```python
ALERT_THRESHOLD = 50
for pid, qty in top:
    if qty > ALERT_THRESHOLD:
        print(f"  ⚠️  ANOMALIE: {pid} = {qty} unités sur 30s")
```
