# Synthèse du Projet Big Data — E-Commerce Analytics Pipeline

**Auteur :** Siwar Gammoudi | **Date :** Juin 2026
**Dépôt :** https://github.com/gammoudisiwar-stack/E-Commerce-Analytics-Pipeline

---

## 1. Problématique

Une entreprise e-commerce possède des millions de transactions. Elle veut :
- **Comprendre le passé** → quels produits se vendent le plus ? quel CA par catégorie ?
- **Réagir en temps réel** → quels produits explosent en ce moment ?

**Solution :** Architecture **Lambda** = Batch (Hadoop) + Streaming (Spark).

---

## 2. Données

**Fichier :** `ecommerce_sales_34500.csv` — 34 500 transactions, 17 colonnes.

| Colonne clé | Rôle dans le projet |
|-------------|---------------------|
| `product_id` | Identifiant du produit vendu |
| `quantity` | Nombre d'unités achetées |
| `category` | Catégorie (Electronics, Fashion, Home…) |
| `total_amount` | Montant payé par le client |
| `order_date` | Date de la transaction |

**Flux de données :**
```
CSV (disque)
  │
  ├─► HDFS (stockage distribué)  ──► MapReduce  ──► résultats batch
  │
  └─► simulateur.py (socket TCP) ──► Spark Streaming ──► Top 5 temps réel
```

---

## 3. Concepts théoriques

### 3.1 Architecture Lambda
Modèle Big Data à deux couches parallèles sur la même source :

```
                  Source de données
                        │
          ┌─────────────┴─────────────┐
          ▼                           ▼
    BATCH LAYER                 SPEED LAYER
  (Hadoop MapReduce)          (Spark Streaming)
  Traite tout l'historique    Traite le flux en direct
  Lent mais précis            Rapide mais approximatif
          │                           │
          ▼                           ▼
   Résultats complets          Résultats immédiats
```

---

### 3.2 HDFS — Hadoop Distributed File System
> **C'est quoi ?** Un système de fichiers qui découpe un gros fichier en blocs et les répartit sur plusieurs machines (DataNodes).

- **NameNode** = le chef (répertoire, sait où sont les blocs)
- **DataNode** = les ouvriers (stockent les blocs réels)
- **Réplication = 2** → chaque bloc est copié sur 2 machines (tolérance aux pannes)

```
CSV 3,4 Mo → découpé en blocs → répliqué sur DataNode1 + DataNode2
```

**Interface web :** http://localhost:9870

---

### 3.3 MapReduce
> **C'est quoi ?** Un modèle de calcul en 2 phases pour traiter de grandes quantités de données en parallèle.

```
Données brutes (CSV)
      │
   [MAP]  ← lit chaque ligne, extrait une paire (clé, valeur)
      │       ex: (P222065, 1), (P211187, 2)...
      │
 [SHUFFLE] ← regroupe automatiquement toutes les valeurs par clé
      │       ex: P222065 → [1, 3, 2, 4, 4]
      │
 [REDUCE] ← agrège les valeurs d'une même clé
      │       ex: P222065 → somme = 14
      │
   Résultat (part-r-00000)
```

**Job 1 — ProductCount :**
- Map : lit `product_id` + `quantity` → émet `(product_id, quantity)`
- Reduce : additionne toutes les quantités → `(product_id, total)`

**Job 2 — CategoryRevenue :**
- Map : lit `category` + `total_amount` → émet `(category, amount)`
- Reduce : additionne les montants → `(category, CA_total)`

**Combiner :** mini-Reduce local avant envoi réseau → optimisation trafic.

---

### 3.4 YARN — Yet Another Resource Negotiator
> **C'est quoi ?** Le gestionnaire de ressources du cluster Hadoop. Il décide quelle machine exécute quel job.

- **ResourceManager** = chef d'orchestre (alloue les ressources)
- **NodeManager** = agent sur chaque machine (exécute les tâches)

**Interface web :** http://localhost:8088

---

### 3.5 Apache Spark — RDD & DataFrame
> **C'est quoi ?** Un moteur de calcul distribué, plus rapide que MapReduce car il travaille **en mémoire** (RAM) plutôt qu'en lecture/écriture disque.

**RDD (Resilient Distributed Dataset)** = collection de données distribuée et tolérante aux pannes.

```python
# Le même calcul que MapReduce en 3 lignes :
lines.map(parse_line)           # extraire (product_id, quantity)
     .filter(lambda x: x)       # ignorer les lignes vides
     .reduceByKey(lambda a, b: a + b)  # additionner par produit
```

**DataFrame + Spark SQL** = même chose mais avec une syntaxe SQL :
```sql
SELECT product_id, SUM(quantity) AS total
FROM ventes
GROUP BY product_id
ORDER BY total DESC
LIMIT 10
```

---

### 3.6 Spark Streaming — DStream & Fenêtre Glissante
> **C'est quoi ?** Extension de Spark pour traiter des flux de données en continu, découpés en micro-batches.

**DStream** = séquence continue de micro-batches (toutes les 5s ici).

**Fenêtre glissante :**
```
Temps  →  0s   5s   10s   15s   20s   25s   30s   35s   40s
Flux   →  [batch][batch][batch][batch][batch][batch][batch]...

Fenêtre 30s, glissement 10s :
  W1 = batches de 0s à 30s  → Top 5 affiché à 30s
  W2 = batches de 10s à 40s → Top 5 affiché à 40s
  W3 = batches de 20s à 50s → Top 5 affiché à 50s
```

**`reduceByKeyAndWindow`** : agrège les données dans la fenêtre, retire automatiquement les données sorties de la fenêtre (invFunc).

**Pipeline Streaming :**
```
simulateur.py          Spark Streaming
(socket TCP 9999)  →  socketTextStream()
                   →  map(product_id, quantity)
                   →  reduceByKeyAndWindow(30s, 10s)
                   →  transform(sort desc)
                   →  Top 5 affiché toutes les 10s
```

---

### 3.7 Docker & Docker Compose
> **C'est quoi ?** Technologie de conteneurs — chaque service (NameNode, DataNode, Spark…) tourne dans son propre environnement isolé.

**Cluster créé :**
```
docker-compose.yml
├── hadoop-master        (NameNode)          → port 9870
├── hadoop-worker1       (DataNode 1)
├── hadoop-worker2       (DataNode 2)
├── hadoop-resourcemanager (YARN)            → port 8088
├── hadoop-nodemanager   (YARN worker)
├── spark-master                             → port 8082
└── spark-worker
```

---

## 4. Processus complet — de l'input à l'output

```
INPUT : ecommerce_sales_34500.csv (34 500 lignes)
         │
         ▼
┌────────────────────────────────────────────────────────┐
│  ÉTAPE 1 : Stockage dans HDFS                          │
│  hdfs dfs -put ecommerce_sales_34500.csv /user/root/   │
│  → fichier répliqué sur 2 DataNodes                    │
└────────────────────┬───────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
┌─────────────────┐     ┌──────────────────────────────┐
│  BATCH (Job 1)  │     │  STREAMING                   │
│  ProductCount   │     │                              │
│                 │     │  simulateur.py               │
│  Map:           │     │  lit le CSV ligne par ligne  │
│  (P222065, 14)  │     │  → envoie sur socket :9999   │
│                 │     │                              │
│  Reduce:        │     │  Spark Streaming             │
│  P222065 → 14   │     │  lit le socket               │
│                 │     │  fenêtre 30s / glissement 10s│
│  OUTPUT:        │     │                              │
│  part-r-00000   │     │  OUTPUT (toutes les 10s) :   │
│  P222065   14   │     │  #1 P212416   6              │
│  P211187   13   │     │  #2 P223547   5              │
│  ...            │     │  #3 P212624   5              │
└─────────────────┘     │  #4 P205274   5              │
                        │  #5 P215316   5              │
┌─────────────────┐     └──────────────────────────────┘
│  BATCH (Job 2)  │
│  CategoryRevenue│
│                 │
│  Map:           │
│  (Electronics,  │
│   266.5)        │
│                 │
│  Reduce:        │
│  Electronics →  │
│  3 319 206 €    │
│                 │
│  OUTPUT:        │
│  part-r-00000   │
│  Electronics    │
│  3319206.5      │
│  Home           │
│  1077681.52     │
│  ...            │
└─────────────────┘
```

---

## 5. Résultats obtenus

### Batch — Quantité vendue par produit (Top 5)
| Rang | product_id | Quantité totale |
|------|-----------|----------------|
| 1 | P222065 | 14 |
| 2 | P247881 | 13 |
| 3 | P211187 | 13 |
| 4 | P234626 | 12 |
| 5+ | (ex æquo) | 11 |

### Batch — CA par catégorie
| Catégorie | CA total | Part |
|-----------|---------|------|
| Electronics | 3 319 206 € | ~58% |
| Home | 1 077 681 € | ~19% |
| Sports | 629 825 € | ~11% |
| Fashion | 471 545 € | ~8% |
| Beauty | 153 019 € | ~3% |
| Toys | 132 013 € | ~2% |
| Grocery | 82 000 € | ~1% |

### Streaming — Top 5 fenêtre glissante 30s
| Rang | product_id | Quantité (30s) |
|------|-----------|---------------|
| 1 | P212416 | 6 |
| 2 | P223547 | 5 |
| 3 | P212624 | 5 |
| 4 | P205274 | 5 |
| 5 | P215316 | 5 |
> *Mis à jour toutes les 10 secondes — le classement évolue en temps réel.*

---

## 6. Comparaison Batch vs Streaming

| Critère | Batch (MapReduce) | Streaming (Spark) |
|---------|-------------------|-------------------|
| **Données traitées** | Tout l'historique (34 500 lignes) | Dernières 30 secondes |
| **Latence** | Minutes | Secondes |
| **Précision** | Exacte | Approximative |
| **Code** | ~100 lignes Java | ~20 lignes Python |
| **Vitesse** | Lecture disque (HDFS) | Traitement RAM |
| **Utilité** | Rapports, stratégie | Alertes, réactivité |

---

## 7. Technologies utilisées

| Outil | Version | Rôle |
|-------|---------|------|
| Hadoop | 3.2.1 | HDFS + YARN + MapReduce |
| Apache Spark | 3.3.0 | Batch RDD/SQL + Streaming |
| Docker Compose | 29.5.2 | Cluster multi-conteneurs |
| Java | 8 | Code MapReduce |
| Maven | 3.8.6 | Build du JAR |
| Python | 3.7–3.11 | Scripts Spark + Simulateur |
| pandas | 2.3.0 | Analyse locale |

---

## 8. Fichiers clés du projet

```
E-Commerce-Analytics-Pipeline/
├── data/
│   ├── ecommerce_sales_34500.csv      ← données source
│   └── generate_data.py               ← génère jusqu'à 1M lignes
├── docker/
│   └── docker-compose.yml             ← cluster 7 conteneurs
├── batch-mapreduce/
│   ├── pom.xml                        ← dépendances Maven/Hadoop
│   └── src/.../productcount/          ← Mapper + Reducer + Driver (Java)
│   └── src/.../categoryrevenue/       ← Mapper + Reducer + Driver (Java)
├── spark-batch/
│   ├── product_count_rdd.py           ← même calcul, 3 lignes Python
│   └── product_count_sql.py           ← version Spark SQL
├── streaming/
│   ├── simulateur.py                  ← envoie le CSV sur socket TCP
│   └── spark_streaming_top5.py        ← Top 5 fenêtre glissante
└── docs/
    ├── RAPPORT.md                     ← rapport complet
    └── SYNTHESE.md                    ← ce fichier
```
