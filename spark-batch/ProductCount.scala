// =============================================================
//  Étape 5 — ProductCount avec Spark (Scala / spark-shell)
// =============================================================
//
//  À coller dans `spark-shell` (dans le conteneur Spark), ou exécuter :
//      spark-shell -i spark-batch/ProductCount.scala
//
//  Démontre les deux approches : RDD et DataFrame/Spark SQL.
// -------------------------------------------------------------

val inputPath = "/project/data/ecommerce_sales_34500.csv"

// ---------- Approche 1 : RDD (map / reduceByKey) ----------
val lines = sc.textFile(inputPath)

val header = lines.first()  // on récupère l'en-tête pour le filtrer

val productCounts = lines
  .filter(line => line != header)                 // retirer l'en-tête
  .map(_.split(","))
  .filter(fields => fields.length >= 7)
  .map(fields => (fields(2), fields(6).toInt))    // (product_id, quantity)
  .reduceByKey(_ + _)

println("===== TOP 10 PRODUITS (RDD) =====")
productCounts
  .takeOrdered(10)(Ordering[Int].reverse.on(_._2))
  .foreach { case (pid, qty) => println(f"$pid%-12s $qty%10d") }


// ---------- Approche 2 : DataFrame + Spark SQL ----------
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(inputPath)

df.createOrReplaceTempView("ventes")

println("===== TOP 10 PRODUITS (Spark SQL) =====")
spark.sql("""
  SELECT product_id, SUM(quantity) AS quantite_totale
  FROM ventes
  GROUP BY product_id
  ORDER BY quantite_totale DESC
  LIMIT 10
""").show(false)

println("===== CHIFFRE D'AFFAIRES PAR CATÉGORIE =====")
spark.sql("""
  SELECT category, ROUND(SUM(total_amount), 2) AS chiffre_affaires
  FROM ventes
  GROUP BY category
  ORDER BY chiffre_affaires DESC
""").show(false)
