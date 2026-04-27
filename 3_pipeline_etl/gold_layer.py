"""
RÉSUMÉ : GOLD LAYER — CALCUL DES AVANTAGES RH
Ce script finalise le pipeline de données :
1. Agrégation : Compte le nombre total d'activités sportives par salarié.
2. Logique Prime : Applique le taux de 5% (TAUX_PRIME) pour les trajets actifs validés.
3. Logique Bien-être : Attribue 5 jours (JOURS_BIEN_ETRE) si le quota de 15 activités est atteint.
4. Distribution : Prépare la table finale pour les tableaux de bord RH et la paie.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Configuration et Chargement des Paramètres
# ───────────────────────────────────────────────
load_dotenv()

# Forcer Spark à utiliser Python3 sur les nœuds de calcul
os.environ["PYSPARK_PYTHON"] = "python3"

DELTA_PATH = os.getenv("DELTA_PATH")

# Paramètres de calcul récupérés depuis le .env pour plus de flexibilité (Note de Cadrage)
TAUX_PRIME = float(os.getenv("TAUX_PRIME", 0.05))
JOURS_BIEN_ETRE = int(os.getenv("JOURS_BIEN_ETRE", 5))

SILVER_SALARIES = os.getenv("SILVER_SALARIES", "silver/salaries")
SILVER_ACTIVITES = os.getenv("SILVER_ACTIVITES", "silver/activites")

print("🏆 GOLD LAYER — Calcul des Avantages (Note de Cadrage)")

# ───────────────────────────────────────────────
# 2. Initialisation Spark Session avec Support Delta
# ───────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Gold_Layer")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ───────────────────────────────────────────────
# 3. Lecture des données Silver (Cleanées et Géocodées)
# ───────────────────────────────────────────────
print("   📥 Lecture des données nettoyées...")

# On charge les données issues de la couche Silver (déjà validées par l'ETL)
df_sal = spark.read.format("delta").load(f"{DELTA_PATH}/{SILVER_SALARIES}")
df_act = spark.read.format("delta").load(f"{DELTA_PATH}/{SILVER_ACTIVITES}")

# ───────────────────────────────────────────────
# 4. Agrégation des données sportives
# ───────────────────────────────────────────────
# On calcule le volume d'activités pour chaque individu
df_stats_act = df_act.groupBy("id_salarie").agg(
    F.count("id").alias("nb_activites")
)

# ───────────────────────────────────────────────
# 5. Calcul des Avantages (Moteur de Règles)
# ───────────────────────────────────────────────
print("   💰 Calcul des primes et journées bien-être...")



df_gold = (
    df_sal.join(df_stats_act, on="id_salarie", how="left")
    .fillna({"nb_activites": 0})
    
    # Règle 1 : Calcul de la prime (Basé sur la déclaration OK et le mode de transport)
    .withColumn(
        "montant_prime",
        F.when(
            (F.col("declaration_ok") == True) &
            (F.col("moyen_deplacement").isin(["Marche/running", "Vélo/Trottinette/Autres"])),
            F.round(F.col("salaire_brut") * TAUX_PRIME, 2)
        ).otherwise(0.0)
    )
    
    # Règle 2 : Attribution des jours Bien-être (Seuil de 15 activités)
    .withColumn(
        "jours_bienetre",
        F.when(F.col("nb_activites") >= 15, JOURS_BIEN_ETRE).otherwise(0)
    )
    
    # Flag pour simplifier le filtrage RH
    .withColumn(
        "prime_sportive",
        F.col("montant_prime") > 0
    )
    .withColumn("date_calcul", F.current_date())
)

# ───────────────────────────────────────────────
# 6. Sélection des colonnes Finales (Format Gold)
# ───────────────────────────────────────────────
df_final = df_gold.select(
    "id_salarie",
    "prime_sportive",
    "montant_prime",
    "jours_bienetre",
    "nb_activites",
    "date_calcul"
)

# ───────────────────────────────────────────────
# 7. Sauvegarde Persistante
# ───────────────────────────────────────────────
print("   💾 Sauvegarde de la couche Gold...")

# Stockage final prêt pour PowerBI, Tableau ou PostgreSQL
df_final.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/gold/avantages")

print(f"   ✅ Gold Layer terminée ! {df_final.count()} salariés analysés.")

# Aperçu pour vérification rapide
df_final.orderBy(F.desc("montant_prime")).show(5)

spark.stop()