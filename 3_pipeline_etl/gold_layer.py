from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

os.environ["PYSPARK_PYTHON"] = "python3"

DELTA_PATH = os.getenv("DELTA_PATH")

TAUX_PRIME = float(os.getenv("TAUX_PRIME", 0.05))
JOURS_BIEN_ETRE = int(os.getenv("JOURS_BIEN_ETRE", 5))

SILVER_SALARIES = os.getenv("SILVER_SALARIES", "silver/salaries")
SILVER_ACTIVITES = os.getenv("SILVER_ACTIVITES", "silver/activites")

print("🏆 GOLD LAYER — Calcul des Avantages (Note de Cadrage)")

# ───────────────────────────────────────────────
# 2. Spark + Delta
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
# 3. Lecture Silver Layer
# ───────────────────────────────────────────────
print("   📥 Lecture des données nettoyées...")

df_sal = spark.read.format("delta").load(f"{DELTA_PATH}/{SILVER_SALARIES}")
df_act = spark.read.format("delta").load(f"{DELTA_PATH}/{SILVER_ACTIVITES}")

# ───────────────────────────────────────────────
# 4. Agrégation des activités
# ───────────────────────────────────────────────
df_stats_act = df_act.groupBy("id_salarie").agg(
    F.count("id").alias("nb_activites")
)

# ───────────────────────────────────────────────
# 5. Calcul des avantages (Note de Cadrage)
# ───────────────────────────────────────────────
print("   💰 Calcul des primes et journées bien-être...")

df_gold = (
    df_sal.join(df_stats_act, on="id_salarie", how="left")
    .fillna({"nb_activites": 0})
    .withColumn(
        "montant_prime",
        F.when(
            (F.col("declaration_ok") == True) &
            (F.col("moyen_deplacement").isin(["Marche/running", "Vélo/Trottinette/Autres"])),
            F.round(F.col("salaire_brut") * TAUX_PRIME, 2)
        ).otherwise(0.0)
    )
    .withColumn(
        "jours_bienetre",
        F.when(F.col("nb_activites") >= 15, JOURS_BIEN_ETRE).otherwise(0)
    )
    .withColumn(
        "prime_sportive",
        F.col("montant_prime") > 0
    )
    .withColumn("date_calcul", F.current_date())
)

# ───────────────────────────────────────────────
# 6. Finalisation du schéma
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
# 7. Sauvegarde Gold Layer
# ───────────────────────────────────────────────
print("   💾 Sauvegarde de la couche Gold...")

df_final.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/gold/avantages")

print(f"   ✅ Gold Layer terminée ! {df_final.count()} salariés analysés.")
df_final.orderBy(F.desc("montant_prime")).show(5)

spark.stop()
