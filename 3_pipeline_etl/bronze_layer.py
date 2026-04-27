"""
Ce script constitue le premier étage de ton architecture Medallion.
 Son rôle est de copier les données telles quelles (as-is) 
 depuis la source PostgreSQL vers ton Data Lake (format Delta). 
On parle d'ingestion "brute" pour garantir la traçabilité des données avant toute transformation.
"""
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Configuration de l'environnement
# ───────────────────────────────────────────────
load_dotenv()

# Forcer l'utilisation de python3 pour les workers Spark
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

# Récupération des secrets et chemins depuis le .env
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DELTA_PATH = os.getenv("DELTA_PATH")

# Configuration de la connexion JDBC (Java Database Connectivity)
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

print("🥉 BRONZE LAYER — Ingestion données brutes...")

# ───────────────────────────────────────────────
# 2. Initialisation de la session Spark avec support Delta Lake
# ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Bronze_Layer") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,"  # Driver pour lire PostgreSQL
            "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Réduction de la verbosité des logs pour plus de clarté
spark.sparkContext.setLogLevel("ERROR")

# ───────────────────────────────────────────────
# 3. Extraction (E de l'ETL) depuis PostgreSQL
# ───────────────────────────────────────────────
print("   📥 Lecture depuis PostgreSQL...")

# On charge les tables sources dans des DataFrames Spark
df_sal = spark.read.jdbc(DB_URL, "salaries", properties=DB_PROPS)
df_act = spark.read.jdbc(DB_URL, "activites", properties=DB_PROPS)
df_spt = spark.read.jdbc(DB_URL, "sports_declares", properties=DB_PROPS)

print(f"   Salariés   : {df_sal.count()} lignes")
print(f"   Activités  : {df_act.count()} lignes")
print(f"   Sports     : {df_spt.count()} lignes")

# ───────────────────────────────────────────────
# 4. Chargement (L de l'ETL) vers le Data Lake
# ───────────────────────────────────────────────
print("   💾 Écriture Bronze Layer → Delta Lake...")

# Sauvegarde au format Delta (Format de stockage optimisé, ACID et scalable)
# Le mode "overwrite" remplace les données existantes à chaque exécution du Bronze
df_sal.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/salaries")
df_act.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/activites")
df_spt.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/sports_declares")

print("   ✅ Bronze Layer écrit !")
print(f"     → {DELTA_PATH}/bronze/salaries")
print(f"     → {DELTA_PATH}/bronze/activites")
print(f"     → {DELTA_PATH}/bronze/sports_declares")

# Fermeture de la session pour libérer les ressources
spark.stop()
print("\n🎉 Bronze Layer terminé !")