from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DELTA_PATH = os.getenv("DELTA_PATH")
GOLD_AVANTAGES_PATH = os.getenv("GOLD_AVANTAGES_PATH", "gold/avantages")

# Connexion PostgreSQL
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ───────────────────────────────────────────────
# 2. Initialisation Spark + Delta Lake
# ───────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Gold_to_Postgres")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ───────────────────────────────────────────────
# 3. Lecture de la couche Gold
# ───────────────────────────────────────────────
print("📥 Lecture de la couche Gold...")

df_gold = spark.read.format("delta").load(f"{DELTA_PATH}/{GOLD_AVANTAGES_PATH}")

# ───────────────────────────────────────────────
# 4. Export vers PostgreSQL
# ───────────────────────────────────────────────
print("📤 Exportation des avantages vers PostgreSQL...")

engine = create_engine(DB_URL)

df_pd = df_gold.toPandas()
df_pd.to_sql("avantages_calcules", engine, if_exists="replace", index=False)

print("✅ Données disponibles dans PostgreSQL pour Power BI !")

spark.stop()
