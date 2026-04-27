"""
RÉSUMÉ : EXPORT GOLD VERS POSTGRESQL (SERVICING LAYER)
Ce script assure la mise à disposition des données finales :
1. Extraction : Lit les avantages calculés depuis le format Delta (Gold).
2. Sécurité : Utilise TRUNCATE au lieu de DROP pour préserver les vues SQL existantes.
3. Compatibilité : Convertit en Pandas pour une insertion simplifiée via SQLAlchemy.
4. Business Ready : Les données sont prêtes pour la consommation par Power BI.
"""

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Configuration de l'environnement
# ───────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DELTA_PATH = os.getenv("DELTA_PATH")
GOLD_AVANTAGES_PATH = os.getenv("GOLD_AVANTAGES_PATH", "gold/avantages")

# URL de connexion PostgreSQL pour SQLAlchemy
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
# 3. Lecture de la couche Gold (Calculs terminés)
# ───────────────────────────────────────────────
print("📥 Lecture de la couche Gold...")

# On récupère les données "Business Ready" stockées dans le Data Lake
df_gold = spark.read.format("delta").load(f"{DELTA_PATH}/{GOLD_AVANTAGES_PATH}")

# ───────────────────────────────────────────────
# 4. Export vers PostgreSQL (Stratégie de préservation)
# ───────────────────────────────────────────────
print("📤 Exportation des avantages vers PostgreSQL...")

# Création du moteur de base de données
engine = create_engine(DB_URL)

# Conversion en DataFrame Pandas pour faciliter l'insertion via to_sql
df_pd = df_gold.toPandas()

# 

# Utilisation d'un bloc de connexion pour garantir la transaction
with engine.connect() as conn:
    # STRATÉGIE : On vide la table sans la supprimer. 
    # Cela permet de garder les droits d'accès et les vues qui dépendent de cette table.
    try:
        # TRUNCATE est plus rapide qu'un DELETE et réinitialise la table
        conn.execute(text("TRUNCATE TABLE avantages_calcules"))
        conn.commit()
        
        # Insertion des nouvelles données en mode "append" (puisqu'on vient de vider)
        df_pd.to_sql("avantages_calcules", engine, if_exists="append", index=False)
        
    except Exception:
        # Cas particulier : Si la table n'existe pas encore (premier lancement), 
        # on utilise "replace" pour créer la structure automatiquement.
        print("⚠️ Table non trouvée, création initiale de la structure...")
        df_pd.to_sql("avantages_calcules", engine, if_exists="replace", index=False)

print("✅ Données disponibles dans PostgreSQL pour Power BI !")

# Fermeture propre de la session
spark.stop()