import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

# Variables .env
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DELTA_PATH = os.getenv("DELTA_PATH")

DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

print("🥉 BRONZE LAYER — Ingestion données brutes...")

# Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,"
            "io.delta:delta-spark_2.12:3.2.1") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Ingestion depuis PostgreSQL ────
print("  📥 Lecture depuis PostgreSQL...")

df_sal = spark.read.jdbc(DB_URL, "salaries", properties=DB_PROPS)
df_act = spark.read.jdbc(DB_URL, "activites", properties=DB_PROPS)
df_spt = spark.read.jdbc(DB_URL, "sports_declares", properties=DB_PROPS)

print(f"  Salariés    : {df_sal.count()} lignes")
print(f"  Activités   : {df_act.count()} lignes")
print(f"  Sports      : {df_spt.count()} lignes")

# ── Écriture Bronze Layer ────
print("  💾 Écriture Bronze Layer → Delta Lake...")

df_sal.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/salaries")
df_act.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/activites")
df_spt.write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/bronze/sports_declares")

print("  ✅ Bronze Layer écrit !")
print(f"     → {DELTA_PATH}/bronze/salaries")
print(f"     → {DELTA_PATH}/bronze/activites")
print(f"     → {DELTA_PATH}/bronze/sports_declares")

spark.stop()
print("\n🎉 Bronze Layer terminé !")
