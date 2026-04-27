from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, DoubleType
import requests
import math
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

ADRESSE_BUREAU = os.getenv("ADRESSE_BUREAU")
DISTANCE_MAX = {
    "Marche/running": int(os.getenv("DISTANCE_MARCHE")),
    "Vélo/Trottinette/Autres": int(os.getenv("DISTANCE_VELO")),
}

DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

# ───────────────────────────────────────────────
# 2. Initialisation Spark + Delta Lake
# ───────────────────────────────────────────────
print("⚡ Démarrage Spark + Delta Lake...")

spark = (
    SparkSession.builder
    .appName("SportDataSolution_ETL")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.1"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ───────────────────────────────────────────────
# 3. Fonctions utilitaires
# ───────────────────────────────────────────────
def geocode_fr(adresse):
    try:
        r = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": adresse, "limit": 1},
            timeout=10
        )
        data = r.json()
        if data["features"]:
            coords = data["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0]
        return None, None
    except:
        return None, None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lon2 - lon1) * p / 2) ** 2)
    return round(2 * R * math.asin(math.sqrt(a)))

# ───────────────────────────────────────────────
# 4. EXTRACTION PostgreSQL
# ───────────────────────────────────────────────
print("📥 Extraction PostgreSQL...")

df_sal = spark.read.jdbc(DB_URL, "salaries", properties=DB_PROPS)
df_act = spark.read.jdbc(DB_URL, "activites", properties=DB_PROPS)

# ───────────────────────────────────────────────
# 5. BRONZE LAYER
# ───────────────────────────────────────────────
# ───────────────────────────────────────────────
# 5. BRONZE LAYER
# ───────────────────────────────────────────────
df_sal.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{DELTA_PATH}/salaries")
df_act.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{DELTA_PATH}/activites")
# ───────────────────────────────────────────────
# 6. TRANSFORMATION — Calcul des distances
# ───────────────────────────────────────────────
print("📍 Calcul des distances...")

lat_bureau, lon_bureau = geocode_fr(ADRESSE_BUREAU)

df_sal_pd = df_sal.toPandas()
resultats = []

for _, row in df_sal_pd.iterrows():
    mode = row["moyen_deplacement"]

    if mode in DISTANCE_MAX:
        lat, lon = geocode_fr(row["adresse"])
        time.sleep(0.2)

        if lat and lon:
            dist = haversine(lat, lon, lat_bureau, lon_bureau)
            resultats.append({
                "id_salarie": int(row["id_salarie"]),
                "declaration_ok": dist <= DISTANCE_MAX[mode],
                "distance_m": float(dist)
            })
            continue

    resultats.append({
        "id_salarie": int(row["id_salarie"]),
        "declaration_ok": True,
        "distance_m": None
    })

from pyspark.sql.types import StructType, StructField, LongType, BooleanType, DoubleType

schema_dist = StructType([
    StructField("id_salarie", LongType(), True),
    StructField("declaration_ok", BooleanType(), True),
    StructField("distance_m", DoubleType(), True)
])

df_distances = spark.createDataFrame(resultats, schema=schema_dist)
# ───────────────────────────────────────────────
# 7. TRANSFORMATION — Calcul des avantages
# ───────────────────────────────────────────────
print("💰 Calcul des avantages...")

# 1. On compte les activités
df_nb_act = (
    df_act.groupBy("id_salarie")
    .count()
    .withColumnRenamed("count", "nb_activites")
)

# 2. On crée le DataFrame final en gardant BIEN toutes les colonnes de df_sal
df_final = (
    df_sal  # Contient id_salarie, nom, prenom, salaire_brut, moyen_deplacement...
    .join(df_nb_act, "id_salarie", "left")
    .join(df_distances.select("id_salarie", "declaration_ok"), "id_salarie", "left")
    .fillna({"nb_activites": 0, "declaration_ok": True})
    .withColumn("prime_sportive",
                (F.col("declaration_ok") == True) &
                (F.col("moyen_deplacement").isin(["Marche/running", "Vélo/Trottinette/Autres"])))
    .withColumn("montant_prime",
                F.when(F.col("prime_sportive"), F.col("salaire_brut") * 0.05)
                 .otherwise(0.0)
                 .cast(DoubleType()))
    .withColumn("jours_bienetre", (F.col("nb_activites") >= 15).cast(BooleanType()))
    .withColumn("date_calcul", F.current_date())
    # ICI : On s'assure de sélectionner EXACTEMENT ce que PostgreSQL attend
    .select(
        "id_salarie",
        "prime_sportive",
        "montant_prime",
        "jours_bienetre",
        F.col("nb_activites").cast(IntegerType()),
        "date_calcul"
    )
)
# ───────────────────────────────────────────────
# 8. GOLD LAYER → Delta Lake
# ───────────────────────────────────────────────
print("💾 Écriture Gold Layer → Delta Lake...")
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{DELTA_PATH}/avantages")
# ───────────────────────────────────────────────
# 9. CHARGEMENT FINAL PostgreSQL
# ───────────────────────────────────────────────
print("📤 Chargement vers PostgreSQL...")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE avantages RESTART IDENTITY"))

df_final.repartition(1).write.jdbc(
    url=DB_URL,
    table="avantages",
    mode="append",
    properties=DB_PROPS
)

print("\n✅ Pipeline terminé avec succès !")
spark.stop()
