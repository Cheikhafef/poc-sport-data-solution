import os
import math
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, BooleanType, DoubleType
)
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

os.environ["PYSPARK_PYTHON"]        = os.getenv("PYSPARK_PYTHON", "python3")
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv("PYSPARK_DRIVER_PYTHON", "python3")

DELTA_PATH = os.getenv("DELTA_PATH", "./delta_lake")

ADRESSE_BUREAU = os.getenv("ADRESSE_BUREAU")

DISTANCE_MAX = {
    "Marche/running":          int(os.getenv("DISTANCE_MARCHE", 15000)),
    "Vélo/Trottinette/Autres": int(os.getenv("DISTANCE_VELO", 25000)),
}

REF_ENTREPRISE_PATH = os.getenv("REF_ENTREPRISE_PATH", "./data/ref_entreprise")
REF_SPORTIF_PATH    = os.getenv("REF_SPORTIF_PATH", "./data/ref_sportif")

print("🥈 SILVER LAYER — Nettoyage + Validation + Jointure référentiels...")

# ───────────────────────────────────────────────
# 2. Initialisation Spark
# ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Silver_Layer") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.2.1"
    ) \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



# ══════════════════════════════════════════════════
# 1. LECTURE BRONZE LAYER
# ══════════════════════════════════════════════════
print("\n  📥 Lecture Bronze Layer...")
try:
    df_sal_bronze = spark.read.format("delta") \
                        .load(f"{DELTA_PATH}/bronze/salaries")
    df_act_bronze = spark.read.format("delta") \
                        .load(f"{DELTA_PATH}/bronze/activites")
except Exception as e:
    print(f"  ❌ Erreur lecture Bronze : {e}")
    spark.stop()
    exit(1)

print(f"  Salaries  Bronze : {df_sal_bronze.count()} lignes")
print(f"  Activites Bronze : {df_act_bronze.count()} lignes")

# ══════════════════════════════════════════════════
# 2. LECTURE RÉFÉRENTIELS DEPUIS LES BUCKETS
#    Dans l'archi encadreur : deux buckets distincts
#    → référentiel entreprise (BU, types de contrat…)
#    → référentiel sportif   (sports valides, seuils…)
# ══════════════════════════════════════════════════
print("\n  📦 Lecture des référentiels (buckets)...")

# ── Référentiel entreprise ───────────────────────
# Contient : les BU valides, les modes de déplacement
# acceptés, les types de contrat reconnus
try:
    df_ref_entreprise = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{REF_ENTREPRISE_PATH}/bu_valides.csv")
    print(f"  Ref entreprise : {df_ref_entreprise.count()} BU chargées")
except Exception:
    # Référentiel minimal en dur si le fichier n'existe pas encore
    print("  ⚠️  Ref entreprise non trouvée → référentiel minimal utilisé")
    df_ref_entreprise = spark.createDataFrame(
        [("Marketing",), ("R&D",), ("Ventes",),
         ("Support",), ("Finance",)],
        ["bu"]
    )

# ── Référentiel sportif ──────────────────────────
# Contient : les types de sports valides,
# les seuils de durée minimale par sport,
# la correspondance sport → catégorie
try:
    df_ref_sportif = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{REF_SPORTIF_PATH}/sports_valides.csv")
    print(f"  Ref sportif    : {df_ref_sportif.count()} sports chargés")
except Exception:
    print("  ⚠️  Ref sportif non trouvé → référentiel minimal utilisé")
    df_ref_sportif = spark.createDataFrame([
        ("Course à pied",  1800, "cardio"),
        ("Randonnée",      3600, "cardio"),
        ("Natation",       1800, "cardio"),
        ("Tennis",         2700, "raquette"),
        ("Badminton",      1800, "raquette"),
        ("Football",       3600, "collectif"),
        ("Rugby",          3600, "collectif"),
        ("Basketball",     3600, "collectif"),
        ("Judo",           3600, "combat"),
        ("Boxe",           2700, "combat"),
        ("Escalade",       5400, "outdoor"),
        ("Triathlon",      5400, "cardio"),
        ("Voile",          3000, "outdoor"),
        ("Équitation",     3600, "outdoor"),
        ("Tennis de table",1800, "raquette"),
        ("Runing",         1800, "cardio"),   # compatibilité Bronze
    ], ["type_sport", "duree_min_s", "categorie"])

# ══════════════════════════════════════════════════
# 3. NETTOYAGE SALARIÉS
# ══════════════════════════════════════════════════
print("\n  🧹 Nettoyage salaries...")

df_sal_clean = df_sal_bronze \
    .filter(F.col("id_salarie").isNotNull()) \
    .filter(F.col("salaire_brut") > 0) \
    .filter(F.col("type_contrat").isin(["CDI", "CDD"])) \
    .dropDuplicates(["id_salarie"])

print(f"  Salaries : {df_sal_bronze.count()} → "
      f"{df_sal_clean.count()} après nettoyage")

# ══════════════════════════════════════════════════
# 4. NETTOYAGE ACTIVITÉS
# ══════════════════════════════════════════════════
print("\n  🧹 Nettoyage activites...")

df_act_clean = df_act_bronze \
    .filter(F.col("type_sport").isNotNull()) \
    .filter(F.col("temps_ecoule_s") > 0) \
    .filter(
        F.col("date_debut").isNotNull() &
        F.col("date_fin").isNotNull()
    ) \
    .filter(
        (F.col("distance_m").isNull()) |
        (F.col("distance_m") >= 0)
    ) \
    .withColumn(
        # Normalisation : "Runing" → "Course à pied"
        # Fait UNE SEULE FOIS ici, Silver est la source de vérité
        "type_sport",
        F.when(F.col("type_sport") == "Runing", "Course à pied")
         .otherwise(F.col("type_sport"))
    ) \
    .dropDuplicates(["id", "id_salarie", "date_debut"])

print(f"  Activites : {df_act_bronze.count()} → "
      f"{df_act_clean.count()} après nettoyage")

# ══════════════════════════════════════════════════
# 5. JOINTURE RÉFÉRENTIEL SPORTIF
#    Enrichit chaque activité avec sa catégorie
#    et valide que le sport est dans le référentiel
# ══════════════════════════════════════════════════
print("\n  🔗 Jointure référentiel sportif...")

df_act_enriched = df_act_clean \
    .join(
        df_ref_sportif.select("type_sport", "categorie", "duree_min_s"),
        on="type_sport",
        how="left"        # left : on garde les activités même si sport inconnu
    ) \
    .withColumn(
        # Activité valide = sport reconnu ET durée suffisante
        "activite_valide",
        F.col("categorie").isNotNull() &
        (F.col("temps_ecoule_s") >= F.col("duree_min_s"))
    )

nb_invalides = df_act_enriched \
    .filter(~F.col("activite_valide")).count()
print(f"  Activites valides apres ref sportif : "
      f"{df_act_enriched.filter(F.col('activite_valide')).count()}")
print(f"  Activites invalides (sport inconnu ou trop courte) : {nb_invalides}")

# ══════════════════════════════════════════════════
# 6. VALIDATION GÉOGRAPHIQUE (API Gouv)
#    Calcule la distance domicile → bureau
#    pour déterminer l'éligibilité à la prime
# ══════════════════════════════════════════════════
print("\n  📍 Validation distances domicile → bureau...")

def geocode_fr(adresse):
    try:
        r = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": adresse, "limit": 1},
            timeout=5
        )
        data = r.json()
        if data["features"]:
            coords = data["features"][0]["geometry"]["coordinates"]
            return float(coords[1]), float(coords[0])
        return None, None
    except Exception:
        return None, None

def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lon2 - lon1) * p / 2) ** 2)
    return round(2 * R * math.asin(math.sqrt(a)))

lat_bureau, lon_bureau = geocode_fr(ADRESSE_BUREAU)
if lat_bureau is None:
    print("  ⚠️  Bureau non géocodé → distances non calculées")

schema_dist = StructType([
    StructField("id_salarie",    IntegerType(), False),
    StructField("declaration_ok", BooleanType(), True),
    StructField("distance_m",    DoubleType(),   True),
])

df_sal_pd = df_sal_clean.toPandas()
resultats  = []

for _, row in df_sal_pd.iterrows():
    mode = row["moyen_deplacement"]

    # Modes non actifs : pas de prime, pas de contrôle bloquant
    if mode not in DISTANCE_MAX:
        resultats.append({
            "id_salarie":    int(row["id_salarie"]),
            "declaration_ok": None,   # ✅ null = pas concerné (pas True !)
            "distance_m":    None,
        })
        continue

    lat, lon = geocode_fr(str(row["adresse"]))
    time.sleep(0.1)

    if lat and lon and lat_bureau:
        dist = haversine(lat, lon, lat_bureau, lon_bureau)
        ok   = bool(dist <= DISTANCE_MAX[mode])
        resultats.append({
            "id_salarie":    int(row["id_salarie"]),
            "declaration_ok": ok,
            "distance_m":    float(dist),
        })
    else:
        # Adresse non trouvée → null, PAS True
        # ✅ En cas de doute on n'accorde pas la prime
        resultats.append({
            "id_salarie":    int(row["id_salarie"]),
            "declaration_ok": None,
            "distance_m":    None,
        })

df_distances = spark.createDataFrame(resultats, schema=schema_dist)

# ══════════════════════════════════════════════════
# 7. JOINTURE FINALE SALARIÉS
#    Salariés nettoyés
#    + distances calculées
#    + référentiel entreprise (validation BU)
# ══════════════════════════════════════════════════
print("\n  🔗 Jointure finale salaries + distances + ref entreprise...")

df_sal_silver = df_sal_clean \
    .join(df_distances, on="id_salarie", how="left") \
    .join(
        df_ref_entreprise.withColumnRenamed("bu", "bu_ref"),
        F.col("bu") == F.col("bu_ref"),
        how="left"
    ) \
    .withColumn(
        "bu_valide",
        F.col("bu_ref").isNotNull()
    ) \
    .drop("bu_ref")

# ══════════════════════════════════════════════════
# 8. ÉCRITURE SILVER LAYER
# ══════════════════════════════════════════════════
print("\n  💾 Ecriture Silver Layer → Delta Lake...")

df_sal_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{DELTA_PATH}/silver/salaries")

df_act_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{DELTA_PATH}/silver/activites")

# Référentiels aussi en Silver pour que Gold les lise facilement
df_ref_sportif.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{DELTA_PATH}/silver/ref_sportif")

df_ref_entreprise.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{DELTA_PATH}/silver/ref_entreprise")

print(f"  ✅ Silver salaries   → {DELTA_PATH}/silver/salaries")
print(f"  ✅ Silver activites  → {DELTA_PATH}/silver/activites")
print(f"  ✅ Silver ref_sportif    → {DELTA_PATH}/silver/ref_sportif")
print(f"  ✅ Silver ref_entreprise → {DELTA_PATH}/silver/ref_entreprise")

# ══════════════════════════════════════════════════
# 9. RAPPORT SILVER
# ══════════════════════════════════════════════════
print("\n  📊 Rapport Silver Layer...")

df_s = spark.read.format("delta").load(f"{DELTA_PATH}/silver/salaries")
df_a = spark.read.format("delta").load(f"{DELTA_PATH}/silver/activites")

nb_actifs = df_s.filter(
    F.col("moyen_deplacement").isin(list(DISTANCE_MAX.keys()))
).count()
nb_ok   = df_s.filter(F.col("declaration_ok") == True).count()
nb_null = df_s.filter(F.col("declaration_ok").isNull()).count()
nb_ko   = df_s.filter(F.col("declaration_ok") == False).count()

print(f"  Salaries Silver       : {df_s.count()}")
print(f"  Activites Silver      : {df_a.count()}")
print(f"  Deplacement actif     : {nb_actifs}")
print(f"  declaration_ok=True   : {nb_ok}")
print(f"  declaration_ok=False  : {nb_ko}  ← ineligibles prime")
print(f"  declaration_ok=null   : {nb_null} ← non concernes ou adresse inconnue")

spark.stop()
print("\n🎉 Silver Layer terminé !")
