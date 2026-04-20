import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

RH_FILE = os.getenv("RH_FILE")
SPORT_FILE = os.getenv("SPORT_FILE")
STRAVA_FILE = os.getenv("STRAVA_FILE")

# ───────────────────────────────────────────────
# 2. Connexion PostgreSQL
# ───────────────────────────────────────────────
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ───────────────────────────────────────────────
# 3. RESET propre des tables
# ───────────────────────────────────────────────
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE activites RESTART IDENTITY CASCADE"))
    conn.execute(text("TRUNCATE TABLE sports_declares RESTART IDENTITY CASCADE"))
    conn.execute(text("TRUNCATE TABLE salaries RESTART IDENTITY CASCADE"))

print("🧹 Tables vidées proprement (TRUNCATE + RESTART IDENTITY).")

# ───────────────────────────────────────────────
# 4. Chargement des données RH
# ───────────────────────────────────────────────
df_rh = pd.read_excel(RH_FILE)

df_rh = df_rh.rename(columns={
    "ID salarié":           "id_salarie",
    "Nom":                  "nom",
    "Prénom":               "prenom",
    "Date de naissance":    "date_naissance",
    "BU":                   "bu",
    "Date d'embauche":      "date_embauche",
    "Salaire brut":         "salaire_brut",
    "Type de contrat":      "type_contrat",
    "Nombre de jours de CP":"nb_jours_cp",
    "Adresse du domicile":  "adresse",
    "Moyen de déplacement": "moyen_deplacement",
})

df_rh.to_sql("salaries", engine, if_exists="append", index=False)
print(f"✅ {len(df_rh)} salariés chargés dans 'salaries'.")

# ───────────────────────────────────────────────
# 5. Chargement des sports déclarés
# ───────────────────────────────────────────────
df_sport = pd.read_excel(SPORT_FILE)

df_sport = df_sport.rename(columns={
    "ID salarié":        "id_salarie",
    "Pratique d'un sport": "sport",
})

df_sport.to_sql("sports_declares", engine, if_exists="append", index=False)
print(f"✅ {len[df_sport]} lignes chargées dans 'sports_declares'.")

# ───────────────────────────────────────────────
# 6. Chargement des activités Strava simulées
# ───────────────────────────────────────────────
df_strava = pd.read_csv(STRAVA_FILE)

df_strava = df_strava.rename(columns={
    "ID":               "id",
    "ID salarié":       "id_salarie",
    "Date de début":    "date_debut",
    "Type":             "type_sport",
    "Distance (m)":     "distance_m",
    "Temps écoulé (s)": "temps_ecoule_s",
    "Date de fin":      "date_fin",
    "Commentaire":      "commentaire",
})

df_strava["date_debut"] = pd.to_datetime(df_strava["date_debut"], format="%d/%m/%Y %H:%M")
df_strava["date_fin"]   = pd.to_datetime(df_strava["date_fin"],   format="%d/%m/%Y %H:%M")
df_strava["distance_m"] = pd.to_numeric(df_strava["distance_m"], errors="coerce")

df_strava = df_strava.drop(columns=["id"])

df_strava.to_sql("activites", engine, if_exists="append", index=False)
print(f"✅ {len(df_strava)} activités chargées dans 'activites'.")

# ───────────────────────────────────────────────
# 7. Fin
# ───────────────────────────────────────────────
print("\n🎉 Toutes les données sources ont été rechargées proprement dans PostgreSQL !")
