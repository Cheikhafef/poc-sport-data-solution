"""
Ce script assure la réinitialisation et le chargement des données brutes dans PostgreSQL. 
Il transforme les fichiers sources (Excel/CSV) 
en tables SQL structurées, constituant la première étape du pipeline (Ingestion).
 """
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv() # Charge les identifiants DB depuis le fichier .env

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

RH_FILE = os.getenv("RH_FILE")
SPORT_FILE = os.getenv("SPORT_FILE")
STRAVA_FILE = os.getenv("STRAVA_FILE")

# ───────────────────────────────────────────────
# 2. Initialisation de la connexion PostgreSQL
# ───────────────────────────────────────────────
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ───────────────────────────────────────────────
# 3. RESET des tables (Nettoyage avant chargement)
# ───────────────────────────────────────────────
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE activites RESTART IDENTITY CASCADE"))
    conn.execute(text("TRUNCATE TABLE sports_declares RESTART IDENTITY CASCADE"))
    conn.execute(text("TRUNCATE TABLE salaries RESTART IDENTITY CASCADE"))

print("🧹 Tables vidées proprement (TRUNCATE + RESTART IDENTITY).")

# ───────────────────────────────────────────────
# 4. Chargement des données RH (Référentiel Salariés)
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
#Insertion dans la table 'salaries'
df_rh.to_sql("salaries", engine, if_exists="append", index=False)
print(f"✅ {len(df_rh)} salariés chargés dans 'salaries'.")

# ───────────────────────────────────────────────
#5. Chargement des sports déclarés (Source déclarative)
# ───────────────────────────────────────────────
df_sport = pd.read_excel(SPORT_FILE)

df_sport = df_sport.rename(columns={
    "ID salarié":        "id_salarie",
    "Pratique d'un sport": "sport",
})

df_sport.to_sql("sports_declares", engine, if_exists="append", index=False)
print(f"✅ {len(df_sport)} lignes chargées dans 'sports_declares'.")

# ───────────────────────────────────────────────
# 6. Chargement des activités Strava (Données dynamiques)
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
# Conversion explicite des dates et typage numérique pour éviter les erreurs Spark plus tard
df_strava["date_debut"] = pd.to_datetime(df_strava["date_debut"], format="%d/%m/%Y %H:%M")
df_strava["date_fin"]   = pd.to_datetime(df_strava["date_fin"],   format="%d/%m/%Y %H:%M")
df_strava["distance_m"] = pd.to_numeric(df_strava["distance_m"], errors="coerce")
# On supprime la colonne ID source pour laisser PostgreSQL gérer sa propre auto-incrémentation
df_strava = df_strava.drop(columns=["id"])

df_strava.to_sql("activites", engine, if_exists="append", index=False)
print(f"✅ {len(df_strava)} activités chargées dans 'activites'.")

# ───────────────────────────────────────────────
# 7. Finalisation
# ───────────────────────────────────────────────
print("\n🎉 Toutes les données sources ont été rechargées proprement dans PostgreSQL !")
