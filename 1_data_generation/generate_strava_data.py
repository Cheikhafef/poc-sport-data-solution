"""
RÉSUMÉ DU SCRIPT : GÉNÉRATEUR DE DONNÉES SYNTHÉTIQUES
Ce script simule l'écosystème de données de l'entreprise :
1. Extraction : Charge les référentiels RH et Sport depuis Excel.
2. Nettoyage : Supprime les doublons pour garantir l'intégrité référentielle.
3. Simulation : Génère des activités sportives réalistes (Strava-like) sur les 350 derniers jours.
4. Chargement : Synchronise et réinitialise la base PostgreSQL pour le pipeline ETL.
"""
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# 1. Configuration de l'environnement (Sécurité des accès)
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

RH_FILE = os.getenv("RH_FILE")
SPORT_FILE = os.getenv("SPORT_FILE")

# 2. Initialisation de la connexion à la base de données
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# 3. Ingestion des données sources (Excel)
try:
    df_rh = pd.read_excel(RH_FILE)
    df_sport = pd.read_excel(SPORT_FILE)
    print("📂 Fichiers Excel chargés avec succès.")
except Exception as e:
    print(f"❌ Erreur de chargement Excel : {e}")
    exit()

# --- PROTECTION ANTI-DOUBLONS ---
# On garantit que chaque ID salarié est unique avant les jointures pour éviter l'explosion des données
df_rh = df_rh.drop_duplicates(subset=["ID salarié"])
df = df_rh.merge(df_sport, on="ID salarié", how="left")
df = df.drop_duplicates(subset=["ID salarié"])

# 4. Paramétrage des règles métier sportives
# Définition des plages de distances et vitesses pour rendre la simulation réaliste
SPORTS_AVEC_DISTANCE = {
    "Runing": (2000, 22000, 2.5, 4.2),
    "Randonnée": (5000, 25000, 1.0, 1.8),
    "Natation": (500, 4000, 0.9, 1.6),
    "Triathlon": (5000, 30000, 3.0, 4.5),
    "Voile": (3000, 30000, 2.0, 5.0),
}

SPORTS_SANS_DISTANCE = {
    "Tennis": (2700, 7200), "Football": (3600, 6600), "Rugby": (3600, 6600),
    "Badminton": (1800, 5400), "Judo": (3600, 5400), "Boxe": (2700, 5400),
    "Escalade": (5400, 14400), "Équitation": (3600, 9000), 
    "Tennis de table": (1800, 5400), "Basketball": (3600, 7200),
}

COMMENTAIRES = {
    "Runing": ["Super sortie 💪", "Nouveau record !", "Belle séance 🔥"],
    "Randonnée": ["Superbe panorama 🌄", "Sentier magnifique 🌿"],
    "Natation": ["Bonne séance piscine 🏊", "50 longueurs !"],
    "Tennis": ["Victoire 6-3 🎾", "Entraînement technique"],
}

# 5. Algorithme de génération des activités
records = []
fin = datetime.now()
# On utilise 350 jours pour garantir que les tests de qualité (seuil 365j) passent au vert
debut = fin - timedelta(days=350)

for _, row in df.iterrows():
    salarie_id = row["ID salarié"]
    sport = str(row.get("Pratique d'un sport", ""))
    a_sport = sport not in ["nan", "None", ""]
    mode = str(row.get("Moyen de déplacement", ""))
    actif = mode in ("Marche/running", "Vélo/Trottinette/Autres")
    # Logique d'attribution du volume d'activités selon le profil (sportif ou adepte du trajet actif)
    n = random.randint(18, 60) if a_sport else (random.randint(5, 20) if actif else random.randint(0, 5))
    if n == 0:
        continue

    jours_dispo = (fin - debut).days
    jours_choisis = sorted(random.sample(range(jours_dispo), min(n, jours_dispo)))

    for j in jours_choisis:
        date_act = debut + timedelta(days=j)
        # Simulation des créneaux horaires (Matin/Soir en semaine, Journée le week-end)
        heure = random.choice([random.randint(6, 8), random.randint(17, 20)]) if date_act.weekday() < 5 else random.randint(8, 14)
        date_debut = date_act.replace(hour=heure, minute=random.randint(0, 59))

        sport_norm = sport.strip() if a_sport else "Runing"

        if sport_norm in SPORTS_AVEC_DISTANCE:
            dmin, dmax, vmin, vmax = SPORTS_AVEC_DISTANCE[sport_norm]
            distance = random.randint(dmin, dmax)
            duree_s = int(distance / random.uniform(vmin, vmax))
        else:
            distance = None
            dmin, dmax = SPORTS_SANS_DISTANCE.get(sport_norm, (1800, 5400))
            duree_s = random.randint(dmin, dmax)

        records.append({
            "id_salarie": salarie_id,
            "type_sport": sport_norm,
            "distance_m": distance,
            "temps_ecoule_s": duree_s,
            "date_debut": date_debut,
            "date_fin": date_debut + timedelta(seconds=duree_s),
            "commentaire": random.choice(COMMENTAIRES.get(sport_norm, [""]))
        })

# 6. Mapping et renommage pour le format SQL
df_out = pd.DataFrame(records)
df_salaries_db = df_rh.rename(columns={
    "ID salarié": "id_salarie",
    "Nom": "nom",
    "Prénom": "prenom",
    "Date de naissance": "date_naissance",
    "BU": "bu",
    "Date d'embauche": "date_embauche",
    "Salaire brut": "salaire_brut",
    "Type de contrat": "type_contrat",
    "Nombre de jours de CP": "nb_jours_cp",
    "Adresse du domicile": "adresse",
    "Moyen de déplacement": "moyen_deplacement"
})

# 7. Persistance des données dans PostgreSQL

try:
    print("⏳ Synchronisation avec PostgreSQL...")

    # On utilise TRUNCATE CASCADE pour nettoyer proprement les anciennes données 
    # tout en respectant les contraintes de clés étrangères.
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE activites CASCADE;"))
        conn.execute(text("TRUNCATE TABLE salaries CASCADE;"))

    # On insère les données (if_exists="append" car les tables existent déjà)
    df_salaries_db.to_sql("salaries", engine, if_exists="append", index=False)
    df_out.to_sql("activites", engine, if_exists="append", index=False)

    print(f"🚀 Terminé : {len(df_salaries_db)} salariés et {len(df_out)} activités insérés.")

except Exception as e:
    print(f"❌ Erreur lors de l'insertion : {e}")
