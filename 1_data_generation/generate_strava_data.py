import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# 1. Charger les variables d'environnement
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

RH_FILE = os.getenv("RH_FILE")
SPORT_FILE = os.getenv("SPORT_FILE")

# 2. Connexion PostgreSQL
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# 3. Chargement des fichiers Excel
try:
    df_rh = pd.read_excel(RH_FILE)
    df_sport = pd.read_excel(SPORT_FILE)
    print("📂 Fichiers Excel chargés avec succès.")
except Exception as e:
    print(f"❌ Erreur de chargement Excel : {e}")
    exit()

# --- PROTECTION ANTI-DOUBLONS ---
df_rh = df_rh.drop_duplicates(subset=["ID salarié"])
df = df_rh.merge(df_sport, on="ID salarié", how="left")
df = df.drop_duplicates(subset=["ID salarié"])

# 4. Paramétrage des sports
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

# 5. Génération des activités
records = []
fin = datetime.now()
debut = fin - timedelta(days=365)

for _, row in df.iterrows():
    salarie_id = row["ID salarié"]
    sport = str(row.get("Pratique d'un sport", ""))
    a_sport = sport not in ["nan", "None", ""]
    mode = str(row.get("Moyen de déplacement", ""))
    actif = mode in ("Marche/running", "Vélo/Trottinette/Autres")

    n = random.randint(18, 60) if a_sport else (random.randint(5, 20) if actif else random.randint(0, 5))
    if n == 0:
        continue

    jours_dispo = (fin - debut).days
    jours_choisis = sorted(random.sample(range(jours_dispo), min(n, jours_dispo)))

    for j in jours_choisis:
        date_act = debut + timedelta(days=j)
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

# 6. Préparation des DataFrames
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

# 7. Insertion SQL
try:
    print("⏳ Synchronisation avec PostgreSQL...")

    df_salaries_db.to_sql("salaries", engine, if_exists="replace", index=False)
    df_out.to_sql("activites", engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE salaries ADD PRIMARY KEY (id_salarie);"))

    print(f"🚀 Terminé : {len(df_salaries_db)} salariés et {len(df_out)} activités insérés.")

except Exception as e:
    print(f"❌ Erreur lors de l'insertion : {e}")
