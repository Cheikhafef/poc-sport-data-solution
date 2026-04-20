import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# Connexion PostgreSQL
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def menu_saisie():
    print("\n" + "="*40)
    print("👟 INTERFACE EMPLOYÉ : DÉCLARATION SPORT")
    print("="*40)

    try:
        id_sal = int(input("👉 Votre ID salarié : "))
        sport  = input("👉 Type de sport : ")

        dist_input = input("👉 Distance en mètres (Entrée si non pertinent) : ").strip()
        distance   = float(dist_input) if dist_input else None

        duree_input = input("👉 Durée en secondes (Entrée = 3600) : ").strip()
        duree       = int(duree_input) if duree_input else 3600

        comm       = input("👉 Commentaire (optionnel) : ").strip()
        date_debut = datetime.now()
        date_fin   = date_debut + timedelta(seconds=duree)

        nouvelle_act = pd.DataFrame([{
            "id_salarie":     id_sal,
            "date_debut":     date_debut.strftime("%Y-%m-%d %H:%M:%S"),
            "type_sport":     sport,
            "distance_m":     distance,
            "temps_ecoule_s": duree,
            "date_fin":       date_fin.strftime("%Y-%m-%d %H:%M:%S"),
            "commentaire":    comm or None,
        }])

        nouvelle_act.to_sql("activites", engine, if_exists="append", index=False)
        print("\n✅ Activité enregistrée avec succès !")
        print("💡 Debezium détecte le changement → Redpanda → Slack...")

    except ValueError as e:
        print(f"\n❌ Erreur de saisie : {e}")

if __name__ == "__main__":
    menu_saisie()
