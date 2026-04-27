import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sport_data_solution")

# Connexion PostgreSQL (SQLAlchemy)
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False,
    future=True
)

st.title("👟 Portail Employé : Déclaration Sport")

# --- FORMULAIRE ---
with st.form("declaration_form"):
    id_sal = st.number_input("Votre ID salarié", step=1, min_value=1)
    sport = st.selectbox("Type de sport", ["Course", "Cyclisme", "Natation", "Marche"])
    distance = st.number_input("Distance (mètres)", min_value=0.0, value=0.0)
    duree = st.number_input("Durée (secondes)", min_value=1, value=3600)
    comm = st.text_area("Commentaire")
    submit = st.form_submit_button("Enregistrer l'activité")

# --- TRAITEMENT ---
if submit:
    try:
        # Validation
        if id_sal <= 0:
            st.error("ID salarié invalide.")
            st.stop()

        # Dates
        date_debut = datetime.utcnow()
        date_fin = date_debut + timedelta(seconds=int(duree))

        # DataFrame
        df = pd.DataFrame([{
            "id_salarie": int(id_sal),
            "date_debut": date_debut,
            "type_sport": sport,
            "distance_m": float(distance),
            "temps_ecoule_s": int(duree),
            "date_fin": date_fin,
            "commentaire": comm
        }])

        # Insertion en base (CORRECT)
        df.to_sql(
            "activites",
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        st.success("✅ Activité envoyée ! Debezium va maintenant traiter l'information.")

    except Exception as e:
        st.error("❌ Erreur lors de l'enregistrement")
        st.code(str(e))