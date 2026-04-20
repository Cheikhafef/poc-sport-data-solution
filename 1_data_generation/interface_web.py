import streamlit as st
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

st.title("👟 Portail Employé : Déclaration Sport")

with st.form("declaration_form"):
    id_sal = st.number_input("Votre ID salarié", step=1)
    sport = st.selectbox("Type de sport", ["Course", "Cyclisme", "Natation", "Marche"])
    distance = st.number_input("Distance (mètres)", min_value=0)
    duree = st.number_input("Durée (secondes)", value=3600)
    comm = st.text_area("Commentaire")
    
    submit = st.form_submit_button("Enregistrer l'activité")

if submit:
    date_debut = datetime.now()
    date_fin = date_debut + timedelta(seconds=duree)
    
    df = pd.DataFrame([{
        "id_salarie": id_sal,
        "date_debut": date_debut,
        "type_sport": sport,
        "distance_m": distance,
        "temps_ecoule_s": duree,
        "date_fin": date_fin,
        "commentaire": comm
    }])
    
    df.to_sql("activites", engine, if_exists="append", index=False)
    st.success("✅ Activité envoyée ! Debezium va maintenant traiter l'information.")
