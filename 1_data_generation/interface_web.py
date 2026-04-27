import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

# ───────────────────────────────────────────────
# 1. CONFIG
# ───────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sport_data_solution")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

st.set_page_config(page_title="Portail Sport", page_icon="👟")
st.title("👟 Portail Employé : Déclaration Sport")

# ───────────────────────────────────────────────
# 2. CHARGEMENT DES SALARIÉS
# ───────────────────────────────────────────────
try:
    df_salaries = pd.read_sql("SELECT id_salarie, nom FROM salaries", engine)

    options = {
        f"{row['nom']} (ID {row['id_salarie']})": row["id_salarie"]
        for _, row in df_salaries.iterrows()
    }

except Exception as e:
    st.warning("⚠️ Impossible de charger les salariés")
    options = {}

# ───────────────────────────────────────────────
# 3. CHOIX DU SALARIÉ
# ───────────────────────────────────────────────
mode = st.radio("Choix du salarié", ["Sélectionner", "Saisir un ID"])

if mode == "Sélectionner" and options:
    selected = st.selectbox("Choisir un salarié", list(options.keys()))
    id_sal = options[selected]
    st.info(f"👤 Salarié sélectionné : {selected}")
else:
    id_sal = st.number_input("Votre ID salarié", step=1, min_value=1)

# ───────────────────────────────────────────────
# 4. FORMULAIRE
# ───────────────────────────────────────────────
with st.form("declaration_form"):
    sport = st.selectbox(
        "Type de sport",
        ["Course", "Cyclisme", "Natation", "Marche"]
    )

    distance = st.number_input(
        "Distance (mètres)",
        min_value=0.0,
        value=0.0
    )

    duree = st.number_input(
        "Durée (secondes)",
        min_value=1,
        value=3600
    )

    comm = st.text_area("Commentaire")

    submit = st.form_submit_button("Enregistrer l'activité")

# ───────────────────────────────────────────────
# 5. TRAITEMENT
# ───────────────────────────────────────────────
if submit:
    try:
        if id_sal <= 0:
            st.error("❌ ID salarié invalide.")
            st.stop()

        # Dates
        date_debut = datetime.now(timezone.utc)
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

        # Insertion DB
        df.to_sql(
            "activites",
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        st.success("✅ Activité enregistrée avec succès !")

    except Exception as e:
        st.error("❌ Erreur lors de l'enregistrement")
        st.code(str(e))