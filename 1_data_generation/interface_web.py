import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER','postgres')}:"
    f"{os.getenv('POSTGRES_PASSWORD','admin123')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5432')}/"
    f"{os.getenv('POSTGRES_DB','sport_data_solution')}"
)
engine = create_engine(DB_URL)

st.set_page_config(page_title="Portail Sport", page_icon="🏅")
st.title("🏅 Portail Employe — Declaration Sport")
st.markdown("*Chaque activite declaree est envoyee automatiquement sur Slack via Debezium -> Redpanda*")
st.divider()

# ── LIMITES METIER ───────────────────────────────
LIMITES = {
    "Course à pied":    {"dist_max": 100000,  "vitesse_min": 2.0, "vitesse_max": 7.0,  "duree_max": 36000},
    "Randonnée":        {"dist_max": 60000,   "vitesse_min": 0.5, "vitesse_max": 2.5,  "duree_max": 50400},
    "Natation":         {"dist_max": 20000,   "vitesse_min": 0.5, "vitesse_max": 2.5,  "duree_max": 28800},
    "Triathlon":        {"dist_max": 200000,  "vitesse_min": 1.5, "vitesse_max": 6.0,  "duree_max": 72000},
    "Voile":            {"dist_max": 500000,  "vitesse_min": 0.5, "vitesse_max": 10.0, "duree_max": 86400},
    "Tennis":           {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 21600},
    "Football":         {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 14400},
    "Rugby":            {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 14400},
    "Badminton":        {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 18000},
    "Judo":             {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 14400},
    "Boxe":             {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 10800},
    "Escalade":         {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 28800},
    "Basketball":       {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 14400},
    "Équitation":       {"dist_max": 80000,   "vitesse_min": 0.5, "vitesse_max": 5.0,  "duree_max": 28800},
    "Tennis de table":  {"dist_max": None,    "vitesse_min": None,"vitesse_max": None,  "duree_max": 14400},
}

SPORTS_AVEC_DISTANCE = ["Course à pied","Randonnée","Natation","Triathlon","Voile","Équitation"]

def valider(sport, distance, duree, avec_distance):
    erreurs = []
    limites = LIMITES.get(sport, {})

    # Duree
    duree_max = limites.get("duree_max", 86400)
    if duree <= 0:
        erreurs.append("La duree doit etre superieure a 0 seconde.")
    if duree > duree_max:
        erreurs.append(f"Duree trop longue pour {sport} : max {round(duree_max/3600,1)}h (vous avez saisi {round(duree/3600,1)}h).")
    if duree < 300:
        erreurs.append("Duree trop courte : minimum 5 minutes.")

    if avec_distance and distance is not None and distance > 0:
        dist_max = limites.get("dist_max")
        vitesse_min = limites.get("vitesse_min")
        vitesse_max = limites.get("vitesse_max")

        # Distance max
        if dist_max and distance > dist_max:
            erreurs.append(f"Distance impossible pour {sport} : max {dist_max/1000:.0f} km (vous avez saisi {distance/1000:.1f} km).")

        # Coherence vitesse
        if duree > 0 and vitesse_min and vitesse_max:
            vitesse = (distance / 1000) / (duree / 3600)
            if vitesse > vitesse_max:
                erreurs.append(f"Vitesse impossible : {vitesse:.1f} km/h pour {sport} (max realiste : {vitesse_max} km/h). Verifiez distance/duree.")
            if vitesse < vitesse_min:
                erreurs.append(f"Vitesse trop lente : {vitesse:.2f} km/h pour {sport} (min : {vitesse_min} km/h). Verifiez distance/duree.")

    return erreurs

# ── CHARGEMENT SALARIES ──────────────────────────
@st.cache_data(ttl=60)
def load_salaries():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id_salarie, prenom, nom FROM salaries ORDER BY nom"))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

try:
    df_sal = load_salaries()
    options = {
        f"{r['prenom']} {r['nom']} (ID {r['id_salarie']})": r["id_salarie"]
        for _, r in df_sal.iterrows()
    }
except Exception as e:
    st.error(f"Connexion PostgreSQL impossible : {e}")
    st.stop()

# ── FORMULAIRE ───────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    mode = st.radio("Mode de selection", ["Liste", "ID manuel"], horizontal=True)
    if mode == "Liste":
        selected = st.selectbox("Salarie", list(options.keys()))
        id_sal = int(options[selected])
        st.info(f"Selectionne : {selected}")
    else:
        id_sal = st.number_input("ID salarie", min_value=1, step=1, value=43015)

    sport = st.selectbox("Type de sport", list(LIMITES.keys()))

with col2:
    avec_distance = sport in SPORTS_AVEC_DISTANCE
    if avec_distance:
        st.info(f"Ce sport necessite une distance.")
        distance = st.number_input("Distance (metres)", min_value=0.0, value=5000.0, step=100.0)
    else:
        st.info(f"Ce sport n'a pas de distance.")
        distance = None

    duree = st.number_input("Duree (secondes)", min_value=1, value=1800, step=60)

    duree_h = int(duree // 3600)
    duree_m = int((duree % 3600) // 60)
    duree_s = int(duree % 60)
    st.caption(f"Soit : {duree_h}h {duree_m}min {duree_s}s")

    commentaire = st.text_area("Commentaire (optionnel)", placeholder="Super seance !")

# Preview vitesse si applicable
if avec_distance and distance and distance > 0 and duree > 0:
    vitesse = (distance / 1000) / (duree / 3600)
    st.caption(f"Vitesse calculee : {vitesse:.1f} km/h")

st.divider()

# ── VALIDATION + SOUMISSION ──────────────────────
if st.button("Enregistrer l'activite", type="primary", use_container_width=True):

    erreurs = valider(sport, distance, duree, avec_distance)

    if erreurs:
        st.error("Activite refusee — donnees invalides :")
        for e in erreurs:
            st.warning(f"• {e}")
    else:
        try:
            date_debut = datetime.now()
            date_fin   = date_debut + timedelta(seconds=int(duree))

            df_insert = pd.DataFrame([{
                "id_salarie":     int(id_sal),
                "date_debut":     date_debut.strftime("%Y-%m-%d %H:%M:%S"),
                "type_sport":     sport,
                "distance_m":     float(distance) if avec_distance and distance else None,
                "temps_ecoule_s": int(duree),
                "date_fin":       date_fin.strftime("%Y-%m-%d %H:%M:%S"),
                "commentaire":    commentaire.strip() if commentaire.strip() else None,
            }])

            with engine.begin() as conn:
                df_insert.to_sql("activites", con=conn, if_exists="append", index=False)

            st.success("Activite enregistree avec succes !")
            st.info("Debezium detecte le changement -> Redpanda -> Slack !")
            

            if avec_distance and distance:
                vitesse = (distance / 1000) / (duree / 3600)
                st.markdown(f"""
                **Recapitulatif valide :**
                - Salarie ID : `{id_sal}`
                - Sport : `{sport}`
                - Distance : `{distance/1000:.1f} km`
                - Duree : `{duree_h}h {duree_m}min`
                - Vitesse : `{vitesse:.1f} km/h`
                - Date : `{date_debut.strftime('%d/%m/%Y %H:%M')}`
                """)
            else:
                st.markdown(f"""
                **Recapitulatif valide :**
                - Salarie ID : `{id_sal}`
                - Sport : `{sport}`
                - Duree : `{duree_h}h {duree_m}min`
                - Date : `{date_debut.strftime('%d/%m/%Y %H:%M')}`
                """)

        except Exception as e:
            st.error(f"Erreur lors de l'enregistrement : {e}")

# ── DERNIERES ACTIVITES ──────────────────────────
st.divider()
st.subheader("10 dernieres activites declarees")

try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                s.prenom || ' ' || s.nom AS salarie,
                a.type_sport,
                ROUND(CAST(a.distance_m AS NUMERIC)/1000, 1) AS km,
                ROUND(CAST(a.temps_ecoule_s AS NUMERIC)/60)  AS minutes,
                a.commentaire,
                a.date_debut::text AS date
            FROM activites a
            JOIN salaries s ON a.id_salarie = s.id_salarie
            ORDER BY a.date_debut DESC
            LIMIT 10
        """))
        df_last = pd.DataFrame(result.fetchall(), columns=result.keys())
    st.dataframe(df_last, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Impossible de charger les activites : {e}")