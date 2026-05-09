import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:admin123@localhost:5432/sport_data_solution")

st.set_page_config(page_title="Simulation Prime", page_icon="💰")
st.title("💰 Simulation — Taux de Prime Sportive")
st.markdown("*Faites glisser le curseur pour simuler différents scénarios*")
st.divider()

taux = st.slider("Taux de la prime sportive (%)", min_value=1, max_value=20, value=5, step=1, format="%d%%")
taux_decimal = taux / 100

try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE a.prime_sportive IS TRUE)   AS nb_primes,
                SUM(s.salaire_brut * :taux)
                    FILTER (WHERE a.prime_sportive IS TRUE)        AS cout_total,
                AVG(s.salaire_brut * :taux)
                    FILTER (WHERE a.prime_sportive IS TRUE)        AS prime_moyenne,
                COUNT(*) FILTER (WHERE a.jours_bienetre > 0)       AS nb_bienetre
            FROM avantages_calcules a
            JOIN salaries s ON a.id_salarie = s.id_salarie
        """), {"taux": taux_decimal})
        row = result.fetchone()

    col1, col2, col3, col4 = st.columns(4)
    cout = float(row[1]) if row[1] else 0
    moy  = float(row[2]) if row[2] else 0
    cout_ref = cout * 5 / taux if taux != 5 else cout
    delta_str = f"{cout - cout_ref:+,.0f} EUR vs 5%" if taux != 5 else "Reference (5%)"

    col1.metric("Eligibles prime",     f"{row[0]}",        "salaries")
    col2.metric("Cout total",          f"{cout:,.0f} EUR", delta_str)
    col3.metric("Prime moyenne",       f"{moy:,.0f} EUR",  "par salarie eligible")
    col4.metric("Eligibles bien-etre", f"{row[3]}",        "salaries")

except Exception as e:
    st.error(f"Erreur : {e}")
    st.stop()

st.divider()
st.subheader("Comparaison des scenarios")

scenarios = []
for t in [3, 5, 8, 10, 15]:
    try:
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT SUM(s.salaire_brut * :taux)
                FILTER (WHERE a.prime_sportive IS TRUE)
                FROM avantages_calcules a
                JOIN salaries s ON a.id_salarie = s.id_salarie
            """), {"taux": t / 100}).fetchone()
        cout_val = float(r[0]) if r[0] else 0
        scenarios.append({
            "Taux": f"{t}%",
            "Cout total estime": f"{cout_val:,.0f} EUR",
            "Statut": "<-- Selectionne" if t == taux else ""
        })
    except Exception:
        scenarios.append({"Taux": f"{t}%", "Cout total estime": "N/A", "Statut": ""})

st.dataframe(pd.DataFrame(scenarios), use_container_width=True, hide_index=True)

st.divider()
st.subheader("Appliquer le taux en base")

if st.button(f"Appliquer {taux}% dans la base", type="primary", use_container_width=True):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE avantages_calcules
                SET montant_prime = s.salaire_brut * :taux
                FROM salaries s
                WHERE avantages_calcules.id_salarie = s.id_salarie
                  AND avantages_calcules.prime_sportive IS TRUE
            """), {"taux": taux_decimal})
        st.success(f"Base mise a jour avec {taux}% !")
        st.info("Actualise Power BI pour voir les changements !")
        st.balloons()
    except Exception as e:
        st.error(f"Erreur mise a jour : {e}")

if taux != 5:
    if st.button("Restaurer 5% (original)", use_container_width=True):
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE avantages_calcules
                    SET montant_prime = s.salaire_brut * 0.05
                    FROM salaries s
                    WHERE avantages_calcules.id_salarie = s.id_salarie
                      AND avantages_calcules.prime_sportive IS TRUE
                """))
            st.success("Taux restaure a 5% !")
            st.rerun()
        except Exception as e:
            st.error(f"Erreur restauration : {e}")
