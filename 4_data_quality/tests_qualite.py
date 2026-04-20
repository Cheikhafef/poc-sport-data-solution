import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

FRAICHEUR_JOURS = int(os.getenv("FRAICHEUR_JOURS", 366))

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ───────────────────────────────────────────────
# 2. Connexion PostgreSQL
# ───────────────────────────────────────────────
engine = create_engine(DB_URL)

# ───────────────────────────────────────────────
# 3. Chargement des données
# ───────────────────────────────────────────────
date_limite = (datetime.now() - timedelta(days=FRAICHEUR_JOURS)).strftime('%Y-%m-%d')

df_sal = pd.read_sql("SELECT * FROM salaries", engine)
df_act = pd.read_sql("SELECT * FROM activites", engine)
df_av  = pd.read_sql("SELECT * FROM avantages", engine)

resultats = []

def log(test, ok, detail="", critique=False):
    statut = "✅ PASS" if ok else "❌ FAIL"
    print(f"   {statut} — {test}" + (f" ({detail})" if detail else ""))
    resultats.append({"test": test, "statut": statut, "detail": detail})
    
    if not ok and critique:
        print(f"\n🚨 STOP ! Erreur critique : {test}. Pipeline interrompu.")
        sys.exit(1)

# =======================================================
#   TESTS QUALITÉ — DONNÉES SALARIÉS
# =======================================================
print("\n" + "="*55)
print("   TESTS QUALITÉ — DONNÉES SALARIÉS")
print("="*55)

doublons = df_sal["id_salarie"].duplicated().sum()
log("Pas de doublons ID salarié", doublons == 0, f"{doublons} doublon(s)", critique=True)

sal_negatifs = (df_sal["salaire_brut"] <= 0).sum()
log("Salaire brut > 0", sal_negatifs == 0, f"{sal_negatifs} salaire(s) invalide(s)")

nb_inval_contrat = (~df_sal["type_contrat"].isin(["CDI", "CDD"])).sum()
log("Type contrat valide (CDI/CDD)", nb_inval_contrat == 0, f"{nb_inval_contrat} contrat(s) invalide(s)")

dates_nulles = df_sal["date_embauche"].isna().sum()
log("Date d'embauche non nulle", dates_nulles == 0, f"{dates_nulles} date(s) manquante(s)")

modes_v = ["Marche/running", "Vélo/Trottinette/Autres", "Transports en commun", "véhicule thermique/électrique"]
nb_m_inval = (~df_sal["moyen_deplacement"].isin(modes_v)).sum()
log("Moyen de déplacement valide", nb_m_inval == 0, f"{nb_m_inval} mode(s) invalide(s)")

# =======================================================
#   TESTS QUALITÉ — DONNÉES ACTIVITÉS
# =======================================================
print("\n" + "="*55)
print("   TESTS QUALITÉ — DONNÉES ACTIVITÉS")
print("="*55)

nb_dist_neg = (df_act["distance_m"] < 0).sum()
log("Distance jamais négative", nb_dist_neg == 0, f"{nb_dist_neg} distance(s) négative(s)")

nb_t_nuls = (df_act["temps_ecoule_s"] <= 0).sum()
log("Temps écoulé > 0 secondes", nb_t_nuls == 0, f"{nb_t_nuls} durée(s) invalide(s)")

df_act["date_debut"] = pd.to_datetime(df_act["date_debut"])
df_act["date_fin"]   = pd.to_datetime(df_act["date_fin"])
nb_dates_inv = (df_act["date_debut"] >= df_act["date_fin"]).sum()
log("Date début < Date fin", nb_dates_inv == 0, f"{nb_dates_inv} activité(s) incohérente(s)", critique=True)

nb_s_nuls = df_act["type_sport"].isna().sum()
log("Type sport non nul", nb_s_nuls == 0, f"{nb_s_nuls} type(s) manquant(s)")

nb_hors_p = (df_act["date_debut"] < pd.to_datetime(date_limite)).sum()
log("Activités dans les 12 derniers mois", nb_hors_p == 0, f"{nb_hors_p} activité(s) hors période")

ids_orphelins = set(df_act["id_salarie"]) - set(df_sal["id_salarie"])
log("ID salarié existe dans RH", len(ids_orphelins) == 0, f"{len(ids_orphelins)} ID inconnu(s)", critique=True)

# =======================================================
#   TESTS QUALITÉ — DONNÉES AVANTAGES
# =======================================================
print("\n" + "="*55)
print("   TESTS QUALITÉ — DONNÉES AVANTAGES")
print("="*55)

log("Montant prime >= 0", (df_av["montant_prime"] < 0).sum() == 0)
log("Nb activités >= 0", (df_av["nb_activites"] < 0).sum() == 0)

df_merged = df_av.merge(df_sal[["id_salarie", "moyen_deplacement"]], on="id_salarie")
primes_err = df_merged[(df_merged["prime_sportive"] == True) & 
                       (~df_merged["moyen_deplacement"].isin(["Marche/running", "Vélo/Trottinette/Autres"]))]
log("Prime uniquement pour déplacement actif", len(primes_err) == 0)

jbe_err = df_av[(df_av["jours_bienetre"] == True) & (df_av["nb_activites"] < 15)]
log("Bien-être uniquement si >= 15 activités", len(jbe_err) == 0)

# =======================================================
#   RAPPORT FINAL
# =======================================================
print("\n" + "="*55)
df_res = pd.DataFrame(resultats)
print(f"   RÉSULTAT FINAL : {(df_res['statut'] == '✅ PASS').sum()} PASS  |  {(df_res['statut'] == '❌ FAIL').sum()} FAIL")
print("="*55)

os.makedirs("data", exist_ok=True)
df_res.to_csv("data/rapport_tests_qualite.csv", index=False, encoding="utf-8-sig")
print("\n✅ Rapport exporté → data/rapport_tests_qualite.csv")
