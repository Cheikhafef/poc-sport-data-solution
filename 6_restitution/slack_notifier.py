import pandas as pd
import requests
import random
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)

# ───────────────────────────────────────────────
# 2. Emojis par sport
# ───────────────────────────────────────────────
EMOJIS = {
    "Runing":          "🏃‍♂️🔥",
    "Randonnée":       "🥾🌄",
    "Natation":        "🏊‍♂️💦",
    "Tennis":          "🎾",
    "Football":        "⚽",
    "Rugby":           "🏉",
    "Badminton":       "🏸",
    "Judo":            "🥋",
    "Boxe":            "🥊",
    "Escalade":        "🧗",
    "Triathlon":       "🏅",
    "Équitation":      "🐴",
    "Tennis de table": "🏓",
    "Basketball":      "🏀",
    "Voile":           "⛵",
}

# ───────────────────────────────────────────────
# 3. Templates de messages
# ───────────────────────────────────────────────
TEMPLATES = {
    "Runing": [
        "Bravo {prenom} {nom} ! Tu viens de courir {distance} km en {duree} min ! Quelle énergie ! 🏃‍♂️🔥",
        "Superbe sortie {prenom} {nom} ! {distance} km avalés en {duree} min ! Respect ! 💪",
    ],
    "Randonnée": [
        "Magnifique {prenom} {nom} ! Une randonnée de {distance} km terminée ! 🥾🌄{commentaire}",
        "Bravo {prenom} {nom} ! {distance} km de randonnée en {duree} min ! Nature lover ! 🌿",
    ],
    "Natation": [
        "Splash ! {prenom} {nom} vient de nager {distance} m en {duree} min ! 🏊‍♂️💦",
        "Bravo {prenom} {nom} ! {distance} m dans l'eau en {duree} min ! Champion(ne) ! 🏊",
    ],
    "Tennis": [
        "Ace ! {prenom} {nom} vient de jouer {duree} min de tennis ! 🎾",
        "Bravo {prenom} {nom} ! Belle séance de tennis de {duree} min ! 🎾",
    ],
    "default": [
        "Bravo {prenom} {nom} ! Belle séance de {sport} de {duree} min ! {emoji}",
        "Superbe {prenom} {nom} ! {duree} min de {sport} au compteur ! {emoji}",
    ],
}

# ───────────────────────────────────────────────
# 4. Formatage du message Slack
# ───────────────────────────────────────────────
def format_message(row, prenom, nom):
    sport = row["type_sport"]
    emoji = EMOJIS.get(sport, "🏅")
    duree = round(row["temps_ecoule_s"] / 60)

    if pd.notna(row["distance_m"]) and row["distance_m"] != "":
        distance = round(float(row["distance_m"]) / 1000, 1)
    else:
        distance = None

    commentaire = ""
    if pd.notna(row["commentaire"]) and str(row["commentaire"]).strip() != "":
        commentaire = f'\n> _{row["commentaire"]}_'

    templates = TEMPLATES.get(sport, TEMPLATES["default"])
    template  = random.choice(templates)

    return template.format(
        prenom=prenom,
        nom=nom,
        sport=sport,
        distance=distance if distance else "?",
        duree=duree,
        emoji=emoji,
        commentaire=commentaire,
    )

# ───────────────────────────────────────────────
# 5. Envoi Slack
# ───────────────────────────────────────────────
def send_slack(message):
    if not WEBHOOK_URL:
        print("⚠️ Aucun webhook Slack configuré dans .env")
        return False
    response = requests.post(WEBHOOK_URL, json={"text": message})
    return response.status_code == 200

# ───────────────────────────────────────────────
# 6. Chargement des données
# ───────────────────────────────────────────────
print("📥 Chargement des activités...")
df_sal = pd.read_sql("SELECT id_salarie, nom, prenom FROM salaries", engine)
df_act = pd.read_sql("""
    SELECT * FROM activites
    ORDER BY date_debut DESC
    LIMIT 10
""", engine)

df = df_act.merge(df_sal, on="id_salarie", how="left")

# ───────────────────────────────────────────────
# 7. Envoi des messages Slack
# ───────────────────────────────────────────────
print(f"\n📲 Envoi de {len(df)} messages Slack...\n")

succes = 0
for _, row in df.iterrows():
    message = format_message(row, row["prenom"], row["nom"])
    ok      = send_slack(message)

    if ok:
        succes += 1
        print(f"  ✅ Envoyé : {message[:70]}...")
    else:
        print(f"  ❌ Échec pour {row['prenom']} {row['nom']}")

print(f"\n🎉 {succes}/{len(df)} messages envoyés sur Slack !")
