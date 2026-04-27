"""
RÉSUMÉ : BOT D'ANIMATION TEMPS RÉEL (KAFKA -> SLACK)
Ce script agit comme un pont entre les données et la communication :
1. Écoute Kafka : Surveille le topic CDC (Debezium) pour détecter les nouvelles lignes dans 'activites'.
2. Enrichissement (Lookup) : Interroge la table 'salaries' pour convertir un ID (ex: 42) en nom complet (ex: Jean Dupont).
3. Décodage Binaire : Gère les formats de données complexes (Base64/Struct) provenant du flux Kafka.
4. Notification : Formate un message personnalisé avec emojis et citations selon le sport pratiqué.
"""

import os
import json
import base64
import struct
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ───────────────────────────────────────────────
# 1. Configuration & Initialisation
# ───────────────────────────────────────────────
load_dotenv()

# Connexion DB pour le "Lookup" (récupérer les noms des salariés)
engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TOPIC       = os.getenv("KAFKA_TOPIC", "sport_db.public.activites")
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ───────────────────────────────────────────────
# 2. Logique d'enrichissement et de décodage
# ───────────────────────────────────────────────

def get_salarie_name(id_salarie):
    """Effectue une jointure applicative pour humaniser le message Slack."""
    try:
        with engine.connect() as conn:
            query = text("SELECT nom, prenom FROM salaries WHERE id_salarie = :id")
            result = conn.execute(query, {"id": id_salarie}).fetchone()
            return f"{result.prenom} {result.nom}" if result else f"Salarié {id_salarie}"
    except Exception:
        return f"Salarié {id_salarie}"

def decode_distance(value):
    """Décode les types de données numériques complexes envoyés par Debezium."""
    if value is None: return 0.0
    try:
        return float(value)
    except:
        # Gestion du format binaire si la distance est encodée par le connecteur
        try:
            decoded = base64.b64decode(value)
            return struct.unpack('>d', decoded)[0] if len(decoded) == 8 else int.from_bytes(decoded, 'big') / 100
        except: return 0.0

# ───────────────────────────────────────────────
# 3. Moteur de formatage de message
# ───────────────────────────────────────────────

def format_message(payload):
    after = payload.get("after", {})
    if not after: return None

    nom_complet = get_salarie_name(after.get("id_salarie"))
    sport       = after.get("type_sport", "Sport")
    dist_km     = round(decode_distance(after.get("distance_m")) / 1000, 1)
    duree_min   = round(int(after.get("temps_ecoule_s") or 0) / 60)
    commentaire = after.get("commentaire") or ""

    # Logique de personnalisation selon les consignes du projet
    if "Course" in sport or "Run" in sport:
        msg = f"Bravo *{nom_complet}* ! Tu viens de courir {dist_km} km en {duree_min} min ! Quelle énergie ! 🏃‍♂️🔥"
    elif "Rando" in sport:
        msg = f"Magnifique *{nom_complet}* ! Une randonnée de {dist_km} km terminée ! 🥾"
    else:
        msg = f"Bravo *{nom_complet}* ! {dist_km} km de {sport} en {duree_min} min ! 🏅"

    if commentaire.strip():
        msg += f'\n> _{commentaire}_'
    return msg

# ───────────────────────────────────────────────
# 4. Boucle de consommation (Stream Processing)
# ───────────────────────────────────────────────
print(f"🎧 Écoute du flux d'activités sur {TOPIC}...")



consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    try:
        payload = message.value.get("payload", {})
        # On ne traite que les opérations de Création ('c') ou de Lecture ('r')
        if payload.get("op") in ("c", "r"):
            slack_msg = format_message(payload)
            if slack_msg and WEBHOOK_URL:
                requests.post(WEBHOOK_URL, json={"text": slack_msg})
                print(f" ✅ Alerte Slack envoyée pour {payload['after']['id_salarie']}")
    except Exception as e:
        print(f" ❌ Erreur : {e}")