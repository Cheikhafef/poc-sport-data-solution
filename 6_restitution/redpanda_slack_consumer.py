import os
import json
import base64
import struct
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# 1. Charger les variables d'environnement
# ───────────────────────────────────────────────
load_dotenv()

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TOPIC       = os.getenv("KAFKA_TOPIC", "sport_db.public.activites")
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

EMOJIS = {
    "Course à pied": "🏃‍♂️🔥", "Randonnée": "🥾🌄",
    "Natation": "🏊‍♂️💦",      "Tennis": "🎾",
    "Football": "⚽",            "Rugby": "🏉",
    "Badminton": "🏸",           "Judo": "🥋",
    "Boxe": "🥊",                "Escalade": "🧗",
    "Triathlon": "🏅",           "Équitation": "🐴",
    "Basketball": "🏀",          "Voile": "⛵",
    "Runing": "🏃‍♂️",
}

# ───────────────────────────────────────────────
# 2. Fonction envoi Slack
# ───────────────────────────────────────────────
def send_slack(message):
    if not WEBHOOK_URL:
        print("⚠️ Aucun webhook Slack configuré (.env manquant)")
        return False
    response = requests.post(WEBHOOK_URL, json={"text": message})
    return response.status_code == 200

# ───────────────────────────────────────────────
# 3. Décodage Debezium (base64 → float)
# ───────────────────────────────────────────────
def decode_distance(value):
    if value is None:
        return None
    try:
        return float(value)
    except:
        try:
            decoded = base64.b64decode(value)
            if len(decoded) == 8:
                return struct.unpack('>d', decoded)[0]
            if len(decoded) == 4:
                return struct.unpack('>f', decoded)[0]
            return int.from_bytes(decoded, 'big') / 100
        except:
            return None

# ───────────────────────────────────────────────
# 4. Formatage du message Slack
# ───────────────────────────────────────────────
def format_message(payload):
    after = payload.get("after", {})
    if not after:
        return None

    id_salarie   = after.get("id_salarie", "?")
    sport        = after.get("type_sport", "Sport")
    distance_raw = after.get("distance_m")
    duree_s      = after.get("temps_ecoule_s", 0)
    commentaire  = after.get("commentaire", "") or ""
    emoji        = EMOJIS.get(sport, "🏅")

    duree_min = round(int(duree_s) / 60) if duree_s else 0
    distance  = decode_distance(distance_raw)

    if distance and distance > 0:
        dist_km = round(distance / 1000, 1)
        msg = f"Bravo (ID {id_salarie}) ! {dist_km} km de {sport} en {duree_min} min ! {emoji}"
    else:
        msg = f"Bravo (ID {id_salarie}) ! Belle séance de {sport} de {duree_min} min ! {emoji}"

    if commentaire.strip():
        msg += f'\n> _{commentaire}_'

    return msg

# ───────────────────────────────────────────────
# 5. Consumer Kafka / Redpanda
# ───────────────────────────────────────────────
print("🎧 En écoute sur Redpanda...")
print(f"   Topic : {TOPIC}")
print(f"   Bootstrap : {BOOTSTRAP}\n")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=15000,
)

nb_messages = 0
nb_slack    = 0

for message in consumer:
    try:
        payload = message.value.get("payload", {})
        op      = payload.get("op", "")

        if op in ("c", "r"):
            slack_msg = format_message(payload)
            if slack_msg:
                nb_messages += 1

                if nb_slack < 5:
                    ok = send_slack(slack_msg)
                    nb_slack += 1
                    statut = "✅" if ok else "❌"
                    print(f"  {statut} [{op}] {slack_msg[:70]}...")
                else:
                    print(f"  📨 [{op}] Message détecté (non envoyé Slack)")

    except Exception as e:
        print(f"  ❌ Erreur : {e}")

print(f"\n🎉 {nb_messages} messages traités | {nb_slack} envoyés sur Slack !")
