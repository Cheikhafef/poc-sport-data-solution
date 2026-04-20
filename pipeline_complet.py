#!/usr/bin/env python3
"""
Pipeline complet Sport Data Solution
Orchestre les 3 couches Médaillon + export
"""
import subprocess
import sys
import time
from datetime import datetime

def run(script, label):
    print(f"\n{'='*55}")
    print(f"  ▶ {label}")
    print(f"{'='*55}")
    start = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False
    )
    duration = round(time.time() - start, 1)
    if result.returncode == 0:
        print(f"  ✅ Terminé en {duration}s")
    else:
        print(f"  ❌ Erreur dans {script}")
        sys.exit(1)
    return duration

if __name__ == "__main__":
    print("\n" + "🚀 "*20)
    print("  PIPELINE SPORT DATA SOLUTION — DÉMARRAGE")
    print("🚀 "*20)
    print(f"  Heure : {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    total_start = time.time()

    # Étape 1 — Bronze Layer
    run("3_pipeline_etl/bronze_layer.py",
        "🥉 BRONZE LAYER — Ingestion depuis PostgreSQL")

    # Étape 2 — Silver Layer
    run("3_pipeline_etl/silver_layer.py",
        "🥈 SILVER LAYER — Nettoyage + Validation distances")

    # Étape 3 — Gold Layer
    run("3_pipeline_etl/gold_layer.py",
        "🏆 GOLD LAYER — Calcul avantages métier")

    # Étape 4 — Export Log System
    run("3_pipeline_etl/gold_to_postgres.py",
        "📤 EXPORT — Gold → PostgreSQL Log System")

    # Étape 5 — Tests qualité
    run("4_data_quality/tests_qualite.py",
        "✅ TESTS QUALITÉ — Great Expectations")

    total = round(time.time() - total_start, 1)

    print("\n" + "="*55)
    print("  🎉 PIPELINE TERMINÉ AVEC SUCCÈS !")
    print(f"  ⏱️  Durée totale : {total}s")
    print("="*55)
    print("""
  📊 Résultats disponibles dans :
     → Delta Lake   : ./delta_lake/gold/avantages
     → PostgreSQL   : table avantages_calcules
     → Power BI     : actualiser le rapport

  📲 Slack temps réel via :
     → python3 6_restitution/redpanda_slack_consumer.py
    """)