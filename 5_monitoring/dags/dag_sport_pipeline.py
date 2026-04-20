import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

# ───────────────────────────────────────────────
# 1. Chemins absolus (depuis .env)
# ───────────────────────────────────────────────
BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "/opt/airflow/dags/poc-sport-data-solution")
SCRIPTS_DIR = f"{BASE_DIR}/3_pipeline_etl"
QUALITY_DIR = f"{BASE_DIR}/4_data_quality"

# ───────────────────────────────────────────────
# 2. Arguments par défaut
# ───────────────────────────────────────────────
default_args = {
    "owner": "sport_data_solution",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ───────────────────────────────────────────────
# 3. DAG principal
# ───────────────────────────────────────────────
dag = DAG(
    dag_id="sport_data_pipeline",
    description="Pipeline complet Sport Data Solution — Bronze > Silver > Gold",
    default_args=default_args,
    schedule_interval="0 6 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sport", "etl", "medallion"],
)

# ───────────────────────────────────────────────
# 4. Tâches Bash
# ───────────────────────────────────────────────
t1_generate = BashOperator(
    task_id="generation_strava",
    bash_command=f"cd {BASE_DIR}/1_data_generation && python3 generate_strava_data.py",
    dag=dag,
)

t2_load = BashOperator(
    task_id="chargement_postgresql",
    bash_command=f"cd {BASE_DIR}/2_database && python3 load_data.py",
    dag=dag,
)

t3_bronze = BashOperator(
    task_id="bronze_layer",
    bash_command=f"cd {SCRIPTS_DIR} && python3 bronze_layer.py",
    dag=dag,
)

t4_silver = BashOperator(
    task_id="silver_layer",
    bash_command=f"cd {SCRIPTS_DIR} && python3 silver_layer.py",
    dag=dag,
)

t5_gold = BashOperator(
    task_id="gold_layer",
    bash_command=f"cd {SCRIPTS_DIR} && python3 gold_layer.py",
    dag=dag,
)

t6_export = BashOperator(
    task_id="gold_vers_postgresql",
    bash_command=f"cd {SCRIPTS_DIR} && python3 gold_to_postgres.py",
    dag=dag,
)

t7_quality = BashOperator(
    task_id="tests_qualite",
    bash_command=f"cd {QUALITY_DIR} && python3 test_quality.py",
    dag=dag,
)

# ───────────────────────────────────────────────
# 5. Monitoring final
# ───────────────────────────────────────────────
def task_monitoring():
    import pandas as pd
    from sqlalchemy import create_engine

    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
    DB_HOST = os.getenv("POSTGRES_HOST", "postgres-sport")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "sport_data_solution")

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    df_sal = pd.read_sql("SELECT COUNT(*) as nb FROM salaries", engine)
    df_act = pd.read_sql("SELECT COUNT(*) as nb FROM activites", engine)
    df_av  = pd.read_sql("SELECT * FROM avantages", engine)

    nb_sal      = df_sal["nb"].iloc[0]
    nb_act      = df_act["nb"].iloc[0]
    nb_prime    = int(df_av["prime_sportive"].sum())
    nb_bienetre = int(df_av["jours_bienetre"].sum())
    cout_primes = float(df_av["montant_prime"].sum())

    rapport = f"""
╔══════════════════════════════════════════════╗
║   RAPPORT MONITORING — {datetime.now().date()}     ║
╠══════════════════════════════════════════════╣
║  Salaries en base       : {nb_sal:<6}              ║
║  Activites en base      : {nb_act:<6}              ║
║  Eligibles prime        : {nb_prime:<6}            ║
║  Cout total primes      : {cout_primes:>10,.2f} €  ║
║  Eligibles bien-etre    : {nb_bienetre:<6}         ║
╚══════════════════════════════════════════════╝
    """

    logging.info(rapport)
    return rapport

t8_monitoring = PythonOperator(
    task_id="rapport_monitoring",
    python_callable=task_monitoring,
    dag=dag,
)

# ───────────────────────────────────────────────
# 6. Ordonnancement
# ───────────────────────────────────────────────
(
    t1_generate
    >> t2_load
    >> t3_bronze
    >> t4_silver
    >> t5_gold
    >> t6_export
    >> t7_quality
    >> t8_monitoring
)
