# 🏋️‍♂️ Sport Data Solution — Pipeline Data Lakehouse complet <br>
Projet Data Engineer — Architecture Medallion (Bronze / Silver / Gold) <br>
Ce projet implémente un pipeline complet, industrialisé, et orchestré permettant :<br>
 * la collecte d’activités sportives (Strava simulé + saisie employé)
 * l’ingestion en temps réel via Debezium + Redpanda (Kafka)
 * le traitement Big Data via Spark + Delta Lake
 * la qualité des données (15 tests automatisés)
 * l’exposition des résultats dans PostgreSQL
 * la visualisation dans Power BI
 * les notifications Slack en temps réel
--------
# 🏗️ Architecture Globale <BR>
![Architecture du pipeline](./docs/architecture.png)

## ⚙️ Technologies <br>
| Composant     | Technologie                   |
|---------------|-------------------------------|
| Langage       | Python 3.12                   |
| Traitement    | Apache Spark 3.x + PySpark    |
| Stockage      | Delta Lake 3.1 + PostgreSQL 16|
| CDC           | Debezium 2.4                  |
| Streaming     | Redpanda (Kafka compatible)   |
| Orchestration | Apache Airflow 2.8 (Docker)   |
| Qualité       | Great Expectations            |
| Notification  | Slack Webhook                 |
| Visualisation | Power BI Desktop              |
| Environnement | Ubuntu WSL2 (Windows 11)      |

## 📁 Structure du projet <br>

poc-sport-data-solution/<br>
├── 1_data_generation/<br>
│   ├── generate_strava_data.py     # Génération 4212 activités simulées<br>
│   └── interface_saisie.py         # Saisie live → Debezium → Slack <br>
├── 2_database/<br>
│   └── load_data.py                # Chargement initial PostgreSQL<br>
├── 3_pipeline_etl/<br>
│   ├── bronze_layer.py             # Ingestion brute → Delta Lake<br>
│   ├── silver_layer.py             # Nettoyage + validation distances <br>
│   ├── gold_layer.py               # Calcul avantages métier <br>
│   ├── gold_to_postgres.py         # Export Gold → Log System <br>
│   └── etl_spark.py                # Pipeline tout-en-un <br>
├── 4_data_quality/<br>
│   └── tests_qualite.py            # 15/15 tests PASS <br>
├── 5_monitoring/ <br>
│   ├── docker-compose.yml          # Airflow + Redpanda + Debezium + Spark <br>
│   └── dags/ <br>
│       └── dag_sport_pipeline.py   # DAG orchestration lundi 6h <br>
├── 6_restitution/ <br>
│   ├── slack_notifier.py           # Envoi manuel Slack <br>
│   ├── rapport_poc_sport.pbix <br>
│   └── redpanda_slack_consumer.py  # Consumer CDC temps réel <br>
├── delta_lake/ <br>
│   ├── bronze/                     # Données brutes <br>
│   ├── silver/                     # Données nettoyées <br>
│   └── gold/                       # Avantages calculés <br>
├── data/ <br>
│   ├── Données_RH.xlsx <br>
│   ├── Données_Sportive.xlsx <br>
│   └── strava_simulated_data.csv <br>
├── pipeline_complet.py             # Orchestration manuelle complète <br>
├── docs/ <br>
│   └── architecture.png <br>
└── README.md <br>


   ------
## 🚀 Lancement rapide <br>

### Prérequis <br>
```bash
# Ubuntu WSL2
sudo apt install openjdk-17-jdk postgresql
pip install pyspark delta-spark pandas sqlalchemy psycopg2-binary kafka-python requests
```

### 1. Démarrer les services <br>
```bash
sudo service postgresql start
cd 5_monitoring && docker-compose up -d
```

### 2. Initialiser la base <br>
```bash
python3 2_database/load_data.py
```

### 3. Lancer le pipeline complet <br>
```bash
python3 pipeline_complet.py
```

### 4. Démarrer le consumer Slack (temps réel) <br>
```bash
# Terminal séparé
python3 6_restitution/redpanda_slack_consumer.py
```

### 5. Tester la saisie live <br>
```bash
python3 1_data_generation/interface_saisie.py
```

## 📊 Résultats POC <br>

| KPI                       | Valeur         |
|---------------------------|----------------|
| Total salariés            | 161            |
| Activités générées        | 4 212          |
| Éligibles prime sportive  | 68             |
| Coût total primes         | 172 482 €      |
| Éligibles jours bien-être | 108            |
| Tests qualité             | **15/15 PASS** |

## 🔑 Configuration <br>
```bash
# PostgreSQL
DB_URL = "postgresql://postgres:admin123@localhost:5432/sport_data_solution"

# Slack Webhook
WEBHOOK_URL = "https://hooks.slack.com/services/..."

# Adresse bureau
ADRESSE_BUREAU = "1362 Av. des Platanes, 34970 Lattes"
```

## 📐 Règles métier <br>

### Prime sportive (5% salaire brut) <br>
- Mode de déplacement : Marche/running ou Vélo/Trottinette
- Distance validée : Marche ≤ 15km / Vélo ≤ 25km
- Validation automatique via API Gouv France

### 5 Jours bien-être <br>
- Minimum **15 activités** physiques dans l'année
- Déclaration via interface ou Strava (à terme)

## 🔄 Évolutions futures <br>
- Connexion API Strava réelle
- Analyse commentaires via **Mistral AI**
- Déploiement cloud (AWS/GCP)
- Interface **Streamlit** pour les salariés



# 🏁 Conclusion <br>
Ce projet démontre :
  * une architecture Data Lakehouse complète
  * un pipeline Big Data industrialisé
  * une orchestration Airflow professionnelle
  * une ingestion temps réel
  * une qualité de données robuste
  * une exposition BI claire