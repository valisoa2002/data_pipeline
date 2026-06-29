# PROJET VALISOA 2026 — Data Pipeline avec Apache Airflow

Pipeline de données orchestré avec **Apache Airflow 3.2.2** et **Docker**, utilisant **PostgreSQL** comme base de métadonnées.

---

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- Docker Compose v2.14.0 ou supérieur
- Au moins **4 Go de RAM** alloués à Docker
- Windows avec PowerShell ou Git Bash

---

## Structure du projet

```
data_pipeline/
├── airflow/
│   ├── dags/           # Vos fichiers DAG Python
│   ├── logs/           # Logs générés par Airflow
│   └── plugins/        # Plugins personnalisés
├── pgdata/             # Données persistantes PostgreSQL (généré automatiquement)
├── docker-compose.yaml
└── README.md
```

---

## Installation et démarrage

### 1. Cloner le dépôt

```bash
git clone git@github.com:valisoa2002/data_pipeline.git
cd data_pipeline
```

### 2. Créer les répertoires nécessaires

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins
```

### 3. Configuration Docker Compose

Le fichier `docker-compose.yaml` utilise le mode **`standalone`** d'Airflow 3.x, qui regroupe tous les composants (scheduler, api-server, dag-processor, triggerer) dans un seul container — idéal pour le développement local.

```yaml
services:
  postgres:
    image: postgres:15
    container_name: data_pipeline-postgres-1
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - ./pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  airflow:
    image: apache/airflow:3.2.2
    container_name: data_pipeline-airflow-1
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
    ports:
      - "8080:8080"
    command: standalone
```

### 4. Démarrer les services

```bash
docker compose up -d
```

Attendre 30 à 40 secondes que tous les services démarrent correctement.

### 5. Récupérer le mot de passe admin

Au premier démarrage, Airflow génère automatiquement un mot de passe pour l'utilisateur `admin` :

```bash
docker logs data_pipeline-airflow-1 2>&1 | grep -i "password"
```
ou pour trouver bien le login

```bash
docker logs data_pipeline-airflow-1 2>&1 | grep -i "password\|admin\|login"
```

Exemple de sortie :
```
Simple auth manager | Password for user 'admin': cESYz3vWFyVrYcZk
```

### 6. Accéder à l'interface web

Ouvrir **http://localhost:8080** dans votre navigateur.

- **Login** : `admin`
- **Mot de passe** : *(celui récupéré à l'étape précédente)*

---

## Développement de DAGs

### Créer un DAG

Placez vos fichiers DAG dans le répertoire `airflow/dags/`. Ils sont automatiquement détectés par Airflow (délai de ~30 secondes).

Exemple de DAG (`airflow/dags/mon_dag.py`) :

```python
from datetime import datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="mon_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",  # Tous les jours à minuit
    catchup=False,
) as dag:

    debut = BashOperator(
        task_id="debut",
        bash_command="echo 'Début du pipeline'",
    )

    @task()
    def traitement():
        print("Traitement des données...")

    debut >> traitement()
```

### Vérifier la détection d'un DAG

```bash
docker exec -it data_pipeline-airflow-1 airflow dags list
```

### Vérifier les erreurs d'import

```bash
docker exec -it data_pipeline-airflow-1 airflow dags list-import-errors
```

### Déclencher un DAG manuellement

```bash
docker exec -it data_pipeline-airflow-1 airflow dags trigger mon_dag
```

Ou via l'interface web : bouton **▶ Trigger DAG** sur la page du DAG.

---

## Commandes utiles

### Gestion des containers

```bash
# Démarrer
docker compose up -d

# Arrêter
docker compose down

# Voir l'état des containers
docker ps

# Voir les logs en temps réel
docker logs data_pipeline-airflow-1 -f --tail=50
```

### Ouvrir un shell dans le container Airflow

```bash
# PowerShell ou CMD
docker exec -it data_pipeline-airflow-1 bash

# Git Bash (désactiver la conversion de chemins)
MSYS_NO_PATHCONV=1 docker exec -it data_pipeline-airflow-1 bash
```

### Lire les logs d'une tâche

```bash
# PowerShell
docker exec -it data_pipeline-airflow-1 cat /opt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=1.log

# Git Bash
MSYS_NO_PATHCONV=1 docker exec -it data_pipeline-airflow-1 cat /opt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=1.log
```

### Forcer la re-détection des DAGs

```bash
docker exec -it data_pipeline-airflow-1 airflow dags reserialize
```

### Déclenchement manuelle des DAGs

```bash
docker exec data_pipeline-airflow-1 airflow dags trigger <dag_function_name> (dag_excel_ingestion,...)
```

### Exemple d'éxecution des requêtes
# Si vous utilisez docker-compose
```bash
docker-compose exec postgres psql -U airflow -d airflow
```
# Ou si vous avez juste un conteneur PostgreSQL
```bash
docker exec -it <nom_container_postgres> psql -U airflow -d airflow
```

---

## Notes importantes

### Utilisation sous Windows

Airflow ne supporte pas Windows nativement. Ce projet utilise Docker pour contourner cette limitation. Quelques points à retenir :

- **Git Bash** convertit automatiquement les chemins Unix (`/opt/...`) en chemins Windows (`C:/Program Files/Git/opt/...`). Pour éviter ce problème, préfixer les commandes avec `MSYS_NO_PATHCONV=1` ou utiliser **PowerShell**.
- Préférer **PowerShell** pour toutes les commandes `docker exec` avec des chemins absolus.

### Pourquoi le mode `standalone` ?

Airflow 3.x avec `LocalExecutor` en configuration multi-containers rencontre des problèmes d'authentification JWT entre le scheduler et l'api-server (bug connu). Le mode `standalone` regroupe tous les composants dans un seul processus, éliminant ces problèmes de communication inter-containers — parfait pour le développement local.

### Persistance des données

- Les **métadonnées Airflow** (DAG runs, états des tâches) sont stockées dans PostgreSQL via le volume `./pgdata`.
- Les **logs des tâches** sont accessibles dans `./airflow/logs/` depuis votre machine.
- Un `docker compose down` ne supprime **pas** les données — elles persistent entre les redémarrages.
- Pour repartir de zéro : `docker compose down -v` (supprime les volumes).

---

## Architecture

```
┌────────────────────────────────────────┐
│         data_pipeline-airflow-1        │
│                                        │
│  ┌──────────┐  ┌───────────────────┐   │
│  │ Scheduler│  │   API Server      │   │
│  └──────────┘  └───────────────────┘   │
│  ┌──────────┐  ┌───────────────────┐   │
│  │Triggerer │  │  DAG Processor    │   │
│  └──────────┘  └───────────────────┘   │
│                                        │
│         LocalExecutor (tasks)          │
└──────────────────┬─────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│       data_pipeline-postgres-1          │
│         PostgreSQL 15 :5432             │
└─────────────────────────────────────────┘
```

---

## Dépannage

| Problème | Solution |
|----------|----------|
| DAG non visible dans l'interface | Attendre 30s puis `airflow dags reserialize` |
| `No such file or directory` avec Git Bash | Utiliser PowerShell ou préfixer avec `MSYS_NO_PATHCONV=1` |
| Mot de passe perdu | `docker logs data_pipeline-airflow-1 2>&1 \| grep password` |
| Container qui ne démarre pas | `docker logs data_pipeline-airflow-1 --tail=50` |
| Tâche bloquée en `queued` | Vérifier que le container Airflow est bien en mode `standalone` |

---

## Technologies utilisées

- [Apache Airflow 3.2.2](https://airflow.apache.org/)
- [PostgreSQL 15](https://www.postgresql.org/)
- [Docker & Docker Compose](https://www.docker.com/)
- Python 3.13