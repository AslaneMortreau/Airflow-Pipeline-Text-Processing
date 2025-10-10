# Pipeline de Traitement de Texte

## Vue d'Ensemble

Ce projet est une infrastructure complète de traitement de texte basée sur Apache Airflow, conçue pour traiter automatiquement des fichiers texte avec segmentation, vérification d'intégrité et reconstruction.

### Fonctionnalités Principales

- **Traitement Automatique** : Exécution toutes les 30 minutes
- **Détection Intelligente** : Surveillance continue du répertoire d'entrée
- **Gestion d'Erreurs Avancée** : Retry avec backoff exponentiel et circuit breaker
- **Tracking Complet** : Base de données PostgreSQL pour éviter les doublons
- **Configuration Dynamique** : Variables Airflow configurables via l'interface web
- **Monitoring** : Logs détaillés et rapports d'erreurs

## Processus de Traitement

### Phase 1 : Initialisation
- **Variables Airflow** : Initialisation automatique des variables de configuration
- **Connexion Base de Données** : Vérification de la connexion PostgreSQL
- **Credentials Sécurisés** : Configuration via l'interface Airflow (pas d'exposition dans le code)

### Phase 2 : Détection des Fichiers
- **Scan du Répertoire** : Recherche de tous les fichiers `.txt`
- **Filtrage Intelligent** : Sélection uniquement des fichiers non traités
- **Tracking MD5** : Évite le retraitement des fichiers déjà traités

### Phase 3 : Traitement des Fichiers
Le traitement suit un processus en 8 étapes :

1. **Lecture du Fichier** : Chargement du contenu texte (UTF-8)
2. **Calcul du Hash Initial** : Génération du checksum MD5 original
3. **Segmentation du Texte** : Découpage en chunks configurables
4. **Calcul des Hashs des Chunks** : Vérification d'intégrité par segment
5. **Sauvegarde des Chunks** : Stockage temporaire des segments
6. **Vérification des Hashs** : Validation de l'intégrité des segments
7. **Reconstruction du Texte** : Assemblage des chunks en fichier final
8. **Comparaison Finale** : Validation que le texte reconstruit = texte original

### Phase 4 : Gestion des Résultats
- **Succès** : Fichier déplacé vers `/output/` avec métadonnées en base
- **Échec Temporaire** : Retry automatique avec backoff exponentiel
- **Échec Définitif** : Déplacement vers Dead Letter Queue pour analyse manuelle

## Architecture

### Diagramme de Flux
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Input Files   │───▶│  Airflow DAG    │───▶│  Output Files   │
│   (.txt files)  │    │   Pipeline      │    │  (processed)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   PostgreSQL    │
                       │  (Tracking DB)  │
                       └─────────────────┘
```

### Composants du Pipeline

#### Tâches Airflow
- **`setup_airflow_variables`** : Initialisation des variables de configuration
- **`check_database_connection`** : Vérification de la connexion PostgreSQL
- **`check_files_and_trigger`** : Détection des fichiers avec variables Airflow
- **`get_unprocessed_files`** : Identification des fichiers non traités
- **`process_all_files`** : Traitement de tous les nouveaux fichiers

#### Mécanismes de Protection
- **Retry avec Backoff Exponentiel** : Gestion des erreurs temporaires
- **Circuit Breaker** : Protection contre les cascades d'échecs
- **Dead Letter Queue** : Isolation des fichiers problématiques

## Statuts de Traitement

Le pipeline utilise 6 statuts pour suivre l'état de chaque fichier :

| Statut | Description | Action Suivante |
|--------|-------------|-----------------|
| **PENDING** | Fichier détecté, en attente | Démarrage du traitement |
| **PROCESSING** | Traitement en cours | Segmentation et vérification |
| **COMPLETED** | Traitement réussi | Fichier déplacé vers `/output/` |
| **FAILED** | Échec temporaire | Retry automatique |
| **RETRYING** | Nouvelle tentative | Retry avec délai progressif |
| **DEAD_LETTER** | Échec définitif | Analyse manuelle requise |

### Cycle de Vie d'un Fichier
```
PENDING → PROCESSING → COMPLETED
    ↓         ↓
FAILED → RETRYING → PROCESSING
    ↓
DEAD_LETTER
```

### Services Docker

- **Airflow Webserver** : Interface web (port 8081)
- **Airflow Scheduler** : Planificateur des tâches
- **Airflow Worker** : Exécuteur des tâches
- **Airflow Triggerer** : Gestionnaire des déclencheurs
- **PostgreSQL** : Base de données de métadonnées et tracking
- **Redis** : Broker Celery pour la distribution des tâches

## Installation et Démarrage

### Installation Rapide

```bash
# Cloner le projet
git clone https://github.com/AslaneMortreau/Airflow-Pipeline-Text-Processing
cd Airflow-Pipeline-Text-Processing


# Démarrer l'infrastructure
docker-compose up -d

# Vérifier le statut
docker-compose ps
```

### Accès aux Services

- **Interface Airflow** : http://localhost:8081
- **PostgreSQL** : localhost:5432 (user: airflow, password: airflow)
- **Redis** : localhost:6379

## Structure du Projet

```
Airflow-Pipeline-Text-Processing/
├── dags/
│   └── dag.py          # Pipeline principal
├── data/
│   ├── input/                  # Répertoire d'entrée des fichiers
│   ├── output/                 # Répertoire de sortie
│   ├── processed/              # Fichiers traités
│   └── dead_letter/           # Fichiers en échec
├── docker-compose.yml          # Configuration Docker
├── Dockerfile                  # Image Airflow personnalisée
├── requirements.txt            # Dépendances Python
├── init-scripts.sql           # Script d'initialisation PostgreSQL
├── .gitignore                 # Fichiers à ignorer
└── README.md                  # Ce fichier
```

## Configuration

### Variables Airflow

Configurez ces variables via l'interface web Airflow (Admin → Variables) :

| Variable | Description | Valeur par Défaut | Impact |
|----------|-------------|-------------------|---------|
| `input_directory` | Répertoire d'entrée des fichiers | `/opt/airflow/data/input` | Change le répertoire surveillé |
| `chunk_size` | Taille des segments (caractères) | `1000` | Affecte la granularité de segmentation |
| `max_retries` | Nombre maximum de tentatives | `3` | Contrôle la persistance en cas d'erreur |
| `circuit_breaker_threshold` | Seuil du circuit breaker | `5` | Protège contre les cascades d'échecs |

### Configuration via Interface Web

1. **Accéder à Airflow** : http://localhost:8081
2. **Admin → Variables** : Gestion des variables
3. **Créer/Modifier** : Ajouter ou modifier les valeurs
4. **Redémarrage automatique** : Les changements sont pris en compte immédiatement

### Exemple de Configuration

```
input_directory = /opt/airflow/data/input
chunk_size = 1000
max_retries = 3
circuit_breaker_threshold = 5
```

### Répertoires

- **Input** : `/opt/airflow/data/input` - Fichiers `.txt` à traiter
- **Output** : `/opt/airflow/data/output` - Fichiers reconstruits
- **Processed** : `/opt/airflow/data/processed` - Fichiers traités avec succès
- **Dead Letter** : `/opt/airflow/data/dead_letter` - Fichiers en échec

## Gestion d'Erreurs

### Système de Retry

- **Backoff Exponentiel** : Délai croissant entre les tentatives
- **Jitter** : Randomisation pour éviter les collisions
- **Configurable** : Nombre de tentatives via variables Airflow

### Circuit Breaker

- **Protection Système** : Arrêt automatique en cas de trop d'échecs
- **Seuil Configurable** : Nombre d'échecs avant activation
- **Récupération** : Réactivation automatique après timeout

### Dead Letter Queue

- **Fichiers en Échec** : Stockage séparé avec métadonnées d'erreur
- **Analyse** : Possibilité d'analyser les causes d'échec
- **Reprocessing** : Retraitement manuel possible

## Monitoring et Logs

### Interface Airflow

- **DAG View** : Vue d'ensemble du pipeline avec statuts des tâches
- **Task Instances** : Détail des exécutions avec logs et métriques
- **Logs** : Logs détaillés de chaque tâche avec niveaux (INFO, WARNING, ERROR)
- **Variables** : Configuration dynamique et historique des modifications
- **Graph View** : Visualisation du flux de données entre les tâches

### Base de Données de Tracking

Table `processed_files` avec colonnes :
- `file_hash` : Hash MD5 unique (clé primaire)
- `file_path` : Chemin original du fichier
- `file_size` : Taille en octets
- `processed_at` : Timestamp de traitement
- `status` : Statut actuel (pending, processing, completed, failed, retrying, dead_letter)
- `output_file` : Chemin du fichier de sortie généré
- `error_message` : Message d'erreur en cas d'échec
- `created_at` : Timestamp de création de l'enregistrement


## Améliorations Futures

### Fonctionnalités Planifiées

- [ ] **Notifications Slack/Email** : Alertes automatiques
- [ ] **Dashboard de Monitoring** : Interface de suivi des fichiers
- [ ] **Validation Multi-Format** : Support JSON, CSV, XML
- [ ] **Système de Priorités** : Traitement par priorité
- [ ] **Compression Automatique** : Compression/décompression
- [ ] **Tests Unitaires** : Suite de tests complète

## Licence

Ce projet est sous licence MIT. 
