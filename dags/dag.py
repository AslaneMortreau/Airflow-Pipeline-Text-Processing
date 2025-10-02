from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import hashlib
import os
import json
import shutil
import time
import random
import logging
from enum import Enum
from dataclasses import dataclass

# Définition des arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Création du DAG
dag = DAG(
    'text_processing_pipeline',
    default_args=default_args,
    description='Pipeline de traitement de texte',
    schedule_interval=timedelta(minutes=30),  # Exécution toutes les 30 minutes
    catchup=False,
    tags=['text-processing'],
)

# Configuration du pipeline
DEFAULT_INPUT_DIR = '/opt/airflow/data/input'
OUTPUT_DIR = '/opt/airflow/data/output'
PROCESSED_DIR = '/opt/airflow/data/processed'
DEAD_LETTER_DIR = '/opt/airflow/data/dead_letter'
TEMP_DIR = '/opt/airflow/temp'
FILE_PATTERN = '*.txt'  # On ne veut traiter que les fichiers texte


def get_input_dir():
    """Récupère le répertoire d'entrée depuis les variables Airflow"""
    try:
        return Variable.get("input_directory", default_var=DEFAULT_INPUT_DIR)
    except Exception:
        logger.warning(f"Variable input_directory non trouvée, utilisation du défaut: {DEFAULT_INPUT_DIR}")
        return DEFAULT_INPUT_DIR

def get_chunk_size():
    """Récupère la taille des chunks depuis les variables Airflow"""
    try:
        return int(Variable.get("chunk_size", default_var=100))
    except Exception:
        logger.warning("Variable chunk_size non trouvée, utilisation du défaut: 100")
        return 100

def get_max_retries():
    """Récupère le nombre max de retry depuis les variables Airflow"""
    try:
        return int(Variable.get("max_retries", default_var=3))
    except Exception:
        logger.warning("Variable max_retries non trouvée, utilisation du défaut: 3")
        return 3

def get_circuit_breaker_threshold():
    """Récupère le seuil du circuit breaker depuis les variables Airflow"""
    try:
        return int(Variable.get("circuit_breaker_threshold", default_var=5))
    except Exception:
        logger.warning("Variable circuit_breaker_threshold non trouvée, utilisation du défaut: 5")
        return 5

# Configuration de la gestion d'erreurs avancée
BASE_DELAY = 1  # Délai de base en secondes
MAX_DELAY = 60  # Délai maximum en secondes
JITTER_FACTOR = 0.1  # Facteur de jitter pour éviter le thundering herd
CIRCUIT_BREAKER_TIMEOUT = 300  # Timeout du circuit breaker en secondes

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    """Statuts possibles pour le traitement d'un fichier"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"

class CircuitBreakerState(Enum):
    """États du circuit breaker"""
    CLOSED = "closed"      # Fonctionnement normal
    OPEN = "open"          # Circuit ouvert, pas de traitement
    HALF_OPEN = "half_open" # Test de récupération

@dataclass
class RetryConfig:
    """Configuration pour les tentatives de retry"""
    max_retries: int = 3
    base_delay: float = BASE_DELAY
    max_delay: float = MAX_DELAY
    jitter_factor: float = JITTER_FACTOR
    exponential_base: float = 2.0

@dataclass
class CircuitBreakerConfig:
    """Configuration du circuit breaker"""
    failure_threshold: int = 5
    timeout: int = CIRCUIT_BREAKER_TIMEOUT
    success_threshold: int = 2  # Succès consécutifs pour fermer le circuit

class CircuitBreaker:
    """Circuit breaker pour protéger contre les cascades d'erreurs"""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.logger = logging.getLogger(f"{__name__}.CircuitBreaker")
    
    def can_execute(self) -> bool:
        """Vérifie si une exécution est autorisée"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                self.logger.info("Circuit breaker: Transition vers HALF_OPEN")
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True
        return False
    
    def record_success(self):
        """Enregistre un succès"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.logger.info("Circuit breaker: Transition vers CLOSED (récupération)")
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Enregistre un échec"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.logger.warning(f"Circuit breaker: Transition vers OPEN (échecs: {self.failure_count})")
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.logger.warning("Circuit breaker: Retour vers OPEN depuis HALF_OPEN")
    
    def _should_attempt_reset(self) -> bool:
        """Vérifie si le circuit breaker doit tenter une réinitialisation"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.config.timeout

class RetryManager:
    """Gestionnaire de retry avec backoff exponentiel et jitter"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.RetryManager")
    
    def calculate_delay(self, attempt: int) -> float:
        """Calcule le délai pour une tentative donnée"""
        # Backoff exponentiel
        delay = self.config.base_delay * (self.config.exponential_base ** attempt)
        
        # Limiter au délai maximum
        delay = min(delay, self.config.max_delay)
        
        # Ajouter du jitter pour éviter le thundering herd
        jitter = delay * self.config.jitter_factor * (2 * random.random() - 1)
        delay += jitter
        
        return max(0, delay)
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Détermine si une retry est recommandée"""
        if attempt >= self.config.max_retries:
            return False
        
        # Types d'erreurs qui ne doivent pas être retryées
        non_retryable_errors = (
            FileNotFoundError,
            PermissionError,
            ValueError,
            TypeError
        )
        
        if isinstance(exception, non_retryable_errors):
            self.logger.warning(f"Erreur non-retryable: {type(exception).__name__}")
            return False
        
        return True
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Exécute une fonction avec retry automatique"""
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                self.logger.info(f"Tentative {attempt + 1}/{self.config.max_retries + 1}")
                result = func(*args, **kwargs)
                
                if attempt > 0:
                    self.logger.info(f"Succès après {attempt + 1} tentatives")
                
                return result
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Tentative {attempt + 1} échouée: {str(e)}")
                
                if not self.should_retry(attempt, e):
                    self.logger.error(f"Arrêt des tentatives: {str(e)}")
                    break
                
                if attempt < self.config.max_retries:
                    delay = self.calculate_delay(attempt)
                    self.logger.info(f"Attente de {delay:.2f}s avant la prochaine tentative")
                    time.sleep(delay)
        
        raise last_exception

# Instances globales (seront initialisées dynamiquement avec les variables Airflow)
circuit_breaker = None
retry_manager = None

def initialize_components():
    """Initialise les composants avec les variables Airflow"""
    global circuit_breaker, retry_manager
    
    # Configuration
    retry_config = RetryConfig(
        max_retries=get_max_retries(),
        base_delay=BASE_DELAY,
        max_delay=MAX_DELAY,
        jitter_factor=JITTER_FACTOR
    )
    
    circuit_config = CircuitBreakerConfig(
        failure_threshold=get_circuit_breaker_threshold(),
        timeout=CIRCUIT_BREAKER_TIMEOUT
    )
    
    circuit_breaker = CircuitBreaker(circuit_config)
    retry_manager = RetryManager(retry_config)
    
    logger.info(f"Composants initialisés - Retry: {retry_config.max_retries}, Circuit Breaker: {circuit_config.failure_threshold}")

def move_to_dead_letter(file_path: str, error_message: str, context: dict) -> str:
    """Déplace un fichier vers le dead letter queue dans le cas d'erreur"""
    logger.info(f"Déplacement vers dead letter queue: {file_path}")
    
    # Créer le répertoire dead letter
    os.makedirs(DEAD_LETTER_DIR, exist_ok=True)
    
    # Générer le nom du fichier dans le dead letter queue
    filename = os.path.basename(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dead_letter_filename = f"failed_{timestamp}_{filename}"
    dead_letter_path = os.path.join(DEAD_LETTER_DIR, dead_letter_filename)
    
    # Déplacer le fichier
    if os.path.exists(file_path):
        shutil.move(file_path, dead_letter_path)
        logger.info(f"Fichier déplacé vers dead letter: {dead_letter_path}")
    
    # Créer un fichier de métadonnées d'erreur
    error_metadata = {
        'original_file': file_path,
        'dead_letter_file': dead_letter_path,
        'error_message': error_message,
        'timestamp': datetime.now().isoformat(),
        'dag_run_id': context.get('dag_run', {}).get('run_id', 'unknown'),
        'task_instance_id': context.get('task_instance', {}).get('task_id', 'unknown')
    }
    
    metadata_file = f"{dead_letter_path}.error.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(error_metadata, f, indent=2, ensure_ascii=False)
    
    logger.error(f"Fichier envoyé au dead letter queue: {dead_letter_filename}")
    return dead_letter_path

def update_file_status(file_hash: str, status: ProcessingStatus, error_message: str = None, **context):
    """Met à jour le statut d'un fichier en base de données"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        update_sql = """
        UPDATE processed_files 
        SET status = %s, processed_at = CURRENT_TIMESTAMP
        WHERE file_hash = %s
        """
        
        hook.run(update_sql, parameters=[status.value, file_hash])
        
        if error_message:
            # Ajouter l'erreur dans un champ séparé 
            error_sql = """
            UPDATE processed_files 
            SET status = %s, processed_at = CURRENT_TIMESTAMP
            WHERE file_hash = %s
            """
            hook.run(error_sql, parameters=[status.value, file_hash])
        
        logger.info(f"Statut mis à jour pour {file_hash[:8]}...: {status.value}")
        
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour du statut: {e}")

def check_database_connection(**context):
    """Vérifie la connexion à la base de données et l'existence de la table"""
    logger.info("Vérification de la connexion à la base de données")
    
    # Initialiser les composants avec les variables Airflow
    initialize_components()
    
    try:
        # Vérifier le circuit breaker
        if not circuit_breaker.can_execute():
            logger.warning("Circuit breaker ouvert - connexion DB refusée")
            raise Exception("Circuit breaker ouvert - service temporairement indisponible")
        
        # Connexion à la base de données Airflow avec retry
        def _connect_to_db():
            hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Vérifier que la table existe
            check_table_sql = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'processed_files' 
            AND table_schema = 'public'
            """
            
            result = hook.get_first(check_table_sql)
            return hook, result
        
        hook, result = retry_manager.execute_with_retry(_connect_to_db)
        circuit_breaker.record_success()
        
        if result[0] > 0:
            logger.info("✓ Table processed_files trouvée")
            
            # Vérifier le nombre d'enregistrements
            count_sql = "SELECT COUNT(*) FROM processed_files"
            count_result = hook.get_first(count_sql)
            logger.info(f"✓ {count_result[0]} fichiers déjà traités enregistrés")
            
            return {
                'table_exists': True,
                'processed_count': count_result[0],
                'status': 'ready',
                'circuit_breaker_state': circuit_breaker.state.value,
                'input_directory': get_input_dir(),
                'chunk_size': get_chunk_size()
            }
        else:
            logger.warning("⚠ Table processed_files non trouvée - elle devrait être créée par init-scripts.sql")
            return {
                'table_exists': False,
                'processed_count': 0,
                'status': 'table_missing',
                'circuit_breaker_state': circuit_breaker.state.value,
                'input_directory': get_input_dir(),
                'chunk_size': get_chunk_size()
            }
            
    except Exception as e:
        circuit_breaker.record_failure()
        logger.error(f"✗ Erreur de connexion à la base de données: {e}")
        raise Exception(f"Impossible de se connecter à la base de données: {e}")

def get_unprocessed_files(**context):
    """Récupère tous les fichiers non traités dans le répertoire d'entrée"""
    # Récupérer le répertoire d'entrée depuis les variables Airflow
    input_dir = get_input_dir()
    logger.info(f"Recherche de fichiers non traités dans: {input_dir}")
    
    # Créer le répertoire s'il n'existe pas
    os.makedirs(input_dir, exist_ok=True)
    
    # Chercher tous les fichiers texte
    txt_files = []
    for file in os.listdir(input_dir):
        if file.endswith('.txt') and os.path.isfile(os.path.join(input_dir, file)):
            file_path = os.path.join(input_dir, file)
            txt_files.append(file_path)
    
    if not txt_files:
        # Créer un fichier de test si aucun fichier n'existe
        test_file_path = os.path.join(input_dir, 'sample_input.txt')
        sample_text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris dignissim, 
        erat nec tristique cursus, diam quam varius nisi, non lobortis libero leo 
        tincidunt est. Aliquam blandit erat id elit ornare, vitae rutrum risus 
        ultricies. Suspendisse potenti. Mauris at mauris ac orci euismod vestibulum.
        Donec cursus auctor nisi pharetra imperdiet. In ac egestas turpis, commodo 
        tincidunt lacus. Quisque nec justo in magna rhoncus eleifend. Aliquam 
        suscipit sapien sed interdum molestie. Nullam non velit iaculis, elementum
        est at, sagittis eros. Nam ut mattis lacus. Duis auctor condimentum nisi, 
        nec pretium nunc fringilla ut. Donec eget condimentum ipsum.
        """
        with open(test_file_path, 'w', encoding='utf-8') as f:
            f.write(sample_text.strip())
        print(f"Fichier de test créé: {test_file_path}")
        txt_files = [test_file_path]
    
    # Connexion à la base de données
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Calculer les hash des fichiers et vérifier s'ils sont déjà traités
    unprocessed_files = []
    processed_count = 0
    
    for file_path in txt_files:
        # Calculer le hash du fichier
        with open(file_path, 'rb') as f:
            file_content = f.read()
        file_hash = hashlib.md5(file_content).hexdigest()
        file_size = len(file_content)
        
        # Vérifier si le fichier a déjà été traité
        check_sql = "SELECT COUNT(*) FROM processed_files WHERE file_hash = %s"
        result = hook.get_first(check_sql, parameters=[file_hash])
        
        if result[0] == 0:
            # Fichier non traité
            unprocessed_files.append({
                'file_path': file_path,
                'file_hash': file_hash,
                'file_size': file_size,
                'filename': os.path.basename(file_path)
            })
            print(f"✓ Fichier à traiter: {os.path.basename(file_path)} (hash: {file_hash[:8]}...)")
        else:
            processed_count += 1
            print(f"⚠ Fichier déjà traité: {os.path.basename(file_path)} (hash: {file_hash[:8]}...)")
    
    print(f"Résumé: {len(unprocessed_files)} fichiers à traiter, {processed_count} déjà traités")
    
    if not unprocessed_files:
        print("Aucun nouveau fichier à traiter")
        return []
    
    # Stocker la liste des fichiers non traités dans XCom
    context['task_instance'].xcom_push(key='unprocessed_files', value=unprocessed_files)
    
    return {
        'total_files': len(txt_files),
        'unprocessed_count': len(unprocessed_files),
        'processed_count': processed_count,
        'files_to_process': [f['filename'] for f in unprocessed_files]
    }

def process_single_file(file_info, **context):
    """Traite un fichier individuel avec gestion d'erreurs avancée"""
    # Initialiser les composants si nécessaire
    initialize_components()
    
    file_path = file_info['file_path']
    file_hash = file_info['file_hash']
    file_size = file_info['file_size']
    filename = file_info['filename']
    
    logger.info(f"Traitement du fichier: {filename} (hash: {file_hash[:8]}...)")
    
    try:
        # Vérifier le circuit breaker
        if not circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker ouvert - traitement de {filename} refusé")
            update_file_status(file_hash, ProcessingStatus.FAILED, "Circuit breaker ouvert", **context)
            move_to_dead_letter(file_path, "Circuit breaker ouvert", context)
            return {
                'filename': filename,
                'file_hash': file_hash,
                'status': 'failed',
                'error': 'Circuit breaker ouvert'
            }
        
        # Marquer comme en cours de traitement
        update_file_status(file_hash, ProcessingStatus.PROCESSING, **context)
        
        def _process_file():
            """Fonction de traitement avec retry automatique"""
            # Lire le fichier
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Calculer le checksum initial
            original_checksum = hashlib.md5(content.encode('utf-8')).hexdigest()
            
            logger.info(f"Fichier lu: {len(content)} caractères, checksum: {original_checksum[:8]}...")
            
            # Créer le répertoire temporaire pour ce fichier
            file_temp_dir = os.path.join(TEMP_DIR, f"file_{file_hash[:8]}")
            os.makedirs(file_temp_dir, exist_ok=True)
            
            try:

                chunk_size = get_chunk_size()
                
                # Segmenter le texte
                chunks = []
                chunk_metadata = []
                
                for i in range(0, len(content), chunk_size):
                    chunk = content[i:i + chunk_size]
                    chunk_id = f"chunk_{i//chunk_size:03d}"
                    
                    # Calculer le checksum du chunk
                    chunk_checksum = hashlib.md5(chunk.encode('utf-8')).hexdigest()
                    
                    # Sauvegarder le chunk
                    chunk_file = os.path.join(file_temp_dir, f"{chunk_id}.txt")
                    with open(chunk_file, 'w', encoding='utf-8') as f:
                        f.write(chunk)
                    
                    chunks.append(chunk_file)
                    chunk_metadata.append({
                        'chunk_id': chunk_id,
                        'chunk_file': chunk_file,
                        'start_pos': i,
                        'end_pos': min(i + chunk_size, len(content)),
                        'size': len(chunk),
                        'checksum': chunk_checksum
                    })
                
                logger.info(f"Segmentation terminée: {len(chunks)} chunks créés")
                
                # Vérifier les chunks
                all_valid = True
                for chunk_info in chunk_metadata:
                    chunk_file = chunk_info['chunk_file']
                    expected_checksum = chunk_info['checksum']
                    
                    if os.path.exists(chunk_file):
                        with open(chunk_file, 'r', encoding='utf-8') as f:
                            chunk_content = f.read()
                        
                        actual_checksum = hashlib.md5(chunk_content.encode('utf-8')).hexdigest()
                        if actual_checksum != expected_checksum:
                            all_valid = False
                            logger.error(f"ERREUR: Chunk {chunk_info['chunk_id']} - Checksum invalide!")
                            break
                
                if not all_valid:
                    raise Exception(f"Échec de la vérification des chunks pour {filename}")
                
                logger.info(f"✓ Vérification des chunks réussie pour {filename}")
                
                # Reconstruire le texte
                sorted_chunks = sorted(chunk_metadata, key=lambda x: x['start_pos'])
                reconstructed_content = ""
                
                for chunk_info in sorted_chunks:
                    chunk_file = chunk_info['chunk_file']
                    with open(chunk_file, 'r', encoding='utf-8') as f:
                        chunk_content = f.read()
                    reconstructed_content += chunk_content
                
                # Vérifier l'intégrité du texte reconstruit
                reconstructed_checksum = hashlib.md5(reconstructed_content.encode('utf-8')).hexdigest()
                
                if reconstructed_checksum != original_checksum:
                    raise Exception(f"Échec de la reconstruction pour {filename} - Checksums différents!")
                
                logger.info(f"✓ Reconstruction réussie pour {filename}")
                
                # Sauvegarder le résultat
                output_filename = f"processed_{filename}"
                output_file_path = os.path.join(OUTPUT_DIR, output_filename)
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    f.write(reconstructed_content)
                
                logger.info(f"✓ Fichier traité sauvegardé: {output_file_path}")
                
                return {
                    'filename': filename,
                    'file_hash': file_hash,
                    'output_file': output_file_path,
                    'status': 'completed'
                }
                
            finally:
                # Nettoyer les fichiers temporaires
                if os.path.exists(file_temp_dir):
                    shutil.rmtree(file_temp_dir)
        
        # Exécuter le traitement avec retry automatique
        result = retry_manager.execute_with_retry(_process_file)
        circuit_breaker.record_success()
        
        # Marquer comme terminé
        update_file_status(file_hash, ProcessingStatus.COMPLETED, **context)
        
        logger.info(f"✓ Traitement réussi pour {filename}")
        return result
        
    except Exception as e:
        circuit_breaker.record_failure()
        error_msg = f"Erreur lors du traitement de {filename}: {str(e)}"
        logger.error(error_msg)
        
        # Marquer comme échoué
        update_file_status(file_hash, ProcessingStatus.FAILED, error_msg, **context)
        
        # Déplacer vers dead letter queue
        dead_letter_path = move_to_dead_letter(file_path, error_msg, context)
        
        return {
            'filename': filename,
            'file_hash': file_hash,
            'status': 'failed',
            'error': error_msg,
            'dead_letter_path': dead_letter_path
        }

def setup_airflow_variables(**context):
    """Configure les variables Airflow par défaut si elles n'existent pas"""
    logger.info("Configuration des variables Airflow")
    
    variables_to_set = {
        'input_directory': DEFAULT_INPUT_DIR,
        'chunk_size': '100',
        'max_retries': '3',
        'circuit_breaker_threshold': '5'
    }
    
    created_variables = []
    existing_variables = []
    
    for var_name, var_value in variables_to_set.items():
        try:
            # Vérifier si la variable existe déjà
            existing_value = Variable.get(var_name)
            existing_variables.append(f"{var_name}={existing_value}")
            logger.info(f"Variable existante: {var_name} = {existing_value}")
        except Exception:
            # Créer la variable si elle n'existe pas
            Variable.set(var_name, var_value)
            created_variables.append(f"{var_name}={var_value}")
            logger.info(f"Variable créée: {var_name} = {var_value}")
    
    return {
        'created_variables': created_variables,
        'existing_variables': existing_variables,
        'total_variables': len(variables_to_set)
    }

def process_all_files(**context):
    """Traite tous les fichiers non traités"""
    # Initialiser les composants si nécessaire
    initialize_components()
    
    logger.info("Début du traitement de tous les fichiers non traités")
    
    # Récupérer la liste des fichiers non traités
    unprocessed_files = context['task_instance'].xcom_pull(key='unprocessed_files')
    
    if not unprocessed_files:
        logger.info("Aucun fichier à traiter")
        return {'processed_count': 0}
    
    results = []
    failed_files = []
    
    for file_info in unprocessed_files:
        try:
            result = process_single_file(file_info, **context)
            results.append(result)
            logger.info(f"✓ Fichier traité avec succès: {file_info['filename']}")
        except Exception as e:
            failed_files.append({
                'filename': file_info['filename'],
                'error': str(e)
            })
            logger.error(f"✗ Erreur lors du traitement de {file_info['filename']}: {e}")
    
    # Enregistrer les résultats en base de données
    if results:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        for result in results:
            insert_sql = """
            INSERT INTO processed_files (file_hash, file_path, file_size, status, output_file)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (file_hash) DO NOTHING
            """
            
            # Récupérer le fichier original pour le chemin
            original_file_path = None
            for file_info in unprocessed_files:
                if file_info['file_hash'] == result['file_hash']:
                    original_file_path = file_info['file_path']
                    break
            
            hook.run(insert_sql, parameters=[
                result['file_hash'],
                original_file_path,
                os.path.getsize(original_file_path),
                result['status'],
                result['output_file']
            ])
    
    logger.info(f"Traitement terminé: {len(results)} succès, {len(failed_files)} échecs")
    
    if failed_files:
        logger.error("Fichiers en échec:")
        for failed in failed_files:
            logger.error(f"  - {failed['filename']}: {failed['error']}")
    
    return {
        'processed_count': len(results),
        'failed_count': len(failed_files),
        'results': results,
        'failed_files': failed_files
    }


# Définition des tâches
# Configuration des variables Airflow
task_setup_variables = PythonOperator(
    task_id='setup_airflow_variables',
    python_callable=setup_airflow_variables,
    dag=dag,
)

# Vérification de la connexion à la base de données
task_check_db = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag,
)

def check_files_and_trigger(**context):
    """Vérifie la présence de fichiers et déclenche le traitement"""
    # Récupérer le répertoire d'entrée depuis les variables Airflow
    try:
        input_dir = get_input_dir()
    except Exception:
        # Fallback vers le répertoire par défaut si les variables ne sont pas disponibles
        input_dir = DEFAULT_INPUT_DIR
        logger.warning(f"Variables Airflow non disponibles, utilisation du répertoire par défaut: {input_dir}")
    
    logger.info(f"Vérification de fichiers dans: {input_dir}")
    
    # Créer le répertoire s'il n'existe pas
    os.makedirs(input_dir, exist_ok=True)
    
    # Chercher des fichiers texte
    txt_files = []
    try:
        for file in os.listdir(input_dir):
            if file.endswith('.txt') and os.path.isfile(os.path.join(input_dir, file)):
                txt_files.append(os.path.join(input_dir, file))
    except FileNotFoundError:
        logger.info(f"Répertoire {input_dir} n'existe pas encore")
        txt_files = []
    
    if txt_files:
        logger.info(f"Fichiers trouvés: {len(txt_files)} fichiers - Pipeline va continuer")
        return True
    else:
        logger.info("Aucun fichier trouvé - Arrêt du pipeline (skip)")
        raise AirflowSkipException("Aucun fichier à traiter dans le répertoire d'entrée")

# Tâche pour vérifier la présence de fichiers
task_check_files = PythonOperator(
    task_id='check_files_and_trigger',
    python_callable=check_files_and_trigger,
    dag=dag,
)

# Tâche pour récupérer les fichiers non traités
task_get_unprocessed_files = PythonOperator(
    task_id='get_unprocessed_files',
    python_callable=get_unprocessed_files,
    dag=dag,
)

# Tâche pour traiter tous les fichiers non traités
task_process_all_files = PythonOperator(
    task_id='process_all_files',
    python_callable=process_all_files,
    dag=dag,
)

# Définition des dépendances du pipeline avec variables Airflow
task_setup_variables >> task_check_db >> task_check_files >> task_get_unprocessed_files >> task_process_all_files
