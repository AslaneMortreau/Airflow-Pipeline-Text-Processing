-- Initialisation de la base de données Airflow
-- (Ces commandes sont déjà exécutées par les variables d'environnement Docker)

-- Création de la table pour tracker les fichiers traités
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    file_hash VARCHAR(32) UNIQUE NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed',
    output_file VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Création des index pour optimiser les performances
CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_processed_files_path ON processed_files(file_path);
CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files(status);
CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at ON processed_files(processed_at);

-- Commentaire sur la table
COMMENT ON TABLE processed_files IS 'Table de tracking des fichiers traités par le pipeline DNA DATA SYMPHONIA';
COMMENT ON COLUMN processed_files.file_hash IS 'Hash MD5 unique du fichier pour éviter les doublons';
COMMENT ON COLUMN processed_files.file_path IS 'Chemin original du fichier traité';
COMMENT ON COLUMN processed_files.status IS 'Statut du traitement: completed, failed, processing';
