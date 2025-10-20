"""
Module pour l'encodage ADN avec mapping Goldman et codes correcteurs Reed-Solomon
"""

import hashlib
import logging
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass
import reedsolo
from bitstring import BitArray

logger = logging.getLogger(__name__)

@dataclass
class DNAChunk:
    """Représente un chunk ADN avec ses métadonnées"""
    chunk_id: str
    index: int
    dna_sequence: str
    original_binary: str
    original_length_bytes: int
    error_correction_code: bytes
    checksum: str
    chunk_size: int

class GoldmanEncoder:
    """
    Encodeur ADN utilisant le mapping Goldman pour convertir les données binaires en séquences ADN
    Le système Goldman utilise des trits (base 3) et évite les homopolymères
    """
    
    # Bases ADN disponibles pour le mapping Goldman
    DNA_BASES = ['A', 'C', 'G', 'T']
    
    def __init__(self, chunk_size: int = 1000, error_correction_symbols: int = 10):
        """
        Initialise l'encodeur Goldman
        
        Args:
            chunk_size: Taille des chunks en caractères
            error_correction_symbols: Nombre de symboles de correction d'erreur Reed-Solomon
        """
        self.chunk_size = chunk_size
        self.error_correction_symbols = error_correction_symbols
        self.rs_codec = reedsolo.RSCodec(error_correction_symbols)
        
    def bytes_to_trits(self, data: bytes) -> List[int]:
        """Convertit des octets en trits (base 3)."""
        if not data:
            return [0]
        value = int.from_bytes(data, byteorder='big', signed=False)
        trits: List[int] = []
        if value == 0:
            trits = [0]
        else:
            while value > 0:
                value, r = divmod(value, 3)
                trits.append(r)
            trits.reverse()
        logger.info(f"Octets convertis en trits: {len(data)} bytes -> {len(trits)} trits")
        return trits

    def trits_to_bytes(self, trits: List[int], target_length_bytes: int) -> bytes:
        """Convertit des trits en octets de longueur fixe (big-endian)."""
        value = 0
        for t in trits:
            value = value * 3 + int(t)
        # Convertir en octets, padding à gauche si nécessaire
        byte_length = max(1, target_length_bytes)
        data = value.to_bytes(byte_length, byteorder='big', signed=False)
        if len(data) < target_length_bytes:
            data = (b"\x00" * (target_length_bytes - len(data))) + data
        elif len(data) > target_length_bytes:
            # Tronquer à droite si trop long (ne devrait pas arriver si target_length correct)
            data = data[-target_length_bytes:]
        return data

    def binary_to_trits(self, binary_string: str) -> List[int]:
        """
        Convertit une chaîne binaire en trits (base 3)
        
        Args:
            binary_string: Chaîne binaire à convertir
            
        Returns:
            Liste des trits
        """
        # Convertir la chaîne binaire en entier
        binary_int = int(binary_string, 2)
        
        # Convertir en base 3 (trits)
        trits = []
        if binary_int == 0:
            trits = [0]
        else:
            while binary_int > 0:
                trits.append(binary_int % 3)
                binary_int //= 3
        
        # Inverser pour avoir l'ordre correct
        trits.reverse()
        
        logger.info(f"Binaire converti en trits: {len(binary_string)} bits -> {len(trits)} trits")
        return trits
    
    def trits_to_binary(self, trits: List[int], target_bit_length: int = None) -> str:
        """
        Convertit des trits en chaîne binaire
        
        Args:
            trits: Liste des trits
            target_bit_length: Longueur binaire souhaitée (padding gauche avec des '0')
            
        Returns:
            Chaîne binaire correspondante
        """
        # Convertir les trits en entier
        trit_int = 0
        for trit in trits:
            trit_int = trit_int * 3 + trit
        
        # Convertir en binaire
        binary_string = bin(trit_int)[2:]  # Enlever le préfixe '0b'
        
        # Préserver la longueur si demandée
        if target_bit_length is not None and len(binary_string) < target_bit_length:
            binary_string = binary_string.zfill(target_bit_length)
        
        logger.info(f"Trits convertis en binaire: {len(trits)} trits -> {len(binary_string)} bits")
        return binary_string
    
    def trits_to_dna(self, trits: List[int]) -> str:
        """
        Convertit des trits en séquence ADN selon le mapping Goldman
        Évite les homopolymères en choisissant une base différente de la précédente
        
        Args:
            trits: Liste des trits à convertir
            
        Returns:
            Séquences ADN correspondante
        """
        if not trits:
            return ""
        
        dna_sequence = ""
        previous_base = None
        
        for trit in trits:
            # Choisir une base différente de la précédente
            if previous_base is None:
                # Premier trit, utiliser les 3 premières bases (A, C, G)
                base = self.DNA_BASES[trit % 3]
            else:
                # Éviter l'homopolymère en choisissant parmi les autres bases
                available_bases = [b for b in self.DNA_BASES if b != previous_base]
                base = available_bases[trit % len(available_bases)]
            
            dna_sequence += base
            previous_base = base
        
        logger.info(f"Trits convertis en ADN: {len(trits)} trits -> {len(dna_sequence)} bases")
        return dna_sequence
    
    def dna_to_trits(self, dna_sequence: str) -> List[int]:
        """
        Convertit une séquence ADN en trits selon le mapping Goldman
        
        Args:
            dna_sequence: Séquences ADN à convertir
            
        Returns:
            Liste des trits correspondants
        """
        trits = []
        previous_base = None
        
        for base in dna_sequence.upper():
            if previous_base is None:
                # Premier trit, utiliser les 3 premières bases (A, C, G)
                trit = self.DNA_BASES[:3].index(base) if base in self.DNA_BASES[:3] else 0
            else:
                # Reconstruire le trit en évitant l'homopolymère
                available_bases = [b for b in self.DNA_BASES if b != previous_base]
                try:
                    trit = available_bases.index(base)
                except ValueError:
                    trit = 0  # Défaut si base non reconnue
            
            trits.append(trit)
            previous_base = base
        
        logger.info(f"ADN converti en trits: {len(dna_sequence)} bases -> {len(trits)} trits")
        return trits

    def bytes_to_dna(self, data: bytes) -> str:
        """Conversion directe octets -> trits -> ADN (Goldman)."""
        trits = self.bytes_to_trits(data)
        return self.trits_to_dna(trits)

    def dna_to_bytes(self, dna_sequence: str, target_length_bytes: int) -> bytes:
        """Conversion directe ADN -> trits -> octets (Goldman)."""
        trits = self.dna_to_trits(dna_sequence)
        return self.trits_to_bytes(trits, target_length_bytes)

    def process_bytes_chunk(self, data: bytes) -> str:
        """
        Traite un chunk d'octets en le convertissant en séquence ADN
        Effectue la conversion: octets → trits → nucléotides (Goldman)
        
        Args:
            data: Chunk d'octets à traiter
            
        Returns:
            Séquences ADN correspondante
        """
        logger.info(f"Traitement d'un chunk octets de {len(data)} bytes")
        
        # Conversion octets → trits → ADN
        dna_sequence = self.bytes_to_dna(data)
        
        # Vérifier qu'il n'y a pas d'homopolymères
        has_homopolymers = any(dna_sequence[i] == dna_sequence[i+1] for i in range(len(dna_sequence)-1))
        if has_homopolymers:
            logger.warning("⚠ Homopolymères détectés dans la séquence ADN!")
        else:
            logger.info("✓ Aucun homopolymère détecté")
        
        logger.info(f"Chunk traité: {len(data)} bytes → {len(dna_sequence)} bases ADN")
        return dna_sequence
    
    def add_error_correction(self, data: bytes) -> bytes:
        """
        Ajoute des codes de correction d'erreur Reed-Solomon
        
        Args:
            data: Données à protéger
            
        Returns:
            Données avec codes de correction
        """
        try:
            encoded_data = self.rs_codec.encode(data)
            logger.info(f"Codes de correction ajoutés: {len(data)} -> {len(encoded_data)} bytes")
            return encoded_data
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout des codes de correction: {e}")
            raise
    
    def decode_with_error_correction(self, encoded_data: bytes) -> bytes:
        """
        Décode les données en corrigeant les erreurs avec Reed-Solomon
        
        Args:
            encoded_data: Données encodées avec codes de correction
            
        Returns:
            Données décodées et corrigées
        """
        try:
            decoded_data, decoded, errata_pos = self.rs_codec.decode(encoded_data)
            logger.info(f"Données décodées avec correction: {len(encoded_data)} -> {len(decoded_data)} bytes")
            if errata_pos:
                logger.info(f"Erreurs corrigées aux positions: {errata_pos}")
            return decoded_data
        except Exception as e:
            logger.error(f"Erreur lors du décodage avec correction: {e}")
            raise
    
    def create_dna_chunks(self, text: str) -> List[DNAChunk]:
        """
        Crée des chunks ADN à partir d'un texte
        
        Args:
            text: Texte à traiter
            
        Returns:
            Liste des chunks ADN
        """
        logger.info(f"Création de chunks ADN pour un texte de {len(text)} caractères")
        
        # Diviser le texte UTF-8 en chunks d'octets de taille appropriée sans casser les code points
        chunks = []
        chunk_index = 0
        
        utf8_bytes = text.encode('utf-8')
        start = 0
        max_bytes = max(1, self.chunk_size)  # réutilise chunk_size comme taille en octets
        
        while start < len(utf8_bytes):
            end = min(start + max_bytes, len(utf8_bytes))
            # Ajuster pour ne pas couper un code point UTF-8
            slice_bytes = utf8_bytes[start:end]
            while True:
                try:
                    slice_bytes.decode('utf-8')
                    break
                except UnicodeDecodeError:
                    end -= 1
                    if end <= start:
                        # Si impossible, forcer au moins 1 octet
                        end = start + 1
                        slice_bytes = utf8_bytes[start:end]
                        break
                    slice_bytes = utf8_bytes[start:end]
            
            # Ajout RS sur les octets du chunk
            chunk_bytes = slice_bytes
            protected_data = self.add_error_correction(chunk_bytes)
            
            # Calculer le checksum du chunk original (octets)
            chunk_checksum = hashlib.md5(chunk_bytes).hexdigest()
            
            # Conversion octets -> ADN via trits
            dna_sequence = self.bytes_to_dna(chunk_bytes)
            
            # Créer le chunk ADN
            dna_chunk = DNAChunk(
                chunk_id=f"dna_chunk_{chunk_index:03d}",
                index=chunk_index,
                dna_sequence=dna_sequence,
                original_binary="",
                original_length_bytes=len(chunk_bytes),
                error_correction_code=protected_data[len(chunk_bytes):],  # Codes de correction seulement
                checksum=chunk_checksum,
                chunk_size=len(chunk_bytes)
            )
            
            chunks.append(dna_chunk)
            chunk_index += 1
            start = end
            logger.info(f"Chunk {chunk_index} créé: {len(chunk_bytes)} bytes → {len(dna_sequence)} bases ADN")
        
        logger.info(f"Création terminée: {len(chunks)} chunks ADN générés")
        return chunks
    
    def reconstruct_text_from_chunks(self, dna_chunks: List[DNAChunk]) -> str:
        """
        Reconstruit le texte original à partir des chunks ADN
        
        Args:
            dna_chunks: Liste des chunks ADN
            
        Returns:
            Texte reconstruit
        """
        logger.info(f"Reconstruction du texte à partir de {len(dna_chunks)} chunks ADN")
        
        # Trier les chunks par index
        sorted_chunks = sorted(dna_chunks, key=lambda x: x.index)
        
        reconstructed_text = ""
        
        for chunk in sorted_chunks:
            try:
                # Décoder ADN -> octets en préservant la longueur d'origine
                chunk_bytes = self.dna_to_bytes(chunk.dna_sequence, target_length_bytes=chunk.original_length_bytes)
                chunk_text = chunk_bytes.decode('utf-8')
                
                reconstructed_text += chunk_text
                
                logger.info(f"Chunk {chunk.chunk_id} reconstruit: {len(chunk_text)} caractères")
                
            except Exception as e:
                logger.error(f"Erreur lors de la reconstruction du chunk {chunk.chunk_id}: {e}")
                raise
        
        logger.info(f"Reconstruction terminée: {len(reconstructed_text)} caractères")
        return reconstructed_text
    
    def validate_chunk_integrity(self, chunk: DNAChunk) -> bool:
        """
        Valide l'intégrité d'un chunk ADN
        
        Args:
            chunk: Chunk ADN à valider
            
        Returns:
            True si le chunk est valide, False sinon
        """
        try:
            # Reconstruire le texte du chunk via ADN -> octets (longueur originale en bytes)
            chunk_bytes = self.dna_to_bytes(chunk.dna_sequence, target_length_bytes=chunk.original_length_bytes)
            chunk_text = chunk_bytes.decode('utf-8')
            
            # Vérifier le checksum sur les octets
            calculated_checksum = hashlib.md5(chunk_bytes).hexdigest()
            
            if calculated_checksum != chunk.checksum:
                logger.error(f"Checksum invalide pour le chunk {chunk.chunk_id}")
                return False
            
            logger.info(f"Chunk {chunk.chunk_id} validé avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la validation du chunk {chunk.chunk_id}: {e}")
            return False

class DNAProcessor:
    """
    Processeur principal pour le pipeline ADN
    """
    
    def __init__(self, chunk_size: int = 1000, error_correction_symbols: int = 10):
        """
        Initialise le processeur ADN
        
        Args:
            chunk_size: Taille des chunks en caractères
            error_correction_symbols: Nombre de symboles de correction d'erreur
        """
        self.encoder = GoldmanEncoder(chunk_size, error_correction_symbols)
        self.chunk_size = chunk_size
        
    def process_text_file(self, file_path: str) -> Dict[str, Any]:
        """
        Traite un fichier texte en le convertissant en chunks ADN
        
        Args:
            file_path: Chemin vers le fichier à traiter
            
        Returns:
            Dictionnaire contenant les métadonnées du traitement
        """
        logger.info(f"Traitement du fichier ADN: {file_path}")
        
        try:
            # Lire le fichier
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Calculer le checksum original
            original_checksum = hashlib.md5(content.encode('utf-8')).hexdigest()
            
            # Créer les chunks ADN
            dna_chunks = self.encoder.create_dna_chunks(content)
            
            # Valider l'intégrité des chunks
            valid_chunks = []
            for chunk in dna_chunks:
                if self.encoder.validate_chunk_integrity(chunk):
                    valid_chunks.append(chunk)
                else:
                    logger.error(f"Chunk invalide détecté: {chunk.chunk_id}")
                    raise Exception(f"Chunk invalide: {chunk.chunk_id}")
            
            # Test de reconstruction
            reconstructed_text = self.encoder.reconstruct_text_from_chunks(valid_chunks)
            reconstructed_checksum = hashlib.md5(reconstructed_text.encode('utf-8')).hexdigest()
            
            if reconstructed_checksum != original_checksum:
                logger.error("Échec de la reconstruction - Checksums différents!")
                raise Exception("Échec de la reconstruction")
            
            logger.info("✓ Reconstruction réussie - Intégrité validée")
            
            return {
                'file_path': file_path,
                'original_checksum': original_checksum,
                'reconstructed_checksum': reconstructed_checksum,
                'dna_chunks_count': len(dna_chunks),
                'total_dna_bases': sum(len(chunk.dna_sequence) for chunk in dna_chunks),
                'chunk_size': self.chunk_size,
                'error_correction_symbols': self.encoder.error_correction_symbols,
                'status': 'success',
                'dna_chunks': dna_chunks
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement ADN: {e}")
            return {
                'file_path': file_path,
                'status': 'error',
                'error_message': str(e)
            }
