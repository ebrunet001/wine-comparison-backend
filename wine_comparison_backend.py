#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backend Flask pour la comparaison de caves à vin
Version optimisée pour production sur Render
"""

import os
import io
import re
import unicodedata
import logging
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from werkzeug.utils import secure_filename
from datetime import datetime
import traceback

app = Flask(__name__)

# Configuration CORS complète pour production
CORS(app, 
     resources={r"/api/*": {"origins": "*"}},
     allow_headers=["Content-Type"],
     methods=["GET", "POST", "OPTIONS"],
     supports_credentials=False)

# Configuration du logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configuration - Utiliser /tmp sur Render
UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

# Créer le dossier uploads s'il n'existe pas
try:
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    logger.info(f"Dossier upload configuré: {UPLOAD_FOLDER}")
except Exception as e:
    logger.error(f"Erreur création dossier: {e}")

# Liste des termes indiquant des accessoires (à exclure)
ACCESSORY_KEYWORDS = [
    'caisse', 'carton', 'coffret', 'box', 'étui', 'emballage',
    'tire-bouchon', 'décanteur', 'verre', 'flute', 'coupe',
    'seau', 'rafraîchisseur', 'thermomètre', 'bouchon', 'capsule',
    'catalogue', 'livre', 'book', 'poster', 'affiche',
    'gift', 'cadeau', 'accessoire', 'outil', 'support'
]

def allowed_file(filename):
    """Vérifier si le fichier est autorisé"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def normalize_string(s):
    """
    Normalise une chaîne pour le fuzzy matching:
    - Supprime les accents
    - Convertit en minuscules
    - Supprime les caractères spéciaux
    - Supprime les espaces multiples
    """
    if pd.isna(s) or s == '':
        return ''
    
    # Convertir en string
    s = str(s)
    
    # Supprimer les accents
    s = unicodedata.normalize('NFD', s)
    s = ''.join(char for char in s if unicodedata.category(char) != 'Mn')
    
    # Convertir en minuscules
    s = s.lower()
    
    # Supprimer tous les caractères spéciaux sauf espaces et lettres/chiffres
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    
    # Supprimer les espaces multiples et trim
    s = ' '.join(s.split())
    
    return s

def is_accessory(wine_name):
    """Détermine si un article est un accessoire basé sur son nom"""
    if pd.isna(wine_name):
        return False
    
    wine_name_lower = str(wine_name).lower()
    
    for keyword in ACCESSORY_KEYWORDS:
        if keyword in wine_name_lower:
            logger.debug(f"Accessoire détecté: {wine_name} (mot-clé: {keyword})")
            return True
    
    return False

def normalize_vintage(vintage):
    """Normalise le millésime"""
    if pd.isna(vintage) or vintage == '' or vintage == 'NV':
        return 1000
    
    try:
        # Essayer de convertir en entier
        vintage_str = str(vintage).strip()
        
        # Gérer les cas comme "2019-2020"
        if '-' in vintage_str:
            vintage_str = vintage_str.split('-')[0]
        
        # Extraire les 4 chiffres
        match = re.search(r'\b(19\d{2}|20\d{2})\b', vintage_str)
        if match:
            return int(match.group(1))
        
        # Tenter une conversion directe
        vintage_int = int(float(vintage_str))
        
        # Vérifier que c'est une année valide
        if 1900 <= vintage_int <= 2030:
            return vintage_int
        else:
            return 1000
            
    except (ValueError, TypeError):
        return 1000

def normalize_size(size, unit='cl'):
    """Normalise la contenance en cl"""
    if pd.isna(size) or size == '':
        return 75  # Défaut: bouteille standard
    
    try:
        size_str = str(size).lower().strip()
        
        # Extraire le nombre
        number_match = re.search(r'(\d+(?:[.,]\d+)?)', size_str)
        if not number_match:
            return 75
        
        number = float(number_match.group(1).replace(',', '.'))
        
        # Déterminer l'unité
        if unit == 'L' or 'l' in size_str or 'litre' in size_str:
            # Convertir de litres en cl
            return int(number * 100)
        elif 'ml' in size_str:
            # Convertir de ml en cl
            return int(number / 10)
        else:
            # Supposer que c'est déjà en cl
            return int(number)
            
    except (ValueError, TypeError):
        return 75

def extract_lwin7(lwin_str):
    """Extrait le LWIN7 d'une chaîne"""
    if pd.isna(lwin_str) or lwin_str == '':
        return None
    
    lwin_str = str(lwin_str).strip()
    
    # Retirer le préfixe "LWIN" s'il existe
    lwin_str = re.sub(r'^LWIN', '', lwin_str, flags=re.IGNORECASE)
    
    # Extraire les 7 chiffres
    match = re.search(r'(\d{7})', lwin_str)
    if match:
        return match.group(1)
    
    # Si on a moins de 7 chiffres, les padder avec des zéros
    digits = re.sub(r'\D', '', lwin_str)
    if digits and len(digits) <= 7:
        return digits.zfill(7)
    
    return None

def create_lwin16(lwin7, vintage, size_cl):
    """Crée un LWIN16 à partir des composants"""
    if not lwin7:
        return None
    
    vintage_str = str(vintage).zfill(4)
    size_str = str(size_cl).zfill(5)
    
    return f"{lwin7}{vintage_str}{size_str}"

def load_csv_file(filepath, delimiter=','):
    """Charge un fichier CSV avec gestion d'encodage"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            # Charger sans interpréter la première ligne comme header
            df = pd.read_csv(filepath, delimiter=delimiter, encoding=encoding, 
                           header=0,  # Première ligne est le header
                           dtype=str, 
                           na_values=['', 'NA', 'N/A', 'null', 'NULL'])
            
            logger.info(f"CSV chargé avec l'encodage {encoding}")
            logger.info(f"Colonnes détectées: {len(df.columns)}")
            logger.info(f"Nombre de lignes: {len(df)}")
            
            # Log des premières lignes pour debug
            if len(df) > 0:
                logger.debug(f"Première ligne de données: {df.iloc[0].tolist()[:10]}...")
            
            return df
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            continue
    
    raise ValueError("Impossible de lire le fichier CSV avec les encodages disponibles")

def process_livredecave(df):
    """Traite le fichier Livre de Cave"""
    processed_data = []
    
    # Convertir les colonnes en liste pour accès par index
    # C=2, E=4, F=5, G=6, H=7, I=8, K=10 (indices 0-based)
    
    for idx, row in df.iterrows():
        try:
            # Récupérer le nom du vin (colonnes C, E, F, G = indices 2, 4, 5, 6)
            wine_parts = []
            col_indices = [2, 4, 5, 6]  # C, E, F, G
            
            for col_idx in col_indices:
                if col_idx < len(row) and pd.notna(row.iloc[col_idx]) and str(row.iloc[col_idx]).strip():
                    wine_parts.append(str(row.iloc[col_idx]).strip())
            
            wine_name = ' '.join(wine_parts)
            
            # Ignorer les lignes vides
            if not wine_name:
                continue
            
            # Ignorer les accessoires
            if is_accessory(wine_name):
                logger.info(f"Ligne {idx+2}: Accessoire ignoré - {wine_name}")
                continue
            
            # Extraire les autres données
            # H = index 7 (millésime)
            vintage = normalize_vintage(row.iloc[7] if len(row) > 7 else '')
            
            # I = index 8 (contenance en L)
            size_l = row.iloc[8] if len(row) > 8 else ''
            size_cl = normalize_size(size_l, unit='L')
            
            # K = index 10 (LWIN7)
            lwin7 = extract_lwin7(row.iloc[10] if len(row) > 10 else '')
            
            # Créer le LWIN16
            lwin16 = create_lwin16(lwin7, vintage, size_cl) if lwin7 else None
            
            # Normaliser le nom pour le fuzzy matching
            wine_normalized = normalize_string(wine_name)
            
            logger.debug(f"Livre de Cave - Ligne {idx+2}: {wine_name[:50]}... | Millésime: {vintage} | Taille: {size_cl}cl | LWIN7: {lwin7}")
            
            processed_data.append({
                'original_row': idx + 2,  # +2 pour l'index Excel (header + 0-based)
                'wine': wine_name,
                'wine_normalized': wine_normalized,
                'vintage': vintage,
                'contenance_cl': size_cl,
                'lwin7': lwin7,
                'lwin16': lwin16
            })
            
        except Exception as e:
            logger.error(f"Erreur ligne {idx+2}: {str(e)}")
            continue
    
    return pd.DataFrame(processed_data)

def process_googlesheet(df):
    """Traite le fichier Google Sheet"""
    processed_data = []
    
    # Colonnes Google Sheet:
    # A=0 (producteur/vin), C=2 (millésime), D=3 (contenance cl), G=6 (LWIN7)
    
    for idx, row in df.iterrows():
        try:
            # Nom du vin (colonne A = index 0)
            wine_name = str(row.iloc[0]).strip() if len(row) > 0 and pd.notna(row.iloc[0]) else ''
            
            # Ignorer les lignes vides
            if not wine_name:
                continue
            
            # Ignorer les accessoires
            if is_accessory(wine_name):
                logger.info(f"Ligne {idx+2}: Accessoire ignoré dans Google Sheet - {wine_name}")
                continue
            
            # Millésime (colonne C = index 2)
            vintage = normalize_vintage(row.iloc[2] if len(row) > 2 else '')
            
            # Contenance (colonne D = index 3, déjà en cl)
            size_cl = normalize_size(row.iloc[3] if len(row) > 3 else '', unit='cl')
            
            # LWIN7 (colonne G = index 6)
            lwin7 = extract_lwin7(row.iloc[6] if len(row) > 6 else '')
            
            # Créer le LWIN16
            lwin16 = create_lwin16(lwin7, vintage, size_cl) if lwin7 else None
            
            # Normaliser le nom pour le fuzzy matching
            wine_normalized = normalize_string(wine_name)
            
            logger.debug(f"Google Sheet - Ligne {idx+2}: {wine_name[:50]}... | Millésime: {vintage} | Taille: {size_cl}cl | LWIN7: {lwin7}")
            
            processed_data.append({
                'wine': wine_name,
                'wine_normalized': wine_normalized,
                'vintage': vintage,
                'contenance_cl': size_cl,
                'lwin7': lwin7,
                'lwin16': lwin16
            })
            
        except Exception as e:
            logger.error(f"Erreur ligne {idx+2} Google Sheet: {str(e)}")
            continue
    
    return pd.DataFrame(processed_data)

def fuzzy_match_wines(livredecave_df, googlesheet_df):
    """Effectue le matching entre les deux dataframes"""
    missing_wines = []
    
    # Créer des index pour optimiser les recherches
    gs_lwin16_set = set(googlesheet_df[googlesheet_df['lwin16'].notna()]['lwin16'])
    
    # Log pour debug
    logger.info(f"Matching de {len(livredecave_df)} vins du livre de cave contre {len(googlesheet_df)} vins du Google Sheet")
    logger.info(f"LWIN16 disponibles dans Google Sheet: {len(gs_lwin16_set)}")
    
    for idx, ld_row in livredecave_df.iterrows():
        matched = False
        match_type = 'none'
        reason = ''
        
        # 1. Essayer le matching par LWIN16
        if ld_row['lwin16'] and ld_row['lwin16'] in gs_lwin16_set:
            matched = True
            match_type = 'LWIN16'
            logger.debug(f"✓ Match LWIN16: {ld_row['wine']}")
            continue
        
        # 2. Si pas de LWIN ou pas de match, essayer le fuzzy matching
        if not matched and ld_row['wine_normalized']:
            best_score = 0
            best_match = None
            best_match_idx = -1
            threshold = 70  # Seuil abaissé à 70%
            
            # Calculer les scores pour tous les vins du Google Sheet
            for gs_idx, gs_row in googlesheet_df.iterrows():
                # Calculer plusieurs types de scores
                score_ratio = fuzz.ratio(ld_row['wine_normalized'], gs_row['wine_normalized'])
                score_partial = fuzz.partial_ratio(ld_row['wine_normalized'], gs_row['wine_normalized'])
                score_token = fuzz.token_sort_ratio(ld_row['wine_normalized'], gs_row['wine_normalized'])
                
                # Prendre le meilleur score des trois méthodes
                score = max(score_ratio, score_partial, score_token)
                
                if score > best_score:
                    best_score = score
                    best_match = gs_row
                    best_match_idx = gs_idx
            
            # Log du meilleur match trouvé
            if best_match is not None:
                logger.debug(f"Meilleur match pour '{ld_row['wine'][:50]}...' (millésime: {ld_row['vintage']}, {ld_row['contenance_cl']}cl)")
                logger.debug(f"  -> '{best_match['wine'][:50]}...' (millésime: {best_match['vintage']}, {best_match['contenance_cl']}cl)")
                logger.debug(f"  Score: {best_score}%")
            
            if best_score >= threshold and best_match is not None:
                # Vérifier le millésime et la contenance
                vintage_match = (ld_row['vintage'] == best_match['vintage']) or \
                               (ld_row['vintage'] == 1000) or \
                               (best_match['vintage'] == 1000)
                
                # Tolérance plus large pour la contenance
                size_match = (abs(ld_row['contenance_cl'] - best_match['contenance_cl']) <= 10) or \
                            (ld_row['contenance_cl'] == 75 and best_match['contenance_cl'] == 75)
                
                if vintage_match and size_match:
                    matched = True
                    match_type = f'Fuzzy ({best_score}%)'
                    logger.info(f"✓ Match Fuzzy confirmé: '{ld_row['wine'][:40]}...' -> '{best_match['wine'][:40]}...' (score: {best_score}%)")
                else:
                    reason = f"Fuzzy match trouvé ({best_score}%) mais "
                    if not vintage_match:
                        reason += f"millésime différent ({ld_row['vintage']} vs {best_match['vintage']}) "
                    if not size_match:
                        reason += f"contenance différente ({ld_row['contenance_cl']}cl vs {best_match['contenance_cl']}cl)"
                    logger.debug(f"✗ Match rejeté: {reason}")
            else:
                reason = f"Pas de correspondance fuzzy suffisante (meilleur score: {best_score}%)"
                # Log détaillé pour les vins importants
                if "roulot" in ld_row['wine_normalized'] or "desjourneys" in ld_row['wine_normalized']:
                    logger.warning(f"⚠️ Vin important non trouvé: '{ld_row['wine']}'")
                    logger.warning(f"  Nom normalisé: '{ld_row['wine_normalized']}'")
                    logger.warning(f"  Meilleur score: {best_score}%")
        
        # Si pas de match, ajouter aux vins manquants
        if not matched:
            missing_wines.append({
                'original_row': ld_row['original_row'],
                'wine': ld_row['wine'],
                'vintage': ld_row['vintage'] if ld_row['vintage'] != 1000 else 'NV',
                'contenance_cl': ld_row['contenance_cl'],
                'lwin7': ld_row['lwin7'],
                'match_type': match_type,
                'reason': reason or 'Aucune correspondance trouvée'
            })
    
    logger.info(f"Résultat du matching: {len(missing_wines)} vins manquants sur {len(livredecave_df)}")
    
    return missing_wines

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de santé"""
    return jsonify({'status': 'healthy', 'version': '2.0'})

@app.route('/api/compare', methods=['OPTIONS'])
def handle_compare_options():
    """Gestion du preflight CORS pour /api/compare"""
    return '', 204

@app.route('/api/compare', methods=['POST'])
def compare_files():
    """Endpoint principal pour comparer les fichiers"""
    try:
        # Vérifier les fichiers
        if 'livredecave' not in request.files or 'googlesheet' not in request.files:
            return jsonify({'error': 'Les deux fichiers sont requis'}), 400
        
        ld_file = request.files['livredecave']
        gs_file = request.files['googlesheet']
        
        # Sauvegarder temporairement les fichiers
        ld_filename = secure_filename(ld_file.filename)
        gs_filename = secure_filename(gs_file.filename)
        
        ld_path = os.path.join(UPLOAD_FOLDER, f"ld_{datetime.now().timestamp()}_{ld_filename}")
        gs_path = os.path.join(UPLOAD_FOLDER, f"gs_{datetime.now().timestamp()}_{gs_filename}")
        
        ld_file.save(ld_path)
        gs_file.save(gs_path)
        
        logger.info("=" * 60)
        logger.info("DÉBUT DE LA COMPARAISON")
        logger.info("=" * 60)
        
        # Charger les fichiers
        livredecave_df = load_csv_file(ld_path, delimiter=';')
        googlesheet_df = load_csv_file(gs_path, delimiter=',')
        
        logger.info(f"Livre de cave: {len(livredecave_df)} lignes, {len(livredecave_df.columns)} colonnes")
        logger.info(f"Google Sheet: {len(googlesheet_df)} lignes, {len(googlesheet_df.columns)} colonnes")
        
        # Traiter les données
        ld_processed = process_livredecave(livredecave_df)
        gs_processed = process_googlesheet(googlesheet_df)
        
        logger.info(f"\nLivre de cave après traitement: {len(ld_processed)} vins (accessoires exclus)")
        logger.info(f"Google Sheet après traitement: {len(gs_processed)} vins (accessoires exclus)")
        
        # Effectuer le matching
        missing_wines = fuzzy_match_wines(ld_processed, gs_processed)
        
        # Préparer les résultats
        results = {
            'livredecave_total': len(ld_processed),
            'googlesheet_total': len(gs_processed),
            'missing_count': len(missing_wines),
            'missing_wines': missing_wines,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info(f"RÉSULTAT: {len(missing_wines)} vins manquants sur {len(ld_processed)}")
        logger.info("=" * 60)
        
        # Nettoyer les fichiers temporaires
        try:
            os.remove(ld_path)
            os.remove(gs_path)
        except Exception as e:
            logger.error(f"Erreur lors de la suppression des fichiers temporaires: {e}")
        
        return jsonify({'success': True, 'results': results})
        
    except Exception as e:
        logger.error(f"Erreur lors de la comparaison: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<download_type>', methods=['OPTIONS'])
def handle_download_options(download_type):
    """Gestion du preflight CORS pour /api/download"""
    return '', 204

@app.route('/api/download/<download_type>', methods=['POST'])
def download_results(download_type):
    """Télécharger les résultats en CSV"""
    try:
        data = request.json
        if not data or 'results' not in data:
            return jsonify({'error': 'Données manquantes'}), 400
        
        results = data['results']
        
        if download_type == 'missing':
            # Créer un DataFrame avec les vins manquants
            df = pd.DataFrame(results['missing_wines'])
            
            # Créer un CSV en mémoire
            output = io.StringIO()
            df.to_csv(output, index=False, encoding='utf-8-sig')
            output.seek(0)
            
            # Convertir en bytes
            output_bytes = io.BytesIO(output.getvalue().encode('utf-8-sig'))
            
            return send_file(
                output_bytes,
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'vins_manquants_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        else:
            return jsonify({'error': 'Type de téléchargement non supporté'}), 400
        
    except Exception as e:
        logger.error(f"Erreur download: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Gestion globale des erreurs"""
    logger.error(f"Erreur non gérée: {str(e)}")
    logger.error(traceback.format_exc())
    return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🍷 Serveur de comparaison de caves - Version Production")
    print("=" * 60)
    print("Améliorations:")
    print("  ✓ Fuzzy matching avec normalisation avancée")
    print("  ✓ Suppression automatique des caractères spéciaux")
    print("  ✓ Exclusion des accessoires")
    print("  ✓ Seuil de similarité: 70%")
    print("  ✓ Configuration CORS pour production")
    print("  ✓ Utilisation de /tmp pour Render")
    print("=" * 60)
    print("Démarrage sur http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)