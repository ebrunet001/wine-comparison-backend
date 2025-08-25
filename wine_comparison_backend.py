#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import os
import io
import tempfile
import logging
from unidecode import unidecode

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

logger.info(f"Dossier upload configuré: {UPLOAD_FOLDER}")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def normalize_string(s):
    """Normalise une chaîne pour la comparaison"""
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = unidecode(s)
    s = ' '.join(s.split())
    return s

def format_contenance(value):
    """Formate la contenance en format LWIN16"""
    if pd.isna(value):
        return None
    
    try:
        if isinstance(value, str):
            value = value.replace(',', '.').replace('L', '').replace('l', '').strip()
        
        value = float(value)
        
        if value > 100:
            value = value / 100
        
        value_int = int(value * 1000)
        
        return f"{value_int:05d}"
    except:
        return None

def create_lwin16(row):
    """Crée la référence LWIN16"""
    if pd.isna(row.get('lwin7')):
        return None
    
    lwin7 = str(row['lwin7']).strip()
    if not lwin7 or lwin7 == 'None':
        return None
    
    try:
        millesime = row.get('millesime', 1000)
        if pd.isna(millesime) or str(millesime).upper() in ['NV', 'N/A', '']:
            millesime = 1000
        else:
            millesime = int(float(millesime))
    except:
        millesime = 1000
    
    contenance = format_contenance(row.get('contenance_cl', row.get('contenance_l')))
    if not contenance:
        return None
    
    return f"{lwin7}{millesime:04d}{contenance}"

def is_accessory(wine_name):
    """Détecte si c'est un accessoire"""
    if pd.isna(wine_name):
        return False
    
    wine_lower = str(wine_name).lower()
    accessory_keywords = ['box', 'carton', 'caisse', 'accessoire', 'coffret', 
                         'etui', 'magnum box', 'gift', 'emballage', 'seau', 
                         'support', 'presentoir', 'glaciere']
    return any(keyword in wine_lower for keyword in accessory_keywords)

def process_livre_cave(df):
    """Traite le fichier Livre de Cave"""
    logger.info(f"Livre de cave: {len(df)} lignes, {len(df.columns)} colonnes")
    
    processed_data = []
    for idx, row in df.iterrows():
        try:
            wine = ' '.join(filter(pd.notna, [
                row.iloc[2] if len(row) > 2 else None,
                row.iloc[4] if len(row) > 4 else None,
                row.iloc[5] if len(row) > 5 else None,
                row.iloc[6] if len(row) > 6 else None
            ]))
            
            if is_accessory(wine):
                logger.info(f"Ligne {idx+1}: Accessoire ignoré - {wine[:50]}")
                continue
            
            millesime = row.iloc[7] if len(row) > 7 else None
            contenance_l = row.iloc[8] if len(row) > 8 else None
            lwin7_raw = row.iloc[10] if len(row) > 10 else None
            
            try:
                lwin7 = str(int(float(lwin7_raw))) if pd.notna(lwin7_raw) and str(lwin7_raw).strip() != '' else None
            except:
                lwin7 = None
            
            contenance_cl = None
            if pd.notna(contenance_l):
                try:
                    cont_str = str(contenance_l).replace(',', '.').replace('L', '').replace('l', '').strip()
                    cont_val = float(cont_str)
                    contenance_cl = cont_val * 100
                except:
                    contenance_cl = None
            
            if wine and wine.strip():
                data_row = {
                    'wine': wine,
                    'millesime': millesime,
                    'contenance_l': contenance_l,
                    'contenance_cl': contenance_cl,
                    'lwin7': lwin7
                }
                
                data_row['lwin16'] = create_lwin16(data_row)
                data_row['wine_normalized'] = normalize_string(wine)
                processed_data.append(data_row)
                
                logger.debug(f"Livre de Cave - Ligne {idx+1}: {wine[:50]}... | Millésime: {millesime} | Taille: {int(contenance_cl) if contenance_cl else 'N/A'}cl | LWIN7: {lwin7}")
        
        except Exception as e:
            logger.error(f"Erreur ligne {idx+1} du livre de cave: {e}")
            continue
    
    return pd.DataFrame(processed_data)

def process_google_sheet(df):
    """Traite le fichier Google Sheet"""
    logger.info(f"Google Sheet: {len(df)} lignes, {len(df.columns)} colonnes")
    
    processed_data = []
    for idx, row in df.iterrows():
        try:
            wine = row.iloc[0] if len(row) > 0 else None
            
            if is_accessory(wine):
                logger.info(f"Ligne {idx+1}: Accessoire ignoré dans Google Sheet - {wine[:50] if wine else 'N/A'}")
                continue
            
            millesime = row.iloc[2] if len(row) > 2 else None
            contenance_cl = row.iloc[3] if len(row) > 3 else None
            
            lwin7_col = row.iloc[6] if len(row) > 6 else None
            lwin7 = None
            if pd.notna(lwin7_col):
                lwin7_str = str(lwin7_col)
                if 'LWIN' in lwin7_str.upper():
                    lwin7 = lwin7_str.upper().replace('LWIN', '').strip()
                else:
                    lwin7 = lwin7_str.strip()
                
                try:
                    lwin7 = str(int(float(lwin7)))
                except:
                    lwin7 = lwin7
            
            if wine and wine.strip():
                data_row = {
                    'wine': wine,
                    'millesime': millesime,
                    'contenance_cl': contenance_cl,
                    'lwin7': lwin7
                }
                
                data_row['lwin16'] = create_lwin16(data_row)
                data_row['wine_normalized'] = normalize_string(wine)
                processed_data.append(data_row)
                
                logger.debug(f"Google Sheet - Ligne {idx+1}: {wine[:50] if wine else 'N/A'}... | Millésime: {millesime} | Taille: {contenance_cl}cl | LWIN7: {lwin7}")
        
        except Exception as e:
            logger.error(f"Erreur ligne {idx+1} du Google Sheet: {e}")
            continue
    
    return pd.DataFrame(processed_data)

def check_millesime_match(ld_row, gs_row):
    """Vérifie si les millésimes correspondent"""
    ld_millesime = ld_row.get('millesime')
    gs_millesime = gs_row.get('millesime')
    
    if pd.isna(ld_millesime) or pd.isna(gs_millesime):
        return True
    
    ld_str = str(ld_millesime).strip().upper()
    gs_str = str(gs_millesime).strip().upper()
    
    nv_values = ['NV', 'N/A', '1000', '']
    
    if ld_str in nv_values and gs_str in nv_values:
        return True
    
    if ld_str in nv_values or gs_str in nv_values:
        return True
    
    try:
        return abs(float(ld_millesime) - float(gs_millesime)) <= 1
    except:
        return ld_str == gs_str

def check_contenance_match(ld_row, gs_row):
    """Vérifie si les contenances correspondent"""
    ld_cont = ld_row.get('contenance_cl')
    gs_cont = gs_row.get('contenance_cl')
    
    if pd.isna(ld_cont) or pd.isna(gs_cont):
        return True
    
    try:
        ld_val = float(str(ld_cont).replace(',', '.'))
        gs_val = float(str(gs_cont).replace(',', '.'))
        
        ratio = ld_val / gs_val if gs_val != 0 else 0
        
        valid_ratios = [1.0, 0.01, 100.0, 0.1, 10.0]
        
        for valid_ratio in valid_ratios:
            if 0.95 <= (ratio / valid_ratio) <= 1.05:
                return True
        
        return False
    except:
        return False

def fuzzy_match_wines(ld_df, gs_df):
    """Version optimisée du fuzzy matching avec cache et early exit"""
    logger.info(f"Matching de {len(ld_df)} vins du livre de cave contre {len(gs_df)} vins du Google Sheet")
    
    missing_wines = []
    matched_count = 0
    fuzzy_matched_count = 0
    
    # Créer des index pour accélération
    gs_by_lwin16 = {}
    for _, gs_row in gs_df.iterrows():
        if pd.notna(gs_row.get('lwin16')):
            gs_by_lwin16[gs_row['lwin16']] = gs_row
    
    logger.info(f"LWIN16 disponibles dans Google Sheet: {len(gs_by_lwin16)}")
    
    # Cache pour les comparaisons déjà effectuées
    comparison_cache = {}
    
    # Pré-calculer les normalisations pour le Google Sheet
    gs_normalized = []
    for _, gs_row in gs_df.iterrows():
        wine_norm = normalize_string(str(gs_row.get('wine', '')))
        gs_normalized.append({
            'row': gs_row,
            'wine_norm': wine_norm,
            'wine_tokens': set(wine_norm.lower().split())
        })
    
    total_rows = len(ld_df)
    
    for idx, (_, ld_row) in enumerate(ld_df.iterrows()):
        # Progress logging tous les 50 items
        if idx % 50 == 0:
            logger.info(f"Progression: {idx}/{total_rows} vins traités...")
        
        wine_display = f"{ld_row.get('wine', 'Sans nom')[:50]}..."
        matched = False
        
        # 1. Vérifier d'abord par LWIN16 (le plus rapide)
        if pd.notna(ld_row.get('lwin16')) and ld_row['lwin16'] in gs_by_lwin16:
            matched = True
            matched_count += 1
            continue
        
        # 2. Si pas de LWIN7, fuzzy matching
        if pd.isna(ld_row.get('lwin7')):
            ld_wine_norm = normalize_string(str(ld_row.get('wine', '')))
            ld_tokens = set(ld_wine_norm.lower().split())
            
            best_match = None
            best_score = 0
            
            # Pré-filtrer par tokens communs (beaucoup plus rapide)
            candidates = []
            for gs_data in gs_normalized:
                # Si au moins 2 tokens en commun, c'est un candidat
                if len(ld_tokens & gs_data['wine_tokens']) >= 2:
                    candidates.append(gs_data)
            
            # Ne faire le fuzzy matching que sur les candidats
            for gs_data in candidates[:100]:  # Limiter à 100 meilleurs candidats
                gs_row = gs_data['row']
                
                # Utiliser le cache
                cache_key = (ld_wine_norm, gs_data['wine_norm'])
                if cache_key in comparison_cache:
                    score = comparison_cache[cache_key]
                else:
                    score = fuzz.ratio(ld_wine_norm, gs_data['wine_norm'])
                    comparison_cache[cache_key] = score
                
                if score > best_score:
                    best_score = score
                    best_match = gs_row
                
                # Early exit si match parfait
                if score >= 95:
                    break
            
            # Vérifier si le meilleur match est acceptable
            if best_match is not None and best_score >= 40:
                # Vérifier millésime et contenance
                if check_millesime_match(ld_row, best_match) and check_contenance_match(ld_row, best_match):
                    matched = True
                    fuzzy_matched_count += 1
                    logger.debug(f"✓ Fuzzy match accepté: {wine_display} -> {best_match.get('wine', '')[:50]}... (score: {best_score}%)")
                else:
                    logger.debug(f"✗ Match rejeté: Fuzzy match trouvé ({best_score}%) mais millésime ou contenance différent")
        
        # Si toujours pas de match, ajouter aux manquants
        if not matched:
            missing_wines.append({
                'wine': ld_row.get('wine', ''),
                'millesime': ld_row.get('millesime', ''),
                'contenance': ld_row.get('contenance_cl', ''),
                'lwin7': ld_row.get('lwin7', ''),
                'reason': 'Non trouvé dans le Google Sheet'
            })
    
    logger.info(f"Résultats: {matched_count} matches LWIN16, {fuzzy_matched_count} fuzzy matches, {len(missing_wines)} manquants")
    return missing_wines

@app.route('/')
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'Wine Comparison API',
        'version': '1.0.0'
    }), 200

@app.route('/api/test')
def test():
    return jsonify({'status': 'API is running'}), 200

@app.route('/api/compare', methods=['POST'])
def compare_files():
    logger.info("=" * 60)
    logger.info("DÉBUT DE LA COMPARAISON")
    logger.info("=" * 60)
    
    try:
        if 'livredecave' not in request.files or 'googlesheet' not in request.files:
            return jsonify({'error': 'Les deux fichiers sont requis'}), 400
        
        ld_file = request.files['livredecave']
        gs_file = request.files['googlesheet']
        
        if not allowed_file(ld_file.filename) or not allowed_file(gs_file.filename):
            return jsonify({'error': 'Format de fichier non supporté'}), 400
        
        # Lecture des fichiers
        if ld_file.filename.endswith('.csv'):
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            ld_df = None
            for encoding in encodings:
                try:
                    ld_file.seek(0)
                    ld_df = pd.read_csv(ld_file, encoding=encoding, sep=None, engine='python')
                    logger.info(f"CSV chargé avec l'encodage {encoding}")
                    break
                except:
                    continue
            if ld_df is None:
                raise ValueError("Impossible de lire le fichier CSV")
        else:
            ld_df = pd.read_excel(ld_file)
        
        logger.info(f"Colonnes détectées: {len(ld_df.columns)}")
        logger.info(f"Nombre de lignes: {len(ld_df)}")
        logger.debug(f"Première ligne de données: {list(ld_df.iloc[0] if len(ld_df) > 0 else [])}...")
        
        if gs_file.filename.endswith('.csv'):
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            gs_df = None
            for encoding in encodings:
                try:
                    gs_file.seek(0)
                    gs_df = pd.read_csv(gs_file, encoding=encoding, sep=None, engine='python')
                    logger.info(f"CSV chargé avec l'encodage {encoding}")
                    break
                except:
                    continue
            if gs_df is None:
                raise ValueError("Impossible de lire le fichier CSV")
        else:
            gs_df = pd.read_excel(gs_file)
        
        logger.info(f"Colonnes détectées: {len(gs_df.columns)}")
        logger.info(f"Nombre de lignes: {len(gs_df)}")
        logger.debug(f"Première ligne de données: {list(gs_df.iloc[0] if len(gs_df) > 0 else [])}...")
        
        logger.info(f"Livre de cave: {len(ld_df)} lignes, {len(ld_df.columns)} colonnes")
        logger.info(f"Google Sheet: {len(gs_df)} lignes, {len(gs_df.columns)} colonnes")
        
        # Traitement des données
        ld_processed = process_livre_cave(ld_df)
        gs_processed = process_google_sheet(gs_df)
        
        logger.info(f"\nLivre de cave après traitement: {len(ld_processed)} vins (accessoires exclus)")
        logger.info(f"Google Sheet après traitement: {len(gs_processed)} vins (accessoires exclus)")
        
        # Comparaison
        missing_wines = fuzzy_match_wines(ld_processed, gs_processed)
        
        # Statistiques
        stats = {
            'total_livredecave': len(ld_processed),
            'total_googlesheet': len(gs_processed),
            'missing_count': len(missing_wines),
            'found_count': len(ld_processed) - len(missing_wines)
        }
        
        logger.info(f"\nRÉSULTATS FINAUX:")
        logger.info(f"  - Total livre de cave: {stats['total_livredecave']}")
        logger.info(f"  - Total Google Sheet: {stats['total_googlesheet']}")
        logger.info(f"  - Vins trouvés: {stats['found_count']}")
        logger.info(f"  - Vins manquants: {stats['missing_count']}")
        
        return jsonify({
            'success': True,
            'stats': stats,
            'missing_wines': missing_wines
        })
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {e}")
        logger.error(f"Traceback: ", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<file_type>', methods=['POST'])
def download_file(file_type):
    try:
        data = request.json
        missing_wines = data.get('missing_wines', [])
        
        if file_type == 'csv':
            df = pd.DataFrame(missing_wines)
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)
            
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8-sig')),
                mimetype='text/csv',
                as_attachment=True,
                download_name='vins_manquants.csv'
            )
        
        elif file_type == 'excel':
            df = pd.DataFrame(missing_wines)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Vins Manquants')
            output.seek(0)
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='vins_manquants.xlsx'
            )
        
        else:
            return jsonify({'error': 'Type de téléchargement non supporté'}), 400
        
    except Exception as e:
        logger.error(f"Erreur download: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🍷 Application de Comparaison de Vins - VERSION OPTIMISÉE")
    print("=" * 65)
    print("🔧 Améliorations:")
    print("   ✅ Timeout augmenté à 5 minutes")
    print("   ✅ Fuzzy matching optimisé avec cache et pre-filtering")
    print("   ✅ Support python-Levenshtein sur Linux")
    print("   ✅ Progress logging pendant le traitement")
    print("")
    print("🚀 Démarrage du serveur sur http://localhost:5000")
    print("📋 Utilisez Ctrl+C pour arrêter le serveur")
    print("=" * 65)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
