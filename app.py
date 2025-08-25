import os
import logging
import pandas as pd
from flask import Flask, request, jsonify, send_from_directory, render_template_string
from flask_cors import CORS
from fuzzywuzzy import fuzz, process
import re
from datetime import datetime
import traceback

# Configuration
app = Flask(__name__)
CORS(app)

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('wine_comparison_backend')

# Dossiers
UPLOAD_FOLDER = 'uploads'
DOWNLOAD_FOLDER = 'downloads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Template HTML intégré
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🍷 Wine Comparison Tool</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 15px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); overflow: hidden; }
        .header { background: linear-gradient(135deg, #8B0000 0%, #DC143C 100%); color: white; padding: 30px; text-align: center; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }
        .content { padding: 30px; }
        .upload-section { background: #f8f9ff; border-radius: 10px; padding: 25px; margin-bottom: 25px; border: 2px dashed #ddd; }
        .upload-section h3 { color: #8B0000; margin-bottom: 15px; }
        .file-input { width: 100%; padding: 15px; border: 2px solid #ddd; border-radius: 8px; margin-bottom: 15px; font-size: 16px; }
        .btn { background: linear-gradient(135deg, #8B0000 0%, #DC143C 100%); color: white; border: none; padding: 15px 30px; border-radius: 8px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
        .btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(139,0,0,0.3); }
        .results { margin-top: 30px; padding: 20px; background: #f8f9ff; border-radius: 10px; }
        .loading { display: none; text-align: center; color: #8B0000; font-size: 18px; }
        .error { color: #dc3545; background: #fff5f5; padding: 15px; border-radius: 8px; margin: 10px 0; }
        .success { color: #28a745; background: #f5fff5; padding: 15px; border-radius: 8px; margin: 10px 0; }
        .progress { width: 100%; height: 6px; background: #eee; border-radius: 3px; overflow: hidden; margin: 15px 0; }
        .progress-bar { height: 100%; background: linear-gradient(90deg, #8B0000, #DC143C); width: 0%; transition: width 0.3s; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🍷 Wine Comparison Tool</h1>
            <p>Comparez vos fichiers de cave avec votre inventaire Google Sheets</p>
        </div>
        
        <div class="content">
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-section">
                    <h3>📊 Google Sheet (Inventaire)</h3>
                    <input type="file" name="google_sheet" class="file-input" accept=".xlsx,.xls,.csv" required>
                    <small>Format: A (producteur/vin), C (millésime), D (contenance cl), G (LWIN7)</small>
                </div>
                
                <div class="upload-section">
                    <h3>📚 Livre de Cave</h3>
                    <input type="file" name="livre_cave" class="file-input" accept=".xlsx,.xls,.csv" required>
                    <small>Format: C/E/F/G (producteur/vin), H (millésime), I (contenance L), K (LWIN7)</small>
                </div>
                
                <button type="submit" class="btn">🚀 Lancer la comparaison</button>
            </form>
            
            <div class="loading" id="loading">
                <div>🔄 Traitement en cours...</div>
                <div class="progress">
                    <div class="progress-bar" id="progressBar"></div>
                </div>
            </div>
            
            <div class="results" id="results" style="display: none;">
                <h3>📋 Résultats de la comparaison</h3>
                <div id="resultContent"></div>
            </div>
        </div>
    </div>

    <script>
        document.getElementById('uploadForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            const progressBar = document.getElementById('progressBar');
            
            loading.style.display = 'block';
            results.style.display = 'none';
            
            // Animation de la barre de progression
            let progress = 0;
            const progressInterval = setInterval(() => {
                progress += Math.random() * 15;
                if (progress > 90) progress = 90;
                progressBar.style.width = progress + '%';
            }, 500);
            
            try {
                const response = await fetch('/compare', {
                    method: 'POST',
                    body: formData
                });
                
                clearInterval(progressInterval);
                progressBar.style.width = '100%';
                
                const result = await response.json();
                
                loading.style.display = 'none';
                results.style.display = 'block';
                
                if (response.ok) {
                    document.getElementById('resultContent').innerHTML = `
                        <div class="success">✅ Comparaison terminée avec succès!</div>
                        <p><strong>Vins manquants trouvés:</strong> ${result.missing_count}</p>
                        <p><strong>Total analysé:</strong> ${result.total_analyzed}</p>
                        <a href="/download/excel" class="btn" style="display: inline-block; text-decoration: none; margin-top: 15px;">
                            📥 Télécharger les résultats Excel
                        </a>
                    `;
                } else {
                    document.getElementById('resultContent').innerHTML = `
                        <div class="error">❌ Erreur: ${result.error}</div>
                    `;
                }
            } catch (error) {
                clearInterval(progressInterval);
                loading.style.display = 'none';
                results.style.display = 'block';
                document.getElementById('resultContent').innerHTML = `
                    <div class="error">❌ Erreur de communication: ${error.message}</div>
                `;
            }
        });
    </script>
</body>
</html>
'''

def normalize_text(text):
    """Normalise le texte pour la comparaison"""
    if pd.isna(text) or not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_lwin7(lwin_text):
    """Extrait les 7 chiffres du LWIN"""
    if pd.isna(lwin_text):
        return None
    
    lwin_str = str(lwin_text).strip()
    if lwin_str.upper().startswith('LWIN'):
        lwin_str = lwin_str[4:]
    
    digits = re.findall(r'\d+', lwin_str)
    if digits and len(digits[0]) >= 7:
        return digits[0][:7]
    return None

def format_contenance(contenance_value):
    """Formate la contenance en code à 5 chiffres"""
    if pd.isna(contenance_value):
        return "00750"
    
    try:
        contenance = float(contenance_value)
        
        if contenance == 0.75 or contenance == 75 or contenance == 750:
            return "00750"
        elif contenance == 1.5 or contenance == 150 or contenance == 1500:
            return "01500"
        elif contenance == 3.0 or contenance == 300 or contenance == 3000:
            return "03000"
        else:
            contenance_cl = int(contenance * 100) if contenance < 10 else int(contenance)
            return f"{contenance_cl:05d}"
    except:
        return "00750"

def format_vintage(vintage_value):
    """Formate le millésime"""
    if pd.isna(vintage_value) or str(vintage_value).strip() == '':
        return "1000"
    
    vintage_str = str(vintage_value).strip().upper()
    if vintage_str in ['NV', 'NON VINTAGE', '']:
        return "1000"
    
    try:
        vintage = int(float(vintage_str))
        if 1800 <= vintage <= 2030:
            return f"{vintage:04d}"
    except:
        pass
    
    return "1000"

def create_lwin16(lwin7, vintage, contenance):
    """Crée un LWIN16"""
    if not lwin7:
        return None
    
    vintage_formatted = format_vintage(vintage)
    contenance_formatted = format_contenance(contenance)
    
    return f"{lwin7}{vintage_formatted}{contenance_formatted}"

def fuzzy_match_wine(wine_name, candidates, threshold=0.4):
    """Matching fuzzy pour les noms de vins"""
    if not wine_name or not candidates:
        return None, 0
    
    wine_normalized = normalize_text(wine_name)
    if not wine_normalized:
        return None, 0
    
    candidates_normalized = {normalize_text(k): k for k in candidates if normalize_text(k)}
    
    if wine_normalized in candidates_normalized:
        return candidates_normalized[wine_normalized], 100
    
    if candidates_normalized:
        match = process.extractOne(wine_normalized, list(candidates_normalized.keys()), scorer=fuzz.token_sort_ratio)
        if match and match[1] >= threshold * 100:
            return candidates_normalized[match[0]], match[1]
    
    return None, 0

@app.route('/')
def index():
    """Page d'accueil"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    """Health check pour Render"""
    return jsonify({'status': 'ok', 'service': 'wine-comparison'})

@app.route('/compare', methods=['POST'])
def compare_files():
    """Compare les fichiers uploadés"""
    try:
        logger.info("🍷 Début de la comparaison")
        
        if 'google_sheet' not in request.files or 'livre_cave' not in request.files:
            return jsonify({'error': 'Les deux fichiers sont requis'}), 400
        
        google_file = request.files['google_sheet']
        cave_file = request.files['livre_cave']
        
        if google_file.filename == '' or cave_file.filename == '':
            return jsonify({'error': 'Fichiers vides'}), 400
        
        # Sauvegarder les fichiers
        google_path = os.path.join(UPLOAD_FOLDER, 'google_sheet.xlsx')
        cave_path = os.path.join(UPLOAD_FOLDER, 'livre_cave.xlsx')
        
        google_file.save(google_path)
        cave_file.save(cave_path)
        
        # Lire les fichiers
        logger.info("📊 Lecture des fichiers...")
        google_df = pd.read_excel(google_path)
        cave_df = pd.read_excel(cave_path)
        
        logger.info(f"Google Sheet: {len(google_df)} lignes")
        logger.info(f"Livre de Cave: {len(cave_df)} lignes")
        
        # Traitement Google Sheet
        google_df = google_df.dropna(subset=[google_df.columns[0]])
        google_df['wine_name'] = google_df.iloc[:, 0].astype(str)
        google_df['vintage'] = google_df.iloc[:, 2] if len(google_df.columns) > 2 else ""
        google_df['contenance'] = google_df.iloc[:, 3] if len(google_df.columns) > 3 else 75
        google_df['lwin7'] = google_df.iloc[:, 6].apply(extract_lwin7) if len(google_df.columns) > 6 else None
        
        # Traitement Livre de Cave  
        cave_df = cave_df.dropna(how='all')
        cave_df['wine_name'] = (cave_df.iloc[:, 2].astype(str) + " " + 
                               cave_df.iloc[:, 4].astype(str) + " " + 
                               cave_df.iloc[:, 5].astype(str) + " " + 
                               cave_df.iloc[:, 6].astype(str)).str.strip()
        cave_df['vintage'] = cave_df.iloc[:, 7] if len(cave_df.columns) > 7 else ""
        cave_df['contenance'] = cave_df.iloc[:, 8] if len(cave_df.columns) > 8 else 0.75
        cave_df['lwin7'] = cave_df.iloc[:, 10].apply(extract_lwin7) if len(cave_df.columns) > 10 else None
        
        # Créer les LWIN16
        google_df['lwin16'] = google_df.apply(
            lambda row: create_lwin16(row['lwin7'], row['vintage'], row['contenance']), axis=1
        )
        cave_df['lwin16'] = cave_df.apply(
            lambda row: create_lwin16(row['lwin7'], row['vintage'], row['contenance']), axis=1
        )
        
        # Index des LWIN16 du Google Sheet
        google_lwin16_set = set(google_df['lwin16'].dropna())
        google_wine_names = google_df['wine_name'].tolist()
        
        # Trouver les vins manquants
        missing_wines = []
        
        for idx, row in cave_df.iterrows():
            wine_name = row['wine_name']
            lwin16 = row['lwin16']
            vintage = row['vintage']
            contenance = row['contenance']
            
            is_missing = True
            match_method = "Aucun"
            match_score = 0
            
            # 1. Test LWIN16
            if lwin16 and lwin16 in google_lwin16_set:
                is_missing = False
                match_method = "LWIN16"
                match_score = 100
            
            # 2. Fuzzy matching si pas de LWIN
            elif not row['lwin7'] or pd.isna(row['lwin7']):
                matched_wine, score = fuzzy_match_wine(wine_name, google_wine_names)
                if matched_wine:
                    is_missing = False
                    match_method = f"Fuzzy ({score:.0f}%)"
                    match_score = score
            
            if is_missing:
                missing_wines.append({
                    'Producteur/Vin': wine_name,
                    'Millésime': vintage,
                    'Contenance': contenance,
                    'LWIN7': row['lwin7'],
                    'LWIN16': lwin16,
                    'Statut': 'MANQUANT',
                    'Méthode': match_method,
                    'Score': match_score
                })
        
        # Sauvegarder les résultats
        results_df = pd.DataFrame(missing_wines)
        excel_path = os.path.join(DOWNLOAD_FOLDER, 'vins_manquants.xlsx')
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            results_df.to_excel(writer, sheet_name='Vins Manquants', index=False)
        
        logger.info(f"✅ Traitement terminé: {len(missing_wines)} vins manquants sur {len(cave_df)}")
        
        return jsonify({
            'success': True,
            'missing_count': len(missing_wines),
            'total_analyzed': len(cave_df),
            'message': f'Analyse terminée: {len(missing_wines)} vins manquants trouvés'
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/download/<download_type>')
def download_file(download_type):
    """Télécharge les résultats"""
    try:
        if download_type == 'excel':
            filename = 'vins_manquants.xlsx'
            return send_from_directory(DOWNLOAD_FOLDER, filename, as_attachment=True)
        else:
            return jsonify({'error': 'Type de téléchargement non supporté'}), 400
    except Exception as e:
        logger.error(f"Erreur download: {e}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    """Gestionnaire d'erreur 404"""
    return jsonify({'error': 'Page non trouvée'}), 404

@app.errorhandler(500)
def internal_error(e):
    """Gestionnaire d'erreur 500"""
    logger.error(f"Erreur 500: {e}")
    return jsonify({'error': 'Erreur interne du serveur'}), 500

if __name__ == '__main__':
    print("🍷 Application Wine Comparison - Prête pour le déploiement!")
    print("=" * 55)
    print(f"📁 Dossier uploads: {UPLOAD_FOLDER}")
    print(f"📁 Dossier downloads: {DOWNLOAD_FOLDER}")
    print("=" * 55)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)