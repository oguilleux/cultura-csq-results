# Contentsquare Results

Outil web qui exporte les KPIs Contentsquare dans un fichier Excel.

## Prérequis

- **Python 3** installé sur votre machine
- Un fichier **`.env`** à la racine du projet contenant vos identifiants API Contentsquare

## Installation

Ouvrir un terminal dans le dossier du projet, puis lancer :

```bash
pip install flask python-dotenv requests openpyxl
```

## Lancement

```bash
python web.py
```

Puis ouvrir **http://localhost:3000** dans votre navigateur.

## Utilisation

1. Remplir les champs (dates, segments, etc.) dans l'interface web
2. Cliquer sur **Lancer** pour lancer l'export
3. Une fois terminé, cliquer sur **Télécharger** pour récupérer le fichier Excel

## Configuration par défaut

Les valeurs par défaut sont modifiables dans le fichier `contentsquare_config.py` :

| Paramètre | Description |
|---|---|
| `START_DATE` / `END_DATE` | Période d'analyse (format `YYYY-MM-DD`) |
| `SEGMENT_IDS_TO_ANALYZE` | Liste des IDs de segments |
| `ANALYZE_BY_DEVICE` | Ventilation par device (desktop/mobile/tablet) |
| `PAGE_GROUP_ID` | ID du groupe de pages |
| `GOAL_IDS` | Liste des IDs d'objectifs |
