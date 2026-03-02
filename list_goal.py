#!/usr/bin/env python3
"""
Liste des goals ContentSquare pour un projet donné
Affiche l'ID et le nom de chaque goal
"""

import os
import requests
from dotenv import load_dotenv

# Charger les identifiants depuis le .env
load_dotenv()
CLIENT_ID = os.getenv("CS_CLIENT_ID")
CLIENT_SECRET = os.getenv("CS_CLIENT_SECRET")
PROJECT_ID = os.getenv("CS_PROJECT_ID")

def get_token(client_id, client_secret, project_id):
    """Obtenir un token d'accès pour l'API ContentSquare"""
    auth_url = "https://api.eu-west-1.production.contentsquare.com/v1/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "metrics",
        "projectId": project_id
    }
    response = requests.post(auth_url, json=payload)
    response.raise_for_status()
    data = response.json()
    token = data.get("access_token")
    endpoint = data.get("endpoint")
    return token, endpoint

def get_goals(endpoint, token, project_id):
    """Récupérer tous les goals du projet"""
    url = f"{endpoint}/v1/goals"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"projectId": project_id}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def main():
    print("="*60)
    print("📋 LISTE DES GOALS CONTENTSQUARE")
    print("="*60)

    try:
        token, endpoint = get_token(CLIENT_ID, CLIENT_SECRET, PROJECT_ID)
        print("✅ Token généré avec succès !")
        print(f"📡 Endpoint API: {endpoint}\n")
    except Exception as e:
        print(f"❌ Erreur d'authentification : {e}")
        return

    try:
        goals_data = get_goals(endpoint, token, PROJECT_ID)
        goals = goals_data.get("payload", [])
        if not goals:
            print("⚠️ Aucun goal trouvé pour ce projet.")
            return

        print(f"✅ {len(goals)} goal(s) trouvé(s) :\n")
        for g in goals:
            goal_id = g.get("id")
            goal_name = g.get("name")
            print(f"ID: {goal_id} → Nom: {goal_name}")

    except Exception as e:
        print(f"❌ Erreur lors de la récupération des goals : {e}")

if __name__ == "__main__":
    main()
