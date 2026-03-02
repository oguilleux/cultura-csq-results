#!/usr/bin/env python3
"""
Script ContentSquare - Conversions E-commerce (VERSION DOCUMENTÉE)
Utilise les endpoints officiels de la Metrics API ContentSquare
"""

import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from contentsquare_config import PAGE_GROUP_MAPPING_ID as CONFIG_PAGE_GROUP_MAPPING_ID
except Exception:
    CONFIG_PAGE_GROUP_MAPPING_ID = 2066672

# Charger les identifiants
load_dotenv()
CLIENT_ID = os.getenv("CS_CLIENT_ID")
CLIENT_SECRET = os.getenv("CS_CLIENT_SECRET")
PROJECT_ID = os.getenv("CS_PROJECT_ID")

SEGMENT_IDS_TO_ANALYZE = [5684436]
ANALYZE_BY_DEVICE = True
DAYS_TO_ANALYZE = 30
GOAL_ID = None
EXPORT_DIR = "exports"


def resolve_mapping_id(value, default=2066672):
    if value is None:
        return default
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


PAGE_GROUP_MAPPING_ID = resolve_mapping_id(CONFIG_PAGE_GROUP_MAPPING_ID)


def get_token(client_id, client_secret, project_id):
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
    return data.get("access_token"), data.get("endpoint")


def get_segments(endpoint, token, project_id):
    url = f"{endpoint}/v1/segments"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"projectId": project_id}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_goals(endpoint, token, project_id):
    url = f"{endpoint}/v1/goals"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"projectId": project_id}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_mappings(endpoint, token, project_id):
    url = f"{endpoint}/v1/mappings"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"projectId": project_id}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_mapping_page_groups(endpoint, token, project_id, mapping_id):
    url = f"{endpoint}/v1/mappings/{mapping_id}/page-groups"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"projectId": project_id}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_page_groups_for_mapping(endpoint, token, project_id, mapping_id):
    mappings_data = get_mappings(endpoint, token, project_id)
    mappings = mappings_data.get("payload", [])
    mapping_name = ""
    for mapping in mappings:
        if str(mapping.get("id")) == str(mapping_id):
            mapping_name = mapping.get("name", "")
            break

    page_groups_data = get_mapping_page_groups(endpoint, token, project_id, mapping_id)
    page_groups = page_groups_data.get("payload", [])

    result = []
    for page_group in page_groups:
        page_group_id = page_group.get("id")
        if page_group_id is None:
            continue
        result.append(
            {
                "id": page_group_id,
                "name": page_group.get("name"),
                "category": page_group.get("category"),
                "mapping_id": mapping_id,
                "mapping_name": mapping_name,
            }
        )

    return sorted(result, key=lambda item: item["id"])


def get_site_metrics(endpoint, token, project_id, start_date, end_date, device="all", segment_ids=None):
    url = f"{endpoint}/v1/metrics/site"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "projectId": project_id,
        "startDate": start_date,
        "endDate": end_date,
        "device": device
    }
    if segment_ids:
        params["segments"] = ",".join(map(str, segment_ids))
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_ecommerce_conversions(endpoint, token, project_id, start_date, end_date, goal_id=None, device="all", segment_ids=None):
    url = f"{endpoint}/v1/metrics/site/conversions"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "projectId": project_id,
        "startDate": start_date,
        "endDate": end_date,
        "device": device
    }
    if goal_id:
        params["goalId"] = goal_id
    if segment_ids:
        params["segments"] = ",".join(map(str, segment_ids))
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_ecommerce_conversion_rate(endpoint, token, project_id, start_date, end_date, goal_id=None, device="all", segment_ids=None):
    url = f"{endpoint}/v1/metrics/site/conversion-rate"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "projectId": project_id,
        "startDate": start_date,
        "endDate": end_date,
        "device": device
    }
    if goal_id:
        params["goalId"] = goal_id
    if segment_ids:
        params["segments"] = ",".join(map(str, segment_ids))
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def extract_metric_value(metrics_data, metric_name, fallback_key=None):
    payload = metrics_data.get("payload", {}) if isinstance(metrics_data, dict) else {}

    # Nouveau format API: payload.values = [{name: "...", value: ...}, ...]
    values = payload.get("values")
    if isinstance(values, list):
        for item in values:
            if isinstance(item, dict) and item.get("name") == metric_name:
                return item.get("value")

    # Ancien format API: payload.<key>
    if fallback_key:
        return payload.get(fallback_key)
    return payload.get(metric_name)


def extract_single_value(metrics_data, preferred_name=None):
    payload = metrics_data.get("payload", {}) if isinstance(metrics_data, dict) else {}

    value = payload.get("value")
    if isinstance(value, (int, float)):
        return value

    values = payload.get("values")
    if isinstance(values, list) and values:
        if preferred_name:
            for item in values:
                if isinstance(item, dict) and item.get("name") == preferred_name:
                    return item.get("value", 0)
        if isinstance(values[0], dict):
            return values[0].get("value", 0)
    return 0


def format_count(value):
    if not isinstance(value, (int, float)):
        return "N/A"
    return f"{int(round(value)):,}"


def format_percentage(value):
    if not isinstance(value, (int, float)):
        return "N/A"
    return f"{value:.2f}%"


def export_ids_file(export_dir, filename_prefix, run_stamp, label, rows):
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    file_path = export_path / f"{filename_prefix}_{run_stamp}.txt"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with file_path.open("w", encoding="utf-8") as f:
        f.write(f"# {label}\n")
        f.write(f"# generated_at_utc: {generated_at}\n")
        f.write(f"# total: {len(rows)}\n")
        f.write("# format: id\\tname\\textra\n")
        for row in rows:
            f.write(f"{row.get('id', '')}\t{row.get('name', '')}\t{row.get('extra', '')}\n")

    return file_path


def display_metrics(label, metrics_data, goal_conversions=None, goal_conversion_rate=None):
    sessions = extract_metric_value(metrics_data, "visits", fallback_key="sessions")
    pageviews = extract_metric_value(metrics_data, "pageviews", fallback_key="pageviews")
    pageview_average = extract_metric_value(metrics_data, "pageviewAverage")
    bounce_rate = extract_metric_value(metrics_data, "bounceRate", fallback_key="bounceRate")

    conversions = extract_metric_value(metrics_data, "conversionCount")
    if conversions is None and goal_conversions:
        conversions = extract_single_value(goal_conversions, preferred_name="conversionCount")

    conv_rate = extract_metric_value(metrics_data, "conversionRate")
    if conv_rate is None and goal_conversion_rate:
        conv_rate = extract_single_value(goal_conversion_rate, preferred_name="conversionRate")

    print(f"\n{label}:")
    print(f"  Sessions/visites:   {format_count(sessions)}")
    if isinstance(pageviews, (int, float)):
        print(f"  Pageviews:          {format_count(pageviews)}")
    elif isinstance(pageview_average, (int, float)):
        print(f"  Pages / session:    {pageview_average:.2f}")
    else:
        print("  Pageviews:          N/A")
    print(f"  Taux de rebond:     {format_percentage(bounce_rate)}")
    if isinstance(conversions, (int, float)):
        print(f"  Conversions:        {format_count(conversions)}")
    if isinstance(conv_rate, (int, float)):
        print(f"  Taux de conversion: {format_percentage(conv_rate)}")


def main():
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    print("="*70)
    print("📊 CONTENTSQUARE - CONVERSIONS E-COMMERCE")
    print("="*70)
    print()
    print("🔧 Configuration:")
    print(f"   Segments: {SEGMENT_IDS_TO_ANALYZE}")
    print(f"   By device: {ANALYZE_BY_DEVICE}")
    print(f"   Période: {DAYS_TO_ANALYZE} jours")
    print(f"   Goal ID: {GOAL_ID}")
    print(f"   Page-group mapping ID (export): {PAGE_GROUP_MAPPING_ID}")
    print(f"   Export dir: {EXPORT_DIR}")
    print()

    # 1. Authentification
    try:
        token, endpoint = get_token(CLIENT_ID, CLIENT_SECRET, PROJECT_ID)
        print("✅ Token généré avec succès !")
        print(f"📡 Endpoint: {endpoint}\n")
    except Exception as e:
        print(f"❌ Erreur d'authentification: {e}")
        return

    # 2. Dates
    days = min(DAYS_TO_ANALYZE, 92)
    end_date = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
    start_date = end_date - timedelta(days=days)
    start_date = start_date.replace(hour=0, minute=0, second=0)
    start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_date_iso = end_date.strftime("%Y-%m-%dT%H:%M:%S.999Z")
    print(f"📅 Période: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')} ({days} jours)\n")

    # 3. Segments
    try:
        segments_data = get_segments(endpoint, token, PROJECT_ID)
        segments = segments_data.get("payload", [])
        print(f"✅ {len(segments)} segment(s) disponible(s)")
        if segments:
            print("\n📋 Segments disponibles:")
            for seg in segments:
                print(f"  - ID: {seg.get('id')} → {seg.get('name')}")
        print()
    except Exception as e:
        print(f"⚠️ Impossible de lister les segments: {e}")
        segments = []

    segment_rows = [{"id": s.get("id"), "name": s.get("name"), "extra": ""} for s in segments]
    segment_file = export_ids_file(EXPORT_DIR, "segments_ids", run_stamp, "Segment IDs", segment_rows)
    print(f"📝 Segment IDs exportés: {segment_file}")

    # 3bis. Goals
    try:
        goals_data = get_goals(endpoint, token, PROJECT_ID)
        goals = goals_data.get("payload", [])
        print(f"✅ {len(goals)} goal(s) disponible(s)")
    except Exception as e:
        print(f"⚠️ Impossible de lister les goals: {e}")
        goals = []

    goal_rows = [{"id": g.get("id"), "name": g.get("name"), "extra": g.get("type", "")} for g in goals]
    goals_file = export_ids_file(EXPORT_DIR, "goals_ids", run_stamp, "Goal IDs", goal_rows)
    print(f"📝 Goal IDs exportés: {goals_file}")

    # 3ter. Page groups
    try:
        page_groups = get_page_groups_for_mapping(endpoint, token, PROJECT_ID, PAGE_GROUP_MAPPING_ID)
        print(f"✅ {len(page_groups)} page group(s) disponible(s) pour mapping {PAGE_GROUP_MAPPING_ID}")
    except Exception as e:
        print(f"⚠️ Impossible de lister les page groups pour mapping {PAGE_GROUP_MAPPING_ID}: {e}")
        page_groups = []

    page_group_rows = [
        {
            "id": pg.get("id"),
            "name": pg.get("name"),
            "extra": (
                f"mappingId={pg.get('mapping_id', '')};"
                f"mapping={pg.get('mapping_name', '')};"
                f"category={pg.get('category', '')}"
            ),
        }
        for pg in page_groups
    ]
    page_groups_file = export_ids_file(EXPORT_DIR, "page_group_ids", run_stamp, "Page Group IDs", page_group_rows)
    print(f"📝 Page Group IDs exportés: {page_groups_file}")
    print()

    # 4. Métriques globales
    print("-"*70)
    print("📊 MÉTRIQUES GLOBALES (Tous visiteurs)")
    print("-"*70)
    try:
        site_metrics = get_site_metrics(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso)
        goal_conversions = get_ecommerce_conversions(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso)
        goal_conv_rate = get_ecommerce_conversion_rate(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso)
        display_metrics("ALL DEVICES", site_metrics, goal_conversions, goal_conv_rate)
    except Exception as e:
        print(f"❌ Erreur: {e}")
    print()

    # 5. Métriques par device
    if ANALYZE_BY_DEVICE:
        print("-"*70)
        print("📱 MÉTRIQUES PAR DEVICE")
        print("-"*70)
        for device in ["desktop", "mobile", "tablet"]:
            try:
                site_metrics = get_site_metrics(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, device=device)
                goal_conversions = get_ecommerce_conversions(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, device=device)
                goal_conv_rate = get_ecommerce_conversion_rate(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, device=device)
                display_metrics(device.upper(), site_metrics, goal_conversions, goal_conv_rate)
            except Exception as e:
                print(f"\n{device.upper()}: ❌ Erreur - {e}")
        print()

    # 6. Métriques par segment
    if SEGMENT_IDS_TO_ANALYZE:
        print("-"*70)
        print("📊 MÉTRIQUES PAR SEGMENT")
        print("-"*70)
        for seg_id in SEGMENT_IDS_TO_ANALYZE:
            seg_name = f"Segment {seg_id}"
            for seg in segments:
                if seg.get("id") == seg_id:
                    seg_name = seg.get("name")
                    break
            try:
                site_metrics = get_site_metrics(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, segment_ids=[seg_id])
                goal_conversions = get_ecommerce_conversions(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, segment_ids=[seg_id])
                goal_conv_rate = get_ecommerce_conversion_rate(endpoint, token, PROJECT_ID, start_date_iso, end_date_iso, segment_ids=[seg_id])
                display_metrics(seg_name, site_metrics, goal_conversions, goal_conv_rate)
            except Exception as e:
                print(f"\n{seg_name}: ❌ Erreur - {e}")
        print()

    print("="*70)
    print("✅ ANALYSE TERMINÉE")
    print("="*70)
    print()
    print("💡 Pour analyser des segments spécifiques, copie les IDs ci-dessus et ajoute-les dans SEGMENT_IDS_TO_ANALYZE")
    print(f"📅 Période: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')} ({days} jours)\n")


if __name__ == "__main__":
    main()
