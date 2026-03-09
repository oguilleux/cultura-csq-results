#!/usr/bin/env python3
"""
Web UI for ContentSquare KPI exporter.
Run: python web.py   ->  http://localhost:3000
"""

import os
import io
import sys
import json
import traceback
from flask import Flask, render_template, request, jsonify, send_file
from dotenv import load_dotenv

load_dotenv()

import app_cs

app = Flask(__name__)


@app.route("/")
def index():
    return render_template(
        "index.html",
        start_date=app_cs.START_DATE or "",
        end_date=app_cs.END_DATE or "",
        by_device=app_cs.ANALYZE_BY_DEVICE,
        page_group_id=app_cs.PAGE_GROUP_ID or "",
        mapping_id=app_cs.PAGE_GROUP_MAPPING_ID,
        segment_ids_json=", ".join(str(s) for s in app_cs.SEGMENT_IDS_TO_ANALYZE),
        goal_ids_json=", ".join(str(g) for g in app_cs.GOAL_IDS),
    )


@app.route("/run", methods=["POST"])
def run_export():
    data = request.get_json()

    app_cs.START_DATE = data.get("start_date") or None
    app_cs.END_DATE = data.get("end_date") or None
    app_cs.SEGMENT_IDS_TO_ANALYZE = app_cs.resolve_segment_ids(
        [s.strip() for s in data.get("segment_ids", "").split(",") if s.strip()]
    )
    app_cs.ANALYZE_BY_DEVICE = bool(data.get("by_device"))
    app_cs.PAGE_GROUP_ID = app_cs.resolve_optional_int(data.get("page_group_id"))
    app_cs.PAGE_GROUP_MAPPING_ID = app_cs.resolve_mapping_id(data.get("mapping_id"))
    app_cs.GOAL_IDS = app_cs.resolve_goal_ids(
        [g.strip() for g in data.get("goal_ids", "").split(",") if g.strip()]
    )

    log_buffer = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = log_buffer

    success = True
    try:
        app_cs.main()
    except Exception:
        print(f"\n--- ERREUR ---\n{traceback.format_exc()}")
        success = False
    finally:
        sys.stdout = old_stdout

    log_output = log_buffer.getvalue()
    return jsonify({"success": success, "log": log_output})


MAPPINGS_CACHE_FILE = os.path.join(app_cs.EXPORT_DIR, "dropdown_mappings.json")
SEGMENTS_FILE = os.path.join(app_cs.EXPORT_DIR, "segments_ids.txt")
GOALS_FILE = os.path.join(app_cs.EXPORT_DIR, "goals_ids.txt")


def parse_ids_txt(filepath):
    """Parse an exported ids .txt file (id\\tname\\textra) into a list of dicts."""
    rows = []
    if not os.path.exists(filepath):
        return rows
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            row_id = parts[0] if len(parts) > 0 else ""
            name = parts[1] if len(parts) > 1 else ""
            extra = parts[2] if len(parts) > 2 else ""
            if row_id:
                try:
                    rows.append({"id": int(row_id), "name": name, "extra": extra})
                except ValueError:
                    continue
    return rows


def parse_page_groups_txt(filepath):
    """Parse page_group_ids.txt, extracting mapping_id from extra field."""
    rows = []
    if not os.path.exists(filepath):
        return rows
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            row_id = parts[0] if len(parts) > 0 else ""
            name = parts[1] if len(parts) > 1 else ""
            extra = parts[2] if len(parts) > 2 else ""
            if not row_id:
                continue
            mapping_id = None
            for kv in extra.split(";"):
                if kv.startswith("mappingId="):
                    mapping_id = int(kv.split("=", 1)[1])
                    break
            rows.append({"id": int(row_id), "name": name, "mapping_id": mapping_id})
    return rows


@app.route("/api/options")
def api_options():
    """Return options for dropdowns, using local cache files or fetching from API."""
    refresh = request.args.get("refresh") == "1"

    need_api = refresh or not (
        os.path.exists(MAPPINGS_CACHE_FILE)
        and os.path.exists(SEGMENTS_FILE)
        and os.path.exists(GOALS_FILE)
    )

    if need_api:
        try:
            token, endpoint = app_cs.get_token(
                app_cs.CLIENT_ID, app_cs.CLIENT_SECRET, app_cs.PROJECT_ID
            )

            # Fetch & save segments
            segments_data = app_cs.get_segments(endpoint, token, app_cs.PROJECT_ID)
            segment_rows = [
                {"id": s.get("id"), "name": s.get("name", ""), "extra": ""}
                for s in segments_data.get("payload", []) if s.get("id") is not None
            ]
            app_cs.export_ids_file(app_cs.EXPORT_DIR, "segments_ids", "Segment IDs", segment_rows)

            # Fetch & save goals
            goals_data = app_cs.get_goals(endpoint, token, app_cs.PROJECT_ID)
            goal_rows = [
                {"id": g.get("id"), "name": g.get("name", ""), "extra": g.get("type", "")}
                for g in goals_data.get("payload", []) if g.get("id") is not None
            ]
            app_cs.export_ids_file(app_cs.EXPORT_DIR, "goals_ids", "Goal IDs", goal_rows)

            # Fetch & save mappings + page groups
            mappings_data = app_cs.get_mappings(endpoint, token, app_cs.PROJECT_ID)
            mappings_raw = mappings_data.get("payload", [])
            mappings = [
                {"id": m["id"], "name": m.get("name", "")}
                for m in mappings_raw if m.get("id") is not None
            ]

            all_page_groups = app_cs.get_all_page_groups(endpoint, token, app_cs.PROJECT_ID)
            page_groups = [
                {
                    "id": pg["id"],
                    "name": pg.get("name", ""),
                    "mapping_id": pg.get("mapping_id"),
                }
                for pg in all_page_groups
            ]

            cached = {"mappings": mappings, "page_groups": page_groups}
            os.makedirs(app_cs.EXPORT_DIR, exist_ok=True)
            with open(MAPPINGS_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cached, f, ensure_ascii=False, indent=2)

        except Exception:
            import traceback
            return jsonify({"error": traceback.format_exc()}), 500

    # Read from local files
    segments = [{"id": r["id"], "name": r["name"]} for r in parse_ids_txt(SEGMENTS_FILE)]
    goals = [{"id": r["id"], "name": r["name"]} for r in parse_ids_txt(GOALS_FILE)]

    with open(MAPPINGS_CACHE_FILE, "r", encoding="utf-8") as f:
        cached = json.load(f)

    return jsonify({**cached, "segments": segments, "goals": goals})


@app.route("/download")
def download():
    path = os.path.join(app_cs.EXPORT_DIR, app_cs.KPI_EXCEL_FILENAME)
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    print("  -> http://localhost:3000")
    app.run(debug=True, port=3000)
