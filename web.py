#!/usr/bin/env python3
"""
Web UI for ContentSquare KPI exporter.
Run: python web.py   ->  http://localhost:3000
"""

import os
import io
import sys
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
        segment_ids=", ".join(str(s) for s in app_cs.SEGMENT_IDS_TO_ANALYZE),
        by_device=app_cs.ANALYZE_BY_DEVICE,
        page_group_id=app_cs.PAGE_GROUP_ID or "",
        mapping_id=app_cs.PAGE_GROUP_MAPPING_ID,
        goal_ids=", ".join(str(g) for g in app_cs.GOAL_IDS),
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


@app.route("/download")
def download():
    path = os.path.join(app_cs.EXPORT_DIR, app_cs.KPI_EXCEL_FILENAME)
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    print("  -> http://localhost:3000")
    app.run(debug=True, port=3000)
