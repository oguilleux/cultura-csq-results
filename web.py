#!/usr/bin/env python3
"""
Web UI for ContentSquare KPI exporter.
Run: python web.py   →  http://localhost:5000
"""

import os
import io
import threading
from flask import Flask, render_template_string, request, jsonify, send_file
from dotenv import load_dotenv

load_dotenv()

import app_cs

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ContentSquare KPI Exporter</title>
<style>
  :root {
    --bg: #0f172a;
    --card: #1e293b;
    --border: #334155;
    --accent: #6366f1;
    --accent-hover: #818cf8;
    --green: #22c55e;
    --red: #ef4444;
    --text: #f1f5f9;
    --muted: #94a3b8;
    --input-bg: #0f172a;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    padding: 2rem;
  }
  .container { max-width: 720px; margin: 0 auto; }
  h1 {
    font-size: 1.8rem;
    font-weight: 700;
    margin-bottom: .25rem;
    background: linear-gradient(135deg, var(--accent), #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .subtitle { color: var(--muted); margin-bottom: 2rem; font-size: .9rem; }
  .card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
  }
  .card h2 {
    font-size: 1rem;
    font-weight: 600;
    margin-bottom: 1rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .05em;
    font-size: .8rem;
  }
  .form-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }
  .form-group { display: flex; flex-direction: column; gap: .35rem; }
  .form-group.full { grid-column: 1 / -1; }
  label { font-size: .8rem; color: var(--muted); font-weight: 500; }
  input, select {
    background: var(--input-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: .6rem .8rem;
    color: var(--text);
    font-size: .9rem;
    transition: border-color .2s;
  }
  input:focus, select:focus {
    outline: none;
    border-color: var(--accent);
  }
  .hint { font-size: .7rem; color: var(--muted); }
  .checkbox-row {
    display: flex;
    align-items: center;
    gap: .5rem;
    margin-top: .5rem;
  }
  .checkbox-row input[type=checkbox] {
    width: 18px; height: 18px;
    accent-color: var(--accent);
  }
  .btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: .5rem;
    width: 100%;
    padding: .8rem 1.5rem;
    background: var(--accent);
    color: white;
    border: none;
    border-radius: 10px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: background .2s, transform .1s;
  }
  .btn:hover { background: var(--accent-hover); }
  .btn:active { transform: scale(.98); }
  .btn:disabled {
    opacity: .5;
    cursor: not-allowed;
    transform: none;
  }
  .btn-download {
    background: var(--green);
    margin-top: 1rem;
  }
  .btn-download:hover { background: #16a34a; }
  #log-box {
    background: var(--input-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: .75rem;
    line-height: 1.6;
    max-height: 400px;
    overflow-y: auto;
    white-space: pre-wrap;
    display: none;
    color: var(--muted);
  }
  .status {
    display: inline-flex;
    align-items: center;
    gap: .4rem;
    padding: .3rem .8rem;
    border-radius: 20px;
    font-size: .8rem;
    font-weight: 500;
    margin-bottom: 1rem;
  }
  .status.running { background: #1e3a5f; color: #60a5fa; }
  .status.done { background: #14532d; color: #4ade80; }
  .status.error { background: #450a0a; color: #fca5a5; }
  .spinner {
    width: 14px; height: 14px;
    border: 2px solid transparent;
    border-top-color: currentColor;
    border-radius: 50%;
    animation: spin .6s linear infinite;
    display: inline-block;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="container">
  <h1>ContentSquare KPI Exporter</h1>
  <p class="subtitle">Configure et lance l'export de tes KPIs en quelques clics.</p>

  <form id="config-form">
    <div class="card">
      <h2>Periode d'analyse</h2>
      <div class="form-grid">
        <div class="form-group">
          <label for="start_date">Date de debut</label>
          <input type="date" id="start_date" name="start_date" value="{{ start_date }}" required>
        </div>
        <div class="form-group">
          <label for="end_date">Date de fin</label>
          <input type="date" id="end_date" name="end_date" value="{{ end_date }}" required>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Segments</h2>
      <div class="form-grid">
        <div class="form-group full">
          <label for="segment_ids">IDs des segments (la 1ere = reference)</label>
          <input type="text" id="segment_ids" name="segment_ids" value="{{ segment_ids }}"
                 placeholder="ex: 6383684, 6383688, 6383692">
          <span class="hint">Separes par des virgules. Laisser vide pour ignorer.</span>
        </div>
        <div class="form-group full">
          <div class="checkbox-row">
            <input type="checkbox" id="by_device" name="by_device" {{ 'checked' if by_device }}>
            <label for="by_device">Analyser par device (desktop / mobile / tablette)</label>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Page Groups</h2>
      <div class="form-grid">
        <div class="form-group">
          <label for="page_group_id">Page Group ID</label>
          <input type="text" id="page_group_id" name="page_group_id" value="{{ page_group_id }}"
                 placeholder="Optionnel">
        </div>
        <div class="form-group">
          <label for="mapping_id">Mapping ID</label>
          <input type="text" id="mapping_id" name="mapping_id" value="{{ mapping_id }}">
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Goals</h2>
      <div class="form-group">
        <label for="goal_ids">Goal IDs</label>
        <input type="text" id="goal_ids" name="goal_ids" value="{{ goal_ids }}"
               placeholder="ex: 1816096">
        <span class="hint">Separes par des virgules.</span>
      </div>
    </div>

    <button type="submit" class="btn" id="run-btn">
      Lancer l'export
    </button>
  </form>

  <div class="card" id="result-card" style="display:none; margin-top:1.5rem;">
    <div id="status-badge"></div>
    <div id="log-box"></div>
    <a id="download-link" style="display:none;">
      <button type="button" class="btn btn-download">Telecharger le fichier Excel</button>
    </a>
  </div>
</div>

<script>
const form = document.getElementById('config-form');
const runBtn = document.getElementById('run-btn');
const resultCard = document.getElementById('result-card');
const statusBadge = document.getElementById('status-badge');
const logBox = document.getElementById('log-box');
const downloadLink = document.getElementById('download-link');

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  runBtn.disabled = true;
  runBtn.innerHTML = '<span class="spinner"></span> Export en cours...';
  resultCard.style.display = 'block';
  statusBadge.innerHTML = '<span class="status running"><span class="spinner"></span> Export en cours...</span>';
  logBox.style.display = 'block';
  logBox.textContent = '';
  downloadLink.style.display = 'none';

  const data = Object.fromEntries(new FormData(form));
  data.by_device = document.getElementById('by_device').checked;

  try {
    const res = await fetch('/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    const result = await res.json();
    logBox.textContent = result.log;
    if (result.success) {
      statusBadge.innerHTML = '<span class="status done">Export termine avec succes</span>';
      downloadLink.href = '/download';
      downloadLink.style.display = 'block';
    } else {
      statusBadge.innerHTML = '<span class="status error">Erreur durant l\'export</span>';
    }
  } catch (err) {
    statusBadge.innerHTML = '<span class="status error">Erreur reseau</span>';
    logBox.textContent = err.toString();
  }
  runBtn.disabled = false;
  runBtn.innerHTML = 'Lancer l\'export';
  logBox.scrollTop = logBox.scrollHeight;
});
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(
        HTML,
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

    # Apply config from form
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

    # Capture stdout
    import sys
    log_buffer = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = log_buffer

    success = True
    try:
        app_cs.main()
    except Exception as e:
        print(f"\n--- ERREUR ---\n{e}")
        success = False
    finally:
        sys.stdout = old_stdout

    return jsonify({"success": success, "log": log_buffer.getvalue()})


@app.route("/download")
def download():
    path = os.path.join(app_cs.EXPORT_DIR, app_cs.KPI_EXCEL_FILENAME)
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    print("  → http://localhost:3000")
    app.run(debug=True, port=3000)
