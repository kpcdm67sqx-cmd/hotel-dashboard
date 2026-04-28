"""
Flask application: serves the dashboard and JSON API.
Run with:  python app.py
"""

import logging
import os
import threading
import webbrowser
from datetime import date, datetime, timedelta

from flask import Flask, jsonify, render_template, request

import database as db
import parser as hp
import pdf_parser as pp
import otb_parser as op
import watcher

# True when running on Render (cloud) — watcher and local imports are disabled
IS_CLOUD = bool(os.environ.get("RENDER"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

_import_status = {"running": False, "progress": 0, "total": 0, "message": ""}


# ── API routes ───────────────────────────────────────────────────────────────

@app.get("/api/hotels")
def api_hotels():
    return jsonify(db.get_all_hotels(allowed_hotels=hp.HOTELS_FILTER))


@app.get("/api/summary")
def api_summary():
    selected_date = request.args.get("date", str(date.today()))
    rows = db.get_summary_for_date(selected_date, allowed_hotels=hp.HOTELS_FILTER)
    return jsonify(rows)


@app.get("/api/hotel/<int:hotel_id>/metrics")
def api_hotel_metrics(hotel_id: int):
    end = request.args.get("end", str(date.today()))
    start = request.args.get("start", str(date.today() - timedelta(days=30)))
    rows = db.get_daily_metrics(hotel_id=hotel_id, start_date=start, end_date=end)
    return jsonify(rows)


@app.get("/api/otb/<int:hotel_id>/insights")
def api_otb_insights(hotel_id: int):
    if IS_CLOUD:
        return jsonify({})
    hotels = db.get_all_hotels(allowed_hotels=hp.HOTELS_FILTER)
    hotel = next((h for h in hotels if h["id"] == hotel_id), None)
    if not hotel:
        return jsonify({}), 404
    return jsonify(op.get_otb_insights(hotel["name"]))


@app.get("/api/otb/summary")
def api_otb_summary():
    return jsonify(db.get_otb_summary(allowed_hotels=hp.HOTELS_FILTER))


@app.get("/api/otb/<int:hotel_id>")
def api_otb(hotel_id: int):
    return jsonify(db.get_otb_data(hotel_id))


@app.get("/ping")
def ping():
    return jsonify({"ok": True})


@app.get("/api/status")
def api_status():
    return jsonify({
        "last_update":     db.get_latest_import_time(),
        "last_otb_update": db.get_latest_otb_import_time(),
        "import":          _import_status,
    })


@app.post("/api/reimport")
def api_reimport():
    if IS_CLOUD:
        return jsonify({"error": "Reimport não disponível na versão cloud"}), 403
    if _import_status["running"]:
        return jsonify({"error": "Import already running"}), 409
    threading.Thread(target=_run_full_import, daemon=True).start()
    return jsonify({"started": True})


# ── Main page ────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template("index.html")


# ── Background helpers ───────────────────────────────────────────────────────

def _run_full_import():
    _import_status.update({"running": True, "progress": 0, "total": 0, "message": "A iniciar..."})

    def on_progress(done, total, path, skipped=0):
        _import_status.update({
            "progress": done,
            "total": total,
            "message": f"{done}/{total} ({skipped} sem alterações) — {path[-50:]}",
        })

    try:
        _import_status["message"] = "A importar relatórios Excel..."
        total_rows = hp.import_all(progress_callback=on_progress)

        _import_status["message"] = "A importar PDFs (Manager's Report / Saldos)..."
        pdf_rows = pp.import_all_pdfs(progress_callback=on_progress)

        _import_status["message"] = "A importar OTB (On The Books)..."
        otb_rows = op.import_all_otb(progress_callback=on_progress)

        _import_status["message"] = (
            f"Concluído: {total_rows} Excel + {pdf_rows} PDF + {otb_rows} OTB"
        )
    except Exception as exc:
        logger.error("Full import failed: %s", exc)
        _import_status["message"] = f"Erro: {exc}"
    finally:
        _import_status["running"] = False


# ── Startup ──────────────────────────────────────────────────────────────────

def main():
    db.init_db()

    if not IS_CLOUD:
        watcher.start(hp.ROOT)
        threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
        logger.info("Dashboard disponível em http://localhost:5000")
        logger.info("Na primeira utilização clique em 'Reimportar tudo' para carregar todos os dados.")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
