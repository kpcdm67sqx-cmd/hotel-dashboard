"""
Flask application: serves the dashboard and JSON API.
Run with:  python app.py
"""

import logging
import os
import threading
import time
import urllib.request
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request

import database as db
import parser as hp
import pdf_parser as pp
import otb_parser as op
import reviews_parser as rp
import booking_reviews_parser as brp
import google_reviews as gr
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
        return jsonify(db.get_otb_insights_db(hotel_id))
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


@app.post("/api/google-reviews/sync")
def api_google_sync():
    if IS_CLOUD:
        return jsonify({"error": "Sincronização manual não disponível na cloud"}), 403
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        return jsonify({"error": "GOOGLE_PLACES_API_KEY não configurada"}), 400
    threading.Thread(target=lambda: gr.import_google_reviews(api_key), daemon=True).start()
    return jsonify({"started": True})




@app.get("/api/reviews/summary")
def api_reviews_summary():
    return jsonify(db.get_reviews_summary(allowed_hotels=hp.HOTELS_FILTER))


@app.get("/api/reviews/<int:hotel_id>/scores")
def api_reviews_scores(hotel_id: int):
    return jsonify(db.get_review_scores(hotel_id))


@app.get("/api/reviews/<int:hotel_id>/complaints")
def api_reviews_complaints(hotel_id: int):
    period = request.args.get("period")
    return jsonify(db.get_review_complaints(hotel_id, period))


@app.get("/api/reviews/<int:hotel_id>/keywords")
def api_reviews_keywords(hotel_id: int):
    period = request.args.get("period")
    return jsonify(db.get_review_keywords(hotel_id, period))


@app.get("/api/reviews/<int:hotel_id>/compset")
def api_reviews_compset(hotel_id: int):
    period = request.args.get("period")
    return jsonify(db.get_review_compset(hotel_id, period))


@app.get("/api/reviews/<int:hotel_id>/booking")
def api_reviews_booking(hotel_id: int):
    period = request.args.get("period")  # YYYY-MM-01
    start  = request.args.get("start")
    end    = request.args.get("end")
    if period and not start:
        import calendar
        y, m = int(period[:4]), int(period[5:7])
        start = period
        last_day = calendar.monthrange(y, m)[1]
        end = f"{y}-{m:02d}-{last_day}"
    return jsonify(db.get_booking_reviews(hotel_id, start, end))


@app.get("/ping")
def ping():
    return jsonify({"ok": True})


@app.get("/api/status")
def api_status():
    imp = dict(_import_status)
    if not imp["running"] and watcher.is_importing:
        imp["running"] = True
        imp["message"] = "A importar ficheiros alterados…"
    return jsonify({
        "last_update":     db.get_latest_import_time(),
        "last_otb_update": db.get_latest_otb_import_time(),
        "import":          imp,
    })


@app.post("/api/reimport")
def api_reimport():
    if IS_CLOUD:
        return jsonify({"error": "Reimport não disponível na versão cloud"}), 403
    if _import_status["running"]:
        return jsonify({"error": "Import already running"}), 409
    threading.Thread(target=_run_full_import, kwargs={"since_year": 2026}, daemon=True).start()
    return jsonify({"started": True})


@app.post("/api/reimport-recent")
def api_reimport_recent():
    if IS_CLOUD:
        return jsonify({"error": "Reimport não disponível na versão cloud"}), 403
    if _import_status["running"]:
        return jsonify({"error": "Import already running"}), 409
    days = int(request.args.get("days", 7))
    threading.Thread(target=_run_full_import, kwargs={"since_year": 2026, "since_days": days}, daemon=True).start()
    return jsonify({"started": True, "days": days})


@app.post("/api/reimport-booking")
def api_reimport_booking():
    if IS_CLOUD:
        return jsonify({"error": "Reimport não disponível na versão cloud"}), 403
    if _import_status["running"]:
        return jsonify({"error": "Import already running"}), 409
    threading.Thread(target=_run_booking_import, daemon=True).start()
    return jsonify({"started": True})


# ── Main page ────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    resp = app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ── Background helpers ───────────────────────────────────────────────────────

def _run_booking_import():
    _import_status.update({"running": True, "progress": 0, "total": 0, "message": "A importar Reviews Booking.com..."})
    def on_progress(done, total, path, skipped=0):
        _import_status.update({"progress": done, "total": total, "message": f"{done}/{total} — {path[-60:]}"})
    try:
        count = brp.import_all_booking_reviews(progress_callback=on_progress)
        _import_status["message"] = f"Concluído: {count} reviews Booking.com importados"
    except Exception as exc:
        logger.error("Booking import failed: %s", exc)
        _import_status["message"] = f"Erro: {exc}"
    finally:
        _import_status["running"] = False


def _run_full_import(since_year: int | None = None, since_days: int | None = None):
    _import_status.update({"running": True, "progress": 0, "total": 0, "message": "A iniciar..."})

    since_mtime = None
    if since_days is not None:
        since_mtime = time.time() - since_days * 86400

    def on_progress(done, total, path, skipped=0):
        _import_status.update({
            "progress": done,
            "total": total,
            "message": f"{done}/{total} ({skipped} sem alterações) — {path[-50:]}",
        })

    try:
        _import_status["message"] = "A importar relatórios Excel..."
        total_rows = hp.import_all(progress_callback=on_progress, since_year=since_year, since_mtime=since_mtime)

        _import_status["message"] = "A importar PDFs (Manager's Report / Saldos)..."
        pdf_rows = pp.import_all_pdfs(progress_callback=on_progress, since_year=since_year, since_mtime=since_mtime)

        _import_status["message"] = "A importar OTB (On The Books)..."
        otb_rows = op.import_all_otb(progress_callback=on_progress)

        _import_status["message"] = "A importar Reviews..."
        rev_rows = rp.import_all_reviews(Path(hp.ROOT), progress_callback=on_progress)

        _import_status["message"] = "A importar Reviews Booking.com..."
        booking_rows = brp.import_all_booking_reviews(progress_callback=on_progress)

        _import_status["message"] = (
            f"Concluído: {total_rows} Excel + {pdf_rows} PDF + {otb_rows} OTB"
            f" + {rev_rows} Reviews + {booking_rows} Booking"
        )
    except Exception as exc:
        logger.error("Full import failed: %s", exc)
        _import_status["message"] = f"Erro: {exc}"
    finally:
        _import_status["running"] = False


# ── Startup ──────────────────────────────────────────────────────────────────

def _daily_reviews_sync():
    """Runs Google Reviews sync every 24 h while the PC app is running."""
    while True:
        try:
            gkey = os.environ.get("GOOGLE_PLACES_API_KEY", "")
            if gkey:
                gr.import_google_reviews(gkey)
        except Exception as e:
            logger.error("Daily Google sync failed: %s", e)
        time.sleep(24 * 60 * 60)


def _scheduled_morning_import():
    """Runs a full import every day at 10:00. On startup, catches up if last import was over 12h ago."""
    import datetime as _dt

    # Catch-up: se o último import foi há mais de 12h, corre agora.
    # Retenta até 10 vezes (de 30s em 30s) para aguardar que a rede fique disponível.
    for attempt in range(10):
        try:
            last = db.get_latest_import_time()
            if last:
                last_dt = _dt.datetime.fromisoformat(last.replace(" ", "T").split(".")[0])
                if (_dt.datetime.now() - last_dt).total_seconds() > 12 * 3600:
                    logger.info("Catch-up: último import há mais de 12h, a importar agora…")
                    _run_full_import(since_year=2026)
            break  # ligou à BD com sucesso — sai do loop de retry
        except Exception as e:
            if attempt < 9:
                logger.warning("Catch-up falhou (tentativa %d/10), sem rede? A tentar em 30s… (%s)", attempt + 1, e)
                time.sleep(30)
            else:
                logger.error("Catch-up abortado após 10 tentativas: %s", e)

    # Delayed PDF scan: aguarda 5 min para o OneDrive sincronizar e importa PDFs novos
    try:
        time.sleep(5 * 60)
        logger.info("Delayed PDF scan: a verificar PDFs novos após sincronização OneDrive…")
        pp.import_all_pdfs(since_year=2025)
    except Exception as e:
        logger.error("Delayed PDF scan failed: %s", e)

    while True:
        now = _dt.datetime.now()
        next_run = now.replace(hour=10, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += _dt.timedelta(days=1)
        time.sleep((next_run - now).total_seconds())
        logger.info("Importação automática diária (10h)...")
        try:
            _run_full_import(since_year=2026)
        except Exception as e:
            logger.error("Scheduled import failed: %s", e)


def _keep_render_alive():
    """Ping the Render cloud service every 10 min so it never sleeps."""
    url = "https://hotel-dashboard-jeli.onrender.com/ping"
    while True:
        time.sleep(10 * 60)
        try:
            urllib.request.urlopen(url, timeout=15)
            logger.info("Render keep-alive ping OK")
        except Exception as e:
            logger.warning("Render keep-alive ping failed: %s", e)


def main():
    db.init_db()

    if not IS_CLOUD:
        watcher.start(hp.ROOT)
        threading.Thread(target=_keep_render_alive, daemon=True).start()
        threading.Thread(target=_daily_reviews_sync, daemon=True).start()
        threading.Thread(target=_scheduled_morning_import, daemon=True).start()
        threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
        logger.info("Dashboard disponível em http://localhost:5000")
        logger.info("Na primeira utilização clique em 'Reimportar tudo' para carregar todos os dados.")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
