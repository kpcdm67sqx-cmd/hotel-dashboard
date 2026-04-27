"""
OTB (On The Books) weekly Excel parser.
Finds the most recent OTB 2026 file per hotel and parses 3 analysis blocks.

Column layout (0-indexed) — identical across all hotels:
  8  = month label (JAN-DEZ) or None for annual total
  9  = occ% current
  10 = occ% comparison
  11 = nights current total
  14 = nights comparison total
  17 = var nights          18 = var nights %
  23 = receita total current
  24 = receita total comparison
  25 = var receita total
  26 = var receita total %
  27 = receita quartos current
  30 = receita quartos comparison
  33 = var receita quartos
  34 = var receita quartos %
  39 = receita F&B current
  42 = receita F&B comparison
  45 = var F&B
  46 = var F&B %
  51 = receita outros current
  52 = receita outros comparison
  53 = var outros
  54 = var outros %
  55 = receita SPA current
  56 = receita SPA comparison
  57 = var SPA
  58 = var SPA %
  59 = ADR current
  62 = ADR comparison
  65 = var ADR
  66 = var ADR %
"""

import re
import logging
import warnings
from pathlib import Path

import openpyxl

import database as db

logger = logging.getLogger(__name__)

ROOT = r"C:\Users\Bruno Barbosa\OneDrive - Amazing Evolution, S.A\Sales, Marketing & Revenue - Relatórios Hotéis"

HOTELS_FILTER = {
    "1905 Zinos Palace",
    "Hotel da Graciosa",
    "Land of Alandroal",
    "Luster",
    "Palácio Sta. Catarina",
    "Sleep and Nature",
    "Solar dos Cantos",
    "The Shipyard Angra",
}

OTB_GLOBS = [
    "*/OTB*2026*/*.xlsx",
    "*/OTB*2026*/**/*.xlsx",
]

MONTH_LABELS = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN",
                "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
MONTH_MAP = {m: i + 1 for i, m in enumerate(MONTH_LABELS)}
MONTH_TOTAL = 0  # sentinel for the annual total row


def _date_from_filename(name: str) -> str | None:
    m = re.search(r"(\d{2})[_.](\d{2})[_.](\d{4})", name)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def _hotel_name_from_path(file_path: str) -> str:
    rel = Path(file_path).relative_to(ROOT)
    return rel.parts[0]


def _find_otb_files_for_hotel(hotel_name: str) -> list[Path]:
    """Return all OTB files for a hotel sorted by mtime descending."""
    root = Path(ROOT)
    files = []
    seen: set[Path] = set()
    for pattern in OTB_GLOBS:
        for f in root.glob(pattern):
            if f in seen or f.name.startswith("~"):
                continue
            seen.add(f)
            if f.parts[len(root.parts)] == hotel_name:
                files.append(f)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _find_latest_otb_per_hotel() -> dict[str, Path]:
    """Return {hotel_name: otb_path_with_newest_filename_date}."""
    root = Path(ROOT)
    hotel_files: dict[str, list[Path]] = {h: [] for h in HOTELS_FILTER}

    seen: set[Path] = set()
    for pattern in OTB_GLOBS:
        for f in root.glob(pattern):
            if f in seen or f.name.startswith("~"):
                continue
            seen.add(f)
            hotel = f.parts[len(root.parts)]
            if hotel in hotel_files:
                hotel_files[hotel].append(f)

    result = {}
    for hotel, files in hotel_files.items():
        if files:
            # Sort by date in filename (primary) so OneDrive mtime quirks don't win
            files.sort(key=lambda p: (_date_from_filename(p.name) or "", p.stat().st_mtime), reverse=True)
            result[hotel] = files[0]
    return result


def _parse_otb_sheet(ws) -> dict | None:
    """Return {analysis_type: [month_dict, ...]} for 3 analysis blocks."""
    rows = list(ws.iter_rows(values_only=True))

    block_starts: dict[str, int | None] = {
        "sdly": None, "closed_month": None, "budget": None
    }
    for i, row in enumerate(rows):
        for cell in row[8:13]:
            if not isinstance(cell, str):
                continue
            u = cell.upper().strip()
            if "SAME DATE LAST YEAR" in u and block_starts["sdly"] is None:
                block_starts["sdly"] = i
            elif "TOTAL M" in u and "FECHADO" in u and block_starts["closed_month"] is None:
                block_starts["closed_month"] = i
            elif "BUDGET" in u and ("VS" in u or "2026" in u) and block_starts["budget"] is None:
                block_starts["budget"] = i

    if not any(v is not None for v in block_starts.values()):
        return None

    results = {}
    for block_type, header_row in block_starts.items():
        if header_row is None:
            continue

        # Find first 'JAN' row (within 12 rows of the header)
        data_start = None
        for i in range(header_row + 1, min(header_row + 12, len(rows))):
            if rows[i][8] == "JAN":
                data_start = i
                break
        if data_start is None:
            continue

        months = []
        for i in range(data_start, data_start + 13):  # 12 months + annual total
            if i >= len(rows):
                break
            row = rows[i]
            label = row[8]
            month_num = MONTH_MAP.get(label, MONTH_TOTAL) if isinstance(label, str) else MONTH_TOTAL

            def _f(v):
                try:
                    return float(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            def _i(v):
                try:
                    return int(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            # Extend row if shorter than expected (some files may be narrower)
            r = list(row) + [None] * max(0, 67 - len(row))

            months.append({
                "month": month_num,
                # Occupancy
                "occ_pct_current":          _f(r[9]),
                "occ_pct_comparison":       _f(r[10]),
                "variance_nights":          _i(r[17]),
                "variance_pct":             _f(r[18]),
                # Nights
                "nights_current":           _i(r[11]),
                "nights_comparison":        _i(r[14]),
                # Receita Total
                "total_revenue_current":    _f(r[23]),
                "total_revenue_comparison": _f(r[24]),
                "total_revenue_variance":   _f(r[25]),
                "total_revenue_var_pct":    _f(r[26]),
                # Receita Quartos
                "room_revenue_current":     _f(r[27]),
                "room_revenue_comparison":  _f(r[30]),
                "room_revenue_variance":    _f(r[33]),
                "room_revenue_var_pct":     _f(r[34]),
                # Receita F&B
                "fb_revenue_current":       _f(r[39]),
                "fb_revenue_comparison":    _f(r[42]),
                "fb_revenue_variance":      _f(r[45]),
                "fb_revenue_var_pct":       _f(r[46]),
                # Receita Outros
                "other_revenue_current":    _f(r[51]),
                "other_revenue_comparison": _f(r[52]),
                "other_revenue_variance":   _f(r[53]),
                "other_revenue_var_pct":    _f(r[54]),
                # Receita SPA
                "spa_revenue_current":      _f(r[55]),
                "spa_revenue_comparison":   _f(r[56]),
                "spa_revenue_variance":     _f(r[57]),
                "spa_revenue_var_pct":      _f(r[58]),
                # ADR
                "adr_current":              _f(r[59]),
                "adr_comparison":           _f(r[62]),
                "adr_variance":             _f(r[65]),
                "adr_var_pct":              _f(r[66]),
            })

        if months:
            results[block_type] = months

    return results if results else None


def parse_otb_file(file_path: str) -> dict | None:
    """Parse OTB Excel file — returns {analysis_type: [rows]} or None."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.worksheets[0]  # first sheet = most recent week
        result = _parse_otb_sheet(ws)
        wb.close()
        return result
    except Exception as exc:
        logger.warning("Cannot parse OTB %s: %s", file_path, exc)
        return None


def import_otb_for_hotel(hotel_name: str, file_path: Path, force: bool = False) -> int:
    mtime = file_path.stat().st_mtime
    if not force and db.is_file_unchanged(str(file_path), mtime):
        return -1

    otb_date = _date_from_filename(file_path.name)

    data = parse_otb_file(str(file_path))
    if not data:
        db.log_import(str(file_path), "empty", 0)
        db.update_file_cache(str(file_path), mtime)
        return 0

    hotel_id = db.upsert_hotel(hotel_name, str(Path(ROOT) / hotel_name))

    rows = []
    for analysis_type, months in data.items():
        for m in months:
            rows.append({
                "hotel_id": hotel_id,
                "analysis_type": analysis_type,
                "month": m["month"],
                "occ_pct_current":          m["occ_pct_current"],
                "occ_pct_comparison":       m["occ_pct_comparison"],
                "nights_current":           m["nights_current"],
                "nights_comparison":        m["nights_comparison"],
                "variance_nights":          m["variance_nights"],
                "variance_pct":             m["variance_pct"],
                "total_revenue_current":    m["total_revenue_current"],
                "total_revenue_comparison": m["total_revenue_comparison"],
                "total_revenue_variance":   m["total_revenue_variance"],
                "total_revenue_var_pct":    m["total_revenue_var_pct"],
                "room_revenue_current":     m["room_revenue_current"],
                "room_revenue_comparison":  m["room_revenue_comparison"],
                "room_revenue_variance":    m["room_revenue_variance"],
                "room_revenue_var_pct":     m["room_revenue_var_pct"],
                "fb_revenue_current":       m["fb_revenue_current"],
                "fb_revenue_comparison":    m["fb_revenue_comparison"],
                "fb_revenue_variance":      m["fb_revenue_variance"],
                "fb_revenue_var_pct":       m["fb_revenue_var_pct"],
                "other_revenue_current":    m["other_revenue_current"],
                "other_revenue_comparison": m["other_revenue_comparison"],
                "other_revenue_variance":   m["other_revenue_variance"],
                "other_revenue_var_pct":    m["other_revenue_var_pct"],
                "spa_revenue_current":      m["spa_revenue_current"],
                "spa_revenue_comparison":   m["spa_revenue_comparison"],
                "spa_revenue_variance":     m["spa_revenue_variance"],
                "spa_revenue_var_pct":      m["spa_revenue_var_pct"],
                "adr_current":              m["adr_current"],
                "adr_comparison":           m["adr_comparison"],
                "adr_variance":             m["adr_variance"],
                "adr_var_pct":              m["adr_var_pct"],
                "otb_date": otb_date,
                "source_file": str(file_path),
            })

    try:
        db.upsert_otb_metrics(rows)
        db.log_import(str(file_path), "ok", len(rows))
        db.update_file_cache(str(file_path), mtime)
        return len(rows)
    except Exception as exc:
        db.log_import(str(file_path), "error", 0, str(exc))
        logger.error("DB error for OTB %s: %s", file_path, exc)
        return 0


def import_all_otb(progress_callback=None, force: bool = False) -> int:
    hotel_files = _find_latest_otb_per_hotel()
    total = 0
    skipped = 0
    items = list(hotel_files.items())
    for i, (hotel_name, file_path) in enumerate(items):
        count = import_otb_for_hotel(hotel_name, file_path, force=force)
        if count == -1:
            skipped += 1
        elif count > 0:
            total += count
        if progress_callback:
            progress_callback(i + 1, len(items), str(file_path), skipped)

    logger.info("OTB import: %d rows (%d hotels skipped unchanged)", total, skipped)
    return total


def get_otb_insights(hotel_name: str) -> dict:
    """
    Compare the two most recent OTB files for a hotel and return:
      - changes: significant week-on-week movements
      - suggestions: actionable recommendations based on current data
    """
    from datetime import date as _date

    files = _find_otb_files_for_hotel(hotel_name)
    if not files:
        return {}

    current_file  = files[0]
    previous_file = files[1] if len(files) >= 2 else None

    current_data  = parse_otb_file(str(current_file))
    previous_data = parse_otb_file(str(previous_file)) if previous_file else None

    if not current_data:
        return {}

    result = {
        "current_date":  _date_from_filename(current_file.name),
        "previous_date": _date_from_filename(previous_file.name) if previous_file else None,
        "changes":     [],
        "suggestions": [],
    }

    # ── Week-on-week changes (compare nights_current in SDLY block) ──────────
    if previous_data and "sdly" in current_data and "sdly" in previous_data:
        curr_by_month = {r["month"]: r for r in current_data["sdly"] if r["month"] != MONTH_TOTAL}
        prev_by_month = {r["month"]: r for r in previous_data["sdly"] if r["month"] != MONTH_TOTAL}

        changes = []
        for month in range(1, 13):
            c = curr_by_month.get(month)
            p = prev_by_month.get(month)
            if not c or not p:
                continue
            nights_delta  = (c["nights_current"]  or 0) - (p["nights_current"]  or 0)
            occ_delta_pp  = ((c["occ_pct_current"] or 0) - (p["occ_pct_current"] or 0)) * 100
            rev_delta     = (c["total_revenue_current"] or 0) - (p["total_revenue_current"] or 0)
            adr_delta     = (c["adr_current"] or 0) - (p["adr_current"] or 0)

            if abs(nights_delta) < 3 and abs(occ_delta_pp) < 0.5:
                continue  # noise

            changes.append({
                "month":        month,
                "label":        MONTH_LABELS[month - 1],
                "nights_delta": int(nights_delta),
                "occ_delta_pp": round(occ_delta_pp, 1),
                "rev_delta":    round(rev_delta, 2),
                "adr_delta":    round(adr_delta, 2),
            })

        # Sort by absolute occ movement, keep top 6
        changes.sort(key=lambda x: abs(x["occ_delta_pp"]), reverse=True)
        result["changes"] = changes[:6]

    # ── Suggestions ──────────────────────────────────────────────────────────
    today = _date.today()
    current_month = today.month
    suggestions = []

    budget_by_month = {}
    if "budget" in current_data:
        budget_by_month = {r["month"]: r for r in current_data["budget"] if r["month"] != MONTH_TOTAL}

    sdly_by_month = {}
    if "sdly" in current_data:
        sdly_by_month = {r["month"]: r for r in current_data["sdly"] if r["month"] != MONTH_TOTAL}

    for month in range(current_month, 13):
        label = MONTH_LABELS[month - 1]
        months_away = month - current_month

        b = budget_by_month.get(month)
        s = sdly_by_month.get(month)

        # Budget gap
        if b and b["occ_pct_current"] is not None and b["occ_pct_comparison"]:
            gap_pct = (b["occ_pct_current"] - b["occ_pct_comparison"]) / b["occ_pct_comparison"]
            gap_pp  = (b["occ_pct_current"] - b["occ_pct_comparison"]) * 100
            if gap_pct < -0.20 and months_away <= 2:
                suggestions.append({
                    "priority": "high",
                    "month": month,
                    "text": (f"{label}: {abs(gap_pp):.0f}pp abaixo do Budget com {months_away+1} "
                             f"{'mês' if months_away==0 else 'meses'} para a data — "
                             f"ação urgente: rever pricing e aumentar distribuição OTA.")
                })
            elif gap_pct < -0.15:
                suggestions.append({
                    "priority": "medium",
                    "month": month,
                    "text": (f"{label}: {abs(gap_pp):.0f}pp abaixo do Budget — "
                             f"considerar promoções ou revisão de preços.")
                })
            elif gap_pct > 0.10:
                suggestions.append({
                    "priority": "opportunity",
                    "month": month,
                    "text": (f"{label}: {gap_pp:.0f}pp acima do Budget — "
                             f"oportunidade de aumentar ADR sem risco de perda de ocupação.")
                })

        # SDLY gap (only if no budget signal already added for this month)
        already = any(sg["month"] == month for sg in suggestions)
        if s and not already and s["occ_pct_current"] is not None and s["occ_pct_comparison"]:
            gap_pp_sdly = (s["occ_pct_current"] - s["occ_pct_comparison"]) * 100
            if gap_pp_sdly < -10:
                suggestions.append({
                    "priority": "warning",
                    "month": month,
                    "text": (f"{label}: {abs(gap_pp_sdly):.0f}pp abaixo do ano anterior — "
                             f"monitorizar e considerar campanha de visibilidade.")
                })
            elif gap_pp_sdly > 8 and months_away >= 2:
                suggestions.append({
                    "priority": "opportunity",
                    "month": month,
                    "text": (f"{label}: {gap_pp_sdly:.0f}pp acima do ano anterior — "
                             f"forte procura, avaliar subida de preços.")
                })

    # Sort: high → medium → warning → opportunity; keep top 5
    priority_order = {"high": 0, "medium": 1, "warning": 2, "opportunity": 3}
    suggestions.sort(key=lambda x: (priority_order.get(x["priority"], 9), x["month"]))
    result["suggestions"] = suggestions[:5]

    return result


def is_otb_report(file_path: str) -> bool:
    if not file_path.lower().endswith(".xlsx"):
        return False
    name = Path(file_path).name
    if name.startswith("~"):
        return False
    return "OTB" in name and "2026" in file_path
