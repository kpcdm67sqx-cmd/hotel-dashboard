"""
PDF parsers for hotel daily reports.

FORMAT 150 — "150. Manager's Report.pdf" (Host Hotel Systems, most hotels)
  Extracts: rooms_out_of_service (Dia col), total_revenue (Dia col), fb_revenue (Dia col)

FORMAT 101 — "101. Saldos de contas pendentes.pdf"
  Extracts: pending_balance (Total line)

FORMAT ZINOS — "Report Manager.pdf" (1905 Zinos Palace / NewHotel PMS)
  Extracts: rooms_out_of_service (Dia col), total_revenue (Dia col), fb_revenue (Dia col)
  Numbers are English-formatted (comma=thousands, dot=decimal)
"""

import re
import logging
import warnings
from pathlib import Path

import pdfplumber

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

PDF_GLOBS = [
    "*/Relatórios Diários*/**/150. Manager*.pdf",
    "*/Relatórios diários*/**/150. Manager*.pdf",
    "*/Relatórios Diários*/**/101. Saldos*.pdf",
    "*/Relatórios diários*/**/101. Saldos*.pdf",
    "*/Relatórios Diários*/**/Report Manager.pdf",
    "*/Relatórios diários*/**/Report Manager.pdf",
]

# PT float: "2 727,36" or "772,53" — space=thousands, comma=decimal
# Falls back to bare integer
_NUM_PT = re.compile(r"(\d{1,3}(?:\s\d{3})*),(\d+)|(\d+)")

# EN float: "2,517.19" or "482.40" — comma=thousands, dot=decimal
_NUM_EN = re.compile(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)")


def _path_has_year_gte(f: Path, since_year: int) -> bool:
    years = re.findall(r'(?<!\d)(20\d{2})(?!\d)', str(f.parent))
    return any(int(y) >= since_year for y in years)


def _first_pt_number(text: str) -> float | None:
    m = _NUM_PT.search(text)
    if not m:
        return None
    if m.group(1) is not None:
        return float(m.group(1).replace(" ", "") + "." + m.group(2))
    return float(m.group(3))


def _first_en_number(text: str) -> float | None:
    m = _NUM_EN.search(text)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def _date_from_path(file_path: str) -> str | None:
    """Extract YYYY-MM-DD from the folder structure containing the file."""
    parts = Path(file_path).parts

    # Pattern 1: folder named YYYY_MM_DD  (e.g. Land of Alandroal, Shipyard)
    for part in parts:
        m = re.match(r"^(\d{4})_(\d{2})_(\d{2})$", part)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Pattern 2a: folder named DD.MM.YYYY  (e.g. 22.04.2026 — most hotels)
    # Pattern 2b: folder named DD-MM-YYYY  (e.g. 23-04-2026 — Solar dos Cantos)
    for part in parts:
        m = re.match(r"^(\d{2})[.\-](\d{2})[.\-](\d{4})$", part)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Pattern 3: ...\YYYY\MM. MonthName\DD\ hierarchy  (e.g. 1905 Zinos Palace)
    for i, part in enumerate(parts):
        if re.match(r"^\d{4}$", part) and i + 2 < len(parts):
            month_part = parts[i + 1]
            day_part = parts[i + 2]
            month_m = re.match(r"^(\d{2})[\.\s]", month_part)
            day_m = re.match(r"^(\d{1,2})$", day_part)
            if month_m and day_m:
                return f"{part}-{month_m.group(1)}-{day_m.group(1).zfill(2)}"

    return None


def _hotel_name_from_path(file_path: str) -> str:
    rel = Path(file_path).relative_to(ROOT)
    return rel.parts[0]


def _extract_text(file_path: str) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)


def parse_manager_report_150(file_path: str) -> dict | None:
    """Parse '150. Manager's Report.pdf' (Host Hotel Systems format)."""
    try:
        text = _extract_text(file_path)
    except Exception as exc:
        logger.warning("Cannot read %s: %s", file_path, exc)
        return None

    result = {}
    for line in text.splitlines():
        # Only take the FIRST match for each field (labels repeat in the Desvio section)
        if "rooms_out_of_service" not in result and "Quartos Fora Servi" in line:
            rest = re.split(r"Quartos Fora Servi[çc\?]?o", line, maxsplit=1)
            if len(rest) > 1:
                n = _first_pt_number(rest[1])
                if n is not None:
                    result["rooms_out_of_service"] = int(n)
        elif "total_revenue" not in result and "Receitas: Total" in line:
            rest = line.split("Receitas: Total", 1)[1]
            n = _first_pt_number(rest)
            if n is not None:
                result["total_revenue"] = n
        elif "fb_revenue" not in result and re.search(r"Receitas:\s*F\s*[&e]\s*B", line):
            rest = re.split(r"Receitas:\s*F\s*[&e]\s*B", line, maxsplit=1)[1]
            n = _first_pt_number(rest)
            if n is not None:
                result["fb_revenue"] = n

    return result if result else None


def parse_manager_report_zinos(file_path: str) -> dict | None:
    """Parse 'Report Manager.pdf' (1905 Zinos Palace / NewHotel format)."""
    try:
        text = _extract_text(file_path)
    except Exception as exc:
        logger.warning("Cannot read %s: %s", file_path, exc)
        return None

    result = {}
    comidas = None
    bebidas = None

    for line in text.splitlines():
        if "Alojamentos Inact" in line or "Alojamentos Inat" in line:
            rest = re.split(r"Alojamentos Inat?ct?ivos?", line, maxsplit=1)
            if len(rest) > 1:
                n = _first_en_number(rest[1])
                if n is not None:
                    result["rooms_out_of_service"] = int(n)
        elif "Total Receitas C/" in line:
            rest = line.split("Total Receitas C/", 1)[1]
            n = _first_en_number(rest)
            if n is not None:
                result["total_revenue"] = n
        elif "Comidas C/ Impostos" in line:
            rest = line.split("Comidas C/ Impostos", 1)[1]
            n = _first_en_number(rest)
            if n is not None:
                comidas = n
        elif "Bebidas C/ Impostos" in line:
            rest = line.split("Bebidas C/ Impostos", 1)[1]
            n = _first_en_number(rest)
            if n is not None:
                bebidas = n

    if comidas is not None or bebidas is not None:
        result["fb_revenue"] = (comidas or 0.0) + (bebidas or 0.0)

    return result if result else None


def parse_saldos_pendentes(file_path: str) -> dict | None:
    """Parse '101. Saldos de contas pendentes.pdf'."""
    try:
        text = _extract_text(file_path)
    except Exception as exc:
        logger.warning("Cannot read %s: %s", file_path, exc)
        return None

    last_total = None
    for line in text.splitlines():
        m = re.match(r"^Total\s+(-?[\d\s,]+)", line.strip())
        if m:
            raw = m.group(1).strip()
            negative = raw.startswith("-")
            n = _first_pt_number(raw.lstrip("-").strip())
            if n is not None:
                last_total = -n if negative else n

    return {"pending_balance": last_total} if last_total is not None else None


def _detect_pdf_type(file_path: str) -> str | None:
    name = Path(file_path).name
    if name == "150. Manager's Report.pdf" or re.match(r"^150\.\s+Manager", name):
        return "manager_150"
    if name == "Report Manager.pdf":
        return "manager_zinos"
    if re.match(r"^101\.\s+Saldos", name):
        return "saldos"
    return None


def parse_pdf_file(file_path: str) -> dict | None:
    pdf_type = _detect_pdf_type(file_path)
    if pdf_type == "manager_150":
        return parse_manager_report_150(file_path)
    if pdf_type == "manager_zinos":
        return parse_manager_report_zinos(file_path)
    if pdf_type == "saldos":
        return parse_saldos_pendentes(file_path)
    return None


def is_pdf_report(file_path: str) -> bool:
    if not (file_path.endswith(".pdf") or file_path.endswith(".PDF")):
        return False
    name = Path(file_path).name
    if name.startswith("~"):
        return False
    in_daily = any(kw in file_path for kw in ("Relatórios Diários", "Relatórios diários"))
    return in_daily and _detect_pdf_type(file_path) is not None


def import_pdf_file(file_path: str, force: bool = False) -> int:
    hotel_name = _hotel_name_from_path(file_path)
    if HOTELS_FILTER and hotel_name not in HOTELS_FILTER:
        return 0

    mtime = Path(file_path).stat().st_mtime
    if not force and db.is_file_unchanged(file_path, mtime):
        return -1

    date = _date_from_path(file_path)
    if not date:
        logger.warning("Cannot determine date for %s", file_path)
        # Do NOT cache — will retry on next import after fix
        return 0

    metrics = parse_pdf_file(file_path)
    if not metrics:
        db.log_import(file_path, "empty", 0)
        db.update_file_cache(file_path, mtime)
        return 0

    hotel_id = db.upsert_hotel(hotel_name, str(Path(ROOT) / hotel_name))
    row = {
        "hotel_id": hotel_id,
        "date": date,
        "rooms_out_of_service": metrics.get("rooms_out_of_service"),
        "total_revenue": metrics.get("total_revenue"),
        "fb_revenue": metrics.get("fb_revenue"),
        "pending_balance": metrics.get("pending_balance"),
    }

    try:
        db.upsert_pdf_metrics([row])
        db.log_import(file_path, "ok", 1)
        db.update_file_cache(file_path, mtime)
        return 1
    except Exception as exc:
        db.log_import(file_path, "error", 0, str(exc))
        logger.error("DB error for %s: %s", file_path, exc)
        return 0


def import_all_pdfs(progress_callback=None, since_year: int | None = None) -> int:
    root = Path(ROOT)
    seen: set[Path] = set()
    files: list[Path] = []

    for glob_pattern in PDF_GLOBS:
        for f in root.glob(glob_pattern):
            if f not in seen:
                seen.add(f)
                files.append(f)

    if HOTELS_FILTER:
        files = [f for f in files if f.parts[len(root.parts)] in HOTELS_FILTER]

    if since_year is not None:
        files = [f for f in files if _path_has_year_gte(f, since_year)]

    # Batch cache check — one DB call instead of one per file
    file_mtimes = {str(f): f.stat().st_mtime for f in files}
    files.sort(key=lambda f: file_mtimes[str(f)])
    unchanged_paths = db.get_unchanged_files(file_mtimes)

    total = 0
    skipped = len(unchanged_paths)
    for i, f in enumerate(files):
        if str(f) in unchanged_paths:
            if progress_callback:
                progress_callback(i + 1, len(files), str(f), skipped)
            continue
        try:
            count = import_pdf_file(str(f), force=True)
        except Exception as exc:
            logger.error("Erro ao importar PDF %s: %s", f.name, exc)
            count = 0
        if count > 0:
            total += count
        if progress_callback:
            progress_callback(i + 1, len(files), str(f), skipped)

    logger.info("PDF import: %d files processed (%d skipped unchanged)", total, skipped)
    return total
