"""
Parses daily hotel report Excel files and imports data into the database.

FORMAT A (.xlsx) — "histórico e previsões" (1905 Zinos Palace, 2025 files)
  skiprows=12, date DD-MM-YY, cols: 0=date, 2=rooms, 9=occ%, 11=revenue, 12=avgprice

FORMAT A XLS (.xls) — "histórico e previsões" (1905 Zinos Palace, 2026 files, BIFF2)
  skiprows=12, date DD-MM-YY, cols: 0=date, 2=rooms, 11=occ%, 13=revenue, 14=avgprice
  (2 extra blank cols shift everything right compared to FORMAT_A)

FORMAT B — "150. Histórico e Previsão" / "150. History and Forecast" (most hotels)
  skiprows=6, date as datetime, cols: 0=date, 1=rooms, 9=occ%, 10=revenue, 11=avgprice

FORMAT LUSTER — "150. Histórico e Previsão" from Luster folder
  skiprows=5, date as "DD-abr-YYYY dia" (Portuguese months), cols: 0=date, 1=rooms, 7=occ%, 9=revenue, 10=avgprice
"""

import re
import logging
import warnings
from pathlib import Path

import pandas as pd

import database as db

logger = logging.getLogger(__name__)

ROOT = r"C:\Users\Bruno Barbosa\OneDrive - Amazing Evolution, S.A\Sales, Marketing & Revenue - Relatórios Hotéis"

# Hotels to import. Add or remove names to control which hotels appear in the dashboard.
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

# Multiple globs to cover different folder naming conventions across hotels
DAILY_GLOBS = [
    "*/Relatórios Diários*/**/*.xlsx",   # most hotels
    "*/Relatórios diários*/**/*.xlsx",   # The Shipyard Angra (lowercase d)
    "*/Relatórios Diários*/**/*.xls",    # 1905 Zinos Palace (legacy BIFF2 format)
    "Land of Alandroal/????_??_??/*.xlsx",  # 2026+: flat YYYY_MM_DD folder structure
    "Land of Alandroal/????_??_??/*.xls",
]

COL_NAMES = ["date", "occupancy_rooms", "occupancy_pct", "room_revenue", "avg_room_price"]

_FORMAT_A      = {"skiprows": 12, "usecols": [0, 2,  9, 11, 12], "engine": "openpyxl"}  # 1905 Zinos .xlsx
_FORMAT_A_XLS  = {"skiprows": 12, "usecols": [0, 2, 11, 13, 14], "engine": "xlrd"}      # 1905 Zinos .xls (BIFF2)
_FORMAT_PV     = {"skiprows": 11, "usecols": [0, 1,  9, 11, 12], "engine": "openpyxl"}  # Placid Village
_FORMAT_B      = {"skiprows": 6,  "usecols": [0, 1,  9, 10, 11], "engine": "openpyxl"}  # 150. Histórico (most hotels)
_FORMAT_LUSTER = {"skiprows": 5,  "usecols": [0, 1,  7,  9, 10], "engine": "openpyxl"}  # Luster

# Portuguese short month names → English (for pandas date parsing)
_PT_MONTHS = {
    "jan": "jan", "fev": "feb", "mar": "mar", "abr": "apr",
    "mai": "may", "jun": "jun", "jul": "jul", "ago": "aug",
    "set": "sep", "out": "oct", "nov": "nov", "dez": "dec",
}

# Patch xlrd to tolerate BIFF2 files with non-zero XF slots (1905 Zinos PMS export)
try:
    import xlrd.sheet as _xlrd_sheet
    _orig_biff2 = _xlrd_sheet.Sheet.fixed_BIFF2_xfindex
    def _patched_biff2(self, cell_attr, rowx, colx):
        try:
            return _orig_biff2(self, cell_attr, rowx, colx)
        except AssertionError:
            return 0
    _xlrd_sheet.Sheet.fixed_BIFF2_xfindex = _patched_biff2
except Exception:
    pass


def _detect_format(name: str) -> dict | None:
    n = name.lower()
    ext = Path(name).suffix.lower()
    # "150." or "150 " prefix (Solar dos Cantos uses "150 Hist e Previ.xlsx")
    if re.match(r"150[\. ]", n) or "history and forecast" in n:
        return _FORMAT_B
    if re.match(r"hist[oó]rico e previs[aã]o", n):
        return _FORMAT_B
    if re.match(r"h&f\s", n):
        return _FORMAT_PV
    # "histórico e previsões" — .xls = BIFF2 (2026+), .xlsx = older format
    if re.match(r"hist[oó]rico e.{0,4}previs[oõ]es", n):
        return _FORMAT_A_XLS if ext == ".xls" else _FORMAT_A
    return None


def _hotel_name_from_path(file_path: str) -> str:
    rel = Path(file_path).relative_to(ROOT)
    return rel.parts[0]


def _fix_pt_date(v):
    """Convert Portuguese date strings like '01-abr-2026 qua' to '01-apr-2026'.
    Non-string values (Excel Timestamps) are returned as-is to avoid dayfirst mis-parsing."""
    if not isinstance(v, str):
        return v
    s = v.split()[0]
    for pt, en in _PT_MONTHS.items():
        s = re.sub(pt, en, s, flags=re.IGNORECASE)
    return s


def parse_daily_file(file_path: str) -> list[dict]:
    fmt = _detect_format(Path(file_path).name)
    if fmt is None:
        return []

    # Luster uses different column positions than other FORMAT_B hotels
    if "Luster" in file_path and fmt == _FORMAT_B:
        fmt = _FORMAT_LUSTER

    engine = fmt.get("engine", "openpyxl")

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            xl = pd.ExcelFile(file_path, engine=engine)
        sheets = xl.sheet_names
        frames = []
        for sheet in sheets:
            try:
                frame = xl.parse(
                    sheet,
                    header=None,
                    skiprows=fmt["skiprows"],
                    usecols=fmt["usecols"],
                )
                frames.append(frame)
            except Exception:
                pass
        if not frames:
            return []
        df = pd.concat(frames, ignore_index=True)
    except Exception as exc:
        logger.warning("Cannot read %s: %s", file_path, exc)
        return []

    df.columns = COL_NAMES

    # Normalise date strings: strip day-name suffix and translate Portuguese months
    df["date"] = df["date"].apply(_fix_pt_date)

    # Keep only rows where date is a real date (drops header/subtotal/month rows)
    # dayfirst=True: avoids MM-DD swap for DD-MM-YY string dates (1905 Zinos format)
    parsed_dates = pd.to_datetime(df["date"], errors="coerce", dayfirst=True, format="mixed")
    df = df[parsed_dates.notna()].copy()
    parsed_dates = pd.to_datetime(df["date"], dayfirst=True, format="mixed")
    df["date"] = parsed_dates.dt.date.astype(str)

    # Drop forecast rows (future dates) — files contain both "Histórico" and "Previsão"
    import datetime as _dt
    today_str = str(_dt.date.today())
    df = df[df["date"] <= today_str]

    # Normalise numeric columns — some files use Portuguese comma decimal ("50,00")
    for col in ["occupancy_rooms", "occupancy_pct", "avg_room_price", "room_revenue"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    df = df.dropna(subset=["date"])
    df = df[df["occupancy_rooms"].notna()]

    return df.to_dict(orient="records")


def import_file(file_path: str, force: bool = False) -> int:
    hotel_name = _hotel_name_from_path(file_path)
    if HOTELS_FILTER and hotel_name not in HOTELS_FILTER:
        return 0

    # Skip if file hasn't changed since last import
    mtime = Path(file_path).stat().st_mtime
    if not force and db.is_file_unchanged(file_path, mtime):
        return -1  # -1 = skipped (unchanged)

    hotel_id = db.upsert_hotel(hotel_name, str(Path(ROOT) / hotel_name))

    rows = parse_daily_file(file_path)
    if not rows:
        db.log_import(file_path, "empty", 0)
        db.update_file_cache(file_path, mtime)
        return 0

    for r in rows:
        r["hotel_id"] = hotel_id
        r["source_file"] = file_path

    try:
        count = db.upsert_daily_metrics(rows)
        db.log_import(file_path, "ok", count)
        db.update_file_cache(file_path, mtime)
        return count
    except Exception as exc:
        db.log_import(file_path, "error", 0, str(exc))
        logger.error("DB error for %s: %s", file_path, exc)
        return 0


def _is_relevant(name: str) -> bool:
    return _detect_format(name) is not None and not name.startswith("~")


def _path_has_year_gte(f: Path, since_year: int) -> bool:
    """Return True if the folder path (not filename) contains a 20XX year >= since_year.
    Handles standalone year folders (\\2025\\) and date-named folders (19_08_2025, 07.01.2025).
    Excludes filename so that report names like 'previsoes 2025.xls' don't trigger a false match.
    """
    years = re.findall(r'(?<!\d)(20\d{2})(?!\d)', str(f.parent))
    return any(int(y) >= since_year for y in years)


def _is_annual_report(name: str) -> bool:
    """True for full-year Excel snapshot files (any format)."""
    n = name.lower()
    return bool(
        re.match(r"150[\. ]", n) or
        re.match(r"hist[oó]rico e.{0,4}previs[oõ]es", n) or
        re.match(r"hist[oó]rico e previs[aã]o", n) or
        re.match(r"h&f\s", n)
    )


def _snapshot_sort_key(f: Path) -> str:
    """Sort key for deduplicating annual report files.
    Prefers the date embedded in the filename over mtime — OneDrive can update
    mtime on old files during sync, causing a stale snapshot to appear newer."""
    name = f.name
    # ISO: YYYY-MM-DD (e.g. "v2026-05-03")
    m = re.search(r'(20\d{2})-(\d{2})-(\d{2})', name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # DD-MM-YYYY or DD.MM.YYYY (e.g. "24-04-2026")
    m = re.search(r'(\d{2})[.\-](\d{2})[.\-](20\d{2})', name)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # Fall back to mtime when no date is parseable from the filename
    import datetime as _dt
    return _dt.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')


def _deduplicate_annual_reports(files: list, root) -> list:
    """For annual 'Histórico e Previsão' files, keep only the most recent
    per (hotel, year) — each file spans the full year so older copies are redundant."""
    annual: dict = {}
    others: list = []
    for f in files:
        if not _is_annual_report(f.name):
            others.append(f)
            continue
        hotel = f.parts[len(root.parts)]
        # Year from filename first, then from folder path
        year_m = re.search(r'(20\d{2})', f.name)
        if not year_m:
            year_m = re.search(r'(20\d{2})', str(f.parent))
        year = year_m.group(1) if year_m else "unknown"
        key = (hotel, year)
        if key not in annual or _snapshot_sort_key(f) > _snapshot_sort_key(annual[key]):
            annual[key] = f
    return others + list(annual.values())


def import_all(progress_callback=None, since_year: int | None = None, since_mtime: float | None = None) -> int:
    root = Path(ROOT)
    seen = set()
    files = []
    for glob in DAILY_GLOBS:
        for f in root.glob(glob):
            if f not in seen and _is_relevant(f.name):
                seen.add(f)
                files.append(f)

    # Apply hotel filter at file level for efficiency
    if HOTELS_FILTER:
        files = [f for f in files if f.parts[len(root.parts)] in HOTELS_FILTER]

    # Limit to recent years when requested (avoids scanning thousands of old files)
    if since_year is not None:
        files = [f for f in files if _path_has_year_gte(f, since_year)]

    # Limit to recently modified files (for fast "import recent" runs)
    if since_mtime is not None:
        files = [f for f in files if f.stat().st_mtime >= since_mtime]

    # For "150. Histórico e Previsão" annual reports, keep only the most recent
    # file per (hotel, year) — each file already contains the full year's history.
    files = _deduplicate_annual_reports(files, root)

    # Process oldest files first so newer files always win the UPSERT
    file_mtimes = {str(f): f.stat().st_mtime for f in files}
    files.sort(key=lambda f: file_mtimes[str(f)])

    # Batch cache check — one DB call instead of one per file
    unchanged_paths = db.get_unchanged_files(file_mtimes)

    total = 0
    skipped = len(unchanged_paths)
    for i, f in enumerate(files):
        if str(f) in unchanged_paths:
            if progress_callback:
                progress_callback(i + 1, len(files), str(f), skipped)
            continue
        try:
            count = import_file(str(f), force=True)
        except Exception as exc:
            logger.error("Erro ao importar %s: %s", f.name, exc)
            count = 0
        if count > 0:
            total += count
        if progress_callback:
            progress_callback(i + 1, len(files), str(f), skipped)

    logger.info("Import complete: %d rows from %d files (%d skipped unchanged)", total, len(files) - skipped, skipped)
    return total


def is_daily_report(file_path: str) -> bool:
    name = Path(file_path).name
    hotel = _hotel_name_from_path(file_path) if ROOT in file_path else ""
    in_daily_folder = any(
        kw in file_path for kw in ("Relatórios Diários", "Relatórios diários")
    )
    is_alandroal_flat = (
        "Land of Alandroal" in file_path
        and bool(re.search(r"[\\/]\d{4}_\d{2}_\d{2}[\\/]", file_path))
    )
    return (
        (file_path.endswith(".xlsx") or file_path.endswith(".xls"))
        and (in_daily_folder or is_alandroal_flat)
        and _is_relevant(name)
        and (not HOTELS_FILTER or hotel in HOTELS_FILTER)
    )
