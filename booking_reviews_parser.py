"""
Parser for Booking.com review CSV exports.

Place one CSV (or XLSX) per hotel inside:
  C:\\Users\\Bruno Barbosa\\OneDrive - Amazing Evolution, S.A\\Booking - Comentários\\[Hotel Name]\\

The file is the direct export from Booking.com extranet (Reviews section).
Expected columns (Portuguese):
  Data do comentário, Nome do hóspede, Número da reserva,
  Título do comentário, Comentário positivo, Comentário negativo,
  Pontuação de comentários, Funcionários, Limpeza, Localização,
  Comodidades, Conforto, Relação preço-qualidade, Resposta do alojamento
"""

import logging
import re
from pathlib import Path

import pandas as pd

import database as db

logger = logging.getLogger(__name__)

BOOKING_ROOT = r"C:\Users\Bruno Barbosa\OneDrive - Amazing Evolution, S.A\Booking - Comentários"

# Column name mappings (Booking.com export → internal)
_COL_MAP = {
    "Data do comentário":        "review_date",
    "Nome do hóspede":           "guest_name",
    "Número da reserva":         "reservation_number",
    "Título do comentário":      "title",
    "Comentário positivo":       "positive_comment",
    "Comentário negativo":       "negative_comment",
    "Pontuação de comentários":  "overall_score",
    "Funcionários":              "staff_score",
    "Limpeza":                   "cleanliness_score",
    "Localização":               "location_score",
    "Comodidades":               "facilities_score",
    "Conforto":                  "comfort_score",
    "Relação preço-qualidade":   "value_score",
    "Resposta do alojamento":    "property_response",
    "Tipo de grupo":             "traveler_type",
    "Tipo de viajante":          "traveler_type",
    "Tipo de hóspede":           "traveler_type",
}


def _read_file(file_path: Path) -> pd.DataFrame | None:
    try:
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, encoding="utf-8-sig")
        else:
            df = pd.read_excel(file_path, engine="openpyxl")
    except Exception as exc:
        logger.warning("Não foi possível ler %s: %s", file_path, exc)
        return None

    # Rename columns to internal names
    df = df.rename(columns=_COL_MAP)

    required = {"review_date", "overall_score"}
    if not required.issubset(df.columns):
        logger.warning("Ficheiro %s não tem colunas esperadas", file_path)
        return None

    return df


def _aggregate_monthly_scores(df: pd.DataFrame, hotel_id: int) -> list[dict]:
    """Aggregate reviews: one monthly row per month + one overall row at current month.
    The overall row uses today's period so it always appears as the latest score,
    matching the global average shown on Booking.com.
    """
    import datetime
    df = df.copy()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df = df.dropna(subset=["review_date", "overall_score"])
    df["overall_score"] = pd.to_numeric(df["overall_score"], errors="coerce")
    df = df.dropna(subset=["overall_score"])
    df["period"] = df["review_date"].dt.to_period("M").dt.to_timestamp()

    has_response_col = "property_response" in df.columns

    rows = []
    for period, group in df.groupby("period"):
        if has_response_col:
            responded = group["property_response"].notna() & (group["property_response"].astype(str).str.strip() != "")
            resp_rate = round(float(responded.sum() / len(group) * 100), 1)
        else:
            resp_rate = None
        rows.append({
            "hotel_id":          hotel_id,
            "platform":          "booking",
            "period":            str(period.date()),
            "score":             round(float(group["overall_score"].mean()), 2),
            "num_reviews":       int(len(group)),
            "response_rate":     resp_rate,
            "avg_response_hours": None,
        })

    # Overall score: weighted average (últimos 36 meses, reviews mais recentes pesam mais)
    today_dt = pd.Timestamp.today().normalize()
    cutoff = today_dt - pd.DateOffset(months=36)
    recent = df[df["review_date"] >= cutoff].copy()
    if recent.empty:
        recent = df.copy()
    # Linear weight: 0 no cutoff → 1 hoje
    span_days = max((today_dt - cutoff).days, 1)
    recent["weight"] = ((recent["review_date"] - cutoff).dt.days / span_days).clip(lower=0)
    total_weight = recent["weight"].sum()
    if total_weight > 0:
        weighted_score = float((recent["overall_score"] * recent["weight"]).sum() / total_weight)
    else:
        weighted_score = float(recent["overall_score"].mean())

    if has_response_col:
        responded_recent = recent["property_response"].notna() & (recent["property_response"].astype(str).str.strip() != "")
        overall_resp_rate = round(float(responded_recent.sum() / max(len(recent), 1) * 100), 1)
    else:
        overall_resp_rate = None

    today = datetime.date.today().replace(day=1)
    rows.append({
        "hotel_id":          hotel_id,
        "platform":          "booking",
        "period":            str(today),
        "score":             round(weighted_score, 1),
        "num_reviews":       0,
        "response_rate":     overall_resp_rate,
        "avg_response_hours": None,
    })
    return rows


def _parse_individual_reviews(df: pd.DataFrame, hotel_id: int) -> list[dict]:
    """Parse individual review rows for booking_reviews table."""
    df = df.copy()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df = df.dropna(subset=["review_date"])

    score_cols = ["overall_score", "staff_score", "cleanliness_score",
                  "location_score", "facilities_score", "comfort_score", "value_score"]
    for col in score_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    str_cols = ["guest_name", "reservation_number", "title",
                "positive_comment", "negative_comment", "property_response", "traveler_type"]
    for col in str_cols:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = df[col].where(df[col].notna(), None)

    rows = []
    for _, r in df.iterrows():
        def _f(v):
            return float(v) if pd.notna(v) else None
        rows.append({
            "hotel_id":          hotel_id,
            "review_date":       str(r["review_date"].date()),
            "guest_name":        r.get("guest_name") if pd.notna(r.get("guest_name")) else None,
            "reservation_number": str(r["reservation_number"]) if pd.notna(r.get("reservation_number")) else None,
            "title":             r.get("title") if pd.notna(r.get("title")) else None,
            "positive_comment":  r.get("positive_comment") if pd.notna(r.get("positive_comment")) else None,
            "negative_comment":  r.get("negative_comment") if pd.notna(r.get("negative_comment")) else None,
            "overall_score":     _f(r.get("overall_score")),
            "staff_score":       _f(r.get("staff_score")),
            "cleanliness_score": _f(r.get("cleanliness_score")),
            "location_score":    _f(r.get("location_score")),
            "facilities_score":  _f(r.get("facilities_score")),
            "comfort_score":     _f(r.get("comfort_score")),
            "value_score":       _f(r.get("value_score")),
            "property_response": r.get("property_response") if pd.notna(r.get("property_response")) else None,
            "traveler_type":     r.get("traveler_type") if pd.notna(r.get("traveler_type")) else None,
        })
    return rows


def import_booking_file(file_path: Path, hotel_name: str) -> int:
    df = _read_file(file_path)
    if df is None or df.empty:
        return 0

    hotel_id = db.upsert_hotel(hotel_name, str(file_path.parent))

    # 1. Upsert monthly aggregates into review_scores (shows on existing dashboard)
    monthly = _aggregate_monthly_scores(df, hotel_id)
    if monthly:
        db.upsert_review_scores(monthly)

    # 2. Upsert individual reviews into booking_reviews table
    reviews = _parse_individual_reviews(df, hotel_id)
    if reviews:
        db.upsert_booking_reviews(reviews)
        logger.info("%s: %d reviews da Booking.com importados", hotel_name, len(reviews))
        return len(reviews)

    return 0


def import_all_booking_reviews(progress_callback=None) -> int:
    root = Path(BOOKING_ROOT)
    if not root.exists():
        logger.warning("Pasta Booking - Comentários não encontrada: %s", root)
        return 0

    files = []
    for hotel_dir in root.iterdir():
        if not hotel_dir.is_dir():
            continue
        for ext in ("*.csv", "*.xlsx", "*.xls"):
            files.extend(hotel_dir.glob(ext))

    total = 0
    for i, f in enumerate(files):
        hotel_name = f.parent.name
        try:
            count = import_booking_file(f, hotel_name)
            total += count
        except Exception as exc:
            logger.error("Erro ao importar Booking reviews %s: %s", f.name, exc)
        if progress_callback:
            progress_callback(i + 1, len(files), str(f))

    logger.info("Booking reviews: %d reviews importados de %d ficheiros", total, len(files))
    return total
