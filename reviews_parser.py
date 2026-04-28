"""
Reviews parser — reads standardized Excel files from OneDrive.

Place one file per hotel at:
  [Hotel]/Reviews/reviews.xlsx   (ou avaliações.xlsx)

Sheets:
  Scores   — Plataforma | Ano | Mês | Score | Num_Reviews | Taxa_Resposta | Tempo_Resposta_h
  Queixas  — Ano | Mês | Departamento | Queixa | Volume | Sentimento
  Palavras — Ano | Mês | Palavra | Frequência | Sentimento
  Compset  — Ano | Mês | Concorrente | Plataforma | Score_Concorrente | Rank
"""
import logging
from pathlib import Path

import pandas as pd

import database as db

logger = logging.getLogger(__name__)

REVIEWS_GLOBS = [
    "*/Reviews/*.xlsx",
    "*/Avaliações/*.xlsx",
    "*/avaliacoes/*.xlsx",
]


def is_reviews_file(path: str) -> bool:
    p = Path(path)
    return p.suffix.lower() == ".xlsx" and p.parent.name.lower() in {
        "reviews", "avaliações", "avaliacoes"
    }


def import_reviews_file(file_path: str, hotel_id: int) -> dict:
    counts = {"scores": 0, "complaints": 0, "keywords": 0, "compset": 0}
    p = Path(file_path)
    try:
        xl = pd.ExcelFile(p, engine="openpyxl")
    except Exception as e:
        logger.error("Não foi possível abrir %s: %s", file_path, e)
        return counts

    if "Scores" in xl.sheet_names:
        try:
            df = xl.parse("Scores")
            rows = []
            for _, r in df.iterrows():
                platform = str(r.get("Plataforma", "")).strip().lower()
                year = _int(r.get("Ano"))
                month = _int(r.get("Mês"))
                if not platform or not year or not month:
                    continue
                rows.append({
                    "hotel_id": hotel_id,
                    "platform": platform,
                    "period": f"{year}-{month:02d}-01",
                    "score": _float(r.get("Score")),
                    "num_reviews": _int(r.get("Num_Reviews")),
                    "response_rate": _float(r.get("Taxa_Resposta")),
                    "avg_response_hours": _float(r.get("Tempo_Resposta_h")),
                })
            counts["scores"] = db.upsert_review_scores(rows)
        except Exception as e:
            logger.error("Erro sheet Scores: %s", e)

    if "Queixas" in xl.sheet_names:
        try:
            df = xl.parse("Queixas")
            rows = []
            for _, r in df.iterrows():
                dept = str(r.get("Departamento", "")).strip()
                complaint = str(r.get("Queixa", "")).strip()
                year = _int(r.get("Ano"))
                month = _int(r.get("Mês"))
                if not dept or not complaint or not year or not month:
                    continue
                rows.append({
                    "hotel_id": hotel_id,
                    "period": f"{year}-{month:02d}-01",
                    "department": dept,
                    "complaint": complaint,
                    "volume": _int(r.get("Volume")) or 1,
                    "sentiment": str(r.get("Sentimento", "negativo")).strip().lower(),
                })
            counts["complaints"] = db.upsert_review_complaints(rows)
        except Exception as e:
            logger.error("Erro sheet Queixas: %s", e)

    if "Palavras" in xl.sheet_names:
        try:
            df = xl.parse("Palavras")
            rows = []
            for _, r in df.iterrows():
                keyword = str(r.get("Palavra", "")).strip()
                year = _int(r.get("Ano"))
                month = _int(r.get("Mês"))
                if not keyword or not year or not month:
                    continue
                rows.append({
                    "hotel_id": hotel_id,
                    "period": f"{year}-{month:02d}-01",
                    "keyword": keyword,
                    "frequency": _int(r.get("Frequência")) or 1,
                    "sentiment": str(r.get("Sentimento", "neutro")).strip().lower(),
                })
            counts["keywords"] = db.upsert_review_keywords(rows)
        except Exception as e:
            logger.error("Erro sheet Palavras: %s", e)

    if "Compset" in xl.sheet_names:
        try:
            df = xl.parse("Compset")
            rows = []
            for _, r in df.iterrows():
                competitor = str(r.get("Concorrente", "")).strip()
                platform = str(r.get("Plataforma", "")).strip().lower()
                year = _int(r.get("Ano"))
                month = _int(r.get("Mês"))
                if not competitor or not platform or not year or not month:
                    continue
                rows.append({
                    "hotel_id": hotel_id,
                    "period": f"{year}-{month:02d}-01",
                    "competitor": competitor,
                    "platform": platform,
                    "competitor_score": _float(r.get("Score_Concorrente")),
                    "our_rank": _int(r.get("Rank")),
                })
            counts["compset"] = db.upsert_review_compset(rows)
        except Exception as e:
            logger.error("Erro sheet Compset: %s", e)

    return counts


def import_all_reviews(root: Path, progress_callback=None) -> int:
    from parser import HOTELS_FILTER
    files = []
    for pat in REVIEWS_GLOBS:
        files.extend(root.glob(pat))
    if HOTELS_FILTER:
        files = [f for f in files if f.parts[len(root.parts)] in HOTELS_FILTER]

    total = 0
    for i, f in enumerate(files):
        hotel_name = f.parts[len(root.parts)]
        hotel_id = db.upsert_hotel(hotel_name, str(f.parent.parent))
        counts = import_reviews_file(str(f), hotel_id)
        total += sum(counts.values())
        if progress_callback:
            progress_callback(i + 1, len(files), str(f))
    return total


def _float(v):
    try:
        return float(v)
    except Exception:
        return None


def _int(v):
    try:
        return int(float(v))
    except Exception:
        return None
