"""
TripAdvisor Content API integration — fetches rating, review count
and recent review texts for each hotel.

Free tier: 5 000 requests/month (no credit card required).
Register at: tripadvisor.com/developers
"""
import json
import logging
import re
import urllib.parse
import urllib.request
from datetime import date

import database as db

logger = logging.getLogger(__name__)

TA_BASE = "https://api.content.tripadvisor.com/api/v1"

_STOP = {
    "o","a","os","as","um","uma","de","do","da","dos","das","em","no","na",
    "nos","nas","e","é","que","se","para","com","por","foi","tem","muito",
    "mais","mas","não","sim","ao","até","já","bem","só","há","ser","ter",
    "the","and","is","in","of","to","was","we","had","our","very","but",
    "this","at","it","for","are","not","be","on","as","an","my","i","were",
    "have","from","with","all","also","its","no","or","so","they","their",
    "hotel","quarto","quartos","estadia","stay","room","rooms",
}


def _get(path: str, params: dict) -> dict:
    url = f"{TA_BASE}/{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read())


def find_location_id(hotel_name: str, api_key: str) -> str | None:
    try:
        data = _get("location/search", {
            "searchQuery": hotel_name,
            "category": "hotels",
            "language": "pt",
            "key": api_key,
        })
        results = data.get("data", [])
        if results:
            lid = str(results[0]["location_id"])
            logger.info("TripAdvisor location ID para %s: %s", hotel_name, lid)
            return lid
        logger.warning("Nenhum resultado TripAdvisor para: %s", hotel_name)
    except Exception as e:
        logger.error("find_location_id falhou para %s: %s", hotel_name, e)
    return None


def fetch_details(location_id: str, api_key: str) -> dict | None:
    try:
        return _get(f"location/{location_id}/details", {
            "language": "pt",
            "currency": "EUR",
            "key": api_key,
        })
    except Exception as e:
        logger.error("fetch_details TA falhou para %s: %s", location_id, e)
    return None


def fetch_reviews(location_id: str, api_key: str) -> list:
    try:
        data = _get(f"location/{location_id}/reviews", {
            "language": "pt",
            "key": api_key,
        })
        return data.get("data", [])
    except Exception as e:
        logger.error("fetch_reviews TA falhou para %s: %s", location_id, e)
    return []


def _keywords_from_texts(hotel_id: int, period: str, texts: list[str]) -> list[dict]:
    freq: dict[str, int] = {}
    for text in texts:
        for word in re.findall(r"[a-záàâãéèêíóôõúüçA-ZÁÀÂÃÉÈÊÍÓÔÕÚÜÇ]{3,}", text.lower()):
            if word not in _STOP:
                freq[word] = freq.get(word, 0) + 1
    return [
        {"hotel_id": hotel_id, "period": period, "keyword": k,
         "frequency": v, "sentiment": "neutro"}
        for k, v in sorted(freq.items(), key=lambda x: -x[1])[:50]
    ]


def import_tripadvisor_reviews(api_key: str, allowed_hotels=None) -> int:
    if not api_key:
        logger.warning("TRIPADVISOR_API_KEY não definida — TripAdvisor ignorado.")
        return 0

    from parser import HOTELS_FILTER
    hotels = db.get_all_hotels(allowed_hotels=allowed_hotels or HOTELS_FILTER)
    today  = date.today()
    period = f"{today.year}-{today.month:02d}-01"
    total  = 0

    for hotel in hotels:
        hotel_id   = hotel["id"]
        hotel_name = hotel["name"]

        location_id = db.get_hotel_ta_location_id(hotel_id)
        if not location_id:
            location_id = find_location_id(hotel_name, api_key)
            if location_id:
                db.set_hotel_ta_location_id(hotel_id, location_id)

        if not location_id:
            logger.warning("Sem location ID TripAdvisor para %s.", hotel_name)
            continue

        details = fetch_details(location_id, api_key)
        if not details:
            continue

        rating  = details.get("rating")
        num_rev = details.get("num_reviews")

        try:
            rating  = float(rating)  if rating  else None
            num_rev = int(num_rev)   if num_rev else None
        except Exception:
            pass

        db.upsert_review_scores([{
            "hotel_id":           hotel_id,
            "platform":           "tripadvisor",
            "period":             period,
            "score":              rating,
            "num_reviews":        num_rev,
            "response_rate":      None,
            "avg_response_hours": None,
        }])
        total += 1

        reviews = fetch_reviews(location_id, api_key)
        if reviews:
            texts   = [r.get("text", "") for r in reviews if r.get("text")]
            kw_rows = _keywords_from_texts(hotel_id, period, texts)
            if kw_rows:
                db.upsert_review_keywords(kw_rows)

        logger.info("TripAdvisor: %s — %.1f estrelas (%s reviews)",
                    hotel_name, rating or 0, num_rev or 0)

    return total
