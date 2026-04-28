"""
Google Places API integration — fetches rating, review count and
recent review texts for each hotel and stores them in review_scores
and review_keywords.
"""
import json
import logging
import re
import urllib.parse
import urllib.request
from datetime import date

import database as db

logger = logging.getLogger(__name__)

PLACES_BASE = "https://maps.googleapis.com/maps/api/place"

# Common Portuguese and English stop words to skip in word cloud
_STOP = {
    "o","a","os","as","um","uma","de","do","da","dos","das","em","no","na",
    "nos","nas","e","é","que","se","para","com","por","foi","tem","muito",
    "mais","mas","não","sim","ao","até","já","bem","só","há","ser","ter",
    "the","and","is","in","of","to","was","we","had","our","very","but",
    "this","at","it","for","are","not","be","on","as","an","my","i","were",
    "have","from","with","all","also","its","no","or","so","they","their",
    "hotel","quarto","quartos","hotel","estadia",
}


def _get(endpoint: str, params: dict) -> dict:
    url = f"{PLACES_BASE}/{endpoint}/json?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=12) as r:
        return json.loads(r.read())


def find_place_id(hotel_name: str, api_key: str) -> str | None:
    try:
        data = _get("findplacefromtext", {
            "input": hotel_name,
            "inputtype": "textquery",
            "fields": "place_id,name",
            "key": api_key,
        })
        candidates = data.get("candidates", [])
        if candidates:
            pid = candidates[0]["place_id"]
            logger.info("Google Place ID encontrado para %s: %s", hotel_name, pid)
            return pid
        logger.warning("Nenhum resultado Google para: %s", hotel_name)
    except Exception as e:
        logger.error("find_place_id falhou para %s: %s", hotel_name, e)
    return None


def fetch_details(place_id: str, api_key: str) -> dict | None:
    try:
        data = _get("details", {
            "place_id": place_id,
            "fields": "name,rating,user_ratings_total,reviews",
            "language": "pt",
            "reviews_sort": "newest",
            "key": api_key,
        })
        status = data.get("status")
        if status != "OK":
            logger.warning("Places API status: %s para place_id=%s", status, place_id)
            return None
        return data.get("result")
    except Exception as e:
        logger.error("fetch_details falhou para %s: %s", place_id, e)
    return None


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


def import_google_reviews(api_key: str, allowed_hotels=None) -> int:
    if not api_key:
        logger.warning("GOOGLE_PLACES_API_KEY não definida — Google Reviews ignorado.")
        return 0

    from parser import HOTELS_FILTER
    hotels = db.get_all_hotels(allowed_hotels=allowed_hotels or HOTELS_FILTER)
    today  = date.today()
    period = f"{today.year}-{today.month:02d}-01"
    total  = 0

    for hotel in hotels:
        hotel_id   = hotel["id"]
        hotel_name = hotel["name"]

        # Retrieve or discover Place ID
        place_id = db.get_hotel_place_id(hotel_id)
        if not place_id:
            place_id = find_place_id(hotel_name, api_key)
            if place_id:
                db.set_hotel_place_id(hotel_id, place_id)

        if not place_id:
            logger.warning("Sem Place ID para %s — a ignorar.", hotel_name)
            continue

        details = fetch_details(place_id, api_key)
        if not details:
            continue

        rating  = details.get("rating")
        num_rev = details.get("user_ratings_total")
        reviews = details.get("reviews", [])

        db.upsert_review_scores([{
            "hotel_id":           hotel_id,
            "platform":           "google",
            "period":             period,
            "score":              rating,
            "num_reviews":        num_rev,
            "response_rate":      None,
            "avg_response_hours": None,
        }])
        total += 1

        if reviews:
            texts   = [r.get("text", "") for r in reviews if r.get("text")]
            kw_rows = _keywords_from_texts(hotel_id, period, texts)
            if kw_rows:
                db.upsert_review_keywords(kw_rows)

        logger.info("Google Reviews: %s — %.1f estrelas (%d reviews)",
                    hotel_name, rating or 0, num_rev or 0)

    return total
