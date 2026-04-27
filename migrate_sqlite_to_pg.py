"""
One-time migration: copy all data from hotel_data.db (SQLite) to Supabase (PostgreSQL).

Usage:
    1. Set DATABASE_URL in .env or as environment variable
    2. Run:  python migrate_sqlite_to_pg.py
"""

import os
import sqlite3

from dotenv import load_dotenv

load_dotenv()

import database as db  # noqa: E402  (needs DATABASE_URL loaded first)

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "hotel_data.db")


def _sqlite_rows(conn, query):
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(query).fetchall()]


def main():
    print("A ligar ao SQLite...")
    sq = sqlite3.connect(SQLITE_PATH)

    print("A criar tabelas no PostgreSQL...")
    db.init_db()

    # ── hotels ───────────────────────────────────────────────────────────────
    hotels = _sqlite_rows(sq, "SELECT * FROM hotels ORDER BY id")
    print(f"  {len(hotels)} hotéis encontrados")
    with db.get_conn() as conn:
        for h in hotels:
            conn.execute(
                """
                INSERT INTO hotels (id, name, folder_path)
                VALUES (%(id)s, %(name)s, %(folder_path)s)
                ON CONFLICT (name) DO NOTHING
                """,
                h,
            )
        # Reset sequence so future inserts don't collide
        conn.execute("SELECT setval('hotels_id_seq', (SELECT MAX(id) FROM hotels))")
    print("  hotels migrados ✓")

    # ── daily_metrics ────────────────────────────────────────────────────────
    metrics = _sqlite_rows(sq, "SELECT * FROM daily_metrics ORDER BY id")
    print(f"  {len(metrics)} registos diários encontrados")
    with db.get_conn() as conn:
        for row in metrics:
            row.pop("id", None)
            conn.execute(
                """
                INSERT INTO daily_metrics
                    (hotel_id, date, occupancy_rooms, occupancy_pct, room_revenue,
                     avg_room_price, source_file, imported_at,
                     rooms_out_of_service, total_revenue, fb_revenue, pending_balance)
                VALUES
                    (%(hotel_id)s, %(date)s, %(occupancy_rooms)s, %(occupancy_pct)s,
                     %(room_revenue)s, %(avg_room_price)s, %(source_file)s, %(imported_at)s,
                     %(rooms_out_of_service)s, %(total_revenue)s, %(fb_revenue)s, %(pending_balance)s)
                ON CONFLICT (hotel_id, date) DO NOTHING
                """,
                row,
            )
    print("  daily_metrics migrados ✓")

    # ── otb_metrics ──────────────────────────────────────────────────────────
    otb = _sqlite_rows(sq, "SELECT * FROM otb_metrics")
    print(f"  {len(otb)} registos OTB encontrados")
    if otb:
        db.upsert_otb_metrics(otb)
    print("  otb_metrics migrados ✓")

    # ── file_cache ───────────────────────────────────────────────────────────
    cache = _sqlite_rows(sq, "SELECT * FROM file_cache")
    print(f"  {len(cache)} entradas de cache encontradas")
    with db.get_conn() as conn:
        for row in cache:
            conn.execute(
                """
                INSERT INTO file_cache (file_path, mtime)
                VALUES (%(file_path)s, %(mtime)s)
                ON CONFLICT (file_path) DO NOTHING
                """,
                row,
            )
    print("  file_cache migrado ✓")

    sq.close()
    print("\nMigração concluída com sucesso!")


if __name__ == "__main__":
    main()
