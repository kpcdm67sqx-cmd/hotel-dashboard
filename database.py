import logging
import os
import time
from contextlib import contextmanager

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if _DATABASE_URL.startswith("postgres://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1)

logger = logging.getLogger(__name__)


class _Conn:
    """Thin wrapper around psycopg2 that mimics the sqlite3 connection API."""

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql, params=()):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur

    def executemany(self, sql, rows):
        cur = self._conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        return cur


@contextmanager
def get_conn():
    """Open a DB connection with up to 5 retries on transient SSL/network/DNS errors."""
    _DELAYS = [2, 5, 10, 20, 30]
    last_exc = None
    for attempt in range(5):
        try:
            pg = psycopg2.connect(_DATABASE_URL, connect_timeout=15)
            conn = _Conn(pg)
            try:
                yield conn
                pg.commit()
            except Exception:
                pg.rollback()
                raise
            finally:
                pg.close()
            return
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt < 4:
                delay = _DELAYS[attempt]
                logger.warning("DB connection error (tentativa %d/5): %s — a tentar em %ds...", attempt + 1, exc, delay)
                time.sleep(delay)
            else:
                raise last_exc


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hotels (
                id          BIGSERIAL PRIMARY KEY,
                name        TEXT UNIQUE NOT NULL,
                folder_path TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_metrics (
                id              BIGSERIAL PRIMARY KEY,
                hotel_id        BIGINT NOT NULL REFERENCES hotels(id),
                date            DATE NOT NULL,
                occupancy_rooms INTEGER,
                occupancy_pct   DOUBLE PRECISION,
                room_revenue    DOUBLE PRECISION,
                avg_room_price  DOUBLE PRECISION,
                source_file     TEXT,
                imported_at     TIMESTAMP DEFAULT NOW(),
                UNIQUE(hotel_id, date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_daily_hotel_date
                ON daily_metrics(hotel_id, date)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                id            BIGSERIAL PRIMARY KEY,
                file_path     TEXT NOT NULL,
                status        TEXT NOT NULL,
                rows_upserted INTEGER DEFAULT 0,
                error_msg     TEXT,
                logged_at     TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS file_cache (
                file_path TEXT PRIMARY KEY,
                mtime     DOUBLE PRECISION NOT NULL,
                cached_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS otb_metrics (
                hotel_id                  BIGINT NOT NULL REFERENCES hotels(id),
                analysis_type             TEXT NOT NULL,
                month                     INTEGER NOT NULL,
                occ_pct_current           DOUBLE PRECISION,
                occ_pct_comparison        DOUBLE PRECISION,
                nights_current            INTEGER,
                nights_comparison         INTEGER,
                variance_nights           INTEGER,
                variance_pct              DOUBLE PRECISION,
                total_revenue_current     DOUBLE PRECISION,
                total_revenue_comparison  DOUBLE PRECISION,
                total_revenue_variance    DOUBLE PRECISION,
                total_revenue_var_pct     DOUBLE PRECISION,
                room_revenue_current      DOUBLE PRECISION,
                room_revenue_comparison   DOUBLE PRECISION,
                room_revenue_variance     DOUBLE PRECISION,
                room_revenue_var_pct      DOUBLE PRECISION,
                fb_revenue_current        DOUBLE PRECISION,
                fb_revenue_comparison     DOUBLE PRECISION,
                fb_revenue_variance       DOUBLE PRECISION,
                fb_revenue_var_pct        DOUBLE PRECISION,
                other_revenue_current     DOUBLE PRECISION,
                other_revenue_comparison  DOUBLE PRECISION,
                other_revenue_variance    DOUBLE PRECISION,
                other_revenue_var_pct     DOUBLE PRECISION,
                spa_revenue_current       DOUBLE PRECISION,
                spa_revenue_comparison    DOUBLE PRECISION,
                spa_revenue_variance      DOUBLE PRECISION,
                spa_revenue_var_pct       DOUBLE PRECISION,
                adr_current               DOUBLE PRECISION,
                adr_comparison            DOUBLE PRECISION,
                adr_variance              DOUBLE PRECISION,
                adr_var_pct               DOUBLE PRECISION,
                otb_date                  DATE,
                source_file               TEXT,
                imported_at               TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (hotel_id, analysis_type, month)
            )
        """)
        # Idempotent column migrations
        for table, col, col_type in [
            ("daily_metrics", "rooms_out_of_service", "INTEGER"),
            ("daily_metrics", "total_revenue",        "DOUBLE PRECISION"),
            ("daily_metrics", "fb_revenue",           "DOUBLE PRECISION"),
            ("daily_metrics", "pending_balance",      "DOUBLE PRECISION"),
        ]:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )

        # Idempotent: add platform ID columns to hotels
        conn.execute(
            "ALTER TABLE hotels ADD COLUMN IF NOT EXISTS google_place_id TEXT"
        )

        # Migrate otb_metrics PK to include otb_date so multiple weekly
        # snapshots are kept (enables cloud-side insights comparison).
        # Safe to run repeatedly: checks pg_constraint before acting.
        conn.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'otb_metrics_pkey'
                    AND contype = 'p'
                    AND array_length(conkey, 1) = 3
                ) THEN
                    ALTER TABLE otb_metrics DROP CONSTRAINT otb_metrics_pkey;
                    UPDATE otb_metrics SET otb_date = imported_at::date
                        WHERE otb_date IS NULL;
                    ALTER TABLE otb_metrics
                        ADD PRIMARY KEY (hotel_id, analysis_type, month, otb_date);
                END IF;
            END $$;
        """)

        # ── Reviews tables ───────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_scores (
                hotel_id           BIGINT NOT NULL REFERENCES hotels(id),
                platform           TEXT NOT NULL,
                period             DATE NOT NULL,
                score              DOUBLE PRECISION,
                num_reviews        INTEGER,
                response_rate      DOUBLE PRECISION,
                avg_response_hours DOUBLE PRECISION,
                imported_at        TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (hotel_id, platform, period)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_complaints (
                id          BIGSERIAL PRIMARY KEY,
                hotel_id    BIGINT NOT NULL REFERENCES hotels(id),
                period      DATE NOT NULL,
                department  TEXT NOT NULL,
                complaint   TEXT NOT NULL,
                volume      INTEGER DEFAULT 1,
                sentiment   TEXT DEFAULT 'negativo',
                imported_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rev_complaints_hotel_period
                ON review_complaints(hotel_id, period)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_keywords (
                hotel_id  BIGINT NOT NULL REFERENCES hotels(id),
                period    DATE NOT NULL,
                keyword   TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                sentiment TEXT,
                PRIMARY KEY (hotel_id, period, keyword)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_compset (
                hotel_id         BIGINT NOT NULL REFERENCES hotels(id),
                period           DATE NOT NULL,
                competitor       TEXT NOT NULL,
                platform         TEXT NOT NULL,
                competitor_score DOUBLE PRECISION,
                our_rank         INTEGER,
                PRIMARY KEY (hotel_id, period, competitor, platform)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS booking_reviews (
                id                 BIGSERIAL PRIMARY KEY,
                hotel_id           BIGINT NOT NULL REFERENCES hotels(id),
                review_date        DATE NOT NULL,
                guest_name         TEXT,
                reservation_number TEXT,
                title              TEXT,
                positive_comment   TEXT,
                negative_comment   TEXT,
                overall_score      DOUBLE PRECISION,
                staff_score        DOUBLE PRECISION,
                cleanliness_score  DOUBLE PRECISION,
                location_score     DOUBLE PRECISION,
                facilities_score   DOUBLE PRECISION,
                comfort_score      DOUBLE PRECISION,
                value_score        DOUBLE PRECISION,
                property_response  TEXT,
                traveler_type      TEXT,
                imported_at        TIMESTAMP DEFAULT NOW(),
                UNIQUE (hotel_id, reservation_number)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_booking_reviews_hotel_date
                ON booking_reviews(hotel_id, review_date)
        """)
    add_traveler_type_column()


def upsert_hotel(name: str, folder_path: str) -> int:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO hotels(name, folder_path) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
            (name, folder_path),
        )
        row = conn.execute("SELECT id FROM hotels WHERE name = %s", (name,)).fetchone()
        return row["id"]


def upsert_daily_metrics(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO daily_metrics
                (hotel_id, date, occupancy_rooms, occupancy_pct,
                 room_revenue, avg_room_price, source_file)
            VALUES
                (%(hotel_id)s, %(date)s, %(occupancy_rooms)s, %(occupancy_pct)s,
                 %(room_revenue)s, %(avg_room_price)s, %(source_file)s)
            ON CONFLICT (hotel_id, date) DO UPDATE SET
                occupancy_rooms = EXCLUDED.occupancy_rooms,
                occupancy_pct   = EXCLUDED.occupancy_pct,
                room_revenue    = EXCLUDED.room_revenue,
                avg_room_price  = EXCLUDED.avg_room_price,
                source_file     = EXCLUDED.source_file,
                imported_at     = NOW()
            """,
            rows,
        )
        return len(rows)


def is_file_unchanged(file_path: str, mtime: float) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT mtime FROM file_cache WHERE file_path = %s", (file_path,)
        ).fetchone()
        return row is not None and row["mtime"] == mtime


def get_unchanged_files(file_mtimes: dict) -> set:
    """Batch check: returns set of file paths whose mtime matches the cache.
    One DB call instead of one per file — drastically faster for large imports.
    """
    if not file_mtimes:
        return set()
    paths = list(file_mtimes.keys())
    # Chunk to avoid very large arrays
    unchanged = set()
    chunk_size = 2000
    for i in range(0, len(paths), chunk_size):
        chunk = paths[i:i + chunk_size]
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT file_path, mtime FROM file_cache WHERE file_path = ANY(%s)",
                (chunk,)
            ).fetchall()
        for row in rows:
            p = row["file_path"]
            if p in file_mtimes and abs(file_mtimes[p] - row["mtime"]) < 1e-6:
                unchanged.add(p)
    return unchanged


def update_file_cache(file_path: str, mtime: float):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO file_cache(file_path, mtime) VALUES (%s, %s)
            ON CONFLICT (file_path) DO UPDATE SET mtime = EXCLUDED.mtime, cached_at = NOW()
            """,
            (file_path, mtime),
        )


def log_import(file_path: str, status: str, rows: int = 0, error: str = None):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO import_log(file_path, status, rows_upserted, error_msg) VALUES (%s, %s, %s, %s)",
            (file_path, status, rows, error),
        )


def get_all_hotels(allowed_hotels: set[str] | None = None) -> list[dict]:
    with get_conn() as conn:
        if allowed_hotels:
            rows = conn.execute(
                "SELECT * FROM hotels WHERE name = ANY(%s) ORDER BY name",
                (list(allowed_hotels),),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM hotels ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def get_daily_metrics(hotel_id: int = None, start_date: str = None, end_date: str = None) -> list[dict]:
    query = """
        SELECT h.name as hotel_name, dm.*
        FROM daily_metrics dm
        JOIN hotels h ON h.id = dm.hotel_id
        WHERE 1=1
    """
    params = []
    if hotel_id:
        query += " AND dm.hotel_id = %s"
        params.append(hotel_id)
    if start_date:
        query += " AND dm.date >= %s"
        params.append(start_date)
    if end_date:
        query += " AND dm.date <= %s"
        params.append(end_date)
    query += " ORDER BY dm.date"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def upsert_pdf_metrics(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO daily_metrics (hotel_id, date) VALUES (%(hotel_id)s, %(date)s)
                ON CONFLICT DO NOTHING
                """,
                row,
            )
            conn.execute(
                """
                UPDATE daily_metrics SET
                    rooms_out_of_service = COALESCE(%(rooms_out_of_service)s, rooms_out_of_service),
                    total_revenue        = COALESCE(%(total_revenue)s,        total_revenue),
                    fb_revenue           = COALESCE(%(fb_revenue)s,           fb_revenue),
                    pending_balance      = COALESCE(%(pending_balance)s,      pending_balance),
                    imported_at          = NOW()
                WHERE hotel_id = %(hotel_id)s AND date = %(date)s
                """,
                row,
            )
    return len(rows)


def get_summary_for_date(date: str, allowed_hotels: set[str] | None = None) -> list[dict]:
    params: list = [date]
    hotel_filter = ""
    if allowed_hotels:
        hotel_filter = "AND h.name = ANY(%s)"
        params.append(list(allowed_hotels))
    query = f"""
        SELECT h.name as hotel_name, h.id as hotel_id,
               dm.occupancy_rooms, dm.occupancy_pct,
               dm.room_revenue, dm.avg_room_price,
               dm.rooms_out_of_service, dm.total_revenue,
               dm.fb_revenue, dm.pending_balance,
               dm.date as data_date
        FROM hotels h
        LEFT JOIN daily_metrics dm ON dm.hotel_id = h.id AND dm.date = %s
        WHERE 1=1 {hotel_filter}
        ORDER BY h.name
    """
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def upsert_otb_metrics(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO otb_metrics (
                hotel_id, analysis_type, month,
                occ_pct_current, occ_pct_comparison,
                nights_current, nights_comparison,
                variance_nights, variance_pct,
                total_revenue_current, total_revenue_comparison,
                total_revenue_variance, total_revenue_var_pct,
                room_revenue_current, room_revenue_comparison,
                room_revenue_variance, room_revenue_var_pct,
                fb_revenue_current, fb_revenue_comparison,
                fb_revenue_variance, fb_revenue_var_pct,
                other_revenue_current, other_revenue_comparison,
                other_revenue_variance, other_revenue_var_pct,
                spa_revenue_current, spa_revenue_comparison,
                spa_revenue_variance, spa_revenue_var_pct,
                adr_current, adr_comparison,
                adr_variance, adr_var_pct,
                otb_date, source_file
            ) VALUES (
                %(hotel_id)s, %(analysis_type)s, %(month)s,
                %(occ_pct_current)s, %(occ_pct_comparison)s,
                %(nights_current)s, %(nights_comparison)s,
                %(variance_nights)s, %(variance_pct)s,
                %(total_revenue_current)s, %(total_revenue_comparison)s,
                %(total_revenue_variance)s, %(total_revenue_var_pct)s,
                %(room_revenue_current)s, %(room_revenue_comparison)s,
                %(room_revenue_variance)s, %(room_revenue_var_pct)s,
                %(fb_revenue_current)s, %(fb_revenue_comparison)s,
                %(fb_revenue_variance)s, %(fb_revenue_var_pct)s,
                %(other_revenue_current)s, %(other_revenue_comparison)s,
                %(other_revenue_variance)s, %(other_revenue_var_pct)s,
                %(spa_revenue_current)s, %(spa_revenue_comparison)s,
                %(spa_revenue_variance)s, %(spa_revenue_var_pct)s,
                %(adr_current)s, %(adr_comparison)s,
                %(adr_variance)s, %(adr_var_pct)s,
                %(otb_date)s, %(source_file)s
            )
            ON CONFLICT (hotel_id, analysis_type, month, otb_date) DO UPDATE SET
                occ_pct_current          = EXCLUDED.occ_pct_current,
                occ_pct_comparison       = EXCLUDED.occ_pct_comparison,
                nights_current           = EXCLUDED.nights_current,
                nights_comparison        = EXCLUDED.nights_comparison,
                variance_nights          = EXCLUDED.variance_nights,
                variance_pct             = EXCLUDED.variance_pct,
                total_revenue_current    = EXCLUDED.total_revenue_current,
                total_revenue_comparison = EXCLUDED.total_revenue_comparison,
                total_revenue_variance   = EXCLUDED.total_revenue_variance,
                total_revenue_var_pct    = EXCLUDED.total_revenue_var_pct,
                room_revenue_current     = EXCLUDED.room_revenue_current,
                room_revenue_comparison  = EXCLUDED.room_revenue_comparison,
                room_revenue_variance    = EXCLUDED.room_revenue_variance,
                room_revenue_var_pct     = EXCLUDED.room_revenue_var_pct,
                fb_revenue_current       = EXCLUDED.fb_revenue_current,
                fb_revenue_comparison    = EXCLUDED.fb_revenue_comparison,
                fb_revenue_variance      = EXCLUDED.fb_revenue_variance,
                fb_revenue_var_pct       = EXCLUDED.fb_revenue_var_pct,
                other_revenue_current    = EXCLUDED.other_revenue_current,
                other_revenue_comparison = EXCLUDED.other_revenue_comparison,
                other_revenue_variance   = EXCLUDED.other_revenue_variance,
                other_revenue_var_pct    = EXCLUDED.other_revenue_var_pct,
                spa_revenue_current      = EXCLUDED.spa_revenue_current,
                spa_revenue_comparison   = EXCLUDED.spa_revenue_comparison,
                spa_revenue_variance     = EXCLUDED.spa_revenue_variance,
                spa_revenue_var_pct      = EXCLUDED.spa_revenue_var_pct,
                adr_current              = EXCLUDED.adr_current,
                adr_comparison           = EXCLUDED.adr_comparison,
                adr_variance             = EXCLUDED.adr_variance,
                adr_var_pct              = EXCLUDED.adr_var_pct,
                otb_date                 = EXCLUDED.otb_date,
                source_file              = EXCLUDED.source_file,
                imported_at              = NOW()
            """,
            rows,
        )
    return len(rows)


def get_otb_data(hotel_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT analysis_type, month,
                   occ_pct_current, occ_pct_comparison,
                   nights_current, nights_comparison,
                   variance_nights, variance_pct,
                   total_revenue_current, total_revenue_comparison,
                   total_revenue_variance, total_revenue_var_pct,
                   room_revenue_current, room_revenue_comparison,
                   room_revenue_variance, room_revenue_var_pct,
                   fb_revenue_current, fb_revenue_comparison,
                   fb_revenue_variance, fb_revenue_var_pct,
                   other_revenue_current, other_revenue_comparison,
                   other_revenue_variance, other_revenue_var_pct,
                   spa_revenue_current, spa_revenue_comparison,
                   spa_revenue_variance, spa_revenue_var_pct,
                   adr_current, adr_comparison,
                   adr_variance, adr_var_pct,
                   otb_date
            FROM otb_metrics
            WHERE hotel_id = %s
              AND otb_date = (SELECT MAX(otb_date) FROM otb_metrics WHERE hotel_id = %s)
            ORDER BY analysis_type, month
            """,
            (hotel_id, hotel_id),
        ).fetchall()
        return [dict(r) for r in rows]


def get_otb_insights_db(hotel_id: int) -> dict:
    """Compare the two most recent OTB snapshots from the DB (works on cloud)."""
    with get_conn() as conn:
        # Get the two most recent distinct otb_dates for this hotel
        dates = conn.execute(
            """SELECT DISTINCT otb_date FROM otb_metrics
               WHERE hotel_id = %s AND otb_date IS NOT NULL
               ORDER BY otb_date DESC LIMIT 2""",
            (hotel_id,),
        ).fetchall()

    if not dates:
        return {}

    current_date  = dates[0]["otb_date"]
    previous_date = dates[1]["otb_date"] if len(dates) >= 2 else None

    MONTH_LABELS = ["JAN","FEV","MAR","ABR","MAI","JUN",
                    "JUL","AGO","SET","OUT","NOV","DEZ"]

    def _rows(otb_date):
        with get_conn() as conn:
            return {
                r["month"]: r for r in conn.execute(
                    """SELECT month, nights_current, occ_pct_current,
                              total_revenue_current, adr_current
                       FROM otb_metrics
                       WHERE hotel_id=%s AND analysis_type='sdly' AND otb_date=%s
                         AND month BETWEEN 1 AND 12""",
                    (hotel_id, otb_date),
                ).fetchall()
            }

    curr_by_month = _rows(current_date)
    prev_by_month = _rows(previous_date) if previous_date else {}

    changes = []
    for month in range(1, 13):
        c = curr_by_month.get(month)
        p = prev_by_month.get(month)
        if not c or not p:
            continue
        nights_delta = (c["nights_current"] or 0) - (p["nights_current"] or 0)
        occ_delta_pp = ((c["occ_pct_current"] or 0) - (p["occ_pct_current"] or 0)) * 100
        rev_delta    = (c["total_revenue_current"] or 0) - (p["total_revenue_current"] or 0)
        adr_delta    = (c["adr_current"] or 0) - (p["adr_current"] or 0)
        if abs(nights_delta) < 3 and abs(occ_delta_pp) < 0.5:
            continue
        changes.append({
            "month":        month,
            "label":        MONTH_LABELS[month - 1],
            "nights_delta": int(nights_delta),
            "occ_delta_pp": round(occ_delta_pp, 1),
            "rev_delta":    round(rev_delta, 2),
            "adr_delta":    round(adr_delta, 2),
        })

    changes.sort(key=lambda x: abs(x["occ_delta_pp"]), reverse=True)
    return {
        "current_date":  str(current_date),
        "previous_date": str(previous_date) if previous_date else None,
        "changes":       changes[:6],
        "suggestions":   [],
    }


def get_otb_summary(allowed_hotels: set[str] | None = None) -> list[dict]:
    hotel_filter = ""
    params: list = []
    if allowed_hotels:
        hotel_filter = "AND h.name = ANY(%s)"
        params.append(list(allowed_hotels))
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT h.id as hotel_id, h.name as hotel_name,
                   MAX(o.otb_date) as otb_date
            FROM hotels h
            JOIN otb_metrics o ON o.hotel_id = h.id
            WHERE 1=1 {hotel_filter}
            GROUP BY h.id, h.name
            ORDER BY h.name
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]


def get_latest_import_time() -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(imported_at) as last FROM daily_metrics"
        ).fetchone()
        return str(row["last"]) if row and row["last"] else None


def get_latest_otb_import_time() -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(imported_at) as last FROM otb_metrics"
        ).fetchone()
        return str(row["last"]) if row and row["last"] else None


# ── Google Place ID helpers ──────────────────────────────────────────────────

def get_hotel_place_id(hotel_id: int) -> str | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT google_place_id FROM hotels WHERE id = %s", (hotel_id,)
        ).fetchone()
        return row["google_place_id"] if row else None


def set_hotel_place_id(hotel_id: int, place_id: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE hotels SET google_place_id = %s WHERE id = %s",
            (place_id, hotel_id),
        )




# ── Reviews ──────────────────────────────────────────────────────────────────

def upsert_review_scores(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO review_scores
                (hotel_id, platform, period, score, num_reviews, response_rate, avg_response_hours)
            VALUES
                (%(hotel_id)s, %(platform)s, %(period)s, %(score)s,
                 %(num_reviews)s, %(response_rate)s, %(avg_response_hours)s)
            ON CONFLICT (hotel_id, platform, period) DO UPDATE SET
                score              = EXCLUDED.score,
                num_reviews        = EXCLUDED.num_reviews,
                response_rate      = EXCLUDED.response_rate,
                avg_response_hours = EXCLUDED.avg_response_hours,
                imported_at        = NOW()
            """,
            rows,
        )
    return len(rows)


def upsert_review_complaints(rows: list[dict]) -> int:
    if not rows:
        return 0
    periods = list({r["period"] for r in rows})
    hotel_id = rows[0]["hotel_id"]
    with get_conn() as conn:
        for period in periods:
            conn.execute(
                "DELETE FROM review_complaints WHERE hotel_id = %s AND period = %s",
                (hotel_id, period),
            )
        conn.executemany(
            """
            INSERT INTO review_complaints
                (hotel_id, period, department, complaint, volume, sentiment)
            VALUES
                (%(hotel_id)s, %(period)s, %(department)s,
                 %(complaint)s, %(volume)s, %(sentiment)s)
            """,
            rows,
        )
    return len(rows)


def upsert_review_keywords(rows: list[dict]) -> int:
    if not rows:
        return 0
    periods = list({r["period"] for r in rows})
    hotel_id = rows[0]["hotel_id"]
    with get_conn() as conn:
        for period in periods:
            conn.execute(
                "DELETE FROM review_keywords WHERE hotel_id = %s AND period = %s",
                (hotel_id, period),
            )
        conn.executemany(
            """
            INSERT INTO review_keywords (hotel_id, period, keyword, frequency, sentiment)
            VALUES (%(hotel_id)s, %(period)s, %(keyword)s, %(frequency)s, %(sentiment)s)
            """,
            rows,
        )
    return len(rows)


def upsert_review_compset(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO review_compset
                (hotel_id, period, competitor, platform, competitor_score, our_rank)
            VALUES
                (%(hotel_id)s, %(period)s, %(competitor)s, %(platform)s,
                 %(competitor_score)s, %(our_rank)s)
            ON CONFLICT (hotel_id, period, competitor, platform) DO UPDATE SET
                competitor_score = EXCLUDED.competitor_score,
                our_rank         = EXCLUDED.our_rank
            """,
            rows,
        )
    return len(rows)


def get_reviews_summary(allowed_hotels: set[str] | None = None) -> list[dict]:
    hotel_filter = ""
    params: list = []
    if allowed_hotels:
        hotel_filter = "AND h.name = ANY(%s)"
        params.append(list(allowed_hotels))
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            WITH latest AS (
                SELECT hotel_id, MAX(period) AS max_period
                FROM review_scores
                GROUP BY hotel_id
            )
            SELECT h.id AS hotel_id, h.name AS hotel_name,
                   rs.platform, rs.period::text, rs.score, rs.num_reviews,
                   rs.response_rate, rs.avg_response_hours
            FROM hotels h
            JOIN latest l ON l.hotel_id = h.id
            JOIN review_scores rs ON rs.hotel_id = h.id AND rs.period = l.max_period
            WHERE 1=1 {hotel_filter}
            ORDER BY h.name, rs.platform
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]


def get_review_scores(hotel_id: int, months: int = 14) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT platform, period::text, score, num_reviews,
                   response_rate, avg_response_hours
            FROM review_scores
            WHERE hotel_id = %s
              AND period >= (CURRENT_DATE - (%s || ' months')::interval)::date
            ORDER BY platform, period
            """,
            (hotel_id, str(months)),
        ).fetchall()
        return [dict(r) for r in rows]


def get_review_complaints(hotel_id: int, period: str = None) -> list[dict]:
    query = """
        SELECT period::text, department, complaint, volume, sentiment
        FROM review_complaints WHERE hotel_id = %s
    """
    params: list = [hotel_id]
    if period:
        query += " AND period = %s"
        params.append(period)
    query += " ORDER BY period DESC, volume DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_review_keywords(hotel_id: int, period: str = None) -> list[dict]:
    query = """
        SELECT period::text, keyword, frequency, sentiment
        FROM review_keywords WHERE hotel_id = %s
    """
    params: list = [hotel_id]
    if period:
        query += " AND period = %s"
        params.append(period)
    query += " ORDER BY frequency DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_review_compset(hotel_id: int, period: str = None) -> list[dict]:
    query = """
        SELECT period::text, competitor, platform, competitor_score, our_rank
        FROM review_compset WHERE hotel_id = %s
    """
    params: list = [hotel_id]
    if period:
        query += " AND period = %s"
        params.append(period)
    query += " ORDER BY period DESC, our_rank"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def upsert_booking_reviews(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO booking_reviews
                (hotel_id, review_date, guest_name, reservation_number, title,
                 positive_comment, negative_comment, overall_score, staff_score,
                 cleanliness_score, location_score, facilities_score,
                 comfort_score, value_score, property_response, traveler_type)
            VALUES
                (%(hotel_id)s, %(review_date)s, %(guest_name)s, %(reservation_number)s,
                 %(title)s, %(positive_comment)s, %(negative_comment)s,
                 %(overall_score)s, %(staff_score)s, %(cleanliness_score)s,
                 %(location_score)s, %(facilities_score)s, %(comfort_score)s,
                 %(value_score)s, %(property_response)s, %(traveler_type)s)
            ON CONFLICT (hotel_id, reservation_number) DO UPDATE SET
                review_date        = EXCLUDED.review_date,
                guest_name         = EXCLUDED.guest_name,
                title              = EXCLUDED.title,
                positive_comment   = EXCLUDED.positive_comment,
                negative_comment   = EXCLUDED.negative_comment,
                overall_score      = EXCLUDED.overall_score,
                staff_score        = EXCLUDED.staff_score,
                cleanliness_score  = EXCLUDED.cleanliness_score,
                location_score     = EXCLUDED.location_score,
                facilities_score   = EXCLUDED.facilities_score,
                comfort_score      = EXCLUDED.comfort_score,
                value_score        = EXCLUDED.value_score,
                property_response  = EXCLUDED.property_response,
                traveler_type      = EXCLUDED.traveler_type
            """,
            rows,
        )
    return len(rows)


def get_booking_reviews(hotel_id: int, start_date: str = None, end_date: str = None) -> list[dict]:
    query = """
        SELECT review_date::text, guest_name, reservation_number, title,
               positive_comment, negative_comment, overall_score, staff_score,
               cleanliness_score, location_score, facilities_score,
               comfort_score, value_score, property_response, traveler_type
        FROM booking_reviews WHERE hotel_id = %s
    """
    params: list = [hotel_id]
    if start_date:
        query += " AND review_date >= %s"
        params.append(start_date)
    if end_date:
        query += " AND review_date <= %s"
        params.append(end_date)
    query += " ORDER BY review_date DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def add_traveler_type_column():
    """Adiciona coluna traveler_type se não existir (migração segura)."""
    with get_conn() as conn:
        conn.execute("""
            ALTER TABLE booking_reviews
            ADD COLUMN IF NOT EXISTS traveler_type TEXT
        """)
