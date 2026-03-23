"""Database layer for CapTap — users, scores, leaderboards."""

from __future__ import annotations

import json
import os
import secrets
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL")


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id         SERIAL PRIMARY KEY,
                    email      VARCHAR(255) UNIQUE NOT NULL,
                    username   VARCHAR(50)  NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scores (
                    id            SERIAL PRIMARY KEY,
                    user_id       INTEGER REFERENCES users(id),
                    score         INTEGER NOT NULL,
                    max_score     INTEGER NOT NULL,
                    played_at     DATE NOT NULL DEFAULT CURRENT_DATE,
                    round_results JSONB,
                    created_at    TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, played_at)
                )
            """)
            cur.execute("""
                ALTER TABLE scores
                ADD COLUMN IF NOT EXISTS total_time_seconds FLOAT
            """)
            cur.execute("""
                ALTER TABLE scores
                ADD COLUMN IF NOT EXISTS game_mode VARCHAR(20) DEFAULT 'daily'
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_puzzle (
                    played_at      DATE PRIMARY KEY,
                    tickers        TEXT[] NOT NULL,
                    companies_data JSONB,
                    created_at     TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE daily_puzzle
                ADD COLUMN IF NOT EXISTS companies_data JSONB
            """)
            cur.execute("""
                ALTER TABLE daily_puzzle
                ADD COLUMN IF NOT EXISTS hl_data JSONB
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battles (
                    id               SERIAL PRIMARY KEY,
                    challenger_id    INTEGER REFERENCES users(id),
                    opponent_id      INTEGER REFERENCES users(id),
                    played_at        DATE NOT NULL,
                    challenger_score INTEGER,
                    opponent_score   INTEGER,
                    winner_id        INTEGER REFERENCES users(id),
                    created_at       TIMESTAMP DEFAULT NOW(),
                    UNIQUE(challenger_id, opponent_id, played_at)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battle_puzzles (
                    id             SERIAL PRIMARY KEY,
                    battle_id      VARCHAR(20) UNIQUE NOT NULL,
                    challenger_id  INTEGER REFERENCES users(id),
                    opponent_id    INTEGER REFERENCES users(id),
                    companies_data JSONB NOT NULL,
                    created_at     TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battle_scores (
                    id                 SERIAL PRIMARY KEY,
                    battle_id          VARCHAR(20) NOT NULL,
                    user_id            INTEGER REFERENCES users(id),
                    score              INTEGER NOT NULL,
                    max_score          INTEGER NOT NULL,
                    total_time_seconds FLOAT,
                    round_results      JSONB,
                    created_at         TIMESTAMP DEFAULT NOW(),
                    UNIQUE(battle_id, user_id)
                )
            """)


def get_or_create_user(email: str, username: str) -> dict:
    """Find user by email. Update username if changed, or create new user."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, username FROM users WHERE email = %s",
                (email.lower().strip(),),
            )
            user = cur.fetchone()
            if user:
                if user["username"] != username.strip():
                    cur.execute(
                        "UPDATE users SET username = %s WHERE email = %s RETURNING id, email, username",
                        (username.strip(), email.lower().strip()),
                    )
                    return dict(cur.fetchone())
                return dict(user)
            cur.execute(
                "INSERT INTO users (email, username) VALUES (%s, %s) RETURNING id, email, username",
                (email.lower().strip(), username.strip()),
            )
            return dict(cur.fetchone())


def save_score(
    user_id: int,
    score: int,
    max_score: int,
    round_results: list,
    played_at: str | None = None,
    total_time_seconds: float | None = None,
    game_mode: str = "daily",
) -> bool:
    """Save today's score. Returns False if user already submitted today."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if played_at:
                cur.execute(
                    """
                    INSERT INTO scores (user_id, score, max_score, round_results, played_at, total_time_seconds, game_mode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, played_at) DO NOTHING
                    """,
                    (user_id, score, max_score, json.dumps(round_results), played_at, total_time_seconds, game_mode),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO scores (user_id, score, max_score, round_results, total_time_seconds, game_mode)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, played_at) DO NOTHING
                    """,
                    (user_id, score, max_score, json.dumps(round_results), total_time_seconds, game_mode),
                )
            return cur.rowcount > 0


def _est_today() -> str:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def get_daily_leaderboard() -> list[dict]:
    """Today's scores (EST date) ranked highest to lowest."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    u.username,
                    s.score,
                    s.max_score,
                    ROUND(s.score::numeric / NULLIF(s.max_score, 0) * 100) AS pct,
                    s.total_time_seconds
                FROM scores s
                JOIN users u ON s.user_id = u.id
                WHERE s.played_at = %s
                  AND (s.game_mode = 'daily' OR s.game_mode IS NULL)
                ORDER BY s.score DESC, s.total_time_seconds ASC NULLS LAST
            """, (_est_today(),))
            return [dict(r) for r in cur.fetchall()]


def _est_week_start() -> str:
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("America/New_York")).date()
    monday = today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


def get_weekly_leaderboard() -> list[dict]:
    """All-time stats: total score, games played, avg score, ranked by total score."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    u.username,
                    ROUND(AVG(s.score))  AS avg_score,
                    SUM(s.max_score) / COUNT(s.id) AS avg_max,
                    COUNT(s.id)          AS days_played,
                    SUM(s.score)         AS total_score
                FROM scores s
                JOIN users u ON s.user_id = u.id
                GROUP BY u.id, u.username
                ORDER BY total_score DESC
            """)
            return [dict(r) for r in cur.fetchall()]


def get_locked_puzzle(est_date: str) -> dict | None:
    """Return today's locked puzzle dict with 'tickers' and 'companies_data', or None."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tickers, companies_data FROM daily_puzzle WHERE played_at = %s",
                (est_date,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"tickers": list(row[0]), "companies_data": row[1]}


def get_user_stats(user_id: int) -> dict:
    """Return streak, games_played, best_score, avg_score for a user."""
    from datetime import date, timedelta
    from zoneinfo import ZoneInfo
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT played_at, score FROM scores WHERE user_id = %s ORDER BY played_at DESC",
                (user_id,),
            )
            rows = cur.fetchall()

    if not rows:
        return {"streak": 0, "games_played": 0, "best_score": 0, "avg_score": 0}

    games_played = len(rows)
    best_score   = max(r[1] for r in rows)
    avg_score    = round(sum(r[1] for r in rows) / games_played)
    played_dates = {r[0] for r in rows}          # set of datetime.date objects

    today = datetime.now(ZoneInfo("America/New_York")).date()
    streak = 0
    check  = today if today in played_dates else today - timedelta(days=1)
    while check in played_dates:
        streak += 1
        check  -= timedelta(days=1)

    return {
        "streak":       streak,
        "games_played": games_played,
        "best_score":   best_score,
        "avg_score":    avg_score,
    }


def lock_puzzle(est_date: str, tickers: list[str], companies_data: list[dict]) -> None:
    """Persist today's puzzle tickers + full company data so cold starts need no yfinance."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO daily_puzzle (played_at, tickers, companies_data)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (played_at) DO UPDATE
                     SET companies_data = EXCLUDED.companies_data
                   WHERE daily_puzzle.companies_data IS NULL""",
                (est_date, tickers, json.dumps(companies_data)),
            )


def get_locked_hl_puzzle(est_date: str) -> list | None:
    """Return today's locked H-or-L puzzle data, or None."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hl_data FROM daily_puzzle WHERE played_at = %s",
                (est_date,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            return row[0]


def lock_hl_puzzle(est_date: str, hl_data: list) -> None:
    """Persist today's H-or-L pairs."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO daily_puzzle (played_at, tickers, hl_data)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (played_at) DO UPDATE
                     SET hl_data = EXCLUDED.hl_data
                   WHERE daily_puzzle.hl_data IS NULL""",
                (est_date, [], json.dumps(hl_data)),
            )


def get_user_today_score(user_id: int) -> dict:
    """Return the user's daily score for today (EST), or {played: False}."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT score, max_score, total_time_seconds
                   FROM scores
                   WHERE user_id = %s AND played_at = %s
                     AND (game_mode = 'daily' OR game_mode IS NULL)""",
                (user_id, _est_today()),
            )
            row = cur.fetchone()
            if row:
                return {"played": True, "score": row[0], "max_score": row[1], "time": row[2]}
            return {"played": False}


def get_all_users() -> list[dict]:
    """Return all users sorted by username."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, username FROM users ORDER BY username ASC")
            return [dict(r) for r in cur.fetchall()]


def save_battle(
    challenger_id: int,
    opponent_id: int,
    challenger_score: int,
    opponent_score: int,
    played_at: str,
) -> bool:
    """Save a battle result. Returns True if saved, False if already exists."""
    winner_id = challenger_id if challenger_score >= opponent_score else opponent_id
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO battles (challenger_id, opponent_id, challenger_score, opponent_score, winner_id, played_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (challenger_id, opponent_id, played_at) DO NOTHING""",
                (challenger_id, opponent_id, challenger_score, opponent_score, winner_id, played_at),
            )
            return cur.rowcount > 0


def create_battle_puzzle(challenger_id: int, opponent_id: int, companies_data: list[dict]) -> str:
    """Generate a unique battle_id, store puzzle, return battle_id."""
    battle_id = secrets.token_urlsafe(10)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO battle_puzzles (battle_id, challenger_id, opponent_id, companies_data)
                   VALUES (%s, %s, %s, %s)""",
                (battle_id, challenger_id, opponent_id, json.dumps(companies_data)),
            )
    return battle_id


def get_battle_puzzle(battle_id: str) -> dict | None:
    """Return puzzle dict with company data and player names, or None if not found."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT bp.battle_id, bp.challenger_id, bp.opponent_id,
                          bp.companies_data,
                          uc.username AS challenger_name,
                          uo.username AS opponent_name
                   FROM battle_puzzles bp
                   JOIN users uc ON uc.id = bp.challenger_id
                   LEFT JOIN users uo ON uo.id = bp.opponent_id
                   WHERE bp.battle_id = %s""",
                (battle_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return dict(row)


def save_battle_score(
    battle_id: str,
    user_id: int,
    score: int,
    max_score: int,
    total_time_seconds: float | None,
    round_results: list,
) -> bool:
    """Save a player's score for a battle. Returns True if saved, False if already exists."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO battle_scores
                       (battle_id, user_id, score, max_score, total_time_seconds, round_results)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (battle_id, user_id) DO NOTHING""",
                (battle_id, user_id, score, max_score, total_time_seconds, json.dumps(round_results)),
            )
            return cur.rowcount > 0


def get_battle_scores(battle_id: str) -> list[dict]:
    """Return list of {user_id, username, score, max_score, total_time_seconds} for a battle."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT bs.user_id, u.username, bs.score, bs.max_score, bs.total_time_seconds
                   FROM battle_scores bs
                   JOIN users u ON u.id = bs.user_id
                   WHERE bs.battle_id = %s
                   ORDER BY bs.created_at ASC""",
                (battle_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def record_battle_result(battle_id: str) -> None:
    """If both players have submitted scores, upsert into the battles W/L table."""
    puzzle = get_battle_puzzle(battle_id)
    if not puzzle:
        return
    scores = get_battle_scores(battle_id)
    if len(scores) < 2:
        return  # Both haven't played yet

    challenger_id = puzzle["challenger_id"]
    opponent_id   = puzzle["opponent_id"]

    challenger_entry = next((s for s in scores if s["user_id"] == challenger_id), None)
    opponent_entry   = next((s for s in scores if s["user_id"] == opponent_id), None)

    if not challenger_entry or not opponent_entry:
        return

    challenger_score = challenger_entry["score"]
    opponent_score   = opponent_entry["score"]
    winner_id = challenger_id if challenger_score >= opponent_score else opponent_id

    from datetime import datetime
    from zoneinfo import ZoneInfo
    played_at = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO battles
                       (challenger_id, opponent_id, challenger_score, opponent_score, winner_id, played_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (challenger_id, opponent_id, played_at) DO UPDATE
                     SET challenger_score = EXCLUDED.challenger_score,
                         opponent_score   = EXCLUDED.opponent_score,
                         winner_id        = EXCLUDED.winner_id""",
                (challenger_id, opponent_id, challenger_score, opponent_score, winner_id, played_at),
            )


def get_battle_leaderboard() -> list[dict]:
    """Return [{username, wins, losses, win_pct}] sorted by wins."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    u.username,
                    COUNT(CASE WHEN b.winner_id = u.id THEN 1 END) AS wins,
                    COUNT(CASE WHEN b.winner_id != u.id THEN 1 END) AS losses,
                    CASE WHEN COUNT(*) = 0 THEN 0
                         ELSE ROUND(COUNT(CASE WHEN b.winner_id = u.id THEN 1 END)::numeric / COUNT(*) * 100)
                    END AS win_pct
                FROM users u
                JOIN battles b ON b.challenger_id = u.id OR b.opponent_id = u.id
                GROUP BY u.id, u.username
                HAVING COUNT(*) > 0
                ORDER BY wins DESC, win_pct DESC
            """)
            return [dict(r) for r in cur.fetchall()]
