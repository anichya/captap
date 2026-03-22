"""Database layer for CapTap — users, scores, leaderboards."""

from __future__ import annotations

import json
import os
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
) -> bool:
    """Save today's score. Returns False if user already submitted today."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if played_at:
                cur.execute(
                    """
                    INSERT INTO scores (user_id, score, max_score, round_results, played_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, played_at) DO NOTHING
                    """,
                    (user_id, score, max_score, json.dumps(round_results), played_at),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO scores (user_id, score, max_score, round_results)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, played_at) DO NOTHING
                    """,
                    (user_id, score, max_score, json.dumps(round_results)),
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
                    ROUND(s.score::numeric / NULLIF(s.max_score, 0) * 100) AS pct
                FROM scores s
                JOIN users u ON s.user_id = u.id
                WHERE s.played_at = %s
                ORDER BY s.score DESC
            """, (_est_today(),))
            return [dict(r) for r in cur.fetchall()]


def _est_week_start() -> str:
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("America/New_York")).date()
    monday = today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


def get_weekly_leaderboard() -> list[dict]:
    """Avg score and games played for the current Mon–Sun week (EST), ranked by avg."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    u.username,
                    ROUND(AVG(s.score))  AS avg_score,
                    SUM(s.max_score) / COUNT(s.id) AS avg_max,
                    COUNT(s.id)          AS days_played
                FROM scores s
                JOIN users u ON s.user_id = u.id
                WHERE s.played_at >= %s
                GROUP BY u.id, u.username
                ORDER BY avg_score DESC
            """, (_est_week_start(),))
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
