"""Flask web server for CapTap."""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf
from flask import Flask, jsonify, render_template, request

EST = ZoneInfo("America/New_York")

sys.path.insert(0, str(Path(__file__).parent))

from market_cap_quiz import (
    Company,
    fetch_sp500_universe,
    format_cap,
    generate_choices,
    get_companies_by_tickers,
    get_daily_companies,
    load_snapshot_for_today,
    save_snapshot,
)
import db

app = Flask(__name__)

APP_URL = os.environ.get("APP_URL", "https://captap.app")

_sp500_universe: list | None = None


def get_universe() -> list:
    global _sp500_universe
    if _sp500_universe is None:
        _sp500_universe = fetch_sp500_universe()
    return _sp500_universe


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Puzzle — Daily Quiz
# ---------------------------------------------------------------------------

@app.route("/api/puzzle")
def puzzle():
    try:
        universe = get_universe()
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        cache = load_snapshot_for_today()

        locked = db.get_locked_puzzle(est_date) if db.DATABASE_URL else None
        if locked and locked.get("companies_data"):
            companies = [
                Company(
                    name=c["name"],
                    ticker=c["ticker"],
                    market_cap_billion_usd=float(c["market_cap_billion_usd"]),
                    ceo=c.get("ceo", "N/A"),
                    headquarters=c.get("headquarters", "N/A"),
                    logo_url=c.get("logo_url", ""),
                    description=c.get("description", ""),
                    fun_fact=c.get("fun_fact", ""),
                    revenue_billion_usd=float(c.get("revenue_billion_usd", 0.0)),
                    full_time_employees=int(c.get("full_time_employees", 0)),
                )
                for c in locked["companies_data"]
            ]
        elif locked:
            companies = get_companies_by_tickers(universe, cache, locked["tickers"])
            if db.DATABASE_URL:
                db.lock_puzzle(est_date, [c.ticker for c in companies],
                               [{"name": c.name, "ticker": c.ticker,
                                 "market_cap_billion_usd": c.market_cap_billion_usd,
                                 "ceo": c.ceo, "headquarters": c.headquarters,
                                 "logo_url": c.logo_url, "description": c.description,
                                 "fun_fact": c.fun_fact,
                                 "revenue_billion_usd": c.revenue_billion_usd,
                                 "full_time_employees": c.full_time_employees} for c in companies])
        else:
            companies = get_daily_companies(universe, cache)
            if db.DATABASE_URL:
                db.lock_puzzle(est_date, [c.ticker for c in companies],
                               [{"name": c.name, "ticker": c.ticker,
                                 "market_cap_billion_usd": c.market_cap_billion_usd,
                                 "ceo": c.ceo, "headquarters": c.headquarters,
                                 "logo_url": c.logo_url, "description": c.description,
                                 "fun_fact": c.fun_fact,
                                 "revenue_billion_usd": c.revenue_billion_usd,
                                 "full_time_employees": c.full_time_employees} for c in companies])
        save_snapshot(cache)

        round_points = [100, 100, 200, 300, 300]
        rounds = []
        for i, company in enumerate(companies):
            choices, correct_idx = generate_choices(company.market_cap_billion_usd)
            rounds.append({
                "name": company.name,
                "ticker": company.ticker,
                "ceo": company.ceo,
                "headquarters": company.headquarters,
                "logo_url": company.logo_url,
                "description": company.description,
                "fun_fact": company.fun_fact,
                "points_available": round_points[i],
                "choices": [format_cap(c) for c in choices],
                "correct_index": correct_idx,
                "actual_cap": format_cap(company.market_cap_billion_usd),
            })

        puzzle_version = os.environ.get("PUZZLE_VERSION", "1")
        return jsonify({"rounds": rounds, "puzzle_version": puzzle_version})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Puzzle — Higher or Lower
# ---------------------------------------------------------------------------

@app.route("/api/puzzle/hl")
def puzzle_hl():
    try:
        universe = get_universe()
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        cache = load_snapshot_for_today()

        # Check DB lock first
        locked_hl = db.get_locked_hl_puzzle(est_date) if db.DATABASE_URL else None
        if locked_hl:
            return jsonify({"pairs": locked_hl})

        # Generate: seed by date + "hl", pick 20 companies, pair them
        rng = random.Random(est_date + "hl")
        candidates = rng.sample(universe, min(60, len(universe)))

        companies: list[Company] = []
        for display_ticker, yf_ticker, name in candidates:
            if len(companies) >= 20:
                break
            cached = cache.get(display_ticker)
            if cached:
                company = Company(
                    name=str(cached["name"]),
                    ticker=display_ticker,
                    market_cap_billion_usd=float(cached["market_cap_billion_usd"]),
                )
                companies.append(company)
            else:
                from market_cap_quiz import fetch_company
                company = fetch_company(display_ticker, yf_ticker, name)
                if company:
                    cache[display_ticker] = {
                        "name": company.name,
                        "market_cap_billion_usd": company.market_cap_billion_usd,
                        "ceo": company.ceo,
                        "headquarters": company.headquarters,
                        "logo_url": company.logo_url,
                        "description": company.description,
                        "fun_fact": company.fun_fact,
                        "revenue_billion_usd": company.revenue_billion_usd,
                        "full_time_employees": company.full_time_employees,
                    }
                    companies.append(company)

        save_snapshot(cache)

        pairs = []
        for i in range(0, min(20, len(companies)), 2):
            if i + 1 >= len(companies):
                break
            a = companies[i]
            b = companies[i + 1]
            correct = "a" if a.market_cap_billion_usd >= b.market_cap_billion_usd else "b"
            pairs.append({
                "a": {"name": a.name, "ticker": a.ticker},
                "b": {"name": b.name, "ticker": b.ticker},
                "correct": correct,
            })

        if db.DATABASE_URL and pairs:
            db.lock_hl_puzzle(est_date, pairs)

        return jsonify({"pairs": pairs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Puzzle — Expert Mode
# ---------------------------------------------------------------------------

@app.route("/api/puzzle/expert")
def puzzle_expert():
    try:
        universe = get_universe()
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        cache = load_snapshot_for_today()

        # Reuse daily puzzle companies if available
        locked = db.get_locked_puzzle(est_date) if db.DATABASE_URL else None
        if locked and locked.get("companies_data"):
            companies = [
                Company(
                    name=c["name"],
                    ticker=c["ticker"],
                    market_cap_billion_usd=float(c["market_cap_billion_usd"]),
                    ceo=c.get("ceo", "N/A"),
                    headquarters=c.get("headquarters", "N/A"),
                    logo_url=c.get("logo_url", ""),
                    description=c.get("description", ""),
                    fun_fact=c.get("fun_fact", ""),
                    revenue_billion_usd=float(c.get("revenue_billion_usd", 0.0)),
                    full_time_employees=int(c.get("full_time_employees", 0)),
                )
                for c in locked["companies_data"]
            ]
        else:
            companies = get_daily_companies(universe, cache)
            save_snapshot(cache)

        rounds = []
        for company in companies[:5]:
            rounds.append({
                "name": company.name,
                "ticker": company.ticker,
                "ceo": company.ceo,
                "headquarters": company.headquarters,
                "description": company.description,
                "fun_fact": company.fun_fact,
                "revenue_billion_usd": round(company.revenue_billion_usd, 2),
                "full_time_employees": company.full_time_employees,
                "actual_cap_billion": round(company.market_cap_billion_usd, 2),
                "actual_cap": format_cap(company.market_cap_billion_usd),
            })

        return jsonify({"rounds": rounds})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@app.route("/api/user", methods=["POST"])
def upsert_user():
    data = request.get_json()
    email    = (data.get("email") or "").strip()
    username = (data.get("username") or "").strip()

    if not email or not username:
        return jsonify({"error": "Email and username are required"}), 400
    if len(username) > 30:
        return jsonify({"error": "Username must be 30 characters or fewer"}), 400

    try:
        user = db.get_or_create_user(email, username)
        return jsonify(user)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users", methods=["GET"])
def list_users():
    try:
        return jsonify(db.get_all_users())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Scores
# ---------------------------------------------------------------------------

@app.route("/api/scores", methods=["POST"])
def submit_score():
    data = request.get_json()
    user_id            = data.get("user_id")
    score              = data.get("score")
    max_score          = data.get("max_score")
    round_results      = data.get("round_results", [])
    total_time_seconds = data.get("total_time_seconds")
    game_mode          = data.get("game_mode", "daily")

    if not all([user_id, score is not None, max_score]):
        return jsonify({"error": "Missing fields"}), 400

    try:
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        saved = db.save_score(
            user_id, score, max_score, round_results, est_date,
            total_time_seconds=total_time_seconds,
            game_mode=game_mode,
        )
        return jsonify({"saved": saved, "already_played": not saved})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Battle
# ---------------------------------------------------------------------------

@app.route("/api/battle", methods=["POST"])
def submit_battle():
    data = request.get_json()
    challenger_id    = data.get("challenger_id")
    opponent_id      = data.get("opponent_id")
    challenger_score = data.get("challenger_score")
    opponent_score   = data.get("opponent_score")
    played_at        = data.get("played_at")

    if not all([challenger_id, opponent_id, challenger_score is not None, opponent_score is not None, played_at]):
        return jsonify({"error": "Missing fields"}), 400

    try:
        saved = db.save_battle(challenger_id, opponent_id, challenger_score, opponent_score, played_at)
        return jsonify({"saved": saved})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/battle/leaderboard", methods=["GET"])
def battle_leaderboard():
    try:
        return jsonify(db.get_battle_leaderboard())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Leaderboards
# ---------------------------------------------------------------------------

@app.route("/api/stats/<int:user_id>")
def user_stats(user_id):
    try:
        return jsonify(db.get_user_stats(user_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leaderboard/daily")
def daily_leaderboard():
    try:
        return jsonify(db.get_daily_leaderboard())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leaderboard/weekly")
def weekly_leaderboard():
    try:
        return jsonify(db.get_weekly_leaderboard())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Market indices (30-min server-side cache)
# ---------------------------------------------------------------------------

_INDICES = [
    ("S&P 500",       "^GSPC"),
    ("NASDAQ",        "^IXIC"),
    ("DOW",           "^DJI"),
    ("FTSE 100",      "^FTSE"),
    ("NIKKEI 225",    "^N225"),
    ("DAX",           "^GDAXI"),
    ("CAC 40",        "^FCHI"),
    ("HANG SENG",     "^HSI"),
    ("RUSSELL 2000",  "^RUT"),
    ("VIX",           "^VIX"),
]
_indices_cache: dict = {"data": [], "ts": 0.0}


@app.route("/api/market-indices")
def market_indices():
    global _indices_cache
    if time.time() - _indices_cache["ts"] < 1800:
        return jsonify(_indices_cache["data"])
    result = []
    for name, symbol in _INDICES:
        try:
            fi = yf.Ticker(symbol).fast_info
            price = fi.last_price
            prev  = fi.previous_close
            if price and prev:
                pct = (price - prev) / prev * 100
                sign = "+" if pct >= 0 else ""
                result.append({
                    "name":   name,
                    "price":  f"{price:,.2f}",
                    "change": f"{sign}{pct:.1f}%",
                })
        except Exception:
            pass
    if result:
        _indices_cache = {"data": result, "ts": time.time()}
    return jsonify(_indices_cache["data"])


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if db.DATABASE_URL:
        db.init_db()
        print("Database initialised.")
    else:
        print("Warning: DATABASE_URL not set — scores will not be saved.")

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
