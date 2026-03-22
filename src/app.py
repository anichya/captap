"""Flask web server for CapTap."""

from __future__ import annotations

import os
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
# Puzzle
# ---------------------------------------------------------------------------

@app.route("/api/puzzle")
def puzzle():
    try:
        universe = get_universe()
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        cache = load_snapshot_for_today()

        locked = db.get_locked_puzzle(est_date) if db.DATABASE_URL else None
        if locked and locked.get("companies_data"):
            # Serve entirely from DB — no yfinance needed
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
                )
                for c in locked["companies_data"]
            ]
        elif locked:
            # Old lock row — has tickers but no company data, fetch from yfinance
            companies = get_companies_by_tickers(universe, cache, locked["tickers"])
            if db.DATABASE_URL:
                db.lock_puzzle(est_date, [c.ticker for c in companies],
                               [{"name": c.name, "ticker": c.ticker,
                                 "market_cap_billion_usd": c.market_cap_billion_usd,
                                 "ceo": c.ceo, "headquarters": c.headquarters,
                                 "logo_url": c.logo_url, "description": c.description,
                                 "fun_fact": c.fun_fact} for c in companies])
        else:
            companies = get_daily_companies(universe, cache)
            if db.DATABASE_URL:
                db.lock_puzzle(est_date, [c.ticker for c in companies],
                               [{"name": c.name, "ticker": c.ticker,
                                 "market_cap_billion_usd": c.market_cap_billion_usd,
                                 "ceo": c.ceo, "headquarters": c.headquarters,
                                 "logo_url": c.logo_url, "description": c.description,
                                 "fun_fact": c.fun_fact} for c in companies])
        save_snapshot(cache)

        round_points = [100, 100, 200, 300, 300]  # 2 easy + 1 medium + 2 hard = 1000 max
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


# ---------------------------------------------------------------------------
# Scores
# ---------------------------------------------------------------------------

@app.route("/api/scores", methods=["POST"])
def submit_score():
    data = request.get_json()
    user_id       = data.get("user_id")
    score         = data.get("score")
    max_score     = data.get("max_score")
    round_results = data.get("round_results", [])

    if not all([user_id, score is not None, max_score]):
        return jsonify({"error": "Missing fields"}), 400

    try:
        est_date = datetime.now(EST).strftime("%Y-%m-%d")
        saved = db.save_score(user_id, score, max_score, round_results, est_date)
        return jsonify({"saved": saved, "already_played": not saved})
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
