"""Market Cap Quiz — Daily 5-round puzzle."""

from __future__ import annotations

import io
import json
import math
import random
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

EST = ZoneInfo("America/New_York")
from pathlib import Path

import re
import requests
import pandas as pd
import yfinance as yf


ROUNDS = 5
SNAPSHOT_PATH = Path(__file__).resolve().parent / "market_cap_quiz_snapshot.json"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
# Log10 offsets for the 5 multiple-choice options (~0.2x, 0.45x, 1x, 2.5x, 5x)
CHOICE_LOG_OFFSETS = (-0.7, -0.35, 0.0, 0.4, 0.7)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Company:
    name: str
    ticker: str
    market_cap_billion_usd: float
    ceo: str = "N/A"
    headquarters: str = "N/A"
    logo_url: str = ""
    description: str = ""
    fun_fact: str = ""
    revenue_billion_usd: float = 0.0
    full_time_employees: int = 0

    @property
    def points_available(self) -> int:
        """3-tier scoring: Easy=100, Medium=200, Hard=300."""
        cap = self.market_cap_billion_usd
        if cap >= 200:
            return 100   # Easy: household names (Apple, Google …)
        if cap >= 30:
            return 200   # Medium: recognisable but not iconic
        return 300       # Hard: obscure small/mid caps


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_cap(billions: float) -> str:
    if billions >= 1_000:
        return f"${billions / 1_000:.2f}T"
    if billions >= 1:
        return f"${billions:.1f}B"
    return f"${billions * 1_000:.0f}M"


# ---------------------------------------------------------------------------
# Multiple-choice generation
# ---------------------------------------------------------------------------

def generate_choices(market_cap: float) -> tuple[list[float], int]:
    """Return (5 shuffled choices, correct index 0-based).

    Choices are log-spaced around the real value so wrong answers are
    plausible but clearly distinct.
    """
    log_cap = math.log10(market_cap)
    choices = [10 ** (log_cap + off) for off in CHOICE_LOG_OFFSETS]
    random.shuffle(choices)
    correct_idx = min(range(len(choices)), key=lambda i: abs(choices[i] - market_cap))
    return choices, correct_idx


# ---------------------------------------------------------------------------
# S&P 500 universe + caching
# ---------------------------------------------------------------------------

def fetch_sp500_universe() -> list[tuple[str, str, str]]:
    """Return [(display_ticker, yf_ticker, name), …] for all S&P 500 companies."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; market-cap-quiz/1.0)"}
    html = requests.get(SP500_WIKI_URL, headers=headers, timeout=15).text
    table = pd.read_html(io.StringIO(html))[0]
    rows: list[tuple[str, str, str]] = []
    for _, row in table.iterrows():
        display_ticker = str(row["Symbol"]).strip()
        yf_ticker = display_ticker.replace(".", "-")
        name = str(row["Security"]).strip()
        rows.append((display_ticker, yf_ticker, name))
    return rows


def load_snapshot_for_today() -> dict[str, dict]:
    if not SNAPSHOT_PATH.exists():
        return {}
    try:
        with SNAPSHOT_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("as_of_date") != datetime.now(EST).strftime("%Y-%m-%d"):
            return {}
        payload = data.get("companies", {})
        if isinstance(payload, dict):
            return payload
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_snapshot(companies: dict[str, dict]) -> None:
    payload = {
        "as_of_date": datetime.now(EST).strftime("%Y-%m-%d"),
        "companies": companies,
    }
    with SNAPSHOT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def fetch_company(display_ticker: str, yf_ticker: str, name: str) -> Company | None:
    try:
        info = yf.Ticker(yf_ticker).get_info()
    except Exception:
        return None
    market_cap = info.get("marketCap")
    if not market_cap:
        return None

    city = info.get("city", "")
    state = info.get("state", "")
    country = info.get("country", "")
    hq_parts = [p for p in [city, state, country] if p]
    headquarters = ", ".join(hq_parts) if hq_parts else "N/A"

    website = info.get("website", "")
    if website:
        domain = website.replace("https://", "").replace("http://", "").lstrip("www.").split("/")[0]
        logo_url = f"https://logo.clearbit.com/{domain}"
    else:
        logo_url = info.get("logo_url", "")

    officers = info.get("companyOfficers") or []
    ceo = next(
        (o.get("name", "") for o in officers if "CEO" in o.get("title", "").upper()),
        officers[0].get("name", "N/A") if officers else "N/A",
    )

    summary = info.get("longBusinessSummary", "")

    # description = first sentence (company intro)
    # fun_fact = most interesting sentence: prefer ones with specific details,
    # numbers, brands, or history over generic operational descriptions
    description = ""
    fun_fact = ""
    if summary:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", summary.strip()) if len(s.strip()) > 40]
        if sentences:
            description = sentences[0]
        interesting_markers = [
            "founded", "billion", "million", "largest", "first", "world",
            "brand", "serves", "customers", "history", "pioneer", "leading",
            "acquired", "invented", "known for", "famous", "headquartered",
        ]
        for s in sentences[1:]:
            if any(m in s.lower() for m in interesting_markers) and len(s) <= 220:
                fun_fact = s
                break
        if not fun_fact and len(sentences) >= 2:
            fun_fact = sentences[1]

    # Expert mode fields
    total_revenue = info.get("totalRevenue")
    revenue_billion_usd = float(total_revenue) / 1_000_000_000 if total_revenue else 0.0
    full_time_employees = int(info.get("fullTimeEmployees") or 0)

    return Company(
        name=name,
        ticker=display_ticker,
        market_cap_billion_usd=float(market_cap) / 1_000_000_000,
        ceo=ceo,
        headquarters=headquarters,
        logo_url=logo_url,
        description=description,
        fun_fact=fun_fact,
        revenue_billion_usd=revenue_billion_usd,
        full_time_employees=full_time_employees,
    )


# ---------------------------------------------------------------------------
# Fetch companies by a locked ticker list
# ---------------------------------------------------------------------------

def get_companies_by_tickers(
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict],
    tickers: list[str],
) -> list[Company]:
    """Return Company objects for an ordered list of tickers (locked puzzle)."""
    ticker_to_yf: dict[str, tuple[str, str]] = {
        disp: (yf_t, name) for disp, yf_t, name in sp500_universe
    }
    companies: list[Company] = []
    for ticker in tickers:
        cached = daily_cache.get(ticker)
        if cached:
            companies.append(Company(
                name=str(cached["name"]),
                ticker=ticker,
                market_cap_billion_usd=float(cached["market_cap_billion_usd"]),
                ceo=str(cached.get("ceo", "N/A")),
                headquarters=str(cached.get("headquarters", "N/A")),
                logo_url=str(cached.get("logo_url", "")),
                description=str(cached.get("description", "")),
                fun_fact=str(cached.get("fun_fact", "")),
                revenue_billion_usd=float(cached.get("revenue_billion_usd", 0.0)),
                full_time_employees=int(cached.get("full_time_employees", 0)),
            ))
        elif ticker in ticker_to_yf:
            yf_ticker, name = ticker_to_yf[ticker]
            company = fetch_company(ticker, yf_ticker, name)
            if company:
                daily_cache[ticker] = {
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
    return companies


# ---------------------------------------------------------------------------
# Daily puzzle selection
# ---------------------------------------------------------------------------

def get_daily_companies(
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict],
) -> list[Company]:
    """Return exactly 2 easy + 1 medium + 2 hard companies (in that order),
    seeded by EST date so every player gets the same puzzle each day."""
    import os
    puzzle_version = os.environ.get("PUZZLE_VERSION", "1")
    random.seed(datetime.now(EST).strftime("%Y-%m-%d") + puzzle_version)
    candidates = random.sample(sp500_universe, len(sp500_universe))

    easy:   list[Company] = []
    medium: list[Company] = []
    hard:   list[Company] = []

    for display_ticker, yf_ticker, name in candidates:
        if len(easy) >= 2 and len(medium) >= 1 and len(hard) >= 2:
            break
        cached = daily_cache.get(display_ticker)
        if cached:
            company = Company(
                name=str(cached["name"]),
                ticker=display_ticker,
                market_cap_billion_usd=float(cached["market_cap_billion_usd"]),
                ceo=str(cached.get("ceo", "N/A")),
                headquarters=str(cached.get("headquarters", "N/A")),
                logo_url=str(cached.get("logo_url", "")),
                description=str(cached.get("description", "")),
                fun_fact=str(cached.get("fun_fact", "")),
                revenue_billion_usd=float(cached.get("revenue_billion_usd", 0.0)),
                full_time_employees=int(cached.get("full_time_employees", 0)),
            )
        else:
            company = fetch_company(display_ticker, yf_ticker, name)
            if company is None:
                continue
            daily_cache[display_ticker] = {
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

        pts = company.points_available
        if pts == 100 and len(easy) < 2:
            easy.append(company)
        elif pts == 200 and len(medium) < 1:
            medium.append(company)
        elif pts == 300 and len(hard) < 2:
            hard.append(company)

    if len(easy) < 2 or len(medium) < 1 or len(hard) < 2:
        raise RuntimeError("Could not find enough companies in each tier for today's puzzle.")

    return easy + medium + hard


# ---------------------------------------------------------------------------
# Battle puzzle selection
# ---------------------------------------------------------------------------

def get_battle_companies(
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict],
    battle_id: str,
) -> list[Company]:
    """Return 5 companies for a battle puzzle, seeded by battle_id.
    Mix: 2 easy + 1 medium + 2 hard (same structure as daily quiz)."""
    rng = random.Random(battle_id)
    candidates = rng.sample(sp500_universe, len(sp500_universe))

    easy:   list[Company] = []
    medium: list[Company] = []
    hard:   list[Company] = []

    for display_ticker, yf_ticker, name in candidates:
        if len(easy) >= 2 and len(medium) >= 1 and len(hard) >= 2:
            break
        cached = daily_cache.get(display_ticker)
        if cached:
            company = Company(
                name=str(cached["name"]),
                ticker=display_ticker,
                market_cap_billion_usd=float(cached["market_cap_billion_usd"]),
                ceo=str(cached.get("ceo", "N/A")),
                headquarters=str(cached.get("headquarters", "N/A")),
                logo_url=str(cached.get("logo_url", "")),
                description=str(cached.get("description", "")),
                fun_fact=str(cached.get("fun_fact", "")),
                revenue_billion_usd=float(cached.get("revenue_billion_usd", 0.0)),
                full_time_employees=int(cached.get("full_time_employees", 0)),
            )
        else:
            company = fetch_company(display_ticker, yf_ticker, name)
            if company is None:
                continue
            daily_cache[display_ticker] = {
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

        pts = company.points_available
        if pts == 100 and len(easy) < 2:
            easy.append(company)
        elif pts == 200 and len(medium) < 1:
            medium.append(company)
        elif pts == 300 and len(hard) < 2:
            hard.append(company)

    if len(easy) < 2 or len(medium) < 1 or len(hard) < 2:
        raise RuntimeError("Not enough companies for battle puzzle")

    return easy + medium + hard


# ---------------------------------------------------------------------------
# Round logic
# ---------------------------------------------------------------------------

def play_round(
    company: Company,
    round_num: int,
    total_score: int,
    max_score: int,
) -> tuple[int, bool]:
    """Display one round and collect player answer. Returns (points_earned, correct)."""
    choices, correct_idx = generate_choices(company.market_cap_billion_usd)

    print(f"\n{'=' * 60}")
    print(f"Round {round_num}/{ROUNDS}  |  Score: {total_score} / {max_score} pts")
    print(f"{'=' * 60}")
    print(f"Company:     {company.name}  ({company.ticker})")
    print(f"CEO:         {company.ceo}")
    print(f"HQ:          {company.headquarters}")
    if company.logo_url:
        print(f"Logo:        {company.logo_url}")
    print(f"Points:      {company.points_available}  "
          f"({'well known — low reward' if company.points_available <= 15 else 'less known — high reward'})")
    print()
    print("What is this company's market cap?")
    print()
    for i, choice in enumerate(choices, 1):
        print(f"  {i})  {format_cap(choice)}")
    print()

    raw = input("Your answer (1-5): ").strip()
    if raw not in {"1", "2", "3", "4", "5"}:
        print(f"\n❌  Invalid input — no points awarded.")
        print(f"    Correct answer: {format_cap(company.market_cap_billion_usd)}")
        return 0, False

    correct = (int(raw) - 1) == correct_idx
    if correct:
        print(f"\n✅  Correct! +{company.points_available} pts")
    else:
        print(f"\n❌  Wrong. The answer was {format_cap(company.market_cap_billion_usd)}")
    return (company.points_available if correct else 0), correct


# ---------------------------------------------------------------------------
# Share card
# ---------------------------------------------------------------------------

def build_share_card(
    companies: list[Company],
    results: list[bool],
    total: int,
    max_total: int,
) -> str:
    date_str = datetime.now(EST).strftime("%B %d, %Y")
    lines = [f"📈 Market Cap Quiz — {date_str}"]
    for i, (company, correct) in enumerate(zip(companies, results), 1):
        icon = "✅" if correct else "❌"
        pts = company.points_available if correct else 0
        lines.append(
            f"{i}. {icon}  {company.name} ({company.ticker})"
            f"  —  {pts}/{company.points_available} pts"
        )
    lines.append(f"\nTotal: {total}/{max_total} pts")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main game loop
# ---------------------------------------------------------------------------

def play_game() -> None:
    print("\nMarket Cap Quiz")
    print("-" * 60)
    print("5 daily rounds. Guess the market cap from 5 choices.")
    print("Obscure companies are worth more points.")
    print("Everyone gets the same puzzle today — share your score!")
    print("-" * 60)

    print("\nLoading S&P 500 list...")
    sp500_universe = fetch_sp500_universe()
    print(f"Loaded {len(sp500_universe)} companies.")

    daily_cache = load_snapshot_for_today()
    if daily_cache:
        print(f"Using cached data ({len(daily_cache)} companies cached for today).")

    print("Fetching today's puzzle companies...")
    companies = get_daily_companies(sp500_universe, daily_cache)
    save_snapshot(daily_cache)

    max_total = sum(c.points_available for c in companies)
    total_score = 0
    results: list[bool] = []

    for i, company in enumerate(companies, 1):
        earned, correct = play_round(company, i, total_score, max_total)
        total_score += earned
        results.append(correct)

    print(f"\n{'#' * 60}")
    print(f"Final Score: {total_score} / {max_total} pts")
    print(f"{'#' * 60}")

    card = build_share_card(companies, results, total_score, max_total)
    print("\n--- Copy and share your result ---")
    print()
    print(card)
    print()


if __name__ == "__main__":
    play_game()
