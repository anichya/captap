"""Market Cap Duel with live S&P 500 data."""

from __future__ import annotations

import json
import random
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import io

import requests
import pandas as pd
import yfinance as yf


ROUND_TIME_LIMIT_SECONDS = 15
POINTS_TO_WIN = 4
WARMUP_TARGET = 100
LEADERBOARD_PATH = Path(__file__).resolve().parent / "market_cap_duel_leaderboard.json"
SNAPSHOT_PATH = Path(__file__).resolve().parent / "market_cap_duel_snapshot.json"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


@dataclass(frozen=True)
class Company:
    name: str
    ticker: str
    market_cap_billion_usd: float
    revenue_billion_usd: float
    ebitda_billion_usd: float


def load_leaderboard() -> list[dict[str, str | int]]:
    if not LEADERBOARD_PATH.exists():
        return []
    try:
        with LEADERBOARD_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, list):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return []


def save_leaderboard(entries: list[dict[str, str | int]]) -> None:
    with LEADERBOARD_PATH.open("w", encoding="utf-8") as file:
        json.dump(entries, file, indent=2)


def print_leaderboard(entries: list[dict[str, str | int]]) -> None:
    print("\nLeaderboard (Top 10)")
    print("-" * 60)
    if not entries:
        print("No scores yet. Win a match to get on the board.")
        return
    for index, entry in enumerate(entries[:10], start=1):
        name = str(entry["name"])
        wins = int(entry["wins"])
        losses = int(entry["losses"])
        points = int(entry["points"])
        stamp = str(entry["played_at"])
        print(f"{index:>2}. {name:<16} W-L {wins}-{losses} | Points {points:<2} | {stamp}")


def timed_input(prompt: str, timeout_seconds: int) -> str | None:
    """Read user input with a live countdown. Returns None on timeout."""
    answer_holder: dict[str, str | None] = {"value": None}
    stop_event = threading.Event()

    def read_input() -> None:
        try:
            answer_holder["value"] = input(prompt).strip()
        except EOFError:
            answer_holder["value"] = None
        finally:
            stop_event.set()

    def show_countdown() -> None:
        for remaining in range(timeout_seconds - 1, -1, -1):
            stop_event.wait(1)
            if stop_event.is_set():
                break
            # Move up one line, overwrite countdown, restore cursor
            sys.stdout.write(f"\033[s\033[1A\r⏱  {remaining:2d}s remaining   \033[u")
            sys.stdout.flush()

    print(f"⏱  {timeout_seconds:2d}s remaining")
    input_thread = threading.Thread(target=read_input, daemon=True)
    countdown_thread = threading.Thread(target=show_countdown, daemon=True)
    countdown_thread.start()
    input_thread.start()
    input_thread.join(timeout=timeout_seconds)
    stop_event.set()
    if input_thread.is_alive():
        print()
        return None
    return answer_holder["value"]


def fetch_sp500_universe() -> list[tuple[str, str, str]]:
    """Return S&P 500 companies: [(display_ticker, yf_ticker, name), ...]."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; market-cap-duel/1.0)"}
    html = requests.get(SP500_WIKI_URL, headers=headers, timeout=15).text
    table = pd.read_html(io.StringIO(html))[0]
    rows: list[tuple[str, str, str]] = []
    for _, row in table.iterrows():
        display_ticker = str(row["Symbol"]).strip()
        yf_ticker = display_ticker.replace(".", "-")
        name = str(row["Security"]).strip()
        rows.append((display_ticker, yf_ticker, name))
    return rows


def load_snapshot_for_today() -> dict[str, dict[str, float | str]]:
    if not SNAPSHOT_PATH.exists():
        return {}
    try:
        with SNAPSHOT_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if data.get("as_of_date") != datetime.now().strftime("%Y-%m-%d"):
            return {}
        payload = data.get("companies", {})
        if isinstance(payload, dict):
            return payload
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_snapshot(companies: dict[str, dict[str, float | str]]) -> None:
    payload = {
        "as_of_date": datetime.now().strftime("%Y-%m-%d"),
        "companies": companies,
    }
    with SNAPSHOT_PATH.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def fetch_company_from_yahoo(
    display_ticker: str, yf_ticker: str, name: str
) -> Company | None:
    try:
        info = yf.Ticker(yf_ticker).get_info()
    except Exception:
        return None

    market_cap = info.get("marketCap")
    revenue = info.get("totalRevenue")
    ebitda = info.get("ebitda")
    if not market_cap or not revenue or ebitda is None:
        return None

    return Company(
        name=name,
        ticker=display_ticker,
        market_cap_billion_usd=float(market_cap) / 1_000_000_000,
        revenue_billion_usd=float(revenue) / 1_000_000_000,
        ebitda_billion_usd=float(ebitda) / 1_000_000_000,
    )



def get_round_pair(
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict[str, float | str]],
) -> tuple[Company, Company]:
    max_attempts = 200
    for _ in range(max_attempts):
        picks = random.sample(sp500_universe, 2)
        companies: list[Company] = []
        for display_ticker, yf_ticker, name in picks:
            cached = daily_cache.get(display_ticker)
            if cached:
                companies.append(
                    Company(
                        name=str(cached["name"]),
                        ticker=display_ticker,
                        market_cap_billion_usd=float(cached["market_cap_billion_usd"]),
                        revenue_billion_usd=float(cached["revenue_billion_usd"]),
                        ebitda_billion_usd=float(cached["ebitda_billion_usd"]),
                    )
                )
                continue

            fresh = fetch_company_from_yahoo(display_ticker, yf_ticker, name)
            if fresh is None:
                continue

            daily_cache[display_ticker] = {
                "name": fresh.name,
                "market_cap_billion_usd": fresh.market_cap_billion_usd,
                "revenue_billion_usd": fresh.revenue_billion_usd,
                "ebitda_billion_usd": fresh.ebitda_billion_usd,
            }
            companies.append(fresh)

        if len(companies) == 2:
            return companies[0], companies[1]

    raise RuntimeError("Could not fetch enough live company data for a round.")


def warmup_cache(
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict[str, float | str]],
    target: int = WARMUP_TARGET,
) -> None:
    """Preload random company fundamentals to reduce first-round latency."""
    if len(daily_cache) >= target:
        print(f"Warmup skipped (cache already has {len(daily_cache)} companies).")
        return

    needed = target - len(daily_cache)
    candidates = [row for row in sp500_universe if row[0] not in daily_cache]
    random.shuffle(candidates)
    selected = candidates[:needed]
    print(f"Warming up live data for up to {len(selected)} companies...")

    fetched = 0
    for display_ticker, yf_ticker, name in selected:
        fresh = fetch_company_from_yahoo(display_ticker, yf_ticker, name)
        if fresh is None:
            continue
        daily_cache[display_ticker] = {
            "name": fresh.name,
            "market_cap_billion_usd": fresh.market_cap_billion_usd,
            "revenue_billion_usd": fresh.revenue_billion_usd,
            "ebitda_billion_usd": fresh.ebitda_billion_usd,
        }
        fetched += 1

    save_snapshot(daily_cache)
    print(f"Warmup complete: +{fetched} fetched, {len(daily_cache)} cached for today.")


def ask_round(
    player_score: int,
    cpu_score: int,
    round_number: int,
    sp500_universe: list[tuple[str, str, str]],
    daily_cache: dict[str, dict[str, float | str]],
) -> tuple[int, int]:
    left, right = get_round_pair(sp500_universe, daily_cache)
    save_snapshot(daily_cache)

    print("\n" + "=" * 60)
    print(f"Round {round_number} | You {player_score} - {cpu_score} CPU")
    print("=" * 60)
    print(f"1) {left.name} ({left.ticker})")
    print(f"2) {right.name} ({right.ticker})")
    print()

    start = time.time()
    guess = timed_input("Which has the higher market cap? Enter 1 or 2: ", ROUND_TIME_LIMIT_SECONDS)
    elapsed = time.time() - start

    if guess is None:
        print("Time is up. CPU wins the round.")
        return player_score, cpu_score + 1
    if guess not in {"1", "2"}:
        print("Invalid input. CPU wins the round.")
        return player_score, cpu_score + 1

    correct_choice = "1" if left.market_cap_billion_usd >= right.market_cap_billion_usd else "2"
    won_round = guess == correct_choice

    print(f"You answered in {elapsed:.2f}s.")
    print(
        f"Answer: {left.ticker} ${left.market_cap_billion_usd:.1f}B vs "
        f"{right.ticker} ${right.market_cap_billion_usd:.1f}B"
    )

    if won_round:
        print("Correct. You win the round.")
        return player_score + 1, cpu_score
    print("Not quite. CPU wins the round.")
    return player_score, cpu_score + 1


def play_game() -> None:
    print("\nMarket Cap Duel")
    print("-" * 60)
    print("Beat the CPU in a best-of-7 showdown.")
    print("Universe: any S&P 500 company.")
    print("Data source: live Yahoo Finance + daily local cache.")
    print("-" * 60)

    print("Loading S&P 500 list...")
    sp500_universe = fetch_sp500_universe()
    print(f"Loaded {len(sp500_universe)} companies.")

    daily_cache = load_snapshot_for_today()
    if daily_cache:
        print(f"Loaded {len(daily_cache)} cached fundamentals for today.")
    warmup_cache(sp500_universe, daily_cache)

    leaderboard = load_leaderboard()
    print_leaderboard(leaderboard)

    player_name = input("\nEnter your player name: ").strip() or "Player"

    player_score = 0
    cpu_score = 0
    round_number = 1
    while player_score < POINTS_TO_WIN and cpu_score < POINTS_TO_WIN:
        player_score, cpu_score = ask_round(
            player_score, cpu_score, round_number, sp500_universe, daily_cache
        )
        round_number += 1

    print("\n" + "#" * 60)
    if player_score > cpu_score:
        print(f"You win the match {player_score}-{cpu_score}.")
    else:
        print(f"CPU wins the match {cpu_score}-{player_score}.")
    print("#" * 60)

    leaderboard.append(
        {
            "name": player_name,
            "wins": player_score,
            "losses": cpu_score,
            "points": player_score,
            "played_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    )
    leaderboard.sort(
        key=lambda x: (int(x["wins"]), int(x["points"]), -int(x["losses"])),
        reverse=True,
    )
    save_leaderboard(leaderboard[:10])
    print_leaderboard(leaderboard[:10])


if __name__ == "__main__":
    play_game()
