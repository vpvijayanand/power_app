"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          NIFTY / BANKNIFTY  1-MINUTE PATTERN RECOGNITION ENGINE             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Source table  : public.index_data  (power_app DB)                          ║
║  Pattern table : public.nifty_patterns                                       ║
║  Config        : loaded from .env in the same directory                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Core tools:                                                                 ║
║   fastdtw   – O(N) Dynamic Time Warping  → shape similarity                 ║
║   scipy     – Savitzky-Golay smoothing   → noise removal                    ║
║   numpy     – normalise + resample       → scale-invariant comparison       ║
║   psycopg2  – Postgres I/O                                                  ║
║   python-dotenv – .env credential loading                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

Install:
    pip install fastdtw scipy pandas numpy psycopg2-binary tqdm colorama python-dotenv

.env file (place in the same directory as this script):
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=power_app
    DB_USER=your_user
    DB_PASSWORD=your_password

Usage:
    # Build the pattern library  (one symbol)
    python nifty_pattern_engine.py --symbol NIFTY --start_date 2024-01-01 --end_date 2024-12-31

    # Both symbols at once
    python nifty_pattern_engine.py --symbol ALL --start_date 2024-01-01 --end_date 2024-12-31

    # Custom thresholds
    python nifty_pattern_engine.py --symbol BANKNIFTY \\
        --start_date 2024-01-01 --end_date 2024-12-31 \\
        --similarity_threshold 75 --min_deviation 25

    # Compare today's live chart vs stored patterns
    python nifty_pattern_engine.py --mode live --symbol NIFTY
    python nifty_pattern_engine.py --mode live --symbol ALL --similarity_threshold 65

    # Wipe and rebuild
    python nifty_pattern_engine.py --symbol ALL --start_date 2023-01-01 --end_date 2024-12-31 --reset
"""

import argparse
import json
import os
import sys
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from colorama import Fore, Style, init
from dotenv import load_dotenv
from fastdtw import fastdtw
from scipy.signal import savgol_filter
from scipy.spatial.distance import euclidean
from tqdm import tqdm

warnings.filterwarnings("ignore")
init(autoreset=True)

# ─────────────────────────────────────────────────────────────────
#  LOAD .env
# ─────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")


def get_db_config() -> dict:
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME",     "power_app"),
        "user":     os.getenv("DB_USER",     ""),
        "password": os.getenv("DB_PASSWORD", ""),
    }


# ─────────────────────────────────────────────────────────────────
#  CONSTANTS  (matching your index_data schema exactly)
# ─────────────────────────────────────────────────────────────────
SOURCE_TABLE  = "index_data"
PATTERN_TABLE = "nifty_patterns"

MARKET_OPEN  = "09:15"
MARKET_CLOSE = "15:29"    # last full 1-min bar before close

# Every day's curve is resampled to this many points before DTW comparison.
# 75 pts ≈ one point every ~5 min — retains shape, fast to compare.
RESAMPLE_POINTS = 75

# Savitzky-Golay filter — smooths 1-min noise, preserves peaks/troughs
SG_WINDOW     = 11        # must be odd; raise to 13/15 for smoother
SG_POLY_ORDER = 2

VALID_SYMBOLS = {"NIFTY", "BANKNIFTY"}

# ASCII sparkline chars
SPARKS = "▁▂▃▄▅▆▇█"


# ─────────────────────────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────

def get_connection():
    cfg = get_db_config()
    try:
        return psycopg2.connect(**cfg)
    except psycopg2.OperationalError as e:
        print(f"{Fore.RED}✗ DB connection failed: {e}{Style.RESET_ALL}")
        print(f"  Check .env  →  DB_HOST={cfg['host']}  DB_NAME={cfg['database']}  DB_USER={cfg['user']}")
        sys.exit(1)


def ensure_pattern_table(conn):
    """Auto-create nifty_patterns if it does not yet exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {PATTERN_TABLE} (
        id                  SERIAL           PRIMARY KEY,
        symbol              VARCHAR(20)      NOT NULL,
        pattern_type        VARCHAR(30)      NOT NULL,

        first_seen_date     DATE             NOT NULL,
        first_seen_open     DOUBLE PRECISION,
        first_seen_close    DOUBLE PRECISION,

        similar_count       INTEGER          NOT NULL DEFAULT 1,
        last_seen_date      DATE,

        -- 75-point normalised close curve stored as a JSON number array
        normalized_series   JSONB            NOT NULL,

        -- Scalar stats from the first occurrence
        deviation_pct       DOUBLE PRECISION NOT NULL,
        open_close_chg_pct  DOUBLE PRECISION,
        max_drawup_pct      DOUBLE PRECISION,
        max_drawdown_pct    DOUBLE PRECISION,
        close_vs_range_pct  DOUBLE PRECISION,

        -- Indicator snapshot at day open
        adx_at_open         DOUBLE PRECISION,
        supertrend_dir      INTEGER,          -- 1=bullish  -1=bearish
        super_power         VARCHAR(20),      -- BUY / SELL / blank

        -- Every matching date: [{{"date":"...","similarity":82.4,"open_close_chg":-0.31}}, ...]
        similar_dates       JSONB            NOT NULL DEFAULT '[]'::jsonb,

        created_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_np_symbol        ON {PATTERN_TABLE} (symbol);
    CREATE INDEX IF NOT EXISTS idx_np_type          ON {PATTERN_TABLE} (pattern_type);
    CREATE INDEX IF NOT EXISTS idx_np_symbol_type   ON {PATTERN_TABLE} (symbol, pattern_type);
    CREATE INDEX IF NOT EXISTS idx_np_similar_count ON {PATTERN_TABLE} (similar_count DESC);
    CREATE INDEX IF NOT EXISTS idx_np_first_seen    ON {PATTERN_TABLE} (first_seen_date);
    CREATE INDEX IF NOT EXISTS idx_np_deviation     ON {PATTERN_TABLE} (deviation_pct);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"{Fore.GREEN}✓ Pattern table '{PATTERN_TABLE}' is ready.{Style.RESET_ALL}")


def fetch_day_candles(conn, trade_date: date, symbol: str) -> pd.DataFrame:
    """Load 1-min candles for a (date, symbol) pair from index_data."""
    sql = """
        SELECT
            timestamp             AS ts,
            open, high, low, close,
            adx,
            supertrend_direction,
            super_power
        FROM  index_data
        WHERE DATE(timestamp) = %s
          AND symbol          = %s
          AND timestamp::time BETWEEN %s AND %s
        ORDER BY timestamp
    """
    df = pd.read_sql(sql, conn, params=(trade_date, symbol, MARKET_OPEN, MARKET_CLOSE))
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def load_all_patterns(conn, symbol: str) -> list[dict]:
    """Load all stored patterns for one symbol and decode JSON series to np.ndarray."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {PATTERN_TABLE} WHERE symbol = %s ORDER BY id",
            (symbol,)
        )
        rows = [dict(r) for r in cur.fetchall()]
    for pat in rows:
        raw = pat["normalized_series"]
        pat["normalized_series"] = np.array(json.loads(raw) if isinstance(raw, str) else raw)
    return rows


def get_trading_dates(conn, symbol: str, start_date: date, end_date: date) -> list[date]:
    sql = """
        SELECT DISTINCT DATE(timestamp) AS trade_date
        FROM   index_data
        WHERE  symbol = %s
          AND  DATE(timestamp) BETWEEN %s AND %s
        ORDER  BY trade_date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, start_date, end_date))
        return [row[0] for row in cur.fetchall()]


def insert_pattern(conn, record: dict) -> int:
    sql = f"""
        INSERT INTO {PATTERN_TABLE} (
            symbol, pattern_type,
            first_seen_date, first_seen_open, first_seen_close,
            normalized_series,
            deviation_pct, open_close_chg_pct,
            max_drawup_pct, max_drawdown_pct, close_vs_range_pct,
            adx_at_open, supertrend_dir, super_power,
            last_seen_date
        ) VALUES (
            %(symbol)s, %(pattern_type)s,
            %(first_seen_date)s, %(first_seen_open)s, %(first_seen_close)s,
            %(normalized_series)s::jsonb,
            %(deviation_pct)s, %(open_close_chg_pct)s,
            %(max_drawup_pct)s, %(max_drawdown_pct)s, %(close_vs_range_pct)s,
            %(adx_at_open)s, %(supertrend_dir)s, %(super_power)s,
            %(first_seen_date)s
        ) RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, record)
        pid = cur.fetchone()[0]
    conn.commit()
    return pid


def update_pattern_match(conn, pattern_id: int, match_date: date,
                          similarity: float, open_close_chg: float):
    """Increment similar_count and append the matching date to the JSONB array."""
    entry = json.dumps([{
        "date":           str(match_date),
        "similarity":     round(similarity, 2),
        "open_close_chg": round(open_close_chg, 4),
    }])
    sql = f"""
        UPDATE {PATTERN_TABLE}
        SET  similar_count  = similar_count + 1,
             last_seen_date = GREATEST(last_seen_date, %s),
             similar_dates  = similar_dates || %s::jsonb,
             updated_at     = NOW()
        WHERE id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (match_date, entry, pattern_id))
    conn.commit()


# ─────────────────────────────────────────────────────────────────
#  SIGNAL PROCESSING
# ─────────────────────────────────────────────────────────────────

def smooth(arr: np.ndarray) -> np.ndarray:
    n   = len(arr)
    win = min(SG_WINDOW, n if n % 2 == 1 else n - 1)
    if win < 5:
        return arr
    return savgol_filter(arr, window_length=win, polyorder=SG_POLY_ORDER)


def normalize(arr: np.ndarray) -> np.ndarray:
    """
    Convert closes → % change from open → scale to [0, 100].
    Shape from ₹18k Nifty and ₹23k Nifty becomes directly comparable.
    """
    pct = (arr - arr[0]) / arr[0] * 100
    lo, hi = pct.min(), pct.max()
    if hi - lo < 1e-8:
        return np.full_like(pct, 50.0)
    return (pct - lo) / (hi - lo) * 100


def resample(arr: np.ndarray, n: int = RESAMPLE_POINTS) -> np.ndarray:
    x_old = np.linspace(0, 1, len(arr))
    x_new = np.linspace(0, 1, n)
    return np.interp(x_new, x_old, arr)


def build_curve(df: pd.DataFrame) -> np.ndarray:
    """Full pipeline: raw close → smooth → normalise → resample to 75 pts."""
    closes = df["close"].to_numpy(dtype=float)
    return resample(normalize(smooth(closes)), RESAMPLE_POINTS)


def curve_complexity(curve: np.ndarray) -> float:
    """
    Standard deviation of the normalised curve (0-100 scale).
    This is the TRUE deviation metric used to filter flat/boring days.

    Typical values for Indian indices:
      < 10  →  extremely flat day, not worth storing
      10-20 →  mildly trending / low volatility
      20-30 →  normal active trading day        ← sweet spot for pattern capture
      30+   →  high volatility / strong trend day

    --min_deviation controls this value (default 15).
    This is completely different from raw price deviation (0.5%-3%)
    which was causing all 304 days to be skipped.
    """
    return float(np.std(curve))


def compute_features(df: pd.DataFrame) -> dict:
    o   = float(df["open"].iloc[0])
    c   = float(df["close"].iloc[-1])
    h   = float(df["high"].max())
    l   = float(df["low"].min())
    rng = max(h - l, 1e-8)

    adx_val = df["adx"].iloc[0]
    st_dir  = df["supertrend_direction"].iloc[0]
    sp_val  = df["super_power"].iloc[0]

    return {
        # Raw price metrics (stored in DB for reference / display)
        "deviation_pct":      round((h - l) / o * 100,    4),   # e.g. 1.2% for Nifty
        "open_close_chg_pct": round((c - o) / o * 100,    4),
        "max_drawup_pct":     round((h - o) / o * 100,    4),
        "max_drawdown_pct":   round((o - l) / o * 100,    4),
        "close_vs_range_pct": round((c - l) / rng * 100,  4),
        "first_seen_open":    o,
        "first_seen_close":   c,
        # Indicator snapshot
        "adx_at_open":        float(adx_val) if pd.notna(adx_val) else None,
        "supertrend_dir":     int(st_dir)    if pd.notna(st_dir)  else None,
        "super_power":        str(sp_val)    if pd.notna(sp_val) and str(sp_val).strip() else None,
    }


def classify_pattern(df: pd.DataFrame, curve: np.ndarray) -> str:
    mid  = RESAMPLE_POINTS // 2
    h1   = curve[:mid].mean()
    h2   = curve[mid:].mean()
    chg  = (df["close"].iloc[-1] - df["open"].iloc[0]) / df["open"].iloc[0] * 100
    vstd = float(np.std(curve))

    if vstd < 5:
        return "SIDEWAYS"
    if chg > 0.3:
        return "UP_TREND"     if h2 > h1 + 10 else "REVERSAL_UP"
    if chg < -0.3:
        return "DOWN_TREND"   if h2 < h1 - 10 else "REVERSAL_DOWN"
    return "VOLATILE"


# ─────────────────────────────────────────────────────────────────
#  DTW SIMILARITY  →  0 – 100 score
# ─────────────────────────────────────────────────────────────────

def dtw_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """
    FastDTW distance converted to a 0-100 similarity score.
    100 = identical shape,  0 = completely different.
    """
    dist, _ = fastdtw(a, b, dist=lambda x, y: abs(x - y))
    max_dist = float(RESAMPLE_POINTS * 100)   # worst-case: each of 75 pts off by 100
    return max(0.0, 1.0 - dist / max_dist) * 100.0


# ─────────────────────────────────────────────────────────────────
#  PROCESS ONE TRADING DAY
# ─────────────────────────────────────────────────────────────────

def process_day(
    conn,
    trade_date: date,
    symbol: str,
    existing_patterns: list[dict],
    similarity_threshold: float,
    min_deviation: float,
    verbose: bool = True,
) -> tuple[str, Optional[int]]:
    """
    Returns ("new" | "matched" | "skipped",  pattern_id | None)
    """
    df = fetch_day_candles(conn, trade_date, symbol)

    if df.empty or len(df) < 30:
        if verbose:
            print(f"  {Fore.YELLOW}⚠  {trade_date} | {symbol}: {len(df)} candles — skip{Style.RESET_ALL}")
        return ("skipped", None)

    features  = compute_features(df)
    curve     = build_curve(df)

    # Gate on NORMALISED curve complexity (std dev on 0-100 scale).
    # Raw price deviation for Nifty is only 0.5-3% — using that as a 30%
    # threshold would skip every single day (as seen in the output).
    # curve_complexity() returns a 0-100 value; --min_deviation=15 is realistic.
    complexity = curve_complexity(curve)
    if complexity < min_deviation:
        if verbose:
            print(
                f"  {Fore.YELLOW}⚠  {trade_date} | {symbol}: "
                f"shape_complexity={complexity:.1f} < {min_deviation} "
                f"(raw_dev={features['deviation_pct']:.2f}%) — skip{Style.RESET_ALL}"
            )
        return ("skipped", None)

    ptype = classify_pattern(df, curve)

    # ── DTW compare against every stored pattern ─────────────────
    best_sim = 0.0
    best_id  = None
    for pat in existing_patterns:
        # A pattern MUST belong to the same category (e.g., UP_TREND) to be matched
        if pat["pattern_type"] != ptype:
            continue
            
        sim = dtw_similarity(curve, pat["normalized_series"])
        if sim > best_sim:
            best_sim = sim
            best_id  = pat["id"]

    if best_sim >= similarity_threshold:
        update_pattern_match(conn, best_id, trade_date, best_sim, features["open_close_chg_pct"])
        for pat in existing_patterns:
            if pat["id"] == best_id:
                pat["similar_count"] += 1
                break
        if verbose:
            print(
                f"  {Fore.CYAN}✓  {trade_date} | {symbol}  MATCHED #{best_id} "
                f"[{ptype}]  sim={best_sim:.1f}%  "
                f"complexity={complexity:.1f}  raw_dev={features['deviation_pct']:.2f}%  "
                f"Δclose={features['open_close_chg_pct']:+.2f}%{Style.RESET_ALL}"
            )
        return ("matched", best_id)

    else:
        record = {
            "symbol":             symbol,
            "pattern_type":       ptype,
            "first_seen_date":    trade_date,
            "normalized_series":  json.dumps(curve.tolist()),
            **features,
        }
        pid    = insert_pattern(conn, record)
        record["id"]               = pid
        record["normalized_series"]= curve         # numpy array for in-memory cache
        record["similar_count"]    = 1
        existing_patterns.append(record)
        if verbose:
            print(
                f"  {Fore.GREEN}★  {trade_date} | {symbol}  NEW #{pid} "
                f"[{ptype}]  complexity={complexity:.1f}  raw_dev={features['deviation_pct']:.2f}%  "
                f"Δclose={features['open_close_chg_pct']:+.2f}%  "
                f"prev_best={best_sim:.1f}%{Style.RESET_ALL}"
            )
        return ("new", pid)


# ─────────────────────────────────────────────────────────────────
#  LIVE MODE
# ─────────────────────────────────────────────────────────────────

def sparkline(series, width: int = 50) -> str:
    arr = np.array(series, dtype=float)
    x_old = np.linspace(0, 1, len(arr))
    x_new = np.linspace(0, 1, width)
    arr   = np.interp(x_new, x_old, arr)
    lo, hi = arr.min(), arr.max()
    if hi == lo:
        return SPARKS[3] * width
    norm = (arr - lo) / (hi - lo)
    return "".join(SPARKS[int(v * (len(SPARKS) - 1))] for v in norm)


def live_compare(conn, symbol: str, similarity_threshold: float, top_n: int = 5):
    today = date.today()
    df    = fetch_day_candles(conn, today, symbol)

    if len(df) < 10:
        print(f"{Fore.RED}Not enough candles for {symbol} today ({len(df)}).{Style.RESET_ALL}")
        return

    features = compute_features(df)
    curve    = build_curve(df)
    ptype    = classify_pattern(df, curve)

    TYPE_COL = {
        "UP_TREND":     Fore.GREEN,
        "REVERSAL_UP":  Fore.LIGHTGREEN_EX,
        "DOWN_TREND":   Fore.RED,
        "REVERSAL_DOWN":Fore.LIGHTRED_EX,
        "SIDEWAYS":     Fore.YELLOW,
        "VOLATILE":     Fore.MAGENTA,
    }

    print(f"\n{Fore.CYAN}{'─'*65}")
    print(f"  LIVE  {symbol}  |  {today}  |  {len(df)} candles")
    print(f"{'─'*65}")
    print(f"  Pattern type     : {TYPE_COL.get(ptype, Fore.WHITE)}{ptype}{Style.RESET_ALL}")
    print(f"  Deviation        : {features['deviation_pct']:.2f}%  (raw price range)")
    print(f"  Shape complexity : {curve_complexity(curve):.1f} / 100  (normalised std-dev)")
    print(f"  Open → Close Δ   : {features['open_close_chg_pct']:+.2f}%")
    print(f"  Max drawup       : {features['max_drawup_pct']:+.2f}%")
    print(f"  Max drawdown     : {features['max_drawdown_pct']:+.2f}%")
    print(f"  ADX              : {features['adx_at_open'] or '—'}")
    st = features["supertrend_dir"]
    print(f"  Supertrend       : {'▲ BULLISH' if st == 1 else '▼ BEARISH' if st == -1 else '—'}")
    print(f"  Super Power      : {features['super_power'] or '—'}")
    print(f"\n  Shape : {sparkline(curve)}")
    print(f"{'─'*65}{Style.RESET_ALL}\n")

    existing = load_all_patterns(conn, symbol)
    if not existing:
        print(f"{Fore.YELLOW}No stored patterns yet. Run backfill first.{Style.RESET_ALL}")
        return

    results = sorted(
        [(dtw_similarity(curve, p["normalized_series"]), p) for p in existing],
        key=lambda x: x[0], reverse=True
    )

    print(f"{Fore.WHITE}Top {top_n} similar patterns  (threshold={similarity_threshold}%):{Style.RESET_ALL}")
    print(f"{'':2}{'ID':<6} {'First Seen':<13} {'Type':<16} {'Sim%':>7}  "
          f"{'#Seen':>6}  {'Dev%':>7}  {'Δclose':>8}  {'ST':>4}  SpPwr")
    print("─" * 82)

    for rank, (sim, pat) in enumerate(results[:top_n], 1):
        col  = TYPE_COL.get(pat.get("pattern_type",""), Fore.WHITE)
        flag = f"{Fore.GREEN}✓" if sim >= similarity_threshold else f"{Fore.YELLOW}~"
        print(
            f"{flag} {col}{pat['id']:<6} {str(pat['first_seen_date']):<13} "
            f"{str(pat.get('pattern_type','?')):<16} {sim:>6.1f}%  "
            f"{pat['similar_count']:>6}  {pat['deviation_pct']:>6.2f}%  "
            f"{pat.get('open_close_chg_pct', 0):>+7.2f}%  "
            f"{'▲' if pat.get('supertrend_dir')==1 else '▼' if pat.get('supertrend_dir')==-1 else '—':>4}  "
            f"{pat.get('super_power') or '—'}{Style.RESET_ALL}"
        )

    # ── Trade confidence block ────────────────────────────────────
    top_matches = [p for sim, p in results[:top_n] if sim >= similarity_threshold]
    if top_matches:
        closes    = [p.get("open_close_chg_pct") or 0 for p in top_matches]
        bull      = [v for v in closes if v > 0]
        bear      = [v for v in closes if v < 0]
        n         = len(closes)
        direction = (
            f"{Fore.GREEN}📈 BULLISH BIAS ({len(bull)}/{n})" if len(bull) > len(bear)
            else f"{Fore.RED}📉 BEARISH BIAS ({len(bear)}/{n})" if len(bear) > len(bull)
            else f"{Fore.YELLOW}↔  NEUTRAL ({n}/{n})"
        )
        print(f"\n{'─'*65}")
        print(f"  Trade Confidence  (top {n} matched patterns)")
        print(f"  Bullish closes : {len(bull)}/{n}  avg {sum(bull)/len(bull):+.2f}%" if bull else f"  Bullish closes : 0/{n}")
        print(f"  Bearish closes : {len(bear)}/{n}  avg {sum(bear)/len(bear):+.2f}%" if bear else f"  Bearish closes : 0/{n}")
        print(f"\n  Bias  →  {direction}{Style.RESET_ALL}")
        print(f"{'─'*65}")
    else:
        print(f"\n{Fore.YELLOW}⚠ No patterns are ≥{similarity_threshold}% similar to today yet.{Style.RESET_ALL}")


# ─────────────────────────────────────────────────────────────────
#  BACKFILL
# ─────────────────────────────────────────────────────────────────

def run_backfill(conn, symbol: str, start_date: date, end_date: date,
                 similarity_threshold: float, min_deviation: float, verbose: bool):
    trading_dates = get_trading_dates(conn, symbol, start_date, end_date)
    if not trading_dates:
        print(f"{Fore.RED}No data for {symbol} in [{start_date}, {end_date}].{Style.RESET_ALL}")
        return

    existing = load_all_patterns(conn, symbol)
    print(f"\n  {symbol}: {len(trading_dates)} days  |  {len(existing)} existing patterns\n")

    stats = {"new": 0, "matched": 0, "skipped": 0}
    for trade_date in tqdm(trading_dates, desc=symbol, unit="day", leave=True):
        outcome, _ = process_day(
            conn, trade_date, symbol, existing,
            similarity_threshold, min_deviation, verbose
        )
        stats[outcome] += 1

    print(
        f"\n  {symbol}  ★new={Fore.GREEN}{stats['new']}{Style.RESET_ALL}  "
        f"✓matched={Fore.CYAN}{stats['matched']}{Style.RESET_ALL}  "
        f"⚠skipped={Fore.YELLOW}{stats['skipped']}{Style.RESET_ALL}  "
        f"total_patterns={len(existing)}"
    )


# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Nifty/BankNifty 1-min Pattern Recognition Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--symbol",               default="NIFTY",
                        help="NIFTY | BANKNIFTY | ALL  (default: NIFTY)")
    parser.add_argument("--start_date",           type=str,
                        help="Start date YYYY-MM-DD  (required for backfill)")
    parser.add_argument("--end_date",             type=str,
                        help="End date   YYYY-MM-DD  (required for backfill)")
    parser.add_argument("--mode",                 default="backfill",
                        choices=["backfill", "live"],
                        help="backfill = build library  |  live = compare today  (default: backfill)")
    parser.add_argument("--similarity_threshold", type=float, default=90.0,
                        help="Match threshold %%  (default 90.0)")
    parser.add_argument("--min_deviation",        type=float, default=15.0,
                        help=(
                            "Min shape complexity to store a pattern (default 15). "
                            "This is the std-dev of the normalised 0-100 curve — NOT raw price deviation. "
                            "Typical range: <10=flat, 10-20=mild, 20-30=active, 30+=high volatility. "
                            "The old default of 30 was incorrectly applied to raw price %% (0.5-3%% for Nifty), "
                            "which caused every day to be skipped."
                        ))
    parser.add_argument("--top_n",                type=int,   default=5,
                        help="Live mode: top N matches to display  (default 5)")
    parser.add_argument("--reset",                action="store_true",
                        help="Drop and recreate the pattern table  (DELETES ALL PATTERNS)")
    parser.add_argument("--quiet",                action="store_true",
                        help="Suppress per-day output (only show totals)")
    args = parser.parse_args()

    symbols = list(VALID_SYMBOLS) if args.symbol.upper() == "ALL" else [args.symbol.upper()]
    for s in symbols:
        if s not in VALID_SYMBOLS:
            print(f"{Fore.RED}Unknown symbol '{s}'. Use NIFTY, BANKNIFTY, or ALL.{Style.RESET_ALL}")
            sys.exit(1)

    print(f"\n{Fore.MAGENTA}{'═'*65}")
    print(f"   NIFTY PATTERN ENGINE")
    print(f"   mode={args.mode.upper()}  symbol={args.symbol.upper()}")
    print(f"   similarity≥{args.similarity_threshold}%   min_shape_complexity≥{args.min_deviation}")
    print(f"{'═'*65}{Style.RESET_ALL}\n")

    conn = get_connection()
    cfg  = get_db_config()
    print(f"{Fore.GREEN}✓ Connected  →  {cfg['database']} @ {cfg['host']}:{cfg['port']}{Style.RESET_ALL}\n")

    if args.reset:
        confirm = input(f"{Fore.RED}⚠  This will DELETE ALL stored patterns. Type YES to confirm: {Style.RESET_ALL}")
        if confirm.strip().upper() == "YES":
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {PATTERN_TABLE} CASCADE")
            conn.commit()
            print(f"{Fore.YELLOW}Table dropped.{Style.RESET_ALL}")

    ensure_pattern_table(conn)

    if args.mode == "live":
        for sym in symbols:
            live_compare(conn, sym, args.similarity_threshold, args.top_n)
    else:
        if not args.start_date or not args.end_date:
            parser.error("--start_date and --end_date are required for backfill mode.")
        sd = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(args.end_date,   "%Y-%m-%d").date()
        for sym in symbols:
            run_backfill(conn, sym, sd, ed,
                         args.similarity_threshold, args.min_deviation,
                         verbose=not args.quiet)

    print(f"\n{Fore.MAGENTA}{'═'*65}{Style.RESET_ALL}\n")
    conn.close()


if __name__ == "__main__":
    main()
