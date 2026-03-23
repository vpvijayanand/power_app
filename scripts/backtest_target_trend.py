"""
backtest_target_trend.py
========================
Python backtest for the "Target Trend [Vijay]" PineScript indicator.

PineScript Logic Replicated:
  • atr_value = SMA(ATR(200), 200) * 0.8
  • sma_high  = SMA(high, length) + atr_value
  • sma_low   = SMA(low, length)  - atr_value
  • trend = True  when close crosses over  sma_high  (bull trend)
  • trend = False when close crosses under sma_low   (bear trend)
  • Stop loss   = sma_low (long) or sma_high (short)
  • Targets     = close ± atr * (5, 10, 15) at entry bar

Usage:
  python backtest_target_trend.py \\
      --start_date 2025-01-01 --end_date 2025-12-31 \\
      --length 10 --target 0 --lot_size 75 --diagnostics
"""

import os
import sys
import argparse
import math
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime

import pandas as pd
from sqlalchemy import create_engine

# ── same pattern as stream_option_chain_new.py ────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config import config

def get_db_engine():
    db_config = config['development']
    return create_engine(db_config.SQLALCHEMY_DATABASE_URI)

FORCE_CLOSE = dtime(15, 15)

# ─── ANSI colours ─────────────────────────────────────────────────────────────
R="\033[0m"; B="\033[1m"; DIM="\033[2m"
CY="\033[96m"; YL="\033[93m"; MG="\033[95m"; GN="\033[92m"; RD="\033[91m"
def gc(v): return GN if v > 0 else (RD if v < 0 else "")
def pct(n, d): return f"{n/d*100:.1f}%" if d else "—"
W = 88

# ─── Database ─────────────────────────────────────────────────────────────────
def fetch_candles(engine, symbol, start_date, end_date):
    """Fetch all candles for the backtest period via SQLAlchemy + pandas."""
    sql = """
        SELECT timestamp, open, high, low, close
        FROM   index_data
        WHERE  symbol = %(symbol)s
          AND  DATE(timestamp) BETWEEN %(start)s AND %(end)s
          AND  CAST(timestamp AS TIME) BETWEEN '09:15:00' AND '15:30:00'
        ORDER  BY timestamp
    """
    df = pd.read_sql(sql, engine, params={"symbol": symbol, "start": start_date, "end": end_date})
    # Convert to list of dicts for the rest of the logic
    rows = df.to_dict("records")
    return rows

def group_by_day(candles):
    days = defaultdict(list)
    for c in candles:
        days[c["timestamp"].date()].append(c)
    return dict(sorted(days.items()))


# ─── Indicator helpers (PineScript replicas) ──────────────────────────────────

def sma(series, period):
    """Simple moving average over a list, NaN until enough bars."""
    result = [None] * len(series)
    for i in range(period - 1, len(series)):
        result[i] = sum(series[i - period + 1: i + 1]) / period
    return result

def atr_series(high_s, low_s, close_s, period):
    """Wilder/true-range ATR series."""
    trs = [None] * len(high_s)
    for i in range(len(high_s)):
        h, l, pc = high_s[i], low_s[i], close_s[i - 1] if i > 0 else close_s[i]
        trs[i] = max(h - l, abs(h - pc), abs(l - pc))
    return trs

def calc_indicators(all_candles, length=10, atr_period=200, atr_sma_period=200, atr_mult=0.8):
    """
    Compute sma_high, sma_low, trend, trend_value for all candles.
    Mimics PineScript:
        atr_value = ta.sma(ta.atr(atr_period), atr_sma_period) * mult
        sma_high  = ta.sma(high, length) + atr_value
        sma_low   = ta.sma(low, length)  - atr_value
    """
    highs  = [c["high"]  for c in all_candles]
    lows   = [c["low"]   for c in all_candles]
    closes = [c["close"] for c in all_candles]

    trs      = atr_series(highs, lows, closes, atr_period)
    atr_val  = sma(trs, atr_sma_period)          # SMA of ATR
    sma_h_raw = sma(highs, length)
    sma_l_raw = sma(lows,  length)

    indicators = []
    trend = None  # starts as NA

    for i, c in enumerate(all_candles):
        av = atr_val[i]
        sh = sma_h_raw[i]
        sl = sma_l_raw[i]

        if av is None or sh is None or sl is None:
            indicators.append({
                "sma_high": None, "sma_low": None,
                "atr_value": None, "trend": None,
                "trend_value": None
            })
            continue

        av_scaled = av * atr_mult
        sma_high  = sh + av_scaled
        sma_low   = sl - av_scaled

        prev_close = closes[i - 1] if i > 0 else c["close"]

        # Crossover: close > sma_high (today AND prev_close <= prev_sma_high)
        prev_ind = indicators[i - 1] if i > 0 else None
        prev_sh  = prev_ind["sma_high"] if prev_ind else None
        prev_sl  = prev_ind["sma_low"]  if prev_ind else None

        if prev_sh is not None and prev_sl is not None:
            if c["close"] > sma_high and prev_close <= prev_sh:
                trend = True   # crossover → bull
            elif c["close"] < sma_low and prev_close >= prev_sl:
                trend = False  # crossunder → bear

        trend_value = sma_low if trend is True else (sma_high if trend is False else None)

        indicators.append({
            "sma_high":    sma_high,
            "sma_low":     sma_low,
            "atr_value":   av_scaled,
            "trend":       trend,
            "trend_value": trend_value,
        })

    return indicators


# ─── Single-day backtest ──────────────────────────────────────────────────────

def backtest_day(day_candles, day_indicators, args):
    """
    Replicate PineScript signal_up / signal_down entry logic:
      signal_up   = ta.change(trend) and not trend[1]   → new bull bar
      signal_down = ta.change(trend) and trend[1]        → new bear bar

    Entry: next bar's open after signal.
    Stop : sma_low (long) or sma_high (short).
    Targets: close ± atr * (5+t, 10+2t, 15+3t)  where t=args.target
    Exit priority: T1 → T2 → T3 → EOD at 15:15
    """
    trades = []
    position   = None   # None | "LONG" | "SHORT"
    entry_info = {}
    force_close = datetime.combine(day_candles[0]["timestamp"].date(), FORCE_CLOSE)

    prev_trend = None  # NA

    for i, (candle, ind) in enumerate(zip(day_candles, day_indicators)):
        ts = candle["timestamp"]
        if ts > force_close and position is None:
            break

        cur_trend = ind["trend"]

        # ── Detect signal on PREVIOUS bar (PineScript evaluates at bar close) ──
        # We enter on this bar's OPEN

        if position is not None:
            stop  = entry_info["stop"]
            t1    = entry_info["t1"]
            t2    = entry_info["t2"]
            t3    = entry_info["t3"]
            direction = entry_info["direction"]

            hit_t3 = hit_t2 = hit_stop = False

            if direction == "LONG":
                if candle["high"] >= t3:
                    close_trade(trades, ts, t3, "target_3"); position = None
                elif candle["high"] >= t2:
                    close_trade(trades, ts, t2, "target_2"); position = None
                elif candle["high"] >= t1:
                    close_trade(trades, ts, t1, "target_1"); position = None
                elif candle["low"] <= stop:
                    close_trade(trades, ts, stop, "stop_loss"); position = None
                elif ts >= force_close:
                    close_trade(trades, ts, candle["close"], "eod_hold"); position = None
            else:  # SHORT
                if candle["low"] <= t3:
                    close_trade(trades, ts, t3, "target_3"); position = None
                elif candle["low"] <= t2:
                    close_trade(trades, ts, t2, "target_2"); position = None
                elif candle["low"] <= t1:
                    close_trade(trades, ts, t1, "target_1"); position = None
                elif candle["high"] >= stop:
                    close_trade(trades, ts, stop, "stop_loss"); position = None
                elif ts >= force_close:
                    close_trade(trades, ts, candle["close"], "eod_hold"); position = None

        # ── Check for signal change on this bar → enter on NEXT bar ──
        if position is None and i + 1 < len(day_candles):
            next_candle = day_candles[i + 1]
            next_ts     = next_candle["timestamp"]
            if next_ts > force_close:
                prev_trend = cur_trend
                continue

            atr = ind["atr_value"]
            if atr is None:
                prev_trend = cur_trend
                continue

            # signal_up: trend changed TO True (was False or None)
            if cur_trend is True and prev_trend is not True:
                direction = "LONG"
                entry_price = next_candle["open"]
                stop_price  = ind["sma_low"]   # PineScript: base = sma_low

                t_add = args.target
                t1 = entry_price + atr * (5  + t_add)
                t2 = entry_price + atr * (10 + t_add * 2)
                t3 = entry_price + atr * (15 + t_add * 3)

                entry_info = {
                    "direction": direction,
                    "stop": stop_price,
                    "t1": t1, "t2": t2, "t3": t3,
                    "atr": atr,
                }
                trades.append({
                    "direction":   direction,
                    "entry_time":  next_ts,
                    "entry_price": entry_price,
                    "stop":        round(stop_price, 2),
                    "t1": round(t1, 2), "t2": round(t2, 2), "t3": round(t3, 2),
                    "exit_time":   None,
                    "exit_price":  None,
                    "exit_reason": None,
                    "pnl_pts":     None,
                })
                position = "LONG"

            # signal_down: trend changed TO False
            elif cur_trend is False and prev_trend is not False:
                direction = "SHORT"
                entry_price = next_candle["open"]
                stop_price  = ind["sma_high"]

                t_add = args.target
                t1 = entry_price - atr * (5  + t_add)
                t2 = entry_price - atr * (10 + t_add * 2)
                t3 = entry_price - atr * (15 + t_add * 3)

                entry_info = {
                    "direction": direction,
                    "stop": stop_price,
                    "t1": t1, "t2": t2, "t3": t3,
                    "atr": atr,
                }
                trades.append({
                    "direction":   direction,
                    "entry_time":  next_ts,
                    "entry_price": entry_price,
                    "stop":        round(stop_price, 2),
                    "t1": round(t1, 2), "t2": round(t2, 2), "t3": round(t3, 2),
                    "exit_time":   None,
                    "exit_price":  None,
                    "exit_reason": None,
                    "pnl_pts":     None,
                })
                position = "SHORT"

        prev_trend = cur_trend

    # Force close any open position
    if position is not None and trades and trades[-1]["exit_time"] is None:
        last_c = day_candles[-1]
        close_trade(trades, force_close, last_c["close"], "eod_hold")

    return trades


def close_trade(trades, ts, price, reason):
    t = trades[-1]
    t["exit_time"]   = ts
    t["exit_price"]  = round(price, 2)
    t["exit_reason"] = reason
    if t["direction"] == "LONG":
        t["pnl_pts"] = round(price - t["entry_price"], 2)
    else:
        t["pnl_pts"] = round(t["entry_price"] - price, 2)


# ─── Reporting ────────────────────────────────────────────────────────────────

def hr(ch="─", w=None): return ch * (w or W)

def print_header(args, n_candles, n_days):
    print(f"\n{'═'*W}")
    print(f"  {B}Target Trend [Vijay] — Backtest{R}")
    print(f"{'═'*W}")
    print(f"  Symbol     : {args.symbol}")
    print(f"  Period     : {args.start_date}  →  {args.end_date}  "
          f"({n_days} trading days  |  {n_candles:,} candles)")
    print(f"  Length     : {args.length}   Target add-on: {args.target}")
    print(f"  ATR        : SMA(ATR({args.atr_period}), {args.atr_sma}) × {args.atr_mult}")
    print(f"  Lot size   : {args.lot_size}")
    print(f"{'═'*W}\n")
    print(f"  {'Date':<12} {'#':<3} {'Dir':<6} {'Entry':>8} {'Stop':>8} "
          f"{'T1':>8} {'T3':>8} {'Exit':>8} {'PnL':>9} {'Reason':<16} {'₹':>12}")
    print(f"  {hr('─', W-2)}")

def print_day(day, trades, lot_size):
    for t in trades:
        if t["pnl_pts"] is None: continue
        col = gc(t["pnl_pts"])
        inr = t["pnl_pts"] * lot_size
        et  = t["entry_time"].strftime("%H:%M")
        xt  = t["exit_time"].strftime("%H:%M") if t["exit_time"] else "--:--"
        print(f"  {str(day):<12} {'1':<3} {t['direction']:<6} "
              f"{t['entry_price']:>8.2f} {t['stop']:>8.2f} "
              f"{t['t1']:>8.2f} {t['t3']:>8.2f} "
              f"{(t['exit_price'] or 0):>8.2f} "
              f"{col}{t['pnl_pts']:>+9.2f}{R} "
              f"{DIM}{(t['exit_reason'] or ''):<16}{R} "
              f"{col}{inr:>+12,.2f}{R}")

def print_summary(all_trades, daily_results, args):
    closed = [t for t in all_trades if t.get("pnl_pts") is not None]
    wins   = [t for t in closed if t["pnl_pts"] > 0]
    losses = [t for t in closed if t["pnl_pts"] < 0]
    total_pts = sum(t["pnl_pts"] for t in closed)
    total_inr = total_pts * args.lot_size
    n = len(closed)

    by_reason = defaultdict(list)
    for t in closed:
        by_reason[t["exit_reason"]].append(t["pnl_pts"])

    print(f"\n{'═'*W}")
    print(f"  {B}SUMMARY{R}")
    print(f"{'═'*W}")
    print(f"  Total trades          : {n}")
    print(f"  Wins / Losses         : {len(wins)} / {len(losses)}  ({pct(len(wins), n)})")
    print(f"  Avg win  (pts)        : {sum(t['pnl_pts'] for t in wins)/len(wins):>+.2f}"  if wins  else "  Avg win               : —")
    print(f"  Avg loss (pts)        : {sum(t['pnl_pts'] for t in losses)/len(losses):>+.2f}" if losses else "  Avg loss              : —")
    print(f"  Total P&L  (pts)      : {gc(total_pts)}{B}{total_pts:>+.2f}{R}")
    print(f"  Total P&L  (₹)        : {gc(total_inr)}{B}₹{total_inr:>+,.2f}{R}")
    print()
    print(f"  ── By exit reason ───────────────────────────────────────────")
    for reason, pts_list in sorted(by_reason.items()):
        tot = sum(pts_list)
        w   = sum(1 for p in pts_list if p > 0)
        print(f"  {reason:<18}:  n={len(pts_list):>4}  W={w:>4}  "
              f"{gc(tot)}{tot:>+10.2f} pts{R}  ₹{tot*args.lot_size:>+12,.2f}")
    print(f"\n{'═'*W}\n")


# ─── Diagnostics ──────────────────────────────────────────────────────────────

def diag_entry_hour(all_trades):
    print(f"\n  {B}── Entry hour breakdown ──────────────────────────────────────────{R}")
    by_h = defaultdict(list)
    for t in all_trades:
        if t.get("pnl_pts") is None: continue
        by_h[t["entry_time"].hour].append(t["pnl_pts"])
    print(f"  {'Hour':<9} {'Trades':>7} {'Wins':>6} {'WinRate':>9} {'AvgPts':>9} {'Total':>10}")
    for h in sorted(by_h):
        pts  = by_h[h]
        wins = sum(1 for p in pts if p > 0)
        tot  = sum(pts)
        avg  = tot / len(pts)
        col  = gc(tot)
        print(f"  {h:02d}:xx     {len(pts):>7} {wins:>6} {pct(wins,len(pts)):>9} "
              f"{col}{avg:>+9.2f}{R} {col}{tot:>+10.2f}{R}")

def diag_dow(daily_results):
    print(f"\n  {B}── Day-of-week breakdown ─────────────────────────────────────────{R}")
    names = ["Mon","Tue","Wed","Thu","Fri"]
    by_d  = defaultdict(list)
    for d in daily_results:
        by_d[d["date"].weekday()].append(d)
    print(f"  {'Day':<5} {'Days':>6} {'Wins':>6} {'WinRate':>9} {'AvgPts':>10} {'Total':>10}")
    for i, nm in enumerate(names):
        days = by_d.get(i, [])
        if not days: continue
        prof = sum(1 for d in days if d["pts"] > 0)
        tot  = sum(d["pts"] for d in days)
        avg  = tot / len(days)
        col  = gc(tot)
        print(f"  {nm:<5} {len(days):>6} {prof:>6} {pct(prof,len(days)):>9} "
              f"{col}{avg:>+10.2f}{R} {col}{tot:>+10.2f}{R}")

def diag_target_analysis(all_trades):
    print(f"\n  {B}── Target hit analysis ───────────────────────────────────────────{R}")
    for reason in ["target_1","target_2","target_3","stop_loss","eod_hold"]:
        ts = [t for t in all_trades if t.get("exit_reason") == reason and t.get("pnl_pts") is not None]
        if not ts: continue
        tot = sum(t["pnl_pts"] for t in ts)
        col = gc(tot)
        print(f"  {reason:<14}: n={len(ts):>4}  total={col}{tot:>+10.2f} pts{R}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Target Trend [Vijay] — Python Backtest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    p.add_argument("--start_date",  required=True)
    p.add_argument("--end_date",    required=True)
    p.add_argument("--symbol",      default="NIFTY")
    p.add_argument("--length",      type=int,   default=10,  help="SMA length (PineScript: length)")
    p.add_argument("--target",      type=int,   default=0,   help="Target add-on (PineScript: target)")
    p.add_argument("--atr_period",  type=int,   default=200, help="ATR period")
    p.add_argument("--atr_sma",     type=int,   default=200, help="SMA period for ATR smoothing")
    p.add_argument("--atr_mult",    type=float, default=0.8, help="ATR multiplier (PineScript: 0.8)")
    p.add_argument("--lot_size",    type=int,   default=75)
    p.add_argument("--diagnostics", action="store_true", default=False)
    args = p.parse_args()

    # ── Connect via app.config (same as stream_option_chain_new.py) ────────────
    try:
        engine = get_db_engine()
    except Exception as e:
        print(f"[ERROR] DB engine init failed: {e}"); sys.exit(1)

    print(f"Fetching candles for {args.symbol} {args.start_date} → {args.end_date} …")
    try:
        candles = fetch_candles(engine, args.symbol, args.start_date, args.end_date)
    except Exception as e:
        print(f"[ERROR] Data fetch failed: {e}"); sys.exit(1)

    if not candles:
        print("No data found. Check symbol and date range."); sys.exit(0)

    day_map = group_by_day(candles)
    n_days  = len(day_map)

    print(f"Computing indicators (ATR-{args.atr_period} SMA-{args.atr_sma} × {args.atr_mult}) …")

    # ── Compute indicators across ALL candles (important: cross-day warm-up) ──
    all_indicators = calc_indicators(
        candles,
        length=args.length,
        atr_period=args.atr_period,
        atr_sma_period=args.atr_sma,
        atr_mult=args.atr_mult,
    )

    # Build per-day indicator slices (indices track into all_indicators)
    day_ind_map = {}
    idx = 0
    for day, dc in day_map.items():
        day_ind_map[day] = all_indicators[idx: idx + len(dc)]
        idx += len(dc)

    print_header(args, len(candles), n_days)

    # ── Daily loop ─────────────────────────────────────────────────────────────
    all_trades    = []
    daily_results = []

    for day, dc in day_map.items():
        inds   = day_ind_map[day]
        trades = backtest_day(dc, inds, args)
        print_day(day, trades, args.lot_size)

        day_pts = sum(t["pnl_pts"] for t in trades if t["pnl_pts"] is not None)
        all_trades.extend(trades)
        daily_results.append({"date": day, "pts": day_pts, "inr": day_pts * args.lot_size})

    print(f"\n  {hr('─', W-2)}")
    print_summary(all_trades, daily_results, args)

    if args.diagnostics:
        print(f"\n{B}{MG}{'═'*W}")
        print(f"  DIAGNOSTICS")
        print(f"{'═'*W}{R}")
        diag_target_analysis(all_trades)
        diag_entry_hour(all_trades)
        diag_dow(daily_results)
        print(f"\n{B}{CY}{'═'*W}{R}\n")


if __name__ == "__main__":
    main()
