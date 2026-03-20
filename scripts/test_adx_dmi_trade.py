"""
NIFTY Options Backtest Script
==============================
Replays 1-min NIFTY candles bar-by-bar for a given date using the
exact same strategy logic as adx_dmi_auto_trade.py.

Option prices are sourced from option_chain_data (ce_ltp / pe_ltp).

Usage:
    python test_adx_dmi_trade.py
    python test_adx_dmi_trade.py --date=2026-02-03
    python test_adx_dmi_trade.py --date=2026-02-03 --adx=25
    python test_adx_dmi_trade.py --date=2026-02-03 --adx=25 --lots=2

Defaults:
    --date : today
    --adx  : 18
    --lots : 1
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ── env ────────────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://power_app:vijayPowerAIAPP@localhost:5432/power_app'
)

# ── strategy constants  (mirrors adx_dmi_auto_trade.py) ────────────────────────
ADX_PERIOD         = 14
DEFAULT_ADX_THRESH = 18.0
MAX_TRADES_PER_DAY = 3
MARKET_OPEN        = dtime(9, 15)
MARKET_CLOSE       = dtime(15, 15)
DEFAULT_LOT_SIZE   = 65

_W = 110      # display width
SEP  = "─" * _W
SEP2 = "═" * _W


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NIFTY Options Backtest — replays index_data bar-by-bar"
    )
    p.add_argument('--date', default=str(date.today()),
                   help='Backtest date YYYY-MM-DD  (default: today)')
    p.add_argument('--adx', type=float, default=DEFAULT_ADX_THRESH,
                   help=f'ADX threshold            (default: {DEFAULT_ADX_THRESH})')
    p.add_argument('--lots', type=int, default=1,
                   help='Number of lots per trade  (default: 1)')
    return p.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
#  Data helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_day_candles(engine, bt_date: date) -> pd.DataFrame:
    """
    All NIFTY 1-min candles from (bt_date - 5 days) up to 15:15 on bt_date.
    The extra days provide ADX/DI warmup history.
    Returns DataFrame sorted oldest-first, indexed by timestamp.
    """
    warmup_from = datetime.combine(bt_date - timedelta(days=5), dtime(8, 0))
    day_end     = datetime.combine(bt_date, MARKET_CLOSE)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT timestamp, open, high, low, close,
                   ma_20, ma_200, adx
            FROM   index_data
            WHERE  symbol    = 'NIFTY'
              AND  timestamp >= :from_ts
              AND  timestamp <= :to_ts
            ORDER  BY timestamp ASC
        """), conn, params={'from_ts': warmup_from, 'to_ts': day_end})
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.set_index('timestamp')


def get_option_ltp(engine, bt_date: date, option_type: str,
                   strike: float, at_time: datetime) -> float | None:
    """
    Gets the nearest option LTP from option_chain_data at or before `at_time`.
    Falls back to last_price from instruments table.
    Returns None if nothing found.
    """
    col = 'ce_ltp' if option_type == 'CE' else 'pe_ltp'
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT {col}
            FROM   option_chain_data
            WHERE  strike_price       = :strike
              AND  DATE(timestamp)   = :dt
              AND  timestamp         <= :ts
            ORDER  BY timestamp DESC
            LIMIT  1
        """), {'strike': float(strike), 'dt': bt_date, 'ts': at_time}).fetchone()

    if row and row[0] and float(row[0]) > 0:
        return float(row[0])

    # Fallback: instruments table last_price
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT last_price FROM instruments
            WHERE  name            = 'NIFTY'
              AND  instrument_type = :otype
              AND  strike          = :strike
              AND  exchange        = 'NFO'
              AND  expiry          > :dt
            ORDER  BY expiry ASC
            LIMIT  1
        """), {'otype': option_type, 'strike': float(strike), 'dt': bt_date}).fetchone()

    return float(row[0]) if row and row[0] else None


def get_lot_size(engine, bt_date: date, option_type: str, strike: float) -> int:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT lot_size FROM instruments
            WHERE  name            = 'NIFTY'
              AND  instrument_type = :otype
              AND  strike          = :strike
              AND  exchange        = 'NFO'
              AND  expiry          > :dt
            ORDER  BY expiry ASC LIMIT 1
        """), {'otype': option_type, 'strike': float(strike), 'dt': bt_date}).fetchone()
    return int(row[0]) if row and row[0] else DEFAULT_LOT_SIZE


# ══════════════════════════════════════════════════════════════════════════════
#  Indicators  — identical to live script
# ══════════════════════════════════════════════════════════════════════════════

def _wilder(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def calc_dmi(df: pd.DataFrame, period: int = ADX_PERIOD) -> dict:
    h, l, c = df['high'], df['low'], df['close']
    pc  = c.shift(1)
    tr  = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    up  = h - h.shift(1)
    dn  = l.shift(1) - l
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    spdm = _wilder(pdm, period)
    smdm = _wilder(mdm, period)
    str_ = _wilder(tr,  period)
    pdi  = 100.0 * spdm / str_
    mdi  = 100.0 * smdm / str_
    return {'plus_di': float(pdi.iloc[-1]), 'minus_di': float(mdi.iloc[-1])}


def detect_signal(window: pd.DataFrame, adx_thresh: float) -> dict:
    """ADX from DB stored column; DI+/DI- computed locally."""
    result = dict(signal=None, adx=0.0, plus_di=0.0, minus_di=0.0,
                  ma20=0.0, ma200=0.0, close=0.0)
    if len(window) < ADX_PERIOD + 5:
        return result
    row   = window.iloc[-1]
    ma20  = float(row['ma_20'])  if pd.notna(row['ma_20'])  else 0.0
    ma200 = float(row['ma_200']) if pd.notna(row['ma_200']) else 0.0
    close = float(row['close'])
    adx   = float(row['adx'])   if pd.notna(row['adx'])    else 0.0
    dmi   = calc_dmi(window, ADX_PERIOD)
    pdi, mdi = dmi['plus_di'], dmi['minus_di']
    result.update(adx=adx, plus_di=pdi, minus_di=mdi,
                  ma20=ma20, ma200=ma200, close=close)
    if adx > adx_thresh:
        if pdi > mdi and ma20 > ma200:
            result['signal'] = 'BUY'
        elif mdi > pdi and ma20 < ma200:
            result['signal'] = 'SELL'
    return result


def strike_for(signal: str, close: float) -> float:
    return (math.floor(close / 100) * 100 if signal == 'BUY'
            else math.ceil(close / 100) * 100)


# ══════════════════════════════════════════════════════════════════════════════
#  Trade record
# ══════════════════════════════════════════════════════════════════════════════

class Trade:
    def __init__(self, num: int, signal: str, opt_type: str, strike: float,
                 open_time: str, open_nifty: float, open_price: float,
                 lot_size: int, lots: int):
        self.num        = num
        self.signal     = signal
        self.opt_type   = opt_type
        self.strike     = strike
        self.open_time  = open_time
        self.open_nifty = open_nifty
        self.open_price = open_price    # option buy price
        self.lot_size   = lot_size
        self.lots       = lots
        self.quantity   = lot_size * lots
        # filled at close
        self.close_time  = '—'
        self.close_nifty = 0.0
        self.close_price = 0.0         # option exit price
        self.reason      = ''
        # running trackers
        self.max_ltp     = open_price
        self.min_ltp     = open_price
        # computed at close
        self.opt_points  = 0.0        # exit_price - entry_price
        self.nifty_pts   = 0.0        # directional nifty movement
        self.pnl         = 0.0
        self.max_pnl     = 0.0
        self.min_pnl     = 0.0
        self.pnl_pct     = 0.0

    def update_ltp(self, ltp: float) -> None:
        self.max_ltp = max(self.max_ltp, ltp)
        self.min_ltp = min(self.min_ltp, ltp)

    def close(self, close_time: str, close_nifty: float,
              close_price: float, reason: str) -> None:
        self.close_time  = close_time
        self.close_nifty = close_nifty
        self.close_price = close_price
        self.reason      = reason
        self.opt_points  = close_price - self.open_price
        self.pnl         = self.opt_points * self.quantity
        self.max_pnl     = (self.max_ltp - self.open_price) * self.quantity
        self.min_pnl     = (self.min_ltp - self.open_price) * self.quantity
        capital          = self.open_price * self.quantity
        self.pnl_pct     = (self.pnl / capital * 100) if capital else 0.0
        self.nifty_pts   = (close_nifty - self.open_nifty
                            if self.signal == 'BUY'
                            else self.open_nifty - close_nifty)


# ══════════════════════════════════════════════════════════════════════════════
#  Backtest engine
# ══════════════════════════════════════════════════════════════════════════════

def run_backtest(engine, bt_date: date,
                 adx_thresh: float, lots: int) -> tuple[list[Trade], list[dict]]:
    print(f"\n{SEP2}")
    print(f"  NIFTY OPTIONS BACKTEST")
    print(f"  Date: {bt_date} ({bt_date.strftime('%A')})   "
          f"ADX Threshold: {adx_thresh}   Lots per trade: {lots}")
    print(SEP2)

    all_df = fetch_day_candles(engine, bt_date)
    if all_df.empty:
        print("  ERROR: No NIFTY candle data found for this date range.")
        return [], []

    # Bars for bt_date in market hours only
    day_mask = (
        (all_df.index.date == bt_date) &
        (all_df.index.time >= MARKET_OPEN) &
        (all_df.index.time <= MARKET_CLOSE)
    )
    day_bars   = all_df[day_mask]

    if day_bars.empty:
        print("  ERROR: No intraday bars found for this date.")
        return [], []

    total_bars = len(day_bars)
    print(f"\n  Candles: {total_bars}  "
          f"({day_bars.index[0].strftime('%H:%M')} → "
          f"{day_bars.index[-1].strftime('%H:%M')})\n")
    print(f"  {'Time':>5}  {'Nifty':>9}  {'ADX':>6}  {'DI+':>6}  {'DI-':>6}  "
          f"{'MA20':>9}  {'MA200':>9}  Bias  Signal  Action")
    print(SEP)

    trades:    list[Trade]  = []
    sig_log:   list[dict]   = []
    open_tr:   Trade | None = None
    trade_no:  int          = 0

    for i, (ts, _) in enumerate(day_bars.iterrows()):
        window     = all_df.loc[:ts]
        sig        = detect_signal(window, adx_thresh)
        adx        = sig['adx']
        signal     = sig['signal']
        close      = sig['close']
        bias       = 'BULL' if sig['ma20'] > sig['ma200'] else 'BEAR'
        is_last    = (i == total_bars - 1) or (ts.time() >= MARKET_CLOSE)
        action_str = ''

        # ── Force close at 15:15 ───────────────────────────────────────
        if is_last and open_tr:
            ltp = get_option_ltp(engine, bt_date, open_tr.opt_type,
                                 open_tr.strike, ts)
            ltp = ltp or open_tr.open_price
            open_tr.close(ts.strftime('%H:%M'), close, ltp, '15:15 squareoff')
            trades.append(open_tr)
            action_str = f"CLOSED (15:15)  {open_tr.opt_type} exit@{ltp:.2f}  PnL={open_tr.pnl:+.2f}"
            open_tr = None

        # ── ADX drop → close ───────────────────────────────────────────
        elif open_tr and adx < adx_thresh:
            ltp = get_option_ltp(engine, bt_date, open_tr.opt_type,
                                 open_tr.strike, ts)
            ltp = ltp or open_tr.open_price
            open_tr.close(ts.strftime('%H:%M'), close, ltp,
                          f'ADX drop ({adx:.1f})')
            trades.append(open_tr)
            action_str = f"CLOSED (ADX={adx:.1f})  {open_tr.opt_type} exit@{ltp:.2f}  PnL={open_tr.pnl:+.2f}"
            open_tr = None

        expected_opt = ('CE' if signal == 'BUY' else 'PE') if signal else None

        # ── Running PnL tracker ────────────────────────────────────────
        if open_tr:
            ltp_cur = get_option_ltp(engine, bt_date, open_tr.opt_type,
                                     open_tr.strike, ts)
            if ltp_cur:
                open_tr.update_ltp(ltp_cur)

        # ── Reversal close ─────────────────────────────────────────────
        if signal and open_tr and open_tr.opt_type != expected_opt:
            ltp = get_option_ltp(engine, bt_date, open_tr.opt_type,
                                 open_tr.strike, ts)
            ltp = ltp or open_tr.open_price
            open_tr.close(ts.strftime('%H:%M'), close, ltp,
                          f'reversal → {signal}')
            trades.append(open_tr)
            action_str = (f"CLOSED (reversal→{signal})  "
                          f"{open_tr.opt_type} exit@{ltp:.2f}  PnL={open_tr.pnl:+.2f}")
            open_tr = None

        # ── Hold same direction ────────────────────────────────────────
        elif signal and open_tr and open_tr.opt_type == expected_opt:
            action_str = f"HOLDING {open_tr.opt_type}@{open_tr.strike:.0f}"

        # ── Open new trade ─────────────────────────────────────────────
        elif signal and not open_tr and trade_no < MAX_TRADES_PER_DAY:
            strike  = strike_for(signal, close)
            opt_ltp = get_option_ltp(engine, bt_date, expected_opt, strike, ts)
            if opt_ltp is not None:
                lot_sz  = get_lot_size(engine, bt_date, expected_opt, strike)
                trade_no += 1
                open_tr  = Trade(trade_no, signal, expected_opt, strike,
                                 ts.strftime('%H:%M'), close, opt_ltp,
                                 lot_sz, lots)
                sig_log.append(dict(
                    no=trade_no, time=ts.strftime('%H:%M'), signal=signal,
                    close=close, adx=adx, pdi=sig['plus_di'],
                    mdi=sig['minus_di'], ma20=sig['ma20'], ma200=sig['ma200'],
                    bias=bias, strike=strike, opt=expected_opt, ltp=opt_ltp,
                ))
                action_str = (f"OPENED #{trade_no} BUY {expected_opt} "
                              f"strike={strike:.0f}  entry={opt_ltp:.2f}  "
                              f"qty={open_tr.quantity}")
            else:
                action_str = f"SIGNAL {signal} — no option price for {expected_opt}@{strike:.0f}"

        elif signal and not open_tr and trade_no >= MAX_TRADES_PER_DAY:
            action_str = f"SIGNAL {signal} — max {MAX_TRADES_PER_DAY} trades reached"

        # Print bar line (only when there's something interesting)
        if action_str or signal:
            print(f"  {ts.strftime('%H:%M'):>5}  {close:>9.2f}  "
                  f"{adx:>6.2f}  {sig['plus_di']:>6.2f}  {sig['minus_di']:>6.2f}  "
                  f"{sig['ma20']:>9.2f}  {sig['ma200']:>9.2f}  "
                  f"{bias:<4}  {signal or '—':>6}  {action_str}")

    return trades, sig_log


# ══════════════════════════════════════════════════════════════════════════════
#  Report
# ══════════════════════════════════════════════════════════════════════════════

def print_report(trades: list[Trade], sig_log: list[dict],
                 bt_date: date, adx_thresh: float) -> None:

    # ── Signal summary ─────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  SIGNALS THAT GENERATED TRADES")
    print(SEP2)
    if sig_log:
        print(f"  {'#':>2}  {'Time':>5}  {'Signal':>5}  {'Nifty':>9}  "
              f"{'ADX':>7}  {'DI+':>7}  {'DI-':>7}  "
              f"{'MA20':>9}  {'MA200':>9}  Bias  "
              f"{'Strike':>7}  Opt  {'Opt LTP':>8}")
        print(SEP)
        for s in sig_log:
            print(f"  {s['no']:>2}  {s['time']:>5}  {s['signal']:>5}  "
                  f"{s['close']:>9.2f}  {s['adx']:>7.2f}  "
                  f"{s['pdi']:>7.2f}  {s['mdi']:>7.2f}  "
                  f"{s['ma20']:>9.2f}  {s['ma200']:>9.2f}  "
                  f"{s['bias']:<4}  {s['strike']:>7.0f}  "
                  f"{s['opt']:>3}  {s['ltp']:>8.2f}")
    else:
        print("  No trades were triggered.")

    # ── Trade detail ───────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  TRADE DETAIL")
    print(SEP2)

    if not trades:
        print("  No trades executed.")
    else:
        hdr = (
            f"  {'#':>2}  {'Opt':>3}  {'Strike':>7}  "
            f"{'Open':>5}  {'NiftyIn':>9}  {'BuyPx':>8}  "
            f"{'Close':>5}  {'NiftyOut':>9}  {'SellPx':>8}  "
            f"{'OptPts':>7}  {'NiftyPts':>8}  "
            f"{'Qty':>4}  {'PnL':>10}  {'PnL%':>6}  "
            f"{'MaxPnL':>10}  {'MinPnL':>10}  Reason"
        )
        print(hdr)
        print(SEP)
        for t in trades:
            print(
                f"  {t.num:>2}  {t.opt_type:>3}  {t.strike:>7.0f}  "
                f"{t.open_time:>5}  {t.open_nifty:>9.2f}  {t.open_price:>8.2f}  "
                f"{t.close_time:>5}  {t.close_nifty:>9.2f}  {t.close_price:>8.2f}  "
                f"{t.opt_points:>+7.2f}  {t.nifty_pts:>+8.2f}  "
                f"{t.quantity:>4}  {t.pnl:>+10.2f}  {t.pnl_pct:>+6.2f}%  "
                f"{t.max_pnl:>+10.2f}  {t.min_pnl:>+10.2f}  "
                f"{t.reason}"
            )

    # ══════════════════════════════════════════════════════════════════════════
    # END-OF-DAY SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    total_pnl       = sum(t.pnl        for t in trades)
    total_capital   = sum(t.open_price * t.quantity for t in trades)
    total_opt_pts   = sum(t.opt_points for t in trades)
    total_nifty_pts = sum(t.nifty_pts  for t in trades)
    winners  = [t for t in trades if t.pnl > 0]
    losers   = [t for t in trades if t.pnl <= 0]
    best     = max(trades, key=lambda t: t.pnl,     default=None)
    worst    = min(trades, key=lambda t: t.pnl,     default=None)
    win_rate = len(winners) / len(trades) * 100 if trades else 0.0

    def pnl_tag(v: float) -> str:
        return "PROFIT" if v > 0 else ("LOSS" if v < 0 else "FLAT")

    print(f"\n{SEP2}")
    print(f"  ★  END-OF-DAY SUMMARY  ★")
    print(SEP2)
    print(f"  Date        : {bt_date}  ({bt_date.strftime('%A')})")
    print(f"  Strategy    : NIFTY Options  |  ADX>{adx_thresh}  |  MA20/MA200 direction filter")
    print(f"  Max trades  : {MAX_TRADES_PER_DAY}/day   Lots/trade: —")
    print(SEP)

    print(f"\n  TRADE SCORECARD  ({len(trades)} trade{'s' if len(trades)!=1 else ''})")
    print()
    if not trades:
        print("  No trades were executed today.")
    else:
        for t in trades:
            cap       = t.open_price * t.quantity
            opt_hi    = t.open_price + t.max_pnl / t.quantity if t.quantity else t.open_price
            opt_lo    = t.open_price + t.min_pnl / t.quantity if t.quantity else t.open_price
            tag       = pnl_tag(t.pnl)
            bar_full  = 20
            filled    = int(abs(t.pnl_pct) / 5)        # 5% → 1 block
            filled    = min(filled, bar_full)
            bar       = ("█" * filled).ljust(bar_full)
            sign      = "+" if t.pnl >= 0 else "-"

            print(f"  ┌─ Trade #{t.num}  {t.signal} → {t.opt_type}  Strike {t.strike:.0f} ─────────────────────────────────────────")
            print(f"  │  Entry  : {t.open_time}  Nifty={t.open_nifty:.2f}   Option buy  price = {t.open_price:.2f}")
            print(f"  │  Exit   : {t.close_time}  Nifty={t.close_nifty:.2f}   Option sell price = {t.close_price:.2f}   [{t.reason}]")
            print(f"  │  Qty    : {t.quantity} ({t.lots} lot × {t.lot_size})   Capital = ₹{cap:,.2f}")
            print(f"  │")
            print(f"  │  Option points  earned : {t.opt_points:>+8.2f}  pts")
            print(f"  │  Nifty  points  moved  : {t.nifty_pts:>+8.2f}  pts  ({'direction correct' if (t.signal=='BUY' and t.nifty_pts>0) or (t.signal=='SELL' and t.nifty_pts>0) else 'direction wrong'})")
            print(f"  │")
            print(f"  │  PnL            : ₹{t.pnl:>+10,.2f}  ({t.pnl_pct:>+.2f}%)   [{tag}]")
            print(f"  │  Max PnL (peak) : ₹{t.max_pnl:>+10,.2f}   (option high = {opt_hi:.2f})")
            print(f"  │  Min PnL (trough): ₹{t.min_pnl:>+10,.2f}  (option low  = {opt_lo:.2f})")
            print(f"  │  [{sign}{bar}] {abs(t.pnl_pct):.1f}%")
            print(f"  └{'─'*88}")
            print()

    print(SEP)
    print(f"\n  DAY TOTALS")
    print()
    print(f"  Signals fired       : {len(sig_log)}")
    print(f"  Trades taken        : {len(trades)}  /  {MAX_TRADES_PER_DAY}  max")
    print(f"  Winners             : {len(winners)}   Losers: {len(losers)}   Win rate: {win_rate:.1f}%")
    print()
    print(f"  Total option pts    : {total_opt_pts:>+10.2f}  pts")
    print(f"  Total Nifty pts     : {total_nifty_pts:>+10.2f}  pts")
    print()
    print(f"  Total PnL           : ₹{total_pnl:>+12,.2f}   [{pnl_tag(total_pnl)}]")
    print(f"  Total capital used  : ₹{total_capital:>12,.2f}")
    if total_capital:
        print(f"  Return on capital   :   {total_pnl/total_capital*100:>+8.2f}%")
    print()
    if best and worst and len(trades) > 1:
        print(f"  Best  trade  : #{best.num}  {best.opt_type}@{best.strike:.0f}  "
              f"PnL=₹{best.pnl:+,.2f}  ({best.opt_points:+.2f} opt pts)")
        print(f"  Worst trade  : #{worst.num}  {worst.opt_type}@{worst.strike:.0f}  "
              f"PnL=₹{worst.pnl:+,.2f}  ({worst.opt_points:+.2f} opt pts)")
        print()
    print(SEP2)



# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()

    try:
        bt_date = date.fromisoformat(args.date)
    except ValueError:
        print(f"ERROR: Invalid date '{args.date}'. Use YYYY-MM-DD format.")
        sys.exit(1)

    adx_thresh = float(args.adx)
    lots       = max(1, int(args.lots))

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        print(f"ERROR: Cannot connect to database: {exc}")
        sys.exit(1)

    trades, sig_log = run_backtest(engine, bt_date, adx_thresh, lots)
    print_report(trades, sig_log, bt_date, adx_thresh)


if __name__ == '__main__':
    main()
