import traceback
from datetime import datetime, date, timedelta
from flask import render_template, request, jsonify, current_app
from sqlalchemy import func, and_
from kiteconnect import KiteConnect

from app.auth.decorators import login_required, admin_required, get_current_user
from app.models import (
    db, User, Instrument, OptionChainData, IndexData, UserTrade
)
from app.trade import trade_bp


# ---------------------------------------------------------------------------
# Helper: get current NIFTY spot from IndexData
# ---------------------------------------------------------------------------
def _get_spot(underlying='NIFTY', query_date=None):
    if query_date is None:
        query_date = date.today()
    latest_ts = db.session.query(func.max(IndexData.timestamp)).filter(
        IndexData.symbol == underlying,
        func.date(IndexData.timestamp) == query_date
    ).scalar()
    if latest_ts:
        row = IndexData.query.filter(
            IndexData.symbol == underlying,
            IndexData.timestamp == latest_ts
        ).first()
        if row:
            return round(row.close, 2)
    return 0


# ---------------------------------------------------------------------------
# Helper: get latest available fetch_date for instruments
# ---------------------------------------------------------------------------
def _latest_fetch_date(underlying='NIFTY'):
    """Return the most recent fetch_date in instruments table."""
    latest = db.session.query(func.max(Instrument.fetch_date)).filter(
        Instrument.name == underlying,
        Instrument.segment == 'NFO-OPT'
    ).scalar()
    return latest or date.today()


# ---------------------------------------------------------------------------
# Helper: calculate ITM strike (CE → below spot, PE → above spot)
# ---------------------------------------------------------------------------
def _itm_strike(spot, option_type, step=50):
    if option_type == 'CE':
        return int(spot // step) * step          # nearest 50 below
    else:
        return (int(spot // step) + 1) * step    # nearest 50 above


# ---------------------------------------------------------------------------
# Helper: get strikes list from Instruments for today
# ---------------------------------------------------------------------------
def _get_strikes(underlying='NIFTY', expiry=None):
    fetch_dt = _latest_fetch_date(underlying)
    filters = [
        Instrument.name == underlying,
        Instrument.segment == 'NFO-OPT',
        Instrument.fetch_date == fetch_dt,
    ]
    if expiry:
        filters.append(Instrument.expiry == expiry)
    strikes = db.session.query(Instrument.strike).filter(
        *filters
    ).distinct().order_by(Instrument.strike).all()
    return sorted(set(float(s[0]) for s in strikes if s[0]))


# ---------------------------------------------------------------------------
# Helper: get nearest weekly expiry
# ---------------------------------------------------------------------------
def _get_nearest_expiry(underlying='NIFTY'):
    today = date.today()
    fetch_dt = _latest_fetch_date(underlying)
    exp = db.session.query(func.min(Instrument.expiry)).filter(
        Instrument.name == underlying,
        Instrument.segment == 'NFO-OPT',
        Instrument.expiry >= today,
        Instrument.fetch_date == fetch_dt
    ).scalar()
    return exp


# ---------------------------------------------------------------------------
# Helper: OI analysis for a strike
# ---------------------------------------------------------------------------
def _get_oi_info(underlying, expiry, strike, option_type):
    """Return OI stats for a given strike: current OI, 5-min % change, avg, direction."""
    today = date.today()
    now = datetime.now()

    # Get latest two timestamps (current and ~5 min ago)
    rows = db.session.query(
        OptionChainData.timestamp,
        OptionChainData.ce_oi,
        OptionChainData.pe_oi,
        OptionChainData.ce_oi_change,
        OptionChainData.pe_oi_change,
        OptionChainData.ce_ltp,
        OptionChainData.pe_ltp,
    ).filter(
        OptionChainData.underlying == underlying,
        OptionChainData.expiry_date == expiry,
        OptionChainData.strike_price == strike,
        func.date(OptionChainData.timestamp) == today,
    ).order_by(OptionChainData.timestamp.desc()).limit(6).all()

    if not rows:
        return {
            'current_oi': 0, 'oi_change_pct': 0, 'avg_oi_change': 0,
            'direction': 'NO DATA', 'ltp': 0,
            'ce_oi_change_pct': 0, 'pe_oi_change_pct': 0
        }

    latest = rows[0]
    oi_field = 'ce_oi' if option_type == 'CE' else 'pe_oi'
    change_field = 'ce_oi_change' if option_type == 'CE' else 'pe_oi_change'
    ltp_field = 'ce_ltp' if option_type == 'CE' else 'pe_ltp'

    current_oi = getattr(latest, oi_field) or 0
    current_change = getattr(latest, change_field) or 0
    ltp = getattr(latest, ltp_field) or 0

    # Calculate 5-min OI % change
    five_min_ago = None
    if len(rows) >= 2:
        five_min_ago = rows[-1]  # oldest in window

    prev_oi = getattr(five_min_ago, oi_field) if five_min_ago else 0
    prev_oi = prev_oi or 0
    oi_change_pct = 0
    if prev_oi > 0:
        oi_change_pct = round(((current_oi - prev_oi) / prev_oi) * 100, 2)

    # Average OI change over last rows
    changes = [getattr(r, change_field) or 0 for r in rows]
    avg_oi = round(sum(changes) / len(changes), 0) if changes else 0

    # Price change for direction
    ltp_prev = getattr(five_min_ago, ltp_field) if five_min_ago else ltp
    ltp_prev = ltp_prev or ltp
    price_up = ltp >= ltp_prev

    # CE / PE OI change % for display
    ce_change_pct = 0
    pe_change_pct = 0
    if five_min_ago:
        ce_prev = five_min_ago.ce_oi or 0
        pe_prev = five_min_ago.pe_oi or 0
        if ce_prev: ce_change_pct = round(((latest.ce_oi or 0) - ce_prev) / ce_prev * 100, 2)
        if pe_prev: pe_change_pct = round(((latest.pe_oi or 0) - pe_prev) / pe_prev * 100, 2)

    # Direction logic
    oi_up = current_change > 0
    if oi_up and price_up:
        direction = 'LONG BUILDUP'
    elif oi_up and not price_up:
        direction = 'SHORT BUILDUP'
    elif not oi_up and price_up:
        direction = 'SHORT COVERING'
    else:
        direction = 'LONG UNWINDING'

    return {
        'current_oi': current_oi,
        'oi_change_pct': oi_change_pct,
        'avg_oi_change': avg_oi,
        'direction': direction,
        'ltp': ltp,
        'ce_oi_change_pct': ce_change_pct,
        'pe_oi_change_pct': pe_change_pct,
    }


# ---------------------------------------------------------------------------
# Helper: find Instrument object for a given selection
# ---------------------------------------------------------------------------
def _find_instrument(underlying, expiry, strike, option_type):
    """Find the Instrument row matching NIFTY <expiry> <strike> CE/PE."""
    fetch_dt = _latest_fetch_date(underlying)
    return Instrument.query.filter(
        Instrument.name == underlying,
        Instrument.expiry == expiry,
        Instrument.strike == strike,
        Instrument.instrument_type == option_type,
        Instrument.segment == 'NFO-OPT',
        Instrument.fetch_date == fetch_dt
    ).first()


# ===================================================================
# ROUTE: Manual Trade Page
# ===================================================================
@trade_bp.route('/manual')
@login_required
@admin_required
def manual_trade():
    underlying = 'NIFTY'
    spot = _get_spot(underlying)
    expiry = _get_nearest_expiry(underlying)

    # All expiries
    fetch_dt = _latest_fetch_date(underlying)
    expiries = db.session.query(Instrument.expiry).filter(
        Instrument.name == underlying,
        Instrument.segment == 'NFO-OPT',
        Instrument.expiry >= date.today(),
        Instrument.fetch_date == fetch_dt
    ).distinct().order_by(Instrument.expiry).all()
    expiries = [e[0] for e in expiries if e[0]]

    # Strikes
    strikes = _get_strikes(underlying, expiry)

    # ITM defaults — fallback to mid-strike if spot is unavailable
    effective_spot = spot
    if not effective_spot and strikes:
        effective_spot = strikes[len(strikes) // 2]
    itm_ce = _itm_strike(effective_spot, 'CE') if effective_spot else 0
    itm_pe = _itm_strike(effective_spot, 'PE') if effective_spot else 0

    # Open trades
    open_trades = UserTrade.query.filter(
        UserTrade.trade_status == 'OPEN',
        UserTrade.trade_date == date.today()
    ).order_by(UserTrade.entry_time.desc()).all()

    # Latest 3 closed trades
    closed_trades = UserTrade.query.filter(
        UserTrade.trade_status == 'CLOSED',
        UserTrade.trade_date == date.today()
    ).order_by(UserTrade.exit_time.desc()).limit(3).all()

    # Active users count
    active_users = User.query.filter(
        User.is_active == True,
        User.trade_mode.in_(['Live', 'Paper'])
    ).count()

    return render_template(
        'trade/manual_trade.html',
        spot=spot,
        underlying=underlying,
        expiry=str(expiry) if expiry else '',
        expiries=expiries,
        strikes=strikes,
        itm_ce=itm_ce,
        itm_pe=itm_pe,
        open_trades=open_trades,
        closed_trades=closed_trades,
        active_users=active_users,
    )


# ===================================================================
# API: OI Info (AJAX – auto refresh every 30s)
# ===================================================================
@trade_bp.route('/manual/oi-info')
@login_required
@admin_required
def oi_info():
    underlying = request.args.get('underlying', 'NIFTY')
    expiry = request.args.get('expiry', '')
    strike = request.args.get('strike', 0, type=float)
    option_type = request.args.get('option_type', 'CE')

    if not expiry or not strike:
        return jsonify({'error': 'Missing parameters'}), 400

    info = _get_oi_info(underlying, expiry, strike, option_type)
    info['spot'] = _get_spot(underlying)
    return jsonify(info)


# ===================================================================
# API: Get Strikes (AJAX – when expiry changes)
# ===================================================================
@trade_bp.route('/manual/strikes')
@login_required
@admin_required
def get_strikes():
    underlying = request.args.get('underlying', 'NIFTY')
    expiry = request.args.get('expiry', '')
    if not expiry:
        return jsonify([])
    strikes = _get_strikes(underlying, expiry)
    spot = _get_spot(underlying)
    itm_ce = _itm_strike(spot, 'CE') if spot else 0
    itm_pe = _itm_strike(spot, 'PE') if spot else 0
    return jsonify({'strikes': strikes, 'itm_ce': itm_ce, 'itm_pe': itm_pe})


# ===================================================================
# POST: BUY
# ===================================================================
@trade_bp.route('/manual/buy', methods=['POST'])
@login_required
@admin_required
def buy():
    try:
        data = request.get_json()
        underlying = data.get('underlying', 'NIFTY')
        expiry_str = data.get('expiry', '')
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', 'CE')

        if not expiry_str or not strike:
            return jsonify({'error': 'Missing strike or expiry'}), 400

        expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()

        # Check duplicate: any OPEN trade for this exact instrument?
        existing = UserTrade.query.filter(
            UserTrade.trade_status == 'OPEN',
            UserTrade.trade_symbol.isnot(None),
            UserTrade.strike_price == strike,
            UserTrade.option_type == option_type,
            UserTrade.expiry_date == expiry,
        ).first()
        if existing:
            return jsonify({'error': f'Duplicate! OPEN trade already exists for {option_type} {strike}'}), 400

        # Find instrument from DB
        instrument = _find_instrument(underlying, expiry, strike, option_type)
        if not instrument:
            return jsonify({'error': f'Instrument not found for {underlying} {expiry} {strike} {option_type}'}), 404

        exchange_lot = instrument.lot_size or 75  # fallback NIFTY lot
        trading_symbol = instrument.tradingsymbol
        instrument_token = instrument.instrument_token

        spot = _get_spot(underlying)
        oi_info = _get_oi_info(underlying, expiry, strike, option_type)

        # Get active users
        users = User.query.filter(
            User.is_active == True,
            User.trade_mode.in_(['Live', 'Paper'])
        ).all()

        if not users:
            return jsonify({'error': 'No active users found'}), 404

        results = {'total_users': len(users), 'live_orders': 0, 'paper_orders': 0, 'failed_orders': 0, 'errors': []}

        for user in users:
            try:
                quantity = exchange_lot * (user.lot_size or 1)
                entry_price = oi_info['ltp'] or 0
                entry_time = datetime.now()
                kite_order_id = None
                actual_price = entry_price

                if user.trade_mode == 'Live':
                    # LIVE: place real order via Kite
                    if not user.api_key or not user.access_token:
                        results['failed_orders'] += 1
                        results['errors'].append(f'{user.name}: No Kite credentials')
                        continue

                    try:
                        kite = KiteConnect(api_key=user.api_key)
                        kite.set_access_token(user.access_token)

                        order_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NFO,
                            tradingsymbol=trading_symbol,
                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                            quantity=quantity,
                            product=kite.PRODUCT_MIS,
                            order_type=kite.ORDER_TYPE_MARKET,
                        )
                        kite_order_id = str(order_id)

                        # Fetch order details for actual price
                        import time
                        time.sleep(0.5)
                        orders = kite.orders()
                        for o in orders:
                            if str(o.get('order_id')) == kite_order_id:
                                actual_price = o.get('average_price', entry_price) or entry_price
                                entry_time = o.get('order_timestamp', entry_time) or entry_time
                                break

                        results['live_orders'] += 1
                    except Exception as e:
                        results['failed_orders'] += 1
                        results['errors'].append(f'{user.name}: {str(e)}')
                        continue
                else:
                    # PAPER: simulate
                    results['paper_orders'] += 1

                trade = UserTrade(
                    trade_date=date.today(),
                    user_id=user.id,
                    nifty_price=spot,
                    trade_symbol=trading_symbol,
                    trade_instrument_token=instrument_token,
                    option_type=option_type,
                    strike_price=strike,
                    expiry_date=expiry,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    actual_entry_price=actual_price,
                    lot_size=exchange_lot,
                    quantity=quantity,
                    trade_type='BUY',
                    trade_status='OPEN',
                    trade_mode=user.trade_mode,
                    kite_order_id_entry=kite_order_id,
                    oi_trend=oi_info['direction'],
                    avg_oi_change_5min=oi_info['avg_oi_change'],
                    capital_used=quantity * entry_price,
                )
                db.session.add(trade)

            except Exception as e:
                results['failed_orders'] += 1
                results['errors'].append(f'{user.name}: {str(e)}')
                continue

        db.session.commit()
        return jsonify(results)

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'BUY error: {traceback.format_exc()}')
        return jsonify({'error': str(e)}), 500


# ===================================================================
# POST: SELL (Common Exit)
# ===================================================================
@trade_bp.route('/manual/sell', methods=['POST'])
@login_required
@admin_required
def sell():
    try:
        data = request.get_json()
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', 'CE')
        expiry_str = data.get('expiry', '')

        if not strike or not expiry_str:
            return jsonify({'error': 'Missing parameters'}), 400

        expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()

        open_trades = UserTrade.query.filter(
            UserTrade.trade_status == 'OPEN',
            UserTrade.strike_price == strike,
            UserTrade.option_type == option_type,
            UserTrade.expiry_date == expiry,
        ).all()

        if not open_trades:
            return jsonify({'error': 'No open trades found for this instrument'}), 404

        results = {'total': len(open_trades), 'closed': 0, 'failed': 0, 'errors': []}

        # Get current LTP
        underlying = 'NIFTY'
        oi_info = _get_oi_info(underlying, str(expiry), strike, option_type)
        current_ltp = oi_info['ltp'] or 0

        for trade in open_trades:
            try:
                user = User.query.get(trade.user_id)
                if not user:
                    results['failed'] += 1
                    results['errors'].append(f'User {trade.user_id} not found')
                    continue

                exit_price = current_ltp
                exit_time = datetime.now()
                kite_order_id_exit = None
                actual_exit = exit_price

                if trade.trade_mode == 'Live' and user.api_key and user.access_token:
                    try:
                        kite = KiteConnect(api_key=user.api_key)
                        kite.set_access_token(user.access_token)

                        order_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NFO,
                            tradingsymbol=trade.trade_symbol,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=trade.quantity,
                            product=kite.PRODUCT_MIS,
                            order_type=kite.ORDER_TYPE_MARKET,
                        )
                        kite_order_id_exit = str(order_id)

                        import time
                        time.sleep(0.5)
                        orders = kite.orders()
                        for o in orders:
                            if str(o.get('order_id')) == kite_order_id_exit:
                                actual_exit = o.get('average_price', exit_price) or exit_price
                                exit_time = o.get('order_timestamp', exit_time) or exit_time
                                break
                    except Exception as e:
                        results['failed'] += 1
                        results['errors'].append(f'{user.name}: {str(e)}')
                        continue

                # Calculate PnL
                closing_pnl = (exit_price - trade.entry_price) * trade.quantity
                pnl_pct = 0
                if trade.capital_used and trade.capital_used > 0:
                    pnl_pct = round((closing_pnl / trade.capital_used) * 100, 2)

                trade.exit_time = exit_time
                trade.exit_price = exit_price
                trade.actual_exit_price = actual_exit
                trade.trade_status = 'CLOSED'
                trade.closing_pnl = round(closing_pnl, 2)
                trade.pnl_percentage = pnl_pct
                trade.kite_order_id_exit = kite_order_id_exit
                trade.max_pnl = closing_pnl  # snapshot
                trade.min_pnl = closing_pnl  # snapshot

                results['closed'] += 1

            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f'Trade {trade.id}: {str(e)}')
                continue

        db.session.commit()
        return jsonify(results)

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'SELL error: {traceback.format_exc()}')
        return jsonify({'error': str(e)}), 500


# ===================================================================
# API: Orders list (AJAX – tabs / filters / pagination)
# ===================================================================
@trade_bp.route('/manual/orders')
@login_required
@admin_required
def orders():
    tab = request.args.get('tab', 'open')  # open / closed / all
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    user_id = request.args.get('user_id', '', type=str)
    option_type = request.args.get('option_type', '')
    trade_mode = request.args.get('trade_mode', '')
    page = request.args.get('page', 1, type=int)
    per_page = 20

    q = UserTrade.query

    if tab == 'open':
        q = q.filter(UserTrade.trade_status == 'OPEN')
    elif tab == 'closed':
        q = q.filter(UserTrade.trade_status == 'CLOSED')

    # Date filter
    if date_from:
        try:
            q = q.filter(UserTrade.trade_date >= datetime.strptime(date_from, '%Y-%m-%d').date())
        except ValueError:
            pass
    else:
        q = q.filter(UserTrade.trade_date == date.today())

    if date_to:
        try:
            q = q.filter(UserTrade.trade_date <= datetime.strptime(date_to, '%Y-%m-%d').date())
        except ValueError:
            pass

    if user_id:
        q = q.filter(UserTrade.user_id == int(user_id))
    if option_type:
        q = q.filter(UserTrade.option_type == option_type)
    if trade_mode:
        q = q.filter(UserTrade.trade_mode == trade_mode)

    q = q.order_by(UserTrade.entry_time.desc())
    pagination = q.paginate(page=page, per_page=per_page, error_out=False)
    trades = pagination.items

    data = []
    for t in trades:
        user = User.query.get(t.user_id)
        data.append({
            'id': t.id,
            'user': user.name if user else f'User#{t.user_id}',
            'symbol': t.trade_symbol,
            'option_type': t.option_type,
            'strike': t.strike_price,
            'entry_time': t.entry_time.strftime('%H:%M:%S') if t.entry_time else '',
            'entry_price': t.entry_price,
            'exit_time': t.exit_time.strftime('%H:%M:%S') if t.exit_time else '',
            'exit_price': t.exit_price,
            'qty': t.quantity,
            'mode': t.trade_mode,
            'oi_trend': t.oi_trend or '',
            'status': t.trade_status,
            'pnl': t.closing_pnl,
            'pnl_pct': t.pnl_percentage,
        })

    return jsonify({
        'orders': data,
        'page': pagination.page,
        'pages': pagination.pages,
        'total': pagination.total,
        'has_next': pagination.has_next,
        'has_prev': pagination.has_prev,
    })


# ===================================================================
# ROUTE: Options Trade Page
# ===================================================================
@trade_bp.route('/options')
@login_required
@admin_required
def options_trade():
    underlying = 'NIFTY'
    spot = _get_spot(underlying)
    expiry = _get_nearest_expiry(underlying)

    fetch_dt = _latest_fetch_date(underlying)
    expiries = db.session.query(Instrument.expiry).filter(
        Instrument.name == underlying,
        Instrument.segment == 'NFO-OPT',
        Instrument.expiry >= date.today(),
        Instrument.fetch_date == fetch_dt
    ).distinct().order_by(Instrument.expiry).all()
    expiries = [e[0] for e in expiries if e[0]]

    strikes = _get_strikes(underlying, expiry)

    effective_spot = spot
    if not effective_spot and strikes:
        effective_spot = strikes[len(strikes) // 2]
    itm_ce = _itm_strike(effective_spot, 'CE') if effective_spot else 0
    itm_pe = _itm_strike(effective_spot, 'PE') if effective_spot else 0

    # Get active client users
    client_users = User.query.filter(
        User.is_active == True,
        User.user_type == 'Client'
    ).all()

    # Check for any open trades today
    open_ce = UserTrade.query.filter(
        UserTrade.trade_status == 'OPEN',
        UserTrade.option_type == 'CE',
        UserTrade.trade_date == date.today()
    ).first()
    open_pe = UserTrade.query.filter(
        UserTrade.trade_status == 'OPEN',
        UserTrade.option_type == 'PE',
        UserTrade.trade_date == date.today()
    ).first()

    return render_template(
        'trade/options_trade.html',
        spot=spot,
        underlying=underlying,
        expiry=str(expiry) if expiry else '',
        expiries=expiries,
        strikes=strikes,
        itm_ce=itm_ce,
        itm_pe=itm_pe,
        client_users=client_users,
        has_open_ce=open_ce is not None,
        has_open_pe=open_pe is not None,
    )


# ===================================================================
# API: LTP History for Chart (per strike + option_type)
# ===================================================================
@trade_bp.route('/options/ltp')
@login_required
@admin_required
def options_ltp():
    """Return the last N LTP values for a given strike/type from option_chain_data."""
    underlying = request.args.get('underlying', 'NIFTY')
    expiry_str = request.args.get('expiry', '')
    strike = request.args.get('strike', 0, type=float)
    option_type = request.args.get('option_type', 'CE')
    limit = request.args.get('limit', 60, type=int)

    if not expiry_str or not strike:
        return jsonify({'error': 'Missing parameters'}), 400

    try:
        expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'Invalid expiry format'}), 400

    today = date.today()
    ltp_field = OptionChainData.ce_ltp if option_type == 'CE' else OptionChainData.pe_ltp

    rows = db.session.query(
        OptionChainData.timestamp, ltp_field
    ).filter(
        OptionChainData.underlying == underlying,
        OptionChainData.expiry_date == expiry,
        OptionChainData.strike_price == strike,
        func.date(OptionChainData.timestamp) == today,
        ltp_field.isnot(None),
        ltp_field > 0,
    ).order_by(OptionChainData.timestamp.desc()).limit(limit).all()

    rows = list(reversed(rows))
    data = [{'t': r[0].strftime('%H:%M:%S'), 'v': float(r[1])} for r in rows]

    # Also return latest OI/IV/Volume stats
    latest_ocd = OptionChainData.query.filter(
        OptionChainData.underlying == underlying,
        OptionChainData.expiry_date == expiry,
        OptionChainData.strike_price == strike,
        func.date(OptionChainData.timestamp) == today,
    ).order_by(OptionChainData.timestamp.desc()).first()

    stats = {}
    if latest_ocd:
        if option_type == 'CE':
            stats = {
                'ltp': latest_ocd.ce_ltp or 0,
                'oi': latest_ocd.ce_oi or 0,
                'oi_change': latest_ocd.ce_oi_change or 0,
                'volume': latest_ocd.ce_volume or 0,
                'change_pct': round(latest_ocd.ce_change_percent or 0, 2),
                'iv': latest_ocd.ce_iv or 0,
            }
        else:
            stats = {
                'ltp': latest_ocd.pe_ltp or 0,
                'oi': latest_ocd.pe_oi or 0,
                'oi_change': latest_ocd.pe_oi_change or 0,
                'volume': latest_ocd.pe_volume or 0,
                'change_pct': round(latest_ocd.pe_change_percent or 0, 2),
                'iv': latest_ocd.pe_iv or 0,
            }
    stats['spot'] = _get_spot(underlying)

    return jsonify({'history': data, 'stats': stats})


# ===================================================================
# POST: Options BUY (Client users only, per-user lot_size)
# ===================================================================
@trade_bp.route('/options/buy', methods=['POST'])
@login_required
@admin_required
def options_buy():
    try:
        data = request.get_json()
        underlying = data.get('underlying', 'NIFTY')
        expiry_str = data.get('expiry', '')
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', 'CE')

        if not expiry_str or not strike:
            return jsonify({'error': 'Missing strike or expiry'}), 400

        expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()

        # Prevent duplicate open trades for same strike/type
        existing = UserTrade.query.filter(
            UserTrade.trade_status == 'OPEN',
            UserTrade.strike_price == strike,
            UserTrade.option_type == option_type,
            UserTrade.expiry_date == expiry,
            UserTrade.trade_date == date.today(),
        ).first()
        if existing:
            return jsonify({'error': f'Already have an OPEN {option_type} trade at {strike}. Exit first.'}), 400

        instrument = _find_instrument(underlying, expiry, strike, option_type)
        if not instrument:
            return jsonify({'error': f'Instrument not found for {underlying} {expiry} {strike} {option_type}'}), 404

        exchange_lot = instrument.lot_size or 75
        trading_symbol = instrument.tradingsymbol
        instrument_token = instrument.instrument_token

        spot = _get_spot(underlying)
        oi_info = _get_oi_info(underlying, expiry, strike, option_type)

        # Only active Client users
        users = User.query.filter(
            User.is_active == True,
            User.user_type == 'Client'
        ).all()

        if not users:
            return jsonify({'error': 'No active client users found'}), 404

        results = {
            'total_users': len(users),
            'live_orders': 0, 'paper_orders': 0,
            'failed_orders': 0, 'errors': [],
            'trades': []
        }

        for user in users:
            try:
                quantity = exchange_lot * (user.lot_size or 1)
                entry_price = oi_info['ltp'] or 0
                entry_time = datetime.now()
                kite_order_id = None
                actual_price = entry_price

                if user.trade_mode == 'Live':
                    if not user.api_key or not user.access_token:
                        results['failed_orders'] += 1
                        results['errors'].append(f'{user.name}: No Kite credentials')
                        continue
                    try:
                        import time as _time
                        kite = KiteConnect(api_key=user.api_key)
                        kite.set_access_token(user.access_token)
                        order_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NFO,
                            tradingsymbol=trading_symbol,
                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                            quantity=quantity,
                            product=kite.PRODUCT_MIS,
                            order_type=kite.ORDER_TYPE_MARKET,
                        )
                        kite_order_id = str(order_id)
                        _time.sleep(0.5)
                        orders = kite.orders()
                        for o in orders:
                            if str(o.get('order_id')) == kite_order_id:
                                actual_price = o.get('average_price', entry_price) or entry_price
                                entry_time = o.get('order_timestamp', entry_time) or entry_time
                                break
                        results['live_orders'] += 1
                    except Exception as e:
                        results['failed_orders'] += 1
                        results['errors'].append(f'{user.name}: {str(e)}')
                        continue
                else:
                    results['paper_orders'] += 1

                trade = UserTrade(
                    trade_date=date.today(),
                    user_id=user.id,
                    nifty_price=spot,
                    trade_symbol=trading_symbol,
                    trade_instrument_token=instrument_token,
                    option_type=option_type,
                    strike_price=strike,
                    expiry_date=expiry,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    actual_entry_price=actual_price,
                    lot_size=exchange_lot,
                    quantity=quantity,
                    trade_type='BUY',
                    trade_status='OPEN',
                    trade_mode=user.trade_mode,
                    kite_order_id_entry=kite_order_id,
                    oi_trend=oi_info['direction'],
                    avg_oi_change_5min=oi_info['avg_oi_change'],
                    capital_used=quantity * entry_price,
                )
                db.session.add(trade)
                results['trades'].append({
                    'user': user.name,
                    'qty': quantity,
                    'price': actual_price,
                    'mode': user.trade_mode,
                })
            except Exception as e:
                results['failed_orders'] += 1
                results['errors'].append(f'{user.name}: {str(e)}')

        db.session.commit()
        return jsonify(results)

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'OPTIONS BUY error: {traceback.format_exc()}')
        return jsonify({'error': str(e)}), 500


# ===================================================================
# POST: Options SELL (Exit only – no direct short sell)
# ===================================================================
@trade_bp.route('/options/sell', methods=['POST'])
@login_required
@admin_required
def options_sell():
    try:
        data = request.get_json()
        strike = float(data.get('strike', 0))
        option_type = data.get('option_type', 'CE')
        expiry_str = data.get('expiry', '')

        if not strike or not expiry_str:
            return jsonify({'error': 'Missing parameters'}), 400

        expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()

        open_trades = UserTrade.query.filter(
            UserTrade.trade_status == 'OPEN',
            UserTrade.strike_price == strike,
            UserTrade.option_type == option_type,
            UserTrade.expiry_date == expiry,
            UserTrade.trade_date == date.today(),
        ).all()

        if not open_trades:
            return jsonify({'error': 'No open trades found for this instrument today'}), 404

        underlying = 'NIFTY'
        oi_info = _get_oi_info(underlying, str(expiry), strike, option_type)
        current_ltp = oi_info['ltp'] or 0

        results = {'total': len(open_trades), 'closed': 0, 'failed': 0, 'errors': [], 'trades': []}

        for trade in open_trades:
            try:
                user = User.query.get(trade.user_id)
                if not user:
                    results['failed'] += 1
                    results['errors'].append(f'User {trade.user_id} not found')
                    continue

                exit_price = current_ltp
                exit_time = datetime.now()
                kite_order_id_exit = None
                actual_exit = exit_price

                if trade.trade_mode == 'Live' and user.api_key and user.access_token:
                    try:
                        import time as _time
                        kite = KiteConnect(api_key=user.api_key)
                        kite.set_access_token(user.access_token)
                        order_id = kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=kite.EXCHANGE_NFO,
                            tradingsymbol=trade.trade_symbol,
                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                            quantity=trade.quantity,
                            product=kite.PRODUCT_MIS,
                            order_type=kite.ORDER_TYPE_MARKET,
                        )
                        kite_order_id_exit = str(order_id)
                        _time.sleep(0.5)
                        orders = kite.orders()
                        for o in orders:
                            if str(o.get('order_id')) == kite_order_id_exit:
                                actual_exit = o.get('average_price', exit_price) or exit_price
                                exit_time = o.get('order_timestamp', exit_time) or exit_time
                                break
                    except Exception as e:
                        results['failed'] += 1
                        results['errors'].append(f'{user.name}: {str(e)}')
                        continue

                closing_pnl = (exit_price - trade.entry_price) * trade.quantity
                pnl_pct = 0
                if trade.capital_used and trade.capital_used > 0:
                    pnl_pct = round((closing_pnl / trade.capital_used) * 100, 2)
                points = round(exit_price - trade.entry_price, 2)

                trade.exit_time = exit_time
                trade.exit_price = exit_price
                trade.actual_exit_price = actual_exit
                trade.trade_status = 'CLOSED'
                trade.closing_pnl = round(closing_pnl, 2)
                trade.pnl_percentage = pnl_pct
                trade.kite_order_id_exit = kite_order_id_exit
                trade.max_pnl = closing_pnl
                trade.min_pnl = closing_pnl

                results['closed'] += 1
                results['trades'].append({
                    'user': user.name,
                    'entry': trade.entry_price,
                    'exit': exit_price,
                    'points': points,
                    'pnl': round(closing_pnl, 2),
                    'qty': trade.quantity,
                })
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f'Trade {trade.id}: {str(e)}')

        db.session.commit()
        return jsonify(results)

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'OPTIONS SELL error: {traceback.format_exc()}')
        return jsonify({'error': str(e)}), 500


# ===================================================================
# API: Kite Orders for Client 2
# ===================================================================
@trade_bp.route('/options/kite-orders')
@login_required
@admin_required
def options_kite_orders():
    """Fetch actual Kite order book for the designated client (user_type=Client, id lowest or 2nd)."""
    # Find 2nd client user (2nd smallest id among clients)
    clients = User.query.filter(
        User.user_type == 'Client',
        User.is_active == True,
    ).order_by(User.id).all()

    if not clients:
        return jsonify({'orders': [], 'user': None})

    # Use 2nd client if exists, else 1st
    target = clients[1] if len(clients) > 1 else clients[0]

    if not target.api_key or not target.access_token:
        return jsonify({'orders': [], 'user': target.name, 'error': 'No Kite credentials'})

    try:
        kite = KiteConnect(api_key=target.api_key)
        kite.set_access_token(target.access_token)
        raw_orders = kite.orders()

        orders = []
        for o in raw_orders:
            orders.append({
                'order_id': o.get('order_id', ''),
                'symbol': o.get('tradingsymbol', ''),
                'type': o.get('transaction_type', ''),
                'qty': o.get('quantity', 0),
                'price': o.get('average_price') or o.get('price', 0),
                'status': o.get('status', ''),
                'time': str(o.get('order_timestamp', '')),
                'product': o.get('product', ''),
            })
        # Latest first
        orders = list(reversed(orders))
        return jsonify({'orders': orders, 'user': target.name})
    except Exception as e:
        current_app.logger.error(f'Kite orders fetch error: {e}')
        return jsonify({'orders': [], 'user': target.name, 'error': str(e)})


# ===================================================================
# API: Running PNL for open trades today
# ===================================================================
@trade_bp.route('/options/pnl')
@login_required
@admin_required
def options_pnl():
    """Return live PNL for all open trades today, using latest LTP from option_chain_data."""
    open_trades = UserTrade.query.filter(
        UserTrade.trade_status == 'OPEN',
        UserTrade.trade_date == date.today(),
    ).all()

    if not open_trades:
        return jsonify({'trades': [], 'total_pnl': 0, 'ce_pnl': 0, 'pe_pnl': 0})

    today = date.today()
    user_pnl = {}
    ce_total = 0
    pe_total = 0

    for trade in open_trades:
        user = User.query.get(trade.user_id)
        user_name = user.name if user else f'User#{trade.user_id}'

        # Get latest LTP from option_chain_data
        ltp_field = OptionChainData.ce_ltp if trade.option_type == 'CE' else OptionChainData.pe_ltp
        latest = db.session.query(ltp_field).filter(
            OptionChainData.underlying == 'NIFTY',
            OptionChainData.expiry_date == trade.expiry_date,
            OptionChainData.strike_price == trade.strike_price,
            func.date(OptionChainData.timestamp) == today,
            ltp_field.isnot(None),
            ltp_field > 0,
        ).order_by(OptionChainData.timestamp.desc()).first()

        current_ltp = float(latest[0]) if latest and latest[0] else trade.entry_price
        pnl = round((current_ltp - trade.entry_price) * trade.quantity, 2)
        points = round(current_ltp - trade.entry_price, 2)

        key = f"{user_name}_{trade.option_type}"
        user_pnl[key] = {
            'user': user_name,
            'option_type': trade.option_type,
            'strike': trade.strike_price,
            'symbol': trade.trade_symbol,
            'entry': trade.entry_price,
            'ltp': current_ltp,
            'points': points,
            'qty': trade.quantity,
            'pnl': pnl,
            'trade_id': trade.id,
        }

        if trade.option_type == 'CE':
            ce_total += pnl
        else:
            pe_total += pnl

    trades_list = list(user_pnl.values())
    total_pnl = round(sum(t['pnl'] for t in trades_list), 2)

    return jsonify({
        'trades': trades_list,
        'total_pnl': total_pnl,
        'ce_pnl': round(ce_total, 2),
        'pe_pnl': round(pe_total, 2),
        'timestamp': datetime.now().strftime('%H:%M:%S'),
    })


# ===================================================================
# API: Today's closed trades with points per user
# ===================================================================
@trade_bp.route('/options/trades-today')
@login_required
@admin_required
def options_trades_today():
    """Return all CLOSED trades today with points gained/lost per user."""
    closed_trades = UserTrade.query.filter(
        UserTrade.trade_status == 'CLOSED',
        UserTrade.trade_date == date.today(),
    ).order_by(UserTrade.exit_time.desc()).all()

    data = []
    for t in closed_trades:
        user = User.query.get(t.user_id)
        user_name = user.name if user else f'User#{t.user_id}'
        points = round((t.exit_price - t.entry_price), 2) if t.exit_price and t.entry_price else 0
        data.append({
            'id': t.id,
            'user': user_name,
            'symbol': t.trade_symbol,
            'option_type': t.option_type,
            'strike': t.strike_price,
            'entry_time': t.entry_time.strftime('%H:%M:%S') if t.entry_time else '',
            'exit_time': t.exit_time.strftime('%H:%M:%S') if t.exit_time else '',
            'entry_price': t.entry_price,
            'exit_price': t.exit_price,
            'points': points,
            'qty': t.quantity,
            'pnl': t.closing_pnl,
            'mode': t.trade_mode,
        })

    return jsonify({'trades': data, 'count': len(data)})
