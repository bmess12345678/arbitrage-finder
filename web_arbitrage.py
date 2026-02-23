"""
Premium Arbitrage Finder - Web Version
Properly devigs sharp books. Shows any +EV bet after juice.
Shows gross and net edge. Comprehensive near-miss logging.
"""

from flask import Flask, render_template, jsonify
import requests
import time
from datetime import datetime
import threading
import os

app = Flask(__name__)

API_KEY = "19c83d930cc9b8bfcd3da28458f38d76"

state = {
    'opportunities': [],
    'last_scan': None,
    'scanning': False,
    'debug_info': []
}

# ============================================================
# MARKETS
# ============================================================

# Game markets via bulk /odds endpoint (all sports)
GAME_LEVEL_MARKETS = [
    ('basketball_ncaab', 'spreads', 'NCAAB Spreads'),
    ('basketball_ncaab', 'totals', 'NCAAB Totals'),
    ('basketball_ncaab', 'h2h', 'NCAAB Moneyline'),
    ('basketball_nba', 'spreads', 'NBA Spreads'),
    ('basketball_nba', 'totals', 'NBA Totals'),
    ('basketball_nba', 'h2h', 'NBA Moneyline'),
    ('icehockey_nhl', 'spreads', 'NHL Puckline'),
    ('icehockey_nhl', 'totals', 'NHL Totals'),
    ('icehockey_nhl', 'h2h', 'NHL Moneyline'),
]

BOOKMAKERS = ['fanduel', 'espnbet', 'draftkings', 'betmgm', 'williamhill_us']
SHARP_BOOKS = ['draftkings', 'betmgm', 'williamhill_us']
TARGET_BOOKS = ['fanduel', 'espnbet']
BOOK_DISPLAY = {
    'fanduel': 'FanDuel', 'espnbet': 'ESPN Bet',
    'draftkings': 'DraftKings', 'betmgm': 'BetMGM', 'williamhill_us': 'Caesars'
}

# Show any bet with positive net edge
MIN_EDGE_NET = 0.1


def log_debug(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    state['debug_info'].append(f"[{ts}] {msg}")
    print(f"[{ts}] {msg}")


# ============================================================
# ODDS MATH
# ============================================================

def american_to_implied(odds):
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)

def devig_pair(prob_a, prob_b):
    total = prob_a + prob_b
    if total <= 0:
        return (0.5, 0.5)
    return (prob_a / total, prob_b / total)

def format_american(odds):
    rounded = int(round(odds))
    return f"+{rounded}" if rounded > 0 else str(rounded)

def implied_to_american(prob):
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return -(prob / (1 - prob)) * 100
    else:
        return ((1 - prob) / prob) * 100


# ============================================================
# FETCH FUNCTIONS
# ============================================================

def fetch_odds(sport, market):
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        'apiKey': API_KEY, 'regions': 'us', 'markets': market,
        'bookmakers': ','.join(BOOKMAKERS), 'oddsFormat': 'american'
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            remaining = response.headers.get('x-requests-remaining', '?')
            used = response.headers.get('x-requests-used', '?')
            log_debug(f"  {sport}/{market}: {len(data)} games (used: {used}, left: {remaining})")
            return data
        else:
            # Log the actual error message
            try:
                err_body = response.json() if response.text else {}
                err_msg = err_body.get('message', response.text[:120])
            except:
                err_msg = response.text[:120]
            log_debug(f"  {sport}/{market}: HTTP {response.status_code} - {err_msg}")
    except Exception as e:
        log_debug(f"  {sport}/{market}: Error - {str(e)[:80]}")
    return None

def fetch_event_odds(sport, event_id, market):
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events/{event_id}/odds"
    params = {
        'apiKey': API_KEY, 'regions': 'us', 'markets': market,
        'bookmakers': ','.join(BOOKMAKERS), 'oddsFormat': 'american'
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            log_debug(f"    Event {event_id[:8]}../{market}: 401 - API key issue")
            return None
    except Exception as e:
        log_debug(f"    Event fetch error: {str(e)[:60]}")
    return None


def fetch_events(sport):
    """Get list of upcoming events for any sport"""
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
    params = {'apiKey': API_KEY}
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            events = response.json()
            log_debug(f"  {sport}: {len(events)} upcoming events")
            return events
        else:
            try:
                err_msg = response.json().get('message', '')
            except:
                err_msg = response.text[:80]
            log_debug(f"  {sport} events: HTTP {response.status_code} - {err_msg}")
    except Exception as e:
        log_debug(f"  {sport} events: Error - {str(e)[:60]}")
    return []


# ============================================================
# PLAYER PROP ANALYSIS
#
# Strategy: Try devig (both sides) first for accurate net edge.
# Fall back to line-comparison for players missing one side.
# ============================================================

def analyze_player_props(games_data, market_name=""):
    if not games_data:
        return []

    opportunities = []
    near_misses = []
    stats = {'players': 0, 'with_sharp': 0, 'with_target': 0, 'devigged': 0, 'line_compared': 0}

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Collect player data: {player: {book: {line, over_odds, under_odds}}}
        players = {}
        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    player = outcome.get('description', '')
                    if not player:
                        continue
                    line = outcome.get('point')
                    odds = outcome.get('price')
                    side = outcome.get('name', '').lower()
                    if line is None or odds is None:
                        continue

                    if player not in players:
                        players[player] = {'game': game_info, 'books': {}}
                    if book_name not in players[player]['books']:
                        players[player]['books'][book_name] = {'line': line}

                    if 'over' in side:
                        players[player]['books'][book_name]['over_odds'] = odds
                    elif 'under' in side:
                        players[player]['books'][book_name]['under_odds'] = odds
                    players[player]['books'][book_name]['line'] = line

        for player, data in players.items():
            stats['players'] += 1
            books = data['books']

            # Sharp books with at least a line
            sharp_available = [sb for sb in SHARP_BOOKS if sb in books]
            if len(sharp_available) < 2:
                continue
            stats['with_sharp'] += 1

            # Target books available
            targets_available = [tb for tb in TARGET_BOOKS if tb in books]
            if not targets_available:
                continue
            stats['with_target'] += 1

            # Try DEVIG approach: sharp books with both sides
            sharp_with_both = [sb for sb in sharp_available
                               if 'over_odds' in books[sb] and 'under_odds' in books[sb]]

            for target in targets_available:
                tb = books[target]
                t_line = tb['line']
                has_both_sides = 'over_odds' in tb and 'under_odds' in tb

                if len(sharp_with_both) >= 2 and has_both_sides:
                    # === FULL DEVIG (both sides available) ===
                    stats['devigged'] += 1

                    # Devig each sharp book
                    sharp_fair_overs = []
                    sharp_lines = []
                    for sb in sharp_with_both:
                        bk = books[sb]
                        ov = american_to_implied(bk['over_odds'])
                        un = american_to_implied(bk['under_odds'])
                        fo, fu = devig_pair(ov, un)
                        sharp_fair_overs.append(fo)
                        sharp_lines.append(bk['line'])

                    consensus_fair_over = sum(sharp_fair_overs) / len(sharp_fair_overs)
                    consensus_line = sum(sharp_lines) / len(sharp_lines)

                    t_over_imp = american_to_implied(tb['over_odds'])
                    t_under_imp = american_to_implied(tb['under_odds'])
                    t_juice_pct = round((t_over_imp + t_under_imp - 1.0) * 100, 1)

                    # Devig target for gross edge
                    t_fair_over, t_fair_under = devig_pair(t_over_imp, t_under_imp)

                    same_line = abs(t_line - consensus_line) < 0.25

                    if same_line:
                        # Same line: compare probabilities directly
                        for side, target_p, t_fair_p, fair_p, odds in [
                            ('OVER', t_over_imp, t_fair_over, consensus_fair_over, tb['over_odds']),
                            ('UNDER', t_under_imp, t_fair_under, 1.0 - consensus_fair_over, tb['under_odds']),
                        ]:
                            net_edge = (fair_p - target_p) * 100
                            gross_edge = (fair_p - t_fair_p) * 100

                            if net_edge >= MIN_EDGE_NET:
                                fair_odds = implied_to_american(fair_p)
                                opportunities.append({
                                    'player': player, 'game': data['game'],
                                    'market': market_name,
                                    'book': BOOK_DISPLAY.get(target, target),
                                    'type': 'player_prop',
                                    'edge': round(net_edge, 1),
                                    'gross_edge': round(gross_edge, 1),
                                    'recommendation': f"{side} {t_line}",
                                    'odds': odds,
                                    'label1_name': f'{BOOK_DISPLAY.get(target, target)} Odds',
                                    'label1_value': format_american(odds),
                                    'label2_name': 'Fair Odds (no vig)',
                                    'label2_value': format_american(fair_odds),
                                    'label3_name': 'Net Edge',
                                    'label3_value': f"+{net_edge:.1f}%",
                                    'target_prob': round(target_p * 100, 1),
                                    'fair_prob': round(fair_p * 100, 1),
                                    'juice_display': f"{t_juice_pct}%",
                                })
                            elif net_edge > -3:
                                near_misses.append((round(net_edge, 1), player,
                                    BOOK_DISPLAY.get(target, target), side, t_line))
                    else:
                        # Different line: use line diff minus actual juice
                        diff = t_line - consensus_line
                        line_edge = abs(diff / consensus_line * 100) if consensus_line != 0 else 0
                        net_edge = line_edge - (t_juice_pct / 2.0)

                        if net_edge >= MIN_EDGE_NET and abs(diff) >= 0.5:
                            side = 'UNDER' if diff > 0 else 'OVER'
                            odds = tb.get('under_odds' if diff > 0 else 'over_odds', 0)
                            opportunities.append({
                                'player': player, 'game': data['game'],
                                'market': market_name,
                                'book': BOOK_DISPLAY.get(target, target),
                                'type': 'player_prop',
                                'edge': round(net_edge, 1),
                                'gross_edge': round(line_edge, 1),
                                'recommendation': f"{side} {t_line}",
                                'odds': odds,
                                'label1_name': f'{BOOK_DISPLAY.get(target, target)} Line',
                                'label1_value': str(t_line),
                                'label2_name': 'Sharp Consensus',
                                'label2_value': str(round(consensus_line, 1)),
                                'label3_name': 'Net Edge',
                                'label3_value': f"+{net_edge:.1f}%",
                                'juice_display': f"{t_juice_pct}%",
                            })
                        elif net_edge > -3 and abs(diff) >= 0.5:
                            side = 'UNDER' if diff > 0 else 'OVER'
                            near_misses.append((round(net_edge, 1), player,
                                BOOK_DISPLAY.get(target, target), side, t_line))

                else:
                    # === FALLBACK: Line comparison only ===
                    # Use when we don't have both sides from enough books
                    stats['line_compared'] += 1

                    consensus_lines = [books[sb]['line'] for sb in sharp_available]
                    consensus = sum(consensus_lines) / len(consensus_lines)

                    diff = t_line - consensus
                    if abs(diff) < 0.5:
                        continue

                    line_edge = abs(diff / consensus * 100) if consensus != 0 else 0

                    # Estimate juice from available data
                    if has_both_sides:
                        t_over_imp = american_to_implied(tb['over_odds'])
                        t_under_imp = american_to_implied(tb['under_odds'])
                        t_juice_pct = round((t_over_imp + t_under_imp - 1.0) * 100, 1)
                    else:
                        t_juice_pct = 5.0  # conservative estimate

                    net_edge = line_edge - (t_juice_pct / 2.0)

                    if net_edge >= MIN_EDGE_NET:
                        side = 'UNDER' if diff > 0 else 'OVER'
                        # Use whichever odds we have
                        if side == 'OVER' and 'over_odds' in tb:
                            odds = tb['over_odds']
                        elif side == 'UNDER' and 'under_odds' in tb:
                            odds = tb['under_odds']
                        else:
                            odds = -110  # fallback display

                        opportunities.append({
                            'player': player, 'game': data['game'],
                            'market': market_name,
                            'book': BOOK_DISPLAY.get(target, target),
                            'type': 'player_prop',
                            'edge': round(net_edge, 1),
                            'gross_edge': round(line_edge, 1),
                            'recommendation': f"{side} {t_line}",
                            'odds': odds,
                            'label1_name': f'{BOOK_DISPLAY.get(target, target)} Line',
                            'label1_value': str(t_line),
                            'label2_name': 'Sharp Consensus',
                            'label2_value': str(round(consensus, 1)),
                            'label3_name': 'Net Edge',
                            'label3_value': f"+{net_edge:.1f}%",
                            'juice_display': f"~{t_juice_pct}%",
                        })
                    elif net_edge > -3:
                        side = 'UNDER' if diff > 0 else 'OVER'
                        near_misses.append((round(net_edge, 1), player,
                            BOOK_DISPLAY.get(target, target), side, t_line))

    # Log stats
    log_debug(f"    Players: {stats['players']} total, {stats['with_sharp']} w/2+ sharp, "
              f"{stats['with_target']} w/target")
    log_debug(f"    Analysis: {stats['devigged']} full devig, {stats['line_compared']} line-only")
    log_debug(f"    Results: {len(opportunities)} +EV bets")

    # Log top near misses
    if near_misses and not opportunities:
        near_misses.sort(key=lambda x: x[0], reverse=True)
        log_debug(f"    Near misses (top 5):")
        for edge, name, book, side, line in near_misses[:5]:
            log_debug(f"      {edge:+.1f}% {name} {side} {line} on {book}")

    return opportunities


# ============================================================
# GAME MARKET ANALYSIS (robust outcome pairing)
# ============================================================

def analyze_game_markets(games_data, market_name=""):
    if not games_data:
        return []

    opportunities = []
    near_misses = []
    games_checked = 0

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        games_checked += 1

        # Collect outcomes per book, keyed by (name, point)
        # {(name, point): {book: odds}}
        outcome_odds = {}
        # Also track paired outcomes per book for devigging
        book_pairs = {}  # {book: {(name, point): odds}}

        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            book_pairs[book_name] = {}
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if not name or odds is None:
                        continue
                    key = (name, float(point) if point is not None else None)
                    if key not in outcome_odds:
                        outcome_odds[key] = {}
                    outcome_odds[key][book_name] = odds
                    book_pairs[book_name][key] = odds

        # Find complementary pairs for devigging
        # For each book, find the two sides of the market
        # They share a market but have different outcome names
        book_devigged = {}  # {book: {outcome_key: fair_prob}}
        book_juice = {}

        for book_name in list(SHARP_BOOKS) + list(TARGET_BOOKS):
            pairs = book_pairs.get(book_name, {})
            keys = list(pairs.keys())
            if len(keys) < 2:
                continue

            # Take first two outcomes as the pair
            # (API returns the two sides of a 2-way market)
            k1, k2 = keys[0], keys[1]
            imp1 = american_to_implied(pairs[k1])
            imp2 = american_to_implied(pairs[k2])
            juice = round((imp1 + imp2 - 1.0) * 100, 1)
            book_juice[book_name] = juice

            fair1, fair2 = devig_pair(imp1, imp2)
            book_devigged[book_name] = {k1: fair1, k2: fair2}

        # Build consensus fair probs from sharp books
        consensus_fair = {}
        for key in outcome_odds:
            fair_probs = []
            for sb in SHARP_BOOKS:
                if sb in book_devigged and key in book_devigged[sb]:
                    fair_probs.append(book_devigged[sb][key])
            if len(fair_probs) >= 2:
                consensus_fair[key] = sum(fair_probs) / len(fair_probs)

        if not consensus_fair:
            continue

        # Compare target books
        for target in TARGET_BOOKS:
            if target not in book_pairs:
                continue

            for key, odds in book_pairs[target].items():
                if key not in consensus_fair:
                    continue

                fair_prob = consensus_fair[key]
                target_implied = american_to_implied(odds)

                # Net edge (after juice)
                net_edge = (fair_prob - target_implied) * 100

                # Gross edge (target devigged vs consensus)
                target_fair = None
                if target in book_devigged and key in book_devigged[target]:
                    target_fair = book_devigged[target][key]
                gross_edge = (fair_prob - target_fair) * 100 if target_fair else net_edge

                juice_pct = book_juice.get(target, 0)

                if net_edge < MIN_EDGE_NET:
                    if net_edge > -3:
                        name, point = key
                        if point is not None:
                            if 'Total' in market_name:
                                dn = f"{name} {point}"
                            else:
                                dn = f"{name} {point:+.1f}"
                        else:
                            dn = f"{name} ML"
                        near_misses.append((round(net_edge, 1), dn,
                            BOOK_DISPLAY.get(target, target), game_info))
                    continue

                # Build display name
                name, point = key
                if point is not None:
                    if 'Total' in market_name:
                        display_name = f"{name} {point}"
                    else:
                        display_name = f"{name} {point:+.1f}"
                else:
                    display_name = f"{name} ML"

                fair_american = implied_to_american(fair_prob)

                opportunities.append({
                    'player': display_name,
                    'game': game_info,
                    'market': market_name,
                    'book': BOOK_DISPLAY.get(target, target),
                    'type': 'game_market',
                    'edge': round(net_edge, 1),
                    'gross_edge': round(gross_edge, 1),
                    'recommendation': f"BET {display_name}",
                    'odds': odds,
                    'label1_name': f'{BOOK_DISPLAY.get(target, target)} Odds',
                    'label1_value': format_american(odds),
                    'label2_name': 'Fair Odds (no vig)',
                    'label2_value': format_american(fair_american),
                    'label3_name': 'Net Edge',
                    'label3_value': f"+{net_edge:.1f}%",
                    'target_prob': round(target_implied * 100, 1),
                    'fair_prob': round(fair_prob * 100, 1),
                    'juice_display': f"{juice_pct}%",
                })

    log_debug(f"    Games: {games_checked}, Outcomes w/consensus: {len(consensus_fair)}, "
              f"+EV bets: {len(opportunities)}")

    if near_misses and not opportunities:
        near_misses.sort(key=lambda x: x[0], reverse=True)
        log_debug(f"    Near misses (top 5):")
        for edge, name, book, game in near_misses[:5]:
            log_debug(f"      {edge:+.1f}% {name} on {book} ({game})")

    return opportunities


# ============================================================
# EVENT-LEVEL PLAYER PROPS (works for any sport)
# ============================================================

# All prop markets to scan, by sport
EVENT_PROP_MARKETS = [
    ('basketball_nba', [
        ('player_points', 'NBA Points'),
        ('player_rebounds', 'NBA Rebounds'),
        ('player_assists', 'NBA Assists'),
        ('player_threes', 'NBA Threes'),
    ]),
    ('basketball_ncaab', [
        ('player_points', 'NCAAB Points'),
        ('player_rebounds', 'NCAAB Rebounds'),
        ('player_assists', 'NCAAB Assists'),
    ]),
    ('icehockey_nhl', [
        ('player_points', 'NHL Points'),
    ]),
]

def fetch_event_props(sport, prop_markets, max_events=15):
    """Fetch player props for a sport via event-level endpoints"""
    all_opps = []
    events = fetch_events(sport)
    if not events:
        return []

    events_to_scan = events[:max_events]
    log_debug(f"  Scanning {len(events_to_scan)} of {len(events)} {sport} events")

    events_with_data = 0
    for event in events_to_scan:
        event_id = event.get('id')
        home = event.get('home_team', '?')
        away = event.get('away_team', '?')

        for prop_market, prop_name in prop_markets:
            event_data = fetch_event_odds(sport, event_id, prop_market)
            if event_data and event_data.get('bookmakers'):
                events_with_data += 1
                opps = analyze_player_props([event_data], prop_name)
                if opps:
                    all_opps.extend(opps)
                    log_debug(f"    {away} @ {home} / {prop_name}: {len(opps)} +EV")
            time.sleep(0.3)

    log_debug(f"  {sport} props: {events_with_data} event/markets had data, {len(all_opps)} +EV bets")
    return all_opps


# ============================================================
# MAIN SCAN
# ============================================================

def scan_markets():
    state['scanning'] = True
    state['debug_info'] = []
    all_opps = []

    log_debug("=== SCAN STARTED ===")
    log_debug(f"Threshold: >{MIN_EDGE_NET}% net edge after juice")

    # 1. Player props via event-level endpoints (all sports)
    log_debug("--- Player Props (event-by-event) ---")
    for sport, prop_markets in EVENT_PROP_MARKETS:
        opps = fetch_event_props(sport, prop_markets)
        all_opps.extend(opps)

    # 2. Game markets via bulk endpoint (spreads/totals/moneylines)
    log_debug("--- Game Markets (bulk) ---")
    for sport, market, name in GAME_LEVEL_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            if opps:
                all_opps.extend(opps)
        time.sleep(0.5)

    all_opps.sort(key=lambda x: x['edge'], reverse=True)

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    log_debug(f"=== SCAN COMPLETE: {len(all_opps)} +EV opportunities ===")


# ============================================================
# ROUTES
# ============================================================

@app.route('/')
def index():
    return render_template('arbitrage.html')

@app.route('/api/scan', methods=['POST'])
def trigger_scan():
    if state['scanning']:
        return jsonify({'error': 'Scan in progress'})
    threading.Thread(target=scan_markets, daemon=True).start()
    return jsonify({'success': True})

@app.route('/api/opportunities')
def get_opportunities():
    return jsonify({
        'opportunities': state['opportunities'],
        'last_scan': state['last_scan'],
        'total': len(state['opportunities']),
        'scanning': state['scanning'],
        'debug': state.get('debug_info', [])
    })

@app.route('/api/key-status')
def key_status():
    """Quick check if API key is working and how many requests remain"""
    url = "https://api.the-odds-api.com/v4/sports"
    params = {'apiKey': API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        remaining = r.headers.get('x-requests-remaining', '?')
        used = r.headers.get('x-requests-used', '?')
        return jsonify({
            'status': 'ok' if r.status_code == 200 else 'error',
            'http_code': r.status_code,
            'requests_used': used,
            'requests_remaining': remaining,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
