"""
Premium Arbitrage Finder - Web Version
Properly devigs sharp books to find true +EV edges after juice.
Only shows actionable BET opportunities.
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

PLAYER_PROP_MARKETS = [
    ('basketball_nba', 'player_points', 'NBA Points'),
    ('basketball_nba', 'player_rebounds', 'NBA Rebounds'),
    ('basketball_nba', 'player_assists', 'NBA Assists'),
    ('basketball_nba', 'player_threes', 'NBA 3-Pointers'),
    ('americanfootball_nfl', 'player_pass_tds', 'NFL Pass TDs'),
    ('americanfootball_nfl', 'player_pass_yds', 'NFL Pass Yards'),
    ('icehockey_nhl', 'player_points', 'NHL Points'),
]

GAME_LEVEL_MARKETS = [
    ('basketball_ncaab', 'spreads', 'NCAAB Spreads'),
    ('basketball_ncaab', 'totals', 'NCAAB Totals'),
    ('basketball_ncaab', 'h2h', 'NCAAB Moneyline'),
]

NCAAB_PROP_MARKETS = [
    ('player_points', 'NCAAB Points'),
    ('player_rebounds', 'NCAAB Rebounds'),
    ('player_assists', 'NCAAB Assists'),
]

BOOKMAKERS = ['fanduel', 'espnbet', 'draftkings', 'betmgm', 'williamhill_us']
SHARP_BOOKS = ['draftkings', 'betmgm', 'williamhill_us']
TARGET_BOOKS = ['fanduel', 'espnbet']
BOOK_DISPLAY = {
    'fanduel': 'FanDuel', 'espnbet': 'ESPN Bet',
    'draftkings': 'DraftKings', 'betmgm': 'BetMGM', 'williamhill_us': 'Caesars'
}

# Minimum NET edge (after juice) to display — show anything profitable
MIN_EDGE_NET = 0.1  # Show any bet with >0.1% net edge (effectively any +EV bet)


def log_debug(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    state['debug_info'].append(f"[{ts}] {msg}")
    print(f"[{ts}] {msg}")


# ============================================================
# ODDS MATH
# ============================================================

def american_to_implied(odds):
    """American odds -> implied probability (0 to 1)"""
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)


def devig_pair(prob_a, prob_b):
    """
    Remove vig from a pair of implied probabilities.
    Returns (fair_a, fair_b) that sum to 1.0.
    """
    total = prob_a + prob_b
    if total <= 0:
        return (0.5, 0.5)
    return (prob_a / total, prob_b / total)


def format_american(odds):
    rounded = int(round(odds))
    return f"+{rounded}" if rounded > 0 else str(rounded)


def implied_to_american(prob):
    """Convert implied probability back to American odds (for display)"""
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
            log_debug(f"  {sport}/{market}: {len(data)} games (API left: {remaining})")
            return data
        else:
            log_debug(f"  {sport}/{market}: HTTP {response.status_code}")
    except Exception as e:
        log_debug(f"  {sport}/{market}: Error - {str(e)[:80]}")
    return None


def fetch_ncaab_events():
    url = f"https://api.the-odds-api.com/v4/sports/basketball_ncaab/events"
    params = {'apiKey': API_KEY}
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            events = response.json()
            log_debug(f"  Found {len(events)} upcoming NCAAB events")
            return events
        else:
            log_debug(f"  NCAAB events: HTTP {response.status_code}")
    except Exception as e:
        log_debug(f"  NCAAB events: Error - {str(e)[:80]}")
    return []


def fetch_event_odds(event_id, market):
    url = f"https://api.the-odds-api.com/v4/sports/basketball_ncaab/events/{event_id}/odds"
    params = {
        'apiKey': API_KEY, 'regions': 'us', 'markets': market,
        'bookmakers': ','.join(BOOKMAKERS), 'oddsFormat': 'american'
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None


# ============================================================
# PLAYER PROP ANALYSIS (devigged — no estimated juice)
# ============================================================

def analyze_player_props(games_data, market_name=""):
    """
    Two scenarios for player props:

    A) SAME LINE across books: Devig sharp books' Over/Under to get fair
       probability. Compare target's juiced implied prob. Edge = fair - implied.
       Already net of juice (same method as game markets).

    B) DIFFERENT LINE on target vs sharp: Use line difference as raw edge,
       then subtract the target book's ACTUAL per-side juice (calculated
       from their own Over + Under implied probs).

    Only shows actionable BET opportunities (positive net edge).
    """
    if not games_data:
        return []

    opportunities = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Collect: {player: {book: {line: X, over_odds: Y, under_odds: Z}}}
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
                    side = outcome.get('name', '').lower()  # "Over" or "Under"
                    if line is None or odds is None or not side:
                        continue

                    if player not in players:
                        players[player] = {'game': game_info, 'books': {}}
                    if book_name not in players[player]['books']:
                        players[player]['books'][book_name] = {'line': line}

                    if 'over' in side:
                        players[player]['books'][book_name]['over_odds'] = odds
                    elif 'under' in side:
                        players[player]['books'][book_name]['under_odds'] = odds
                    # Update line (should be same for over/under)
                    players[player]['books'][book_name]['line'] = line

        # Analyze each player
        for player, data in players.items():
            books = data['books']

            # Get sharp books that have both sides
            sharp_with_both = []
            for sb in SHARP_BOOKS:
                if sb in books and 'over_odds' in books[sb] and 'under_odds' in books[sb]:
                    sharp_with_both.append(sb)
            if len(sharp_with_both) < 2:
                continue

            # Devig sharp books to get fair Over probability at their line
            sharp_fair_overs = []
            sharp_lines = []
            for sb in sharp_with_both:
                bk = books[sb]
                over_imp = american_to_implied(bk['over_odds'])
                under_imp = american_to_implied(bk['under_odds'])
                fair_over, fair_under = devig_pair(over_imp, under_imp)
                sharp_fair_overs.append(fair_over)
                sharp_lines.append(bk['line'])

            consensus_fair_over = sum(sharp_fair_overs) / len(sharp_fair_overs)
            consensus_line = sum(sharp_lines) / len(sharp_lines)

            # Check each target book
            for target in TARGET_BOOKS:
                if target not in books:
                    continue
                tb = books[target]
                if 'over_odds' not in tb or 'under_odds' not in tb:
                    continue

                t_line = tb['line']
                t_over_odds = tb['over_odds']
                t_under_odds = tb['under_odds']
                t_over_imp = american_to_implied(t_over_odds)
                t_under_imp = american_to_implied(t_under_odds)

                # Calculate target book's actual juice
                t_total_imp = t_over_imp + t_under_imp
                t_juice_pct = (t_total_imp - 1.0) * 100  # total overround
                t_juice_per_side = t_juice_pct / 2.0

                same_line = abs(t_line - consensus_line) < 0.25

                if same_line:
                    # === SCENARIO A: Same line — full devig comparison ===
                    # Devig target book to get their fair prob (gross edge)
                    t_fair_over, t_fair_under = devig_pair(t_over_imp, t_under_imp)

                    for side, target_p, odds in [
                        ('OVER', t_over_imp, t_over_odds),
                        ('UNDER', t_under_imp, t_under_odds)
                    ]:
                        fair_p = consensus_fair_over if side == 'OVER' else (1.0 - consensus_fair_over)
                        t_fair_p = t_fair_over if side == 'OVER' else t_fair_under

                        # Net edge: fair vs juiced implied (what you actually capture)
                        net_edge = (fair_p - target_p) * 100
                        # Gross edge: fair vs devigged target (pure pricing diff)
                        gross_edge = (fair_p - t_fair_p) * 100

                        if net_edge < MIN_EDGE_NET:
                            continue

                        fair_odds = implied_to_american(fair_p)

                        opportunities.append({
                            'player': player,
                            'game': data['game'],
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
                            'juice_display': f"{t_juice_pct:.1f}%",
                        })

                else:
                    # === SCENARIO B: Different line — line diff minus actual juice ===
                    diff = t_line - consensus_line
                    line_edge = abs(diff / consensus_line * 100) if consensus_line != 0 else 0

                    if abs(diff) < 0.5 or line_edge < 1.0:
                        continue

                    # Net edge = line edge - actual per-side juice
                    net_edge = line_edge - t_juice_per_side

                    if net_edge < MIN_EDGE_NET:
                        continue

                    if diff > 0:
                        side = 'UNDER'
                        odds = t_under_odds
                    else:
                        side = 'OVER'
                        odds = t_over_odds

                    opportunities.append({
                        'player': player,
                        'game': data['game'],
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
                        'juice_display': f"{t_juice_pct:.1f}%",
                    })

    return opportunities


# ============================================================
# GAME MARKET ANALYSIS (with proper devigging)
# ============================================================

def analyze_game_markets(games_data, market_name=""):
    """
    Properly devigs sharp book lines to find true fair probability,
    then compares target book implied probability (which includes their juice).
    Edge = fair_prob - target_implied_prob.
    This edge is ALREADY net of the target book's juice.
    Only shows positive-edge BET opportunities.
    """
    if not games_data:
        return []

    opportunities = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Step 1: Collect all outcomes per book
        # Structure: {book_name: [(outcome_name, point, odds), ...]}
        book_outcomes = {}

        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            book_outcomes[book_name] = []
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if name and odds is not None:
                        book_outcomes[book_name].append((name, point, odds))

        # Step 2: Identify the two sides of each market
        # For each book, pair complementary outcomes
        # (Team A, Team B) for ML; (Over X, Under X) for totals;
        # (Team A -X, Team B +X) for spreads

        # Collect unique outcome identifiers
        all_outcomes = set()
        for book, outcomes in book_outcomes.items():
            for (name, point, odds) in outcomes:
                if point is not None:
                    all_outcomes.add((name, float(point)))
                else:
                    all_outcomes.add((name, None))

        # For each outcome, find its complement in each book and devig
        # Build: {outcome_key: {book: (raw_implied, devigged_fair)}}
        outcome_fair_probs = {}

        for book_name in SHARP_BOOKS:
            if book_name not in book_outcomes:
                continue

            outcomes = book_outcomes[book_name]
            if len(outcomes) != 2:
                continue  # Need exactly 2 sides

            (name_a, point_a, odds_a) = outcomes[0]
            (name_b, point_b, odds_b) = outcomes[1]

            prob_a = american_to_implied(odds_a)
            prob_b = american_to_implied(odds_b)

            fair_a, fair_b = devig_pair(prob_a, prob_b)

            key_a = (name_a, float(point_a)) if point_a is not None else (name_a, None)
            key_b = (name_b, float(point_b)) if point_b is not None else (name_b, None)

            if key_a not in outcome_fair_probs:
                outcome_fair_probs[key_a] = []
            outcome_fair_probs[key_a].append(fair_a)

            if key_b not in outcome_fair_probs:
                outcome_fair_probs[key_b] = []
            outcome_fair_probs[key_b].append(fair_b)

        # Step 3: Average devigged fair probs across sharp books
        consensus_fair = {}
        for key, fair_list in outcome_fair_probs.items():
            if len(fair_list) >= 2:
                consensus_fair[key] = sum(fair_list) / len(fair_list)

        if not consensus_fair:
            continue

        # Step 4: Also devig target books to get gross edge
        target_fair_probs = {}
        target_juice = {}
        for target in TARGET_BOOKS:
            if target not in book_outcomes:
                continue
            outcomes = book_outcomes[target]
            if len(outcomes) == 2:
                (na, pa, oa) = outcomes[0]
                (nb, pb, ob) = outcomes[1]
                imp_a = american_to_implied(oa)
                imp_b = american_to_implied(ob)
                overround = (imp_a + imp_b - 1.0) * 100  # total juice %
                target_juice[target] = round(overround, 1)
                fair_a, fair_b = devig_pair(imp_a, imp_b)
                key_a = (na, float(pa)) if pa is not None else (na, None)
                key_b = (nb, float(pb)) if pb is not None else (nb, None)
                target_fair_probs[(target, key_a)] = fair_a
                target_fair_probs[(target, key_b)] = fair_b

        # Step 5: Compare target books to fair probability
        for target in TARGET_BOOKS:
            if target not in book_outcomes:
                continue

            for (name, point, odds) in book_outcomes[target]:
                key = (name, float(point)) if point is not None else (name, None)

                if key not in consensus_fair:
                    continue

                fair_prob = consensus_fair[key]
                target_implied = american_to_implied(odds)

                # Net edge: fair prob vs target's juiced implied prob
                # This is what you actually capture after paying their juice
                net_edge = (fair_prob - target_implied) * 100

                if net_edge < MIN_EDGE_NET:
                    continue

                # Gross edge: fair prob vs target's own devigged prob
                # This is the pure pricing disagreement before juice
                target_fair = target_fair_probs.get((target, key))
                if target_fair is not None:
                    gross_edge = (fair_prob - target_fair) * 100
                else:
                    gross_edge = net_edge  # fallback

                # Juice this book is charging on this market
                juice_pct = target_juice.get(target, 0)

                # Build display name
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

    return opportunities


# ============================================================
# NCAAB EVENT-LEVEL PLAYER PROPS
# ============================================================

def fetch_ncaab_player_props():
    all_opps = []
    events = fetch_ncaab_events()
    if not events:
        log_debug("  No NCAAB events - skipping player props")
        return []

    events_to_scan = events[:15]
    log_debug(f"  Scanning props for {len(events_to_scan)} of {len(events)} NCAAB events")

    for event in events_to_scan:
        event_id = event.get('id')
        home = event.get('home_team', '?')
        away = event.get('away_team', '?')
        for prop_market, prop_name in NCAAB_PROP_MARKETS:
            event_data = fetch_event_odds(event_id, prop_market)
            if event_data and event_data.get('bookmakers'):
                opps = analyze_player_props([event_data], prop_name)
                if opps:
                    all_opps.extend(opps)
                    log_debug(f"    {away} @ {home} / {prop_name}: {len(opps)} outliers")
            time.sleep(0.3)

    log_debug(f"  NCAAB player props total: {len(all_opps)} outliers")
    return all_opps


# ============================================================
# MAIN SCAN
# ============================================================

def scan_markets():
    state['scanning'] = True
    state['debug_info'] = []
    all_opps = []

    log_debug("=== SCAN STARTED ===")
    log_debug(f"Threshold: >{MIN_EDGE_NET}% net edge (any +EV bet after juice)")

    log_debug("--- Player Props (NBA/NFL/NHL) ---")
    for sport, market, name in PLAYER_PROP_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_player_props(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} profitable edges")
        time.sleep(0.5)

    log_debug("--- NCAAB Game Markets (devigged) ---")
    for sport, market, name in GAME_LEVEL_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} profitable edges")
        time.sleep(0.5)

    log_debug("--- NCAAB Player Props (event-by-event) ---")
    ncaab_props = fetch_ncaab_player_props()
    all_opps.extend(ncaab_props)

    all_opps.sort(key=lambda x: x['edge'], reverse=True)

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    log_debug(f"=== SCAN COMPLETE: {len(all_opps)} profitable opportunities ===")


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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
