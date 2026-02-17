"""
Premium Arbitrage Finder - Web Version
Runs on Render.com, access from iPhone
NCAA Basketball (game markets + player props) + NBA/NFL/NHL
"""

from flask import Flask, render_template, jsonify
import requests
import time
from datetime import datetime
import threading
import os

app = Flask(__name__)

# API key
API_KEY = "19c83d930cc9b8bfcd3da28458f38d76"

# Global state
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
    'fanduel': 'FanDuel',
    'espnbet': 'ESPN Bet',
    'draftkings': 'DraftKings',
    'betmgm': 'BetMGM',
    'williamhill_us': 'Caesars'
}


def log_debug(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    state['debug_info'].append(f"[{ts}] {msg}")
    print(f"[{ts}] {msg}")


# ============================================================
# ODDS MATH
# ============================================================

def american_to_implied(odds):
    """Convert American odds to implied probability (0 to 1)"""
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)


def format_american(odds):
    """Format American odds with +/- sign"""
    rounded = int(round(odds))
    if rounded > 0:
        return f"+{rounded}"
    else:
        return str(rounded)


# ============================================================
# FETCH FUNCTIONS
# ============================================================

def fetch_odds(sport, market):
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': market,
        'bookmakers': ','.join(BOOKMAKERS),
        'oddsFormat': 'american'
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
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': market,
        'bookmakers': ','.join(BOOKMAKERS),
        'oddsFormat': 'american'
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None


# ============================================================
# PLAYER PROP ANALYSIS
# ============================================================

def analyze_player_props(games_data, market_name=""):
    if not games_data:
        return []

    opportunities = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        players = {}

        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    player = outcome.get('description', outcome.get('name', ''))
                    if not player:
                        continue
                    line = outcome.get('point')
                    odds = outcome.get('price')
                    if line is None or odds is None:
                        continue
                    if player not in players:
                        players[player] = {'game': game_info, 'books': {}}
                    if book_name not in players[player]['books']:
                        players[player]['books'][book_name] = []
                    players[player]['books'][book_name].append({
                        'line': line, 'odds': odds
                    })

        for player, data in players.items():
            books = data['books']
            consensus_books = [b for b in SHARP_BOOKS if b in books]
            if len(consensus_books) < 2:
                continue

            consensus_lines = []
            for cb in consensus_books:
                if books[cb]:
                    consensus_lines.append(books[cb][0]['line'])
            if not consensus_lines:
                continue
            consensus = sum(consensus_lines) / len(consensus_lines)

            for target in TARGET_BOOKS:
                if target not in books or not books[target]:
                    continue
                t_line = books[target][0]['line']
                t_odds = books[target][0]['odds']
                diff = t_line - consensus
                edge = abs(diff / consensus * 100) if consensus != 0 else 0

                if abs(diff) >= 0.5 and edge >= 1:
                    rec = 'UNDER' if diff > 0 else 'OVER'
                    opportunities.append({
                        'player': player,
                        'game': data['game'],
                        'market': market_name,
                        'book': BOOK_DISPLAY.get(target, target),
                        'type': 'player_prop',
                        'edge': round(edge, 1),
                        'recommendation': f"{rec} {t_line}",
                        'odds': t_odds,
                        'label1_name': f'{BOOK_DISPLAY.get(target, target)} Line',
                        'label1_value': str(t_line),
                        'label2_name': 'Sharp Consensus',
                        'label2_value': str(round(consensus, 1)),
                        'label3_name': 'Line Diff',
                        'label3_value': f"{diff:+.1f}",
                    })

    return opportunities


# ============================================================
# GAME MARKET ANALYSIS (fixed: uses implied probability properly)
# ============================================================

def analyze_game_markets(games_data, market_name=""):
    if not games_data:
        return []

    opportunities = []

    market_type = 'moneyline'
    if 'Spread' in market_name:
        market_type = 'spread'
    elif 'Total' in market_name:
        market_type = 'total'

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Group: (outcome_name, point_value) -> {book: odds}
        outcome_map = {}

        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if not name or odds is None:
                        continue

                    if point is not None:
                        key = (name, float(point))
                    else:
                        key = (name, None)

                    if key not in outcome_map:
                        outcome_map[key] = {}
                    outcome_map[key][book_name] = odds

        for (outcome_name, point_val), book_odds in outcome_map.items():
            # Need at least 2 sharp books
            sharp_odds_list = [book_odds[sb] for sb in SHARP_BOOKS if sb in book_odds]
            if len(sharp_odds_list) < 2:
                continue

            # Consensus via implied probability (not raw American odds)
            sharp_probs = [american_to_implied(o) for o in sharp_odds_list]
            consensus_prob = sum(sharp_probs) / len(sharp_probs)

            # Average sharp American odds (for display only)
            avg_sharp_american = sum(sharp_odds_list) / len(sharp_odds_list)

            for target in TARGET_BOOKS:
                if target not in book_odds:
                    continue

                target_odds = book_odds[target]
                target_prob = american_to_implied(target_odds)

                # Edge = difference in implied probability
                edge_pct = abs(target_prob - consensus_prob) * 100

                if edge_pct < 3.0:
                    continue

                # Build display name
                if market_type == 'spread' and point_val is not None:
                    display_name = f"{outcome_name} {point_val:+.1f}"
                elif market_type == 'total' and point_val is not None:
                    display_name = f"{outcome_name} {point_val}"
                else:
                    display_name = f"{outcome_name} ML"

                # Recommendation: is this good value or bad value?
                if target_prob < consensus_prob:
                    # Target book's odds are better (lower implied prob = higher payout)
                    rec = f"BET {display_name}"
                    action = "value"
                else:
                    # Target book's odds are worse (higher implied prob = lower payout)
                    rec = f"FADE {display_name}"
                    action = "avoid"

                opportunities.append({
                    'player': display_name,
                    'game': game_info,
                    'market': market_name,
                    'book': BOOK_DISPLAY.get(target, target),
                    'type': 'game_market',
                    'edge': round(edge_pct, 1),
                    'recommendation': rec,
                    'odds': target_odds,
                    'action': action,
                    'label1_name': f'{BOOK_DISPLAY.get(target, target)} Odds',
                    'label1_value': format_american(target_odds),
                    'label2_name': 'Sharp Consensus',
                    'label2_value': format_american(avg_sharp_american),
                    'label3_name': 'Implied Prob Edge',
                    'label3_value': f"{edge_pct:.1f}%",
                    'target_prob': round(target_prob * 100, 1),
                    'consensus_prob': round(consensus_prob * 100, 1),
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

    log_debug("--- Player Props (NBA/NFL/NHL) ---")
    for sport, market, name in PLAYER_PROP_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_player_props(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} outliers")
        time.sleep(0.5)

    log_debug("--- NCAAB Game Markets ---")
    for sport, market, name in GAME_LEVEL_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} outliers")
        time.sleep(0.5)

    log_debug("--- NCAAB Player Props (event-by-event) ---")
    ncaab_props = fetch_ncaab_player_props()
    all_opps.extend(ncaab_props)

    all_opps.sort(key=lambda x: x['edge'], reverse=True)

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    log_debug(f"=== SCAN COMPLETE: {len(all_opps)} total opportunities ===")


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
