"""
Premium Arbitrage Finder - Web Version
Runs on Render.com, access from iPhone
NOW WITH NCAA BASKETBALL (game markets + player props via event endpoint)
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
# MARKETS - Player props fetched via bulk /odds endpoint
# ============================================================
PLAYER_PROP_MARKETS = [
    # NCAA Basketball - player props (will be fetched event-by-event)
    # These are handled separately below via fetch_ncaab_player_props()

    # NBA
    ('basketball_nba', 'player_points', 'NBA Points'),
    ('basketball_nba', 'player_rebounds', 'NBA Rebounds'),
    ('basketball_nba', 'player_assists', 'NBA Assists'),
    ('basketball_nba', 'player_threes', 'NBA 3-Pointers'),

    # NFL
    ('americanfootball_nfl', 'player_pass_tds', 'NFL Pass TDs'),
    ('americanfootball_nfl', 'player_pass_yds', 'NFL Pass Yards'),

    # NHL
    ('icehockey_nhl', 'player_points', 'NHL Points'),
]

# ============================================================
# GAME-LEVEL MARKETS - spreads, totals, moneylines (bulk endpoint)
# ============================================================
GAME_LEVEL_MARKETS = [
    # NCAA Basketball game markets
    ('basketball_ncaab', 'spreads', 'NCAAB Spreads'),
    ('basketball_ncaab', 'totals', 'NCAAB Totals'),
    ('basketball_ncaab', 'h2h', 'NCAAB Moneyline'),
]

# NCAA player prop market keys to scan per event
NCAAB_PROP_MARKETS = [
    ('player_points', 'NCAAB Player Points'),
    ('player_rebounds', 'NCAAB Player Rebounds'),
    ('player_assists', 'NCAAB Player Assists'),
]

BOOKMAKERS = ['fanduel', 'espnbet', 'draftkings', 'betmgm', 'williamhill_us']


def log_debug(msg):
    """Add debug message with timestamp"""
    ts = datetime.now().strftime('%H:%M:%S')
    state['debug_info'].append(f"[{ts}] {msg}")
    print(f"[{ts}] {msg}")


# ============================================================
# FETCH FUNCTIONS
# ============================================================

def fetch_odds(sport, market):
    """Fetch odds from bulk /odds endpoint (works for NBA/NFL/NHL props + NCAAB game markets)"""
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
            log_debug(f"  {sport}/{market}: {len(data)} games (API calls left: {remaining})")
            return data
        else:
            log_debug(f"  {sport}/{market}: HTTP {response.status_code}")
    except Exception as e:
        log_debug(f"  {sport}/{market}: Error - {str(e)[:80]}")
    return None


def fetch_ncaab_events():
    """Get list of upcoming NCAAB events (game IDs)"""
    url = f"https://api.the-odds-api.com/v4/sports/basketball_ncaab/events"
    params = {
        'apiKey': API_KEY,
    }

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
    """Fetch odds for a single event using /events/{eventId}/odds endpoint"""
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
        else:
            return None
    except:
        return None


# ============================================================
# ANALYSIS FUNCTIONS
# ============================================================

def analyze_player_props(games_data, market_name=""):
    """Analyze player prop markets - find FanDuel/ESPN Bet outliers vs consensus"""
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
                        'line': line,
                        'odds': odds,
                        'name': outcome.get('name', '')
                    })

        # Find outliers
        for player, data in players.items():
            books = data['books']

            # Need at least 3 books for a reliable consensus
            consensus_books = [b for b in ['draftkings', 'betmgm', 'williamhill_us'] if b in books]
            if len(consensus_books) < 2:
                continue

            # Calculate consensus from sharp books
            consensus_lines = []
            for cb in consensus_books:
                if books[cb]:
                    consensus_lines.append(books[cb][0]['line'])

            if not consensus_lines:
                continue

            consensus = sum(consensus_lines) / len(consensus_lines)

            # Check FanDuel
            if 'fanduel' in books and books['fanduel']:
                fd_line = books['fanduel'][0]['line']
                fd_odds = books['fanduel'][0]['odds']
                diff = fd_line - consensus
                edge = abs(diff / consensus * 100) if consensus != 0 else 0

                if abs(diff) >= 0.5 and edge >= 1:
                    opportunities.append({
                        'player': player,
                        'game': data['game'],
                        'market': market_name,
                        'book': 'FanDuel',
                        'line': fd_line,
                        'consensus': round(consensus, 1),
                        'difference': round(diff, 1),
                        'edge': round(edge, 1),
                        'recommendation': 'UNDER' if diff > 0 else 'OVER',
                        'odds': fd_odds,
                        'type': 'player_prop'
                    })

            # Check ESPN Bet
            if 'espnbet' in books and books['espnbet']:
                espn_line = books['espnbet'][0]['line']
                espn_odds = books['espnbet'][0]['odds']
                diff = espn_line - consensus
                edge = abs(diff / consensus * 100) if consensus != 0 else 0

                if abs(diff) >= 0.5 and edge >= 1:
                    opportunities.append({
                        'player': player,
                        'game': data['game'],
                        'market': market_name,
                        'book': 'ESPN Bet',
                        'line': espn_line,
                        'consensus': round(consensus, 1),
                        'difference': round(diff, 1),
                        'edge': round(edge, 1),
                        'recommendation': 'UNDER' if diff > 0 else 'OVER',
                        'odds': espn_odds,
                        'type': 'player_prop'
                    })

    return opportunities


def analyze_game_markets(games_data, market_name=""):
    """Analyze game-level markets (spreads, totals, moneylines) for outliers"""
    if not games_data:
        return []

    opportunities = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Collect all book lines for each outcome
        outcomes_by_name = {}

        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']

            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if not name or odds is None:
                        continue

                    key = name
                    if point is not None:
                        key = f"{name} {point}"

                    if key not in outcomes_by_name:
                        outcomes_by_name[key] = {
                            'name': name,
                            'point': point,
                            'books': {}
                        }
                    outcomes_by_name[key]['books'][book_name] = odds

        # For spreads/totals: compare point values across books
        # For moneylines: compare odds across books
        for key, data in outcomes_by_name.items():
            books = data['books']

            # Need consensus books
            consensus_books = [b for b in ['draftkings', 'betmgm', 'williamhill_us'] if b in books]
            if len(consensus_books) < 2:
                continue

            consensus_odds = sum(books[b] for b in consensus_books) / len(consensus_books)

            # Check FanDuel
            if 'fanduel' in books:
                fd_odds = books['fanduel']
                diff = fd_odds - consensus_odds

                # For American odds, a meaningful difference depends on the range
                # For favorites (negative odds): bigger negative = bigger favorite
                # We look for odds that differ significantly
                if abs(consensus_odds) > 0:
                    # Convert to implied probability to measure edge
                    fd_prob = american_to_prob(fd_odds)
                    cons_prob = american_to_prob(consensus_odds)
                    edge = abs(fd_prob - cons_prob) * 100

                    if edge >= 3:  # 3%+ implied probability difference
                        # Determine which side to bet
                        if fd_prob < cons_prob:
                            rec = f"BET {data['name']}"
                            if data['point'] is not None:
                                rec += f" {data['point']}"
                            rec += " on FanDuel (better odds)"
                        else:
                            rec = f"FADE {data['name']} on FanDuel (worse odds)"

                        opportunities.append({
                            'player': f"{data['name']}" + (f" {data['point']}" if data['point'] else ""),
                            'game': game_info,
                            'market': market_name,
                            'book': 'FanDuel',
                            'line': fd_odds,
                            'consensus': round(consensus_odds, 0),
                            'difference': round(diff, 0),
                            'edge': round(edge, 1),
                            'recommendation': rec,
                            'odds': fd_odds,
                            'type': 'game_market'
                        })

            # Check ESPN Bet
            if 'espnbet' in books:
                espn_odds = books['espnbet']
                diff = espn_odds - consensus_odds

                if abs(consensus_odds) > 0:
                    espn_prob = american_to_prob(espn_odds)
                    cons_prob = american_to_prob(consensus_odds)
                    edge = abs(espn_prob - cons_prob) * 100

                    if edge >= 3:
                        if espn_prob < cons_prob:
                            rec = f"BET {data['name']}"
                            if data['point'] is not None:
                                rec += f" {data['point']}"
                            rec += " on ESPN Bet (better odds)"
                        else:
                            rec = f"FADE {data['name']} on ESPN Bet (worse odds)"

                        opportunities.append({
                            'player': f"{data['name']}" + (f" {data['point']}" if data['point'] else ""),
                            'game': game_info,
                            'market': market_name,
                            'book': 'ESPN Bet',
                            'line': espn_odds,
                            'consensus': round(consensus_odds, 0),
                            'difference': round(diff, 0),
                            'edge': round(edge, 1),
                            'recommendation': rec,
                            'odds': espn_odds,
                            'type': 'game_market'
                        })

    return opportunities


def american_to_prob(odds):
    """Convert American odds to implied probability"""
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


# ============================================================
# NCAAB EVENT-LEVEL PLAYER PROPS
# ============================================================

def fetch_ncaab_player_props():
    """Fetch NCAAB player props event-by-event (required by The Odds API)"""
    all_opps = []

    # Step 1: Get list of events
    events = fetch_ncaab_events()
    if not events:
        log_debug("  No NCAAB events found - skipping player props")
        return []

    # Limit to first 15 events to avoid burning too many API calls
    events_to_scan = events[:15]
    log_debug(f"  Scanning player props for {len(events_to_scan)} of {len(events)} NCAAB events")

    for event in events_to_scan:
        event_id = event.get('id')
        home = event.get('home_team', '?')
        away = event.get('away_team', '?')
        game_info = f"{away} @ {home}"

        for prop_market, prop_name in NCAAB_PROP_MARKETS:
            event_data = fetch_event_odds(event_id, prop_market)

            if event_data and event_data.get('bookmakers'):
                # Wrap in list format so analyze_player_props can process it
                opps = analyze_player_props([event_data], prop_name)
                if opps:
                    all_opps.extend(opps)
                    log_debug(f"    {game_info} / {prop_name}: {len(opps)} outliers")

            # Small delay to be respectful to API
            time.sleep(0.3)

    log_debug(f"  NCAAB player props total: {len(all_opps)} outliers")
    return all_opps


# ============================================================
# MAIN SCAN
# ============================================================

def scan_markets():
    """Scan all markets"""
    state['scanning'] = True
    state['debug_info'] = []
    all_opps = []

    log_debug("=== SCAN STARTED ===")

    # 1. Scan player props via bulk endpoint (NBA, NFL, NHL)
    log_debug("--- Player Props (NBA/NFL/NHL) ---")
    for sport, market, name in PLAYER_PROP_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_player_props(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} outliers found")
        time.sleep(0.5)

    # 2. Scan NCAAB game-level markets (spreads, totals, moneylines)
    log_debug("--- NCAAB Game Markets ---")
    for sport, market, name in GAME_LEVEL_MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            if opps:
                all_opps.extend(opps)
                log_debug(f"    -> {len(opps)} outliers found")
        time.sleep(0.5)

    # 3. Scan NCAAB player props event-by-event
    log_debug("--- NCAAB Player Props (event-by-event) ---")
    ncaab_props = fetch_ncaab_player_props()
    all_opps.extend(ncaab_props)

    # Sort by edge
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
