"""
+EV Finder — Colorado Sportsbooks
Compares all CO-legal books against each other.
Any book that's an outlier vs consensus = +EV bet.
"""

from flask import Flask, render_template, jsonify
import requests
import time
from datetime import datetime
import threading
import os

app = Flask(__name__)

# ============================================================
# API KEY ROTATION
# ============================================================

API_KEYS = [
    "19c83d930cc9b8bfcd3da28458f38d76",
    "1c0914963fab326fc7e3dd488c5cb89b",
    "836b2b862b5c0f0edf90c1c8337c002d",
    "a746929baa0218b074453992586cbcd0",
]
_key_index = 0
_dead_keys = set()

def get_api_key():
    global _key_index
    attempts = 0
    while attempts < len(API_KEYS):
        key = API_KEYS[_key_index % len(API_KEYS)]
        _key_index += 1
        if key not in _dead_keys:
            return key
        attempts += 1
    return None

def mark_key_dead(key):
    _dead_keys.add(key)
    remaining = len(API_KEYS) - len(_dead_keys)
    log_debug(f"  ⚠️ Key ...{key[-6:]} exhausted. {remaining} keys left.")


# ============================================================
# COLORADO-LEGAL SPORTSBOOKS (available on The Odds API free tier)
# ============================================================

# All CO-legal books on the API (<=10 = 1 region cost per call)
ALL_BOOKS = [
    'fanduel', 'draftkings', 'betmgm', 'betrivers',
    'espnbet', 'hardrockbet', 'ballybet', 'betparx',
]

BOOK_DISPLAY = {
    'fanduel': 'FanDuel',
    'draftkings': 'DraftKings',
    'betmgm': 'BetMGM',
    'betrivers': 'BetRivers',
    'espnbet': 'theScore Bet',
    'hardrockbet': 'Hard Rock',
    'ballybet': 'Bally Bet',
    'betparx': 'betPARX',
}

# Markets to scan
GAME_MARKETS = [
    ('basketball_nba', 'h2h', 'NBA Moneyline'),
    ('basketball_ncaab', 'h2h', 'NCAAB Moneyline'),
    ('icehockey_nhl', 'h2h', 'NHL Moneyline'),
]

PROP_MARKETS = [
    ('basketball_nba', [
        ('player_points', 'NBA Points'),
        ('player_rebounds', 'NBA Rebounds'),
    ], 8),
    ('basketball_ncaab', [
        ('player_points', 'NCAAB Points'),
        ('player_rebounds', 'NCAAB Rebounds'),
    ], 8),
]

MIN_EDGE_NET = 0.1  # Show any +EV bet

state = {
    'opportunities': [],
    'last_scan': None,
    'scanning': False,
    'debug_info': []
}

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
    key = get_api_key()
    if not key:
        log_debug(f"  {sport}/{market}: All keys exhausted!")
        return None
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        'apiKey': key, 'regions': 'us,us2', 'markets': market,
        'bookmakers': ','.join(ALL_BOOKS), 'oddsFormat': 'american'
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            data = r.json()
            left = r.headers.get('x-requests-remaining', '?')
            log_debug(f"  {sport}/{market}: {len(data)} games (key ...{key[-6:]}, left: {left})")
            return data
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_odds(sport, market)
        else:
            try:
                msg = r.json().get('message', r.text[:100])
            except:
                msg = r.text[:100]
            log_debug(f"  {sport}/{market}: HTTP {r.status_code} - {msg}")
    except Exception as e:
        log_debug(f"  {sport}/{market}: Error - {str(e)[:80]}")
    return None

def fetch_events(sport):
    key = get_api_key()
    if not key:
        log_debug(f"  {sport}: All keys exhausted!")
        return []
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
    params = {'apiKey': key}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            events = r.json()
            log_debug(f"  {sport}: {len(events)} events (key ...{key[-6:]})")
            return events
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_events(sport)
    except Exception as e:
        log_debug(f"  {sport} events: Error - {str(e)[:60]}")
    return []

def fetch_event_odds(sport, event_id, market):
    key = get_api_key()
    if not key:
        return None
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events/{event_id}/odds"
    params = {
        'apiKey': key, 'regions': 'us,us2', 'markets': market,
        'bookmakers': ','.join(ALL_BOOKS), 'oddsFormat': 'american'
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_event_odds(sport, event_id, market)
    except:
        pass
    return None


# ============================================================
# GAME MARKET ANALYSIS — Any book vs consensus of all others
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

        # Collect both sides per book: {book: {outcome_key: odds}}
        book_pairs = {}
        for bookmaker in game.get('bookmakers', []):
            bk = bookmaker['key']
            book_pairs[bk] = {}
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if not name or odds is None:
                        continue
                    key = (name, float(point) if point is not None else None)
                    book_pairs[bk][key] = odds

        # Devig each book that has exactly 2 sides
        book_devigged = {}  # {book: {key: fair_prob}}
        book_juice = {}
        for bk, pairs in book_pairs.items():
            keys = list(pairs.keys())
            if len(keys) < 2:
                continue
            k1, k2 = keys[0], keys[1]
            imp1 = american_to_implied(pairs[k1])
            imp2 = american_to_implied(pairs[k2])
            book_juice[bk] = round((imp1 + imp2 - 1.0) * 100, 1)
            f1, f2 = devig_pair(imp1, imp2)
            book_devigged[bk] = {k1: f1, k2: f2}

        if len(book_devigged) < 3:
            continue  # Need at least 3 books for meaningful consensus

        # For each book, compute leave-one-out consensus and find outliers
        all_keys = set()
        for bk in book_devigged:
            all_keys.update(book_devigged[bk].keys())

        for eval_book in book_devigged:
            if eval_book not in book_pairs:
                continue

            for key in all_keys:
                if key not in book_pairs[eval_book]:
                    continue
                if key not in book_devigged.get(eval_book, {}):
                    continue

                # Leave-one-out consensus: average fair prob from all OTHER books
                other_fairs = []
                for other_bk in book_devigged:
                    if other_bk == eval_book:
                        continue
                    if key in book_devigged[other_bk]:
                        other_fairs.append(book_devigged[other_bk][key])

                if len(other_fairs) < 2:
                    continue

                consensus_fair = sum(other_fairs) / len(other_fairs)
                eval_implied = american_to_implied(book_pairs[eval_book][key])
                eval_fair = book_devigged[eval_book][key]

                # Net edge: consensus fair - eval book's juiced implied
                net_edge = (consensus_fair - eval_implied) * 100
                # Gross edge: consensus fair - eval book's devigged fair
                gross_edge = (consensus_fair - eval_fair) * 100

                juice_pct = book_juice.get(eval_book, 0)

                if net_edge < MIN_EDGE_NET:
                    if net_edge > -3:
                        name, point = key
                        dn = f"{name} {point:+.1f}" if point is not None else f"{name} ML"
                        near_misses.append((round(net_edge, 1), dn,
                            BOOK_DISPLAY.get(eval_book, eval_book), game_info))
                    continue

                name, point = key
                if point is not None:
                    display_name = f"{name} {point}" if 'Total' in market_name else f"{name} {point:+.1f}"
                else:
                    display_name = f"{name} ML"

                fair_american = implied_to_american(consensus_fair)
                odds = book_pairs[eval_book][key]

                opportunities.append({
                    'player': display_name,
                    'game': game_info,
                    'market': market_name,
                    'book': BOOK_DISPLAY.get(eval_book, eval_book),
                    'type': 'game_market',
                    'edge': round(net_edge, 1),
                    'gross_edge': round(gross_edge, 1),
                    'recommendation': f"BET {display_name}",
                    'odds': odds,
                    'label1_name': f'{BOOK_DISPLAY.get(eval_book, eval_book)} Odds',
                    'label1_value': format_american(odds),
                    'label2_name': f'Fair Odds ({len(other_fairs)} books)',
                    'label2_value': format_american(fair_american),
                    'label3_name': 'Net Edge',
                    'label3_value': f"+{net_edge:.1f}%",
                    'target_prob': round(eval_implied * 100, 1),
                    'fair_prob': round(consensus_fair * 100, 1),
                    'juice_display': f"{juice_pct}%",
                    'consensus_books': len(other_fairs),
                })

    log_debug(f"    {games_checked} games, {len(book_devigged)} books w/data, "
              f"{len(opportunities)} +EV bets")

    if near_misses:
        near_misses.sort(key=lambda x: x[0], reverse=True)
        log_debug(f"    Near misses:")
        for edge, name, book, gm in near_misses[:5]:
            log_debug(f"      {edge:+.1f}% {name} on {book} ({gm})")

    return opportunities


# ============================================================
# PLAYER PROP ANALYSIS — Any book vs consensus of all others
# ============================================================

def analyze_player_props(games_data, market_name=""):
    if not games_data:
        return []

    opportunities = []
    near_misses = []
    stats = {'players': 0, 'same_line': 0, 'diff_line': 0, 'too_few_books': 0}

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Collect: {player: {book: {line, over_odds, under_odds}}}
        players = {}
        for bookmaker in game.get('bookmakers', []):
            bk = bookmaker['key']
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
                    if bk not in players[player]['books']:
                        players[player]['books'][bk] = {'line': line}
                    if 'over' in side:
                        players[player]['books'][bk]['over_odds'] = odds
                    elif 'under' in side:
                        players[player]['books'][bk]['under_odds'] = odds
                    players[player]['books'][bk]['line'] = line

        for player, data in players.items():
            stats['players'] += 1
            books = data['books']

            # Find books with both sides for devigging
            books_with_both = {bk: bdata for bk, bdata in books.items()
                               if 'over_odds' in bdata and 'under_odds' in bdata}

            if len(books_with_both) < 3:
                stats['too_few_books'] += 1
                continue

            # Group by line — only compare books on the same line
            line_groups = {}
            for bk, bdata in books_with_both.items():
                line = bdata['line']
                # Round to nearest 0.25 for grouping
                rounded = round(line * 4) / 4
                if rounded not in line_groups:
                    line_groups[rounded] = {}
                line_groups[rounded][bk] = bdata

            for line_val, group_books in line_groups.items():
                if len(group_books) < 3:
                    stats['diff_line'] += 1
                    continue

                stats['same_line'] += 1

                # Devig each book at this line
                devigged = {}
                juice_map = {}
                for bk, bdata in group_books.items():
                    ov = american_to_implied(bdata['over_odds'])
                    un = american_to_implied(bdata['under_odds'])
                    juice_map[bk] = round((ov + un - 1.0) * 100, 1)
                    fo, fu = devig_pair(ov, un)
                    devigged[bk] = {'over': fo, 'under': fu}

                # For each book, leave-one-out consensus
                for eval_book in group_books:
                    other_over_fairs = []
                    for other_bk in devigged:
                        if other_bk == eval_book:
                            continue
                        other_over_fairs.append(devigged[other_bk]['over'])

                    if len(other_over_fairs) < 2:
                        continue

                    consensus_over = sum(other_over_fairs) / len(other_over_fairs)
                    consensus_under = 1.0 - consensus_over

                    eb = group_books[eval_book]
                    eval_over_imp = american_to_implied(eb['over_odds'])
                    eval_under_imp = american_to_implied(eb['under_odds'])
                    eval_over_fair = devigged[eval_book]['over']
                    eval_under_fair = devigged[eval_book]['under']
                    juice_pct = juice_map[eval_book]

                    for side, eval_imp, eval_fair, consensus_fair, odds in [
                        ('OVER', eval_over_imp, eval_over_fair, consensus_over, eb['over_odds']),
                        ('UNDER', eval_under_imp, eval_under_fair, consensus_under, eb['under_odds']),
                    ]:
                        net_edge = (consensus_fair - eval_imp) * 100
                        gross_edge = (consensus_fair - eval_fair) * 100

                        if net_edge >= MIN_EDGE_NET:
                            fair_odds = implied_to_american(consensus_fair)
                            opportunities.append({
                                'player': player,
                                'game': data['game'],
                                'market': market_name,
                                'book': BOOK_DISPLAY.get(eval_book, eval_book),
                                'type': 'player_prop',
                                'edge': round(net_edge, 1),
                                'gross_edge': round(gross_edge, 1),
                                'recommendation': f"{side} {line_val}",
                                'odds': odds,
                                'label1_name': f'{BOOK_DISPLAY.get(eval_book, eval_book)} Odds',
                                'label1_value': format_american(odds),
                                'label2_name': f'Fair Odds ({len(other_over_fairs)} books)',
                                'label2_value': format_american(fair_odds),
                                'label3_name': 'Net Edge',
                                'label3_value': f"+{net_edge:.1f}%",
                                'target_prob': round(eval_imp * 100, 1),
                                'fair_prob': round(consensus_fair * 100, 1),
                                'juice_display': f"{juice_pct}%",
                                'consensus_books': len(other_over_fairs),
                            })
                        elif net_edge > -3:
                            near_misses.append((round(net_edge, 1), player,
                                BOOK_DISPLAY.get(eval_book, eval_book), side, line_val))

    log_debug(f"    Players: {stats['players']}, same-line groups: {stats['same_line']}, "
              f"too few books: {stats['too_few_books']}, diff-line skipped: {stats['diff_line']}")
    log_debug(f"    Results: {len(opportunities)} +EV bets")

    if near_misses:
        near_misses.sort(key=lambda x: x[0], reverse=True)
        log_debug(f"    Near misses:")
        for edge, name, book, side, line in near_misses[:5]:
            log_debug(f"      {edge:+.1f}% {name} {side} {line} on {book}")

    return opportunities


# ============================================================
# EVENT-LEVEL PROP SCANNING
# ============================================================

def fetch_event_props(sport, prop_markets, max_events=8):
    all_opps = []
    events = fetch_events(sport)
    if not events:
        return []

    events_to_scan = events[:max_events]
    log_debug(f"  Scanning {len(events_to_scan)} of {len(events)} {sport} events")

    for event in events_to_scan:
        eid = event.get('id')
        home = event.get('home_team', '?')
        away = event.get('away_team', '?')

        for prop_market, prop_name in prop_markets:
            if len(_dead_keys) >= len(API_KEYS):
                log_debug("    All keys exhausted — stopping")
                return all_opps
            edata = fetch_event_odds(sport, eid, prop_market)
            if edata and edata.get('bookmakers'):
                opps = analyze_player_props([edata], prop_name)
                if opps:
                    all_opps.extend(opps)
                    log_debug(f"    {away} @ {home} / {prop_name}: {len(opps)} +EV")
            time.sleep(0.3)

    log_debug(f"  {sport} props: {len(all_opps)} +EV bets")
    return all_opps


# ============================================================
# MAIN SCAN
# ============================================================

def scan_markets():
    global _dead_keys
    state['scanning'] = True
    state['debug_info'] = []
    _dead_keys = set()
    all_opps = []

    log_debug("=== SCAN STARTED ===")
    log_debug(f"Books: {', '.join(BOOK_DISPLAY.get(b, b) for b in ALL_BOOKS)}")
    log_debug(f"Strategy: any book vs leave-one-out consensus | Min edge: {MIN_EDGE_NET}%")

    # 1. Player props (event-level)
    log_debug("--- Player Props ---")
    for sport, prop_markets, max_ev in PROP_MARKETS:
        if len(_dead_keys) >= len(API_KEYS):
            log_debug("  All keys exhausted — stopping")
            break
        opps = fetch_event_props(sport, prop_markets, max_events=max_ev)
        all_opps.extend(opps)

    # 2. Moneylines (bulk)
    log_debug("--- Moneylines ---")
    for sport, market, name in GAME_MARKETS:
        if len(_dead_keys) >= len(API_KEYS):
            log_debug("  All keys exhausted — stopping")
            break
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            if opps:
                all_opps.extend(opps)
        time.sleep(0.3)

    all_opps.sort(key=lambda x: x['edge'], reverse=True)

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    active = len(API_KEYS) - len(_dead_keys)
    log_debug(f"=== DONE: {len(all_opps)} +EV bets ({active}/{len(API_KEYS)} keys active) ===")


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
    results = []
    for key in API_KEYS:
        try:
            r = requests.get("https://api.the-odds-api.com/v4/sports",
                             params={'apiKey': key}, timeout=10)
            results.append({
                'key': f'...{key[-6:]}',
                'status': 'ok' if r.status_code == 200 else 'exhausted',
                'remaining': r.headers.get('x-requests-remaining', '?'),
                'used': r.headers.get('x-requests-used', '?'),
            })
        except Exception as e:
            results.append({'key': f'...{key[-6:]}', 'status': 'error'})
    return jsonify({'keys': results})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
