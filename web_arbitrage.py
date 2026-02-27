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

# Colorado-legal books you can actually bet on
CO_BETTABLE = [
    'fanduel', 'draftkings', 'betmgm', 'betrivers',
    'espnbet', 'hardrockbet', 'ballybet',
]

# Non-CO books added purely for better fair odds consensus (≤10 total = 1 region cost)
CONSENSUS_ONLY = ['pinnacle', 'bovada', 'betonlineag']

# All books pulled from API (10 = 1 region, same API cost as before)
ALL_BOOKS = CO_BETTABLE + CONSENSUS_ONLY

BOOK_DISPLAY = {
    'fanduel': 'FanDuel',
    'draftkings': 'DraftKings',
    'betmgm': 'BetMGM',
    'betrivers': 'BetRivers',
    'espnbet': 'theScore Bet',
    'hardrockbet': 'Hard Rock',
    'ballybet': 'Bally Bet',
    'pinnacle': 'Pinnacle',
    'bovada': 'Bovada',
    'betonlineag': 'BetOnline',
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

# Pinnacle gets 3x weight in consensus — sharpest lines due to
# unlimited sharp action, lowest vig, independent pricing
BOOK_WEIGHT = {
    'pinnacle': 3,
}
# All other books default to weight 1

def get_weight(book_key):
    return BOOK_WEIGHT.get(book_key, 1)

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

def quarter_kelly(fair_prob, american_odds):
    """Quarter-Kelly bet size as fraction of bankroll.
    fair_prob = consensus true probability, american_odds = what the book offers."""
    if american_odds >= 0:
        b = american_odds / 100.0  # net win per $1 risked
    else:
        b = 100.0 / abs(american_odds)
    p = fair_prob
    q = 1.0 - p
    kelly = (b * p - q) / b
    if kelly <= 0:
        return 0
    return kelly * 0.25  # quarter-Kelly

DEFAULT_BANKROLL = 3000


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
            if eval_book not in CO_BETTABLE:
                continue  # Only show bets on CO-legal books

            for key in all_keys:
                if key not in book_pairs[eval_book]:
                    continue
                if key not in book_devigged.get(eval_book, {}):
                    continue

                # Leave-one-out weighted consensus: Pinnacle 3x, others 1x
                other_fairs = []
                other_weights = []
                for other_bk in book_devigged:
                    if other_bk == eval_book:
                        continue
                    if key in book_devigged[other_bk]:
                        w = get_weight(other_bk)
                        other_fairs.append(book_devigged[other_bk][key])
                        other_weights.append(w)

                if len(other_fairs) < 2:
                    continue

                total_weight = sum(other_weights)
                consensus_fair = sum(f * w for f, w in zip(other_fairs, other_weights)) / total_weight
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
                kf = quarter_kelly(consensus_fair, odds)

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
                    'kelly_fraction': round(kf * 100, 2),
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

                # For each book, leave-one-out weighted consensus
                for eval_book in group_books:
                    if eval_book not in CO_BETTABLE:
                        continue  # Only show bets on CO-legal books
                    other_over_fairs = []
                    other_weights = []
                    for other_bk in devigged:
                        if other_bk == eval_book:
                            continue
                        w = get_weight(other_bk)
                        other_over_fairs.append(devigged[other_bk]['over'])
                        other_weights.append(w)

                    if len(other_over_fairs) < 2:
                        continue

                    total_weight = sum(other_weights)
                    consensus_over = sum(f * w for f, w in zip(other_over_fairs, other_weights)) / total_weight
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
                            kf = quarter_kelly(consensus_fair, odds)
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
                                'kelly_fraction': round(kf * 100, 2),
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
# ARBITRAGE DETECTION — find guaranteed-profit cross-book pairs
# ============================================================

def find_game_arbs(games_data, market_name=""):
    """Check every pair of books for arb: Side A on Book1 + Side B on Book2 < 100%"""
    if not games_data:
        return []

    arbs = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"

        # Collect: {book: {outcome_key: odds}}
        book_odds = {}
        for bookmaker in game.get('bookmakers', []):
            bk = bookmaker['key']
            book_odds[bk] = {}
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    name = outcome.get('name', '')
                    odds = outcome.get('price')
                    point = outcome.get('point')
                    if not name or odds is None:
                        continue
                    key = (name, float(point) if point is not None else None)
                    book_odds[bk][key] = odds

        # Get all unique outcome keys
        all_keys = set()
        for bk in book_odds:
            all_keys.update(book_odds[bk].keys())

        # For 2-way markets, find the two sides
        keys_list = sorted(all_keys, key=str)
        if len(keys_list) < 2:
            continue

        # Pair them: for h2h it's (Team A, None) and (Team B, None)
        side_a_key = keys_list[0]
        side_b_key = keys_list[1]

        # Check every pair of books: best Side A from any book + best Side B from any book
        best_a_odds = None
        best_a_book = None
        best_b_odds = None
        best_b_book = None

        for bk in book_odds:
            if bk not in CO_BETTABLE:
                continue  # Arbs only between CO-legal books
            if side_a_key in book_odds[bk]:
                odds_a = book_odds[bk][side_a_key]
                if best_a_odds is None or american_to_implied(odds_a) < american_to_implied(best_a_odds):
                    best_a_odds = odds_a
                    best_a_book = bk
            if side_b_key in book_odds[bk]:
                odds_b = book_odds[bk][side_b_key]
                if best_b_odds is None or american_to_implied(odds_b) < american_to_implied(best_b_odds):
                    best_b_odds = odds_b
                    best_b_book = bk

        if best_a_odds is None or best_b_odds is None:
            continue
        if best_a_book == best_b_book:
            continue  # Same book can't arb against itself

        imp_a = american_to_implied(best_a_odds)
        imp_b = american_to_implied(best_b_odds)
        total = imp_a + imp_b

        if total < 1.0:
            profit_pct = round((1.0 - total) * 100, 2)
            name_a, point_a = side_a_key
            name_b, point_b = side_b_key
            dn_a = f"{name_a} ML" if point_a is None else f"{name_a} {point_a:+.1f}"
            dn_b = f"{name_b} ML" if point_b is None else f"{name_b} {point_b:+.1f}"

            # Calculate optimal stakes for $100 total
            stake_a = round(100 * imp_a / (imp_a + imp_b), 2)
            stake_b = round(100 - stake_a, 2)

            arbs.append({
                'player': f"ARB: {dn_a} + {dn_b}",
                'game': game_info,
                'market': market_name,
                'book': f"{BOOK_DISPLAY.get(best_a_book, best_a_book)} / {BOOK_DISPLAY.get(best_b_book, best_b_book)}",
                'type': 'arbitrage',
                'edge': profit_pct,
                'gross_edge': profit_pct,
                'recommendation': f"Guaranteed {profit_pct}% profit",
                'odds': best_a_odds,
                'label1_name': f'{BOOK_DISPLAY.get(best_a_book, best_a_book)}: {dn_a}',
                'label1_value': format_american(best_a_odds),
                'label2_name': f'{BOOK_DISPLAY.get(best_b_book, best_b_book)}: {dn_b}',
                'label2_value': format_american(best_b_odds),
                'label3_name': 'Guaranteed Profit',
                'label3_value': f"+{profit_pct}%",
                'target_prob': round(total * 100, 1),
                'fair_prob': 100.0,
                'juice_display': f"{round(total * 100, 1)}% combined",
                'stake_a': stake_a,
                'stake_b': stake_b,
            })

    if arbs:
        log_debug(f"    🔥 {len(arbs)} ARBITRAGE opportunities found!")
    return arbs


def find_prop_arbs(games_data, market_name=""):
    """Check every pair of books for prop arbs: Over on Book1 + Under on Book2 < 100%"""
    if not games_data:
        return []

    arbs = []

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
            books = data['books']

            # Group by same line
            line_groups = {}
            for bk, bdata in books.items():
                if 'over_odds' not in bdata or 'under_odds' not in bdata:
                    continue
                line = bdata['line']
                rounded = round(line * 4) / 4
                if rounded not in line_groups:
                    line_groups[rounded] = {}
                line_groups[rounded][bk] = bdata

            for line_val, group_books in line_groups.items():
                if len(group_books) < 2:
                    continue

                # Find best Over odds and best Under odds across different books
                best_over_odds = None
                best_over_book = None
                best_under_odds = None
                best_under_book = None

                for bk, bdata in group_books.items():
                    if bk not in CO_BETTABLE:
                        continue  # Arbs only between CO-legal books
                    ov_imp = american_to_implied(bdata['over_odds'])
                    un_imp = american_to_implied(bdata['under_odds'])

                    if best_over_odds is None or ov_imp < american_to_implied(best_over_odds):
                        best_over_odds = bdata['over_odds']
                        best_over_book = bk
                    if best_under_odds is None or un_imp < american_to_implied(best_under_odds):
                        best_under_odds = bdata['under_odds']
                        best_under_book = bk

                if best_over_book == best_under_book:
                    continue
                if best_over_odds is None or best_under_odds is None:
                    continue

                imp_over = american_to_implied(best_over_odds)
                imp_under = american_to_implied(best_under_odds)
                total = imp_over + imp_under

                if total < 1.0:
                    profit_pct = round((1.0 - total) * 100, 2)

                    stake_over = round(100 * imp_over / (imp_over + imp_under), 2)
                    stake_under = round(100 - stake_over, 2)

                    arbs.append({
                        'player': f"ARB: {player}",
                        'game': data['game'],
                        'market': market_name,
                        'book': f"{BOOK_DISPLAY.get(best_over_book, best_over_book)} / {BOOK_DISPLAY.get(best_under_book, best_under_book)}",
                        'type': 'arbitrage',
                        'edge': profit_pct,
                        'gross_edge': profit_pct,
                        'recommendation': f"OVER {line_val} + UNDER {line_val}",
                        'odds': best_over_odds,
                        'label1_name': f'{BOOK_DISPLAY.get(best_over_book, best_over_book)}: Over {line_val}',
                        'label1_value': format_american(best_over_odds),
                        'label2_name': f'{BOOK_DISPLAY.get(best_under_book, best_under_book)}: Under {line_val}',
                        'label2_value': format_american(best_under_odds),
                        'label3_name': 'Guaranteed Profit',
                        'label3_value': f"+{profit_pct}%",
                        'target_prob': round(total * 100, 1),
                        'fair_prob': 100.0,
                        'juice_display': f"{round(total * 100, 1)}% combined",
                        'stake_over': stake_over,
                        'stake_under': stake_under,
                    })

    if arbs:
        log_debug(f"    🔥 {len(arbs)} PROP ARBITRAGE opportunities found!")
    return arbs


# ============================================================
# EVENT-LEVEL PROP SCANNING
# ============================================================

def fetch_event_props(sport, prop_markets, max_events=8):
    all_opps = []
    all_arbs = []
    events = fetch_events(sport)
    if not events:
        return [], []

    events_to_scan = events[:max_events]
    log_debug(f"  Scanning {len(events_to_scan)} of {len(events)} {sport} events")

    for event in events_to_scan:
        eid = event.get('id')
        home = event.get('home_team', '?')
        away = event.get('away_team', '?')

        for prop_market, prop_name in prop_markets:
            if len(_dead_keys) >= len(API_KEYS):
                log_debug("    All keys exhausted — stopping")
                return all_opps, all_arbs
            edata = fetch_event_odds(sport, eid, prop_market)
            if edata and edata.get('bookmakers'):
                opps = analyze_player_props([edata], prop_name)
                arbs = find_prop_arbs([edata], prop_name)
                if opps:
                    all_opps.extend(opps)
                    log_debug(f"    {away} @ {home} / {prop_name}: {len(opps)} +EV")
                if arbs:
                    all_arbs.extend(arbs)
            time.sleep(0.3)

    log_debug(f"  {sport} props: {len(all_opps)} +EV, {len(all_arbs)} arbs")
    return all_opps, all_arbs


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
    log_debug(f"Bettable: {', '.join(BOOK_DISPLAY.get(b, b) for b in CO_BETTABLE)}")
    log_debug(f"Consensus: + {', '.join(BOOK_DISPLAY.get(b, b) for b in CONSENSUS_ONLY)}")
    log_debug(f"Strategy: CO book vs weighted consensus (Pinnacle 3x) | Min edge: {MIN_EDGE_NET}%")

    # 1. Player props (event-level) — +EV and arbs
    log_debug("--- Player Props ---")
    for sport, prop_markets, max_ev in PROP_MARKETS:
        if len(_dead_keys) >= len(API_KEYS):
            log_debug("  All keys exhausted — stopping")
            break
        opps, arbs = fetch_event_props(sport, prop_markets, max_events=max_ev)
        all_opps.extend(opps)
        all_opps.extend(arbs)

    # 2. Moneylines (bulk) — +EV and arbs
    log_debug("--- Moneylines ---")
    for sport, market, name in GAME_MARKETS:
        if len(_dead_keys) >= len(API_KEYS):
            log_debug("  All keys exhausted — stopping")
            break
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_game_markets(games, name)
            arbs = find_game_arbs(games, name)
            if opps:
                all_opps.extend(opps)
            if arbs:
                all_opps.extend(arbs)
        time.sleep(0.3)

    all_opps.sort(key=lambda x: (-1 if x['type'] == 'arbitrage' else 0, -x['edge']))

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    active = len(API_KEYS) - len(_dead_keys)
    arb_count = len([o for o in all_opps if o['type'] == 'arbitrage'])
    ev_count = len(all_opps) - arb_count
    log_debug(f"=== DONE: {ev_count} +EV bets, {arb_count} arbitrage ({active}/{len(API_KEYS)} keys active) ===")


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
        'debug': state.get('debug_info', []),
        'bankroll': DEFAULT_BANKROLL,
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
