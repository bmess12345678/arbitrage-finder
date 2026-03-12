"""
+EV Finder — Sports, Cross-Exchange, Weather, Economic
Sports: CO-legal books vs weighted consensus (Pinnacle/Kalshi 3x)
Cross-Exchange: Kalshi vs Polymarket price disagreements
Weather: Kalshi temperature contracts vs Open-Meteo forecasts
Economic: Kalshi structural arbitrage on related contracts
"""

from flask import Flask, render_template, jsonify, request
import requests
import re
import time
from datetime import datetime
import threading
import os

app = Flask(__name__)

# Password gate — set SCAN_KEY env var on Render to protect API usage
SCAN_KEY = os.environ.get('SCAN_KEY', '')

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
# NOTE: Caesars (williamhill_us) and Fanatics require paid API tier
CONSENSUS_ONLY = ['pinnacle', 'bovada', 'betonlineag']

# All books pulled from API (10 = 1 region, same API cost)
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
    'kalshi': 'Kalshi',
    'polymarket': 'Polymarket',
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
    ('icehockey_nhl', [
        ('player_points', 'NHL Points'),
        ('player_shots_on_goal', 'NHL Shots on Goal'),
    ], 6),
]

MIN_EDGE_NET = 0.1  # Show any +EV bet

# Pinnacle gets 3x weight in consensus — sharpest lines due to
# unlimited sharp action, lowest vig, independent pricing
BOOK_WEIGHT = {
    'pinnacle': 3,
    'kalshi': 3,   # Exchange-priced, no vig — very sharp signal
    'polymarket': 3,  # Exchange-priced, independent from Kalshi
}
# All other books default to weight 1

def get_weight(book_key):
    return BOOK_WEIGHT.get(book_key, 1)

# ============================================================
# KALSHI INTEGRATION — Authenticated API for higher rate limits
# ============================================================

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.environ.get('KALSHI_API_KEY', '')

def kalshi_headers():
    """Return auth headers for Kalshi API if key is configured."""
    if KALSHI_API_KEY:
        return {
            'Authorization': f'Bearer {KALSHI_API_KEY}',
            'Content-Type': 'application/json',
        }
    return {'Content-Type': 'application/json'}

def kalshi_get(url, params=None, timeout=15):
    """Make authenticated GET request to Kalshi."""
    return requests.get(url, params=params, headers=kalshi_headers(), timeout=timeout)

def kalshi_prices(m):
    """Extract yes_bid and yes_ask from a Kalshi market dict, in cents (int).
    Handles both old format (yes_bid: 50) and new format (yes_bid_dollars: '0.5000')."""
    yb = m.get('yes_bid', 0) or 0
    ya = m.get('yes_ask', 0) or 0
    lp = m.get('last_price', 0) or 0

    # If integer fields are missing/zero, try dollar string fields
    if yb == 0:
        yb_str = m.get('yes_bid_dollars', '') or ''
        if yb_str:
            try:
                yb = round(float(yb_str) * 100)
            except (ValueError, TypeError):
                pass
    if ya == 0:
        ya_str = m.get('yes_ask_dollars', '') or ''
        if ya_str:
            try:
                ya = round(float(ya_str) * 100)
            except (ValueError, TypeError):
                pass
    if lp == 0:
        lp_str = m.get('last_price_dollars', '') or ''
        if lp_str:
            try:
                lp = round(float(lp_str) * 100)
            except (ValueError, TypeError):
                pass

    return int(yb), int(ya), int(lp)

def normalize_player_name(name):
    """Normalize for matching: lowercase, strip suffixes."""
    name = ' '.join(name.strip().split()).lower()
    name = re.sub(r'\s+(jr\.?|sr\.?|ii+|iv|v)$', '', name)
    return name

def kalshi_stat_to_market(stat_str):
    """Map Kalshi stat words to our Odds API market keys."""
    s = stat_str.lower()
    if 'point' in s:
        return 'player_points'
    elif 'rebound' in s:
        return 'player_rebounds'
    elif 'assist' in s:
        return 'player_assists'
    elif 'three' in s or '3-pointer' in s or '3pt' in s:
        return 'player_threes'
    return None

def parse_kalshi_prop(market):
    """Parse a Kalshi market dict → (player, market_type, line, over_prob) or None."""
    title = market.get('title', '')
    subtitle = market.get('subtitle', '') or ''
    yes_sub = market.get('yes_sub_title', '') or ''
    event_ticker = market.get('event_ticker', '') or ''
    series_ticker = market.get('series_ticker', '') or ''

    # Reject any title with a comma — always a combo
    if ',' in title:
        return None

    # Fair probability from mid-market price
    yes_bid = market.get('yes_bid', 0) or 0
    yes_ask = market.get('yes_ask', 0) or 0

    if yes_bid > 0 and yes_ask > 0:
        mid_prob = ((yes_bid + yes_ask) / 2) / 100.0
    elif yes_bid > 0:
        mid_prob = yes_bid / 100.0
    elif yes_ask > 0:
        mid_prob = yes_ask / 100.0
    else:
        return None

    if mid_prob <= 0.02 or mid_prob >= 0.98:
        return None

    full_text = f"{title} {subtitle} {yes_sub}"

    def _valid_name(n):
        """Reject names that are clearly combos or garbage."""
        n = n.strip()
        if len(n) < 4 or len(n) > 40:
            return False
        if n.count(' ') > 4:
            return False
        if ',' in n or 'yes' in n.lower() or 'no ' in n.lower():
            return False
        return True

    def _stat_from_context():
        """Guess stat type from event/series ticker."""
        ctx = f"{event_ticker} {series_ticker} {subtitle}".lower()
        if 'point' in ctx or 'pts' in ctx:
            return 'player_points'
        elif 'rebound' in ctx or 'reb' in ctx:
            return 'player_rebounds'
        elif 'assist' in ctx or 'ast' in ctx:
            return 'player_assists'
        elif 'three' in ctx or '3pt' in ctx or '3p' in ctx:
            return 'player_threes'
        return None

    # Pattern 1: "Will [Player] score/have/record [X]+ [stat]"
    m = re.search(
        r'(?:Will\s+)?(.+?)\s+(?:score|have|record|get|make)\s+(\d+(?:\.\d+)?)\+?\s*'
        r'(points?|rebounds?|assists?|three[- ]?pointers?|3[- ]?pointers?|threes?)',
        full_text, re.IGNORECASE
    )
    if m:
        player = m.group(1).strip().rstrip('?')
        line_raw = float(m.group(2))
        stat = m.group(3)
        # "15+ points" means >= 15 = Over 14.5
        if line_raw == int(line_raw) and '.' not in m.group(2):
            line = line_raw - 0.5
        else:
            line = line_raw
        mkt = kalshi_stat_to_market(stat)
        if mkt and _valid_name(player):
            return (player, mkt, line, mid_prob)

    # Pattern 2: "[Player] Over [X] [stat]"
    m = re.search(
        r'(.+?)\s+[Oo]ver\s+(\d+(?:\.\d+)?)\s*(points?|rebounds?|assists?)',
        full_text, re.IGNORECASE
    )
    if m:
        player = m.group(1).strip()
        line = float(m.group(2))
        stat = m.group(3)
        mkt = kalshi_stat_to_market(stat)
        if mkt and _valid_name(player):
            return (player, mkt, line, mid_prob)

    # Pattern 3: Check if title has player name + subtitle has "X+ points" style
    m = re.search(r'(\d+(?:\.\d+)?)\+?\s*(points?|rebounds?|assists?)', full_text, re.IGNORECASE)
    if m:
        line_raw = float(m.group(1))
        stat = m.group(2)
        # Try to extract player from title (everything before the number)
        idx = full_text.lower().find(m.group(0).lower())
        if idx > 2:
            player = full_text[:idx].strip().rstrip(' -–—')
            player = re.sub(r'^(will|can)\s+', '', player, flags=re.IGNORECASE).strip()
            if _valid_name(player):
                if line_raw == int(line_raw) and '.' not in m.group(1):
                    line = line_raw - 0.5
                else:
                    line = line_raw
                mkt = kalshi_stat_to_market(stat)
                if mkt:
                    return (player, mkt, line, mid_prob)

    # Pattern 4: Kalshi compact format — "Player: N+" or "Player: N+ [stat]"
    m = re.search(r'^(.+?):\s*(\d+(?:\.\d+)?)\+\s*(points?|rebounds?|assists?)?', title.strip(), re.IGNORECASE)
    if m:
        player = m.group(1).strip()
        line_raw = float(m.group(2))
        stat_word = m.group(3)
        if stat_word:
            mkt = kalshi_stat_to_market(stat_word)
        else:
            mkt = _stat_from_context()
        if mkt and _valid_name(player):
            if line_raw == int(line_raw):
                line = line_raw - 0.5
            else:
                line = line_raw
            return (player, mkt, line, mid_prob)

    # Pattern 5: Check yes_sub_title for "Player: N+"
    m = re.search(r'^(.+?):\s*(\d+(?:\.\d+)?)\+\s*(points?|rebounds?|assists?)?', yes_sub.strip(), re.IGNORECASE)
    if m:
        player = m.group(1).strip()
        line_raw = float(m.group(2))
        stat_word = m.group(3)
        if stat_word:
            mkt = kalshi_stat_to_market(stat_word)
        else:
            mkt = _stat_from_context()
        if mkt and _valid_name(player):
            if line_raw == int(line_raw):
                line = line_raw - 0.5
            else:
                line = line_raw
            return (player, mkt, line, mid_prob)

    return None


def fetch_kalshi_sports(log_fn=None):
    """
    Fetch Kalshi sports markets (props + game outcomes).
    Returns: {'props': {...}, 'games': {team_name: win_prob}}
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    result = {'props': {}, 'games': {}}
    total_parsed = 0
    total_fetched = 0
    games_matched = 0

    try:
        # Use /markets endpoint — paginate through open markets
        # Filter client-side for basketball props by title parsing
        cursor = ''
        all_props = []

        for page in range(20):  # Up to 20 pages of 200
            params = {
                'status': 'open',
                'limit': 200,
            }
            if cursor:
                params['cursor'] = cursor

            resp = kalshi_get(f"{KALSHI_API}/markets", params=params, timeout=20)
            if resp.status_code == 429:
                log(f"  Kalshi API: rate limited on page {page+1}, waiting 3s...")
                time.sleep(3)
                resp = kalshi_get(f"{KALSHI_API}/markets", params=params, timeout=20)
                if resp.status_code == 429:
                    log(f"  Kalshi API: still rate limited, using {total_fetched} markets so far")
                    break
            if resp.status_code != 200:
                log(f"  Kalshi API: HTTP {resp.status_code}")
                break

            data = resp.json()
            markets = data.get('markets', [])
            total_fetched += len(markets)
            if not markets:
                break

            for mkt in markets:
                title = (mkt.get('title', '') or '').lower()
                subtitle = (mkt.get('subtitle', '') or '').lower()
                full = f"{title} {subtitle}"

                # Skip ANY title with a comma — always a combo/parlay
                if ',' in title:
                    continue

                # Check for single-game outcome (not futures/championship)
                futures_kws = ['championship', 'champion', 'title', 'finals', 'playoff',
                               'season', 'mvp', 'award', 'division', 'conference']
                is_futures = any(kw in title for kw in futures_kws)
                if not is_futures:
                    game_match = re.search(
                        r'(?:will\s+)?(?:the\s+)?(.+?)\s+(?:wins?|beats?|defeats?)\s+(?:the\s+)?(.+)',
                        title, re.IGNORECASE
                    )
                    if game_match:
                        team_str = game_match.group(1).strip().lower()
                        opponent_str = game_match.group(2).strip().lower()
                        # Must have a recognizable opponent
                        has_opponent = any(key in opponent_str for key in NBA_TEAMS.keys())
                        if has_opponent:
                            yb = mkt.get('yes_bid', 0) or 0
                            ya = mkt.get('yes_ask', 0) or 0
                            if yb > 0 or ya > 0:
                                mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0
                                if 0.05 < mid < 0.95:
                                    for key, full_name in NBA_TEAMS.items():
                                        if key in team_str:
                                            result['games'][full_name] = mid
                                            games_matched += 1
                                            break

                # Quick filter: look for player prop keywords
                has_stat = any(kw in full for kw in ['points', 'rebounds', 'assists', 'three-pointer', '3-pointer'])
                has_action = any(kw in full for kw in ['score', 'have', 'record', 'over', 'get', 'make'])
                # Also catch "Player: N+" format (Kalshi's compact style)
                has_compact = bool(re.search(r':\s*\d+\+', full))
                if has_stat and (has_action or '+' in full):
                    all_props.append(mkt)
                elif has_compact:
                    all_props.append(mkt)

            cursor = data.get('cursor', '')
            if not cursor:
                break
            time.sleep(1.0)  # Respect Kalshi's rate limiter

        log(f"  Kalshi: scanned {total_fetched} open markets, {len(all_props)} look like player props")

        # If nothing matched, sample some titles for debugging
        if len(all_props) == 0 and total_fetched > 0:
            log(f"  Kalshi: no props matched keyword filter — sampling raw titles:")
            # Re-fetch a small batch just to show some titles
            try:
                sample_resp = kalshi_get(f"{KALSHI_API}/markets",
                    params={'status': 'open', 'limit': 20}, timeout=10)
                if sample_resp.status_code == 200:
                    for m in sample_resp.json().get('markets', [])[:10]:
                        log(f"    raw: '{m.get('title','')}' | sub='{m.get('subtitle','')}'")
            except:
                pass

        # Parse each candidate
        for mkt in all_props:
            parsed = parse_kalshi_prop(mkt)
            if parsed:
                player, market_type, line, over_prob = parsed
                norm = normalize_player_name(player)
                if norm not in result["props"]:
                    result["props"][norm] = {}
                if market_type not in result["props"][norm]:
                    result["props"][norm][market_type] = {}
                result["props"][norm][market_type][line] = over_prob
                total_parsed += 1

        log(f"  Kalshi: {total_parsed} player props parsed for {len(result['props'])} players")
        log(f"  Kalshi: {games_matched} game outcomes matched for {len(result['games'])} teams")
        for team, prob in list(result['games'].items())[:3]:
            log(f"    → {team}: {prob*100:.0f}% to win")

        # If 0 parsed, dump full fields from first candidate for debugging
        if total_parsed == 0 and all_props:
            m0 = all_props[0]
            log(f"  Kalshi DEBUG — all fields of first candidate:")
            for k, v in m0.items():
                if v and str(v) != '0' and str(v) != 'False':
                    log(f"    {k}: {str(v)[:120]}")

        # Log unparsed candidates so we can see the actual format
        unparsed = [m for m in all_props if parse_kalshi_prop(m) is None]
        if unparsed:
            log(f"  Kalshi: {len(unparsed)} prop candidates NOT parsed — sample titles:")
            for m in unparsed[:5]:
                t = m.get('title', '?')
                s = m.get('subtitle', '')
                ys = m.get('yes_sub_title', '')
                et = m.get('event_ticker', '')
                st = m.get('series_ticker', '')
                yb = m.get('yes_bid', 0)
                ya = m.get('yes_ask', 0)
                log(f"    ✗ title='{t}' sub='{s}' yes_sub='{ys[:60]}' et='{et}' st='{st}' bid={yb} ask={ya}")

        # Log first few successful parses
        for p in list(result['props'].keys())[:3]:
            for mt in result['props'][p]:
                for ln, prob in result['props'][p][mt].items():
                    log(f"    → {p}: {mt} Over {ln} = {prob*100:.0f}%")

    except Exception as e:
        log(f"  Kalshi error: {e}")

    return result

POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"

# NBA team name variations for matching
NBA_TEAMS = {
    'hawks': 'Atlanta Hawks', 'celtics': 'Boston Celtics', 'nets': 'Brooklyn Nets',
    'hornets': 'Charlotte Hornets', 'bulls': 'Chicago Bulls', 'cavaliers': 'Cleveland Cavaliers',
    'mavericks': 'Dallas Mavericks', 'nuggets': 'Denver Nuggets', 'pistons': 'Detroit Pistons',
    'warriors': 'Golden State Warriors', 'rockets': 'Houston Rockets', 'pacers': 'Indiana Pacers',
    'clippers': 'Los Angeles Clippers', 'lakers': 'Los Angeles Lakers', 'grizzlies': 'Memphis Grizzlies',
    'heat': 'Miami Heat', 'bucks': 'Milwaukee Bucks', 'timberwolves': 'Minnesota Timberwolves',
    'pelicans': 'New Orleans Pelicans', 'knicks': 'New York Knicks', 'thunder': 'Oklahoma City Thunder',
    'magic': 'Orlando Magic', '76ers': 'Philadelphia 76ers', 'sixers': 'Philadelphia 76ers',
    'suns': 'Phoenix Suns', 'trail blazers': 'Portland Trail Blazers', 'blazers': 'Portland Trail Blazers',
    'kings': 'Sacramento Kings', 'spurs': 'San Antonio Spurs', 'raptors': 'Toronto Raptors',
    'jazz': 'Utah Jazz', 'wizards': 'Washington Wizards',
}

def fetch_polymarket_sports(log_fn=None):
    """
    Fetch Polymarket sports markets.
    Returns dict with:
      'games': {normalized_team_name: yes_probability}  — for moneyline consensus
      'props': {normalized_player: {market_type: {line: over_prob}}} — for prop consensus
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    result = {'games': {}, 'props': {}}

    try:
        resp = requests.get(f"{POLYMARKET_GAMMA}/markets",
            params={'closed': 'false', 'limit': 500, 'active': 'true', 'tag': 'sports'},
            timeout=15)

        if resp.status_code != 200:
            # Try without tag filter
            resp = requests.get(f"{POLYMARKET_GAMMA}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'},
                timeout=15)

        if resp.status_code != 200:
            log(f"  Polymarket API: HTTP {resp.status_code}")
            return result

        import json as _json
        markets = resp.json() if isinstance(resp.json(), list) else []
        sports_count = 0

        for m in markets:
            q = (m.get('question', '') or '').strip()
            q_low = q.lower()

            outcomes = m.get('outcomes', '[]')
            prices = m.get('outcomePrices', '[]')
            if isinstance(outcomes, str):
                try:
                    outcomes = _json.loads(outcomes)
                    prices = _json.loads(prices)
                except:
                    continue
            if len(outcomes) < 2 or len(prices) < 2:
                continue
            try:
                yes_price = float(prices[0])
            except:
                continue
            if yes_price <= 0.02 or yes_price >= 0.98:
                continue

            # Check if it's a SINGLE-GAME outcome (not futures/championship)
            # Futures: "Will the Thunder win the championship/title/NBA"
            # Games: "Will the Thunder beat the Knicks" / "Thunder vs Knicks"
            is_futures = any(kw in q_low for kw in [
                'championship', 'champion', 'title', 'finals', 'playoff',
                'season', 'mvp', 'award', 'division', 'conference',
                'win the nba', 'win the nfl', 'win the nhl',
            ])
            if is_futures:
                continue

            # Game outcome needs to mention an opponent or "game"/"tonight"/"vs"
            game_match = re.search(
                r'(?:will|do)\s+(?:the\s+)?(.+?)\s+(?:win|beat|defeat)\s+(?:the\s+)?(.+?)[\?\.]?$',
                q_low, re.IGNORECASE
            )
            if game_match:
                team_str = game_match.group(1).strip()
                opponent_str = game_match.group(2).strip()
                # Must have an actual opponent (not "the championship")
                has_opponent = any(key in opponent_str for key in NBA_TEAMS.keys())
                if has_opponent:
                    for key, full_name in NBA_TEAMS.items():
                        if key in team_str:
                            result['games'][full_name] = yes_price
                            sports_count += 1
                            break
                continue

            # Check if it's a player prop: "Will [Player] score X+ points"
            prop_match = re.search(
                r'(?:will\s+)?(.+?)\s+(?:score|have|record|get)\s+(\d+(?:\.\d+)?)\+?\s*'
                r'(points?|rebounds?|assists?|three)',
                q_low, re.IGNORECASE
            )
            if prop_match:
                player = prop_match.group(1).strip()
                line_raw = float(prop_match.group(2))
                stat = prop_match.group(3)
                mkt = kalshi_stat_to_market(stat)
                if mkt:
                    if line_raw == int(line_raw):
                        line = line_raw - 0.5
                    else:
                        line = line_raw
                    norm = normalize_player_name(player)
                    if norm not in result['props']:
                        result['props'][norm] = {}
                    if mkt not in result['props'][norm]:
                        result['props'][norm][mkt] = {}
                    result['props'][norm][mkt][line] = yes_price
                    sports_count += 1

        log(f"  Polymarket: {len(markets)} total markets, {sports_count} sports matched")
        log(f"    Games: {len(result['games'])} team outcomes, Props: {len(result['props'])} players")

        for team, prob in list(result['games'].items())[:3]:
            log(f"    → {team}: {prob*100:.0f}% to win")

    except Exception as e:
        log(f"  Polymarket sports error: {e}")

    return result


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

def analyze_game_markets(games_data, market_name="", poly_games=None, kalshi_games=None):
    if not games_data:
        return []

    opportunities = []
    near_misses = []
    games_checked = 0

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')
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

        # Inject exchange game outcomes (Kalshi + Polymarket) into consensus
        home = game.get('home_team', '')
        away = game.get('away_team', '')
        # Find the outcome keys from existing books
        home_key = None
        away_key = None
        for bk in book_devigged:
            for k in book_devigged[bk]:
                name, pt = k
                if name == home:
                    home_key = k
                elif name == away:
                    away_key = k
            if home_key and away_key:
                break

        for exchange_name, exchange_data in [('kalshi', kalshi_games), ('polymarket', poly_games)]:
            if not exchange_data:
                continue
            ex_home = exchange_data.get(home)
            ex_away = exchange_data.get(away)
            if ex_home and home_key and away_key:
                book_devigged[exchange_name] = {
                    home_key: ex_home,
                    away_key: 1.0 - ex_home,
                }
            elif ex_away and home_key and away_key:
                book_devigged[exchange_name] = {
                    away_key: ex_away,
                    home_key: 1.0 - ex_away,
                }

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
                other_detail = []
                for other_bk in book_devigged:
                    if other_bk == eval_book:
                        continue
                    if key in book_devigged[other_bk]:
                        w = get_weight(other_bk)
                        fair = book_devigged[other_bk][key]
                        other_fairs.append(fair)
                        other_weights.append(w)
                        raw_odds = book_pairs.get(other_bk, {}).get(key)
                        other_detail.append({
                            'book': BOOK_DISPLAY.get(other_bk, other_bk),
                            'fair_prob': round(fair * 100, 1),
                            'fair_odds': format_american(implied_to_american(fair)),
                            'raw_odds': format_american(raw_odds) if raw_odds else '—',
                            'weight': w,
                        })

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
                    'commence': commence,
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
                    'consensus_detail': other_detail,
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

def analyze_player_props(games_data, market_name="", kalshi_props=None, poly_props=None, market_key=""):
    if not games_data:
        return []

    # Determine market_key for Kalshi matching
    if not market_key:
        mn = market_name.lower()
        if 'point' in mn:
            market_key = 'player_points'
        elif 'rebound' in mn:
            market_key = 'player_rebounds'
        elif 'assist' in mn:
            market_key = 'player_assists'

    opportunities = []
    near_misses = []
    stats = {'players': 0, 'same_line': 0, 'diff_line': 0, 'too_few_books': 0}

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')

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
                        players[player] = {'game': game_info, 'commence': commence, 'books': {}}
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

                # Inject Kalshi data if available (no devigging needed — exchange price IS fair)
                if kalshi_props and market_key:
                    norm = normalize_player_name(player)
                    if norm in kalshi_props and market_key in kalshi_props[norm]:
                        k_lines = kalshi_props[norm][market_key]
                        if line_val in k_lines:
                            k_over = k_lines[line_val]
                            devigged['kalshi'] = {'over': k_over, 'under': 1.0 - k_over}

                # Inject Polymarket data if available
                if poly_props and market_key:
                    norm = normalize_player_name(player)
                    if norm in poly_props and market_key in poly_props[norm]:
                        p_lines = poly_props[norm][market_key]
                        if line_val in p_lines:
                            p_over = p_lines[line_val]
                            devigged['polymarket'] = {'over': p_over, 'under': 1.0 - p_over}

                # For each book, leave-one-out weighted consensus
                # Evaluate CO books + Kalshi + Polymarket (exchange-priced, bettable)
                exchange_books = [b for b in ['kalshi', 'polymarket'] if b in devigged]
                eval_candidates = list(group_books.keys()) + exchange_books
                for eval_book in eval_candidates:
                    if eval_book not in CO_BETTABLE and eval_book not in ('kalshi', 'polymarket'):
                        continue  # Only show bets on CO-legal books + exchanges
                    other_over_fairs = []
                    other_weights = []
                    other_detail = []
                    for other_bk in devigged:
                        if other_bk == eval_book:
                            continue
                        w = get_weight(other_bk)
                        other_over_fairs.append(devigged[other_bk]['over'])
                        other_weights.append(w)
                        # Get raw odds and compute vig for display
                        if other_bk in group_books:
                            raw_over = format_american(group_books[other_bk].get('over_odds', 0))
                            raw_under = format_american(group_books[other_bk].get('under_odds', 0))
                            vig = juice_map.get(other_bk, 0)
                        elif other_bk in ('kalshi', 'polymarket'):
                            raw_over = f"{devigged[other_bk]['over']*100:.0f}¢"
                            raw_under = f"{devigged[other_bk]['under']*100:.0f}¢"
                            vig = 0
                        else:
                            raw_over = '—'
                            raw_under = '—'
                            vig = 0
                        other_detail.append({
                            'book': BOOK_DISPLAY.get(other_bk, other_bk),
                            'over_prob': round(devigged[other_bk]['over'] * 100, 1),
                            'over_odds': format_american(implied_to_american(devigged[other_bk]['over'])),
                            'under_odds': format_american(implied_to_american(devigged[other_bk]['under'])),
                            'raw_over': raw_over,
                            'raw_under': raw_under,
                            'vig': vig,
                            'weight': w,
                        })

                    if len(other_over_fairs) < 2:
                        continue

                    total_weight = sum(other_weights)
                    consensus_over = sum(f * w for f, w in zip(other_over_fairs, other_weights)) / total_weight
                    consensus_under = 1.0 - consensus_over

                    # Handle exchange books differently — no American odds, no vig
                    if eval_book in ('kalshi', 'polymarket'):
                        eval_over_imp = devigged[eval_book]['over']
                        eval_under_imp = devigged[eval_book]['under']
                        eval_over_fair = eval_over_imp  # Exchange price IS fair
                        eval_under_fair = eval_under_imp
                        juice_pct = 0
                        over_odds = round(implied_to_american(eval_over_imp))
                        under_odds = round(implied_to_american(eval_under_imp))
                    else:
                        eb = group_books[eval_book]
                        eval_over_imp = american_to_implied(eb['over_odds'])
                        eval_under_imp = american_to_implied(eb['under_odds'])
                        eval_over_fair = devigged[eval_book]['over']
                        eval_under_fair = devigged[eval_book]['under']
                        juice_pct = juice_map.get(eval_book, 0)
                        over_odds = eb['over_odds']
                        under_odds = eb['under_odds']

                    for side, eval_imp, eval_fair, consensus_fair, odds in [
                        ('OVER', eval_over_imp, eval_over_fair, consensus_over, over_odds),
                        ('UNDER', eval_under_imp, eval_under_fair, consensus_under, under_odds),
                    ]:
                        net_edge = (consensus_fair - eval_imp) * 100
                        gross_edge = (consensus_fair - eval_fair) * 100

                        if net_edge >= MIN_EDGE_NET:
                            fair_odds = implied_to_american(consensus_fair)
                            kf = quarter_kelly(consensus_fair, odds)
                            opportunities.append({
                                'player': player,
                                'game': data['game'],
                                'commence': data.get('commence', ''),
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
                                'consensus_detail': other_detail,
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
        commence = game.get('commence_time', '')

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
                'commence': commence,
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
        commence = game.get('commence_time', '')

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
                        players[player] = {'game': game_info, 'commence': commence, 'books': {}}
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
                        'commence': data.get('commence', ''),
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

def fetch_event_props(sport, prop_markets, max_events=8, kalshi_props=None, poly_props=None):
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
                opps = analyze_player_props([edata], prop_name,
                    kalshi_props=kalshi_props, poly_props=poly_props, market_key=prop_market)
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
# KALSHI vs POLYMARKET CROSS-EXCHANGE SCANNER
# ============================================================

POLYMARKET_API = "https://gamma-api.polymarket.com"

def fetch_cross_exchange_opps():
    """Compare Kalshi vs Polymarket prices on overlapping markets."""
    opportunities = []

    try:
        # 1. Fetch Kalshi non-sports markets
        log_debug("  Fetching Kalshi non-sports markets...")
        kalshi_markets = {}
        cursor = ''
        for page in range(5):
            params = {'status': 'open', 'limit': 200}
            if cursor:
                params['cursor'] = cursor
            resp = kalshi_get(f"{KALSHI_API}/markets", params=params, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            mkts = data.get('markets', [])
            if not mkts:
                break
            for m in mkts:
                title = (m.get('title', '') or '').lower().strip()
                if ',' in title:
                    continue  # Skip combos
                yb = m.get('yes_bid', 0) or 0
                ya = m.get('yes_ask', 0) or 0
                if yb <= 0 and ya <= 0:
                    continue
                mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0
                if 0.05 < mid < 0.95:
                    kalshi_markets[title] = {
                        'title': m.get('title', ''),
                        'mid': mid,
                        'yes_bid': yb,
                        'yes_ask': ya,
                        'ticker': m.get('ticker', ''),
                        'event_ticker': m.get('event_ticker', ''),
                    }
            cursor = data.get('cursor', '')
            if not cursor:
                break
            time.sleep(0.3)

        log_debug(f"  Kalshi: {len(kalshi_markets)} tradeable non-combo markets")

        # 2. Fetch Polymarket active markets
        log_debug("  Fetching Polymarket markets...")
        poly_markets = {}
        try:
            resp = requests.get(f"{POLYMARKET_API}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'},
                timeout=15)
            if resp.status_code == 200:
                for m in resp.json():
                    q = (m.get('question', '') or '').lower().strip()
                    outcomes = m.get('outcomes', '[]')
                    prices = m.get('outcomePrices', '[]')
                    if isinstance(outcomes, str):
                        import json as _json
                        try:
                            outcomes = _json.loads(outcomes)
                            prices = _json.loads(prices)
                        except:
                            continue
                    if len(outcomes) >= 2 and len(prices) >= 2:
                        try:
                            yes_price = float(prices[0])
                        except:
                            continue
                        if 0.05 < yes_price < 0.95:
                            poly_markets[q] = {
                                'title': m.get('question', ''),
                                'yes_price': yes_price,
                                'volume': m.get('volume', 0),
                                'slug': m.get('slug', ''),
                            }
        except Exception as e:
            log_debug(f"  Polymarket error: {e}")

        log_debug(f"  Polymarket: {len(poly_markets)} active markets")

        # 3. Fuzzy match and compare
        from difflib import SequenceMatcher
        matches = 0
        for k_title, k_data in kalshi_markets.items():
            best_score = 0
            best_poly = None
            for p_title, p_data in poly_markets.items():
                score = SequenceMatcher(None, k_title, p_title).ratio()
                if score > best_score:
                    best_score = score
                    best_poly = (p_title, p_data)

            if best_score < 0.65 or not best_poly:
                continue

            p_title, p_data = best_poly
            matches += 1

            k_mid = k_data['mid']
            p_yes = p_data['yes_price']
            diff = abs(k_mid - p_yes) * 100

            if diff < 3:  # Less than 3% disagreement — not actionable
                continue

            # Determine which side to bet
            # If Kalshi is cheaper, buy YES on Kalshi
            # If Polymarket is cheaper, buy YES on Polymarket
            if k_mid < p_yes:
                edge = (p_yes - k_mid) * 100
                bet_on = 'Kalshi'
                bet_action = 'BUY YES'
                bet_odds = round(implied_to_american(k_mid))
                fair_odds = round(implied_to_american(p_yes))
            else:
                edge = (k_mid - p_yes) * 100
                bet_on = 'Polymarket'
                bet_action = 'BUY YES'
                bet_odds = round(implied_to_american(p_yes))
                fair_odds = round(implied_to_american(k_mid))

            opportunities.append({
                'player': k_data['title'][:60],
                'game': f"Kalshi {k_mid*100:.0f}¢ vs Poly {p_yes*100:.0f}¢",
                'commence': '',
                'market': 'Cross-Exchange',
                'book': bet_on,
                'type': 'cross_exchange',
                'edge': round(edge, 1),
                'gross_edge': round(edge, 1),
                'recommendation': f"{bet_action} on {bet_on}",
                'odds': bet_odds,
                'label1_name': f'Kalshi',
                'label1_value': f"{k_mid*100:.0f}¢ ({format_american(round(implied_to_american(k_mid)))})",
                'label2_name': f'Polymarket',
                'label2_value': f"{p_yes*100:.0f}¢ ({format_american(round(implied_to_american(p_yes)))})",
                'label3_name': 'Spread',
                'label3_value': f"+{diff:.1f}%",
                'target_prob': round(min(k_mid, p_yes) * 100, 1),
                'fair_prob': round(max(k_mid, p_yes) * 100, 1),
                'juice_display': '—',
                'consensus_books': 2,
                'kelly_fraction': round(quarter_kelly(max(k_mid, p_yes), bet_odds) * 100, 2) if bet_odds != 0 else 0,
            })

        log_debug(f"  Cross-exchange: {matches} matched markets, {len(opportunities)} actionable spreads")

    except Exception as e:
        log_debug(f"  Cross-exchange error: {e}")

    return opportunities


# ============================================================
# WEATHER MARKET SCANNER (Kalshi vs Open-Meteo forecast)
# ============================================================

OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"

def fetch_weather_opps():
    """Compare Kalshi temperature markets against weather model forecasts."""
    opportunities = []

    try:
        # Top 5 cities — keep small to leave Kalshi rate limit budget for econ
        WEATHER_SERIES = {
            'KXHIGHNY':      {'city': 'NYC',       'lat': 40.78, 'lon': -73.97},
            'KXHIGHCHI':     {'city': 'Chicago',   'lat': 41.88, 'lon': -87.63},
            'KXHIGHMIA':     {'city': 'Miami',     'lat': 25.76, 'lon': -80.19},
            'KXHIGHLAX':     {'city': 'LA',        'lat': 34.05, 'lon': -118.24},
            'KXHIGHDEN':     {'city': 'Denver',    'lat': 39.74, 'lon': -104.98},
        }

        weather_mkts = []
        series_with_data = 0
        log_debug("  Fetching Kalshi weather via series tickers...")

        for series_ticker, info in WEATHER_SERIES.items():
            try:
                resp = kalshi_get(f"{KALSHI_API}/markets",
                    params={'series_ticker': series_ticker, 'status': 'open', 'limit': 50},
                    timeout=10)
                if resp.status_code == 200:
                    mkts = resp.json().get('markets', [])
                    # Debug: log first market's fields for first series
                    if mkts and series_with_data == 0 and len(weather_mkts) == 0:
                        sample = mkts[0]
                        price_fields = {k: v for k, v in sample.items() if any(
                            w in k.lower() for w in ['price', 'bid', 'ask', 'yes', 'no', 'last', 'volume'])}
                        log_debug(f"    DEBUG {series_ticker}: {len(mkts)} raw markets, sample title: {sample.get('title', '?')[:60]}")
                        log_debug(f"    DEBUG price fields: {price_fields}")
                    count = 0
                    for m in mkts:
                        yb, ya, lp = kalshi_prices(m)
                        has_price = yb > 0 or ya > 0 or lp > 0
                        if has_price:
                            # Normalize into dict for downstream code
                            m['yes_bid'] = yb if yb > 0 else (lp if lp > 0 else 0)
                            m['yes_ask'] = ya if ya > 0 else (lp if lp > 0 else 0)
                            m['_city_info'] = info
                            m['_series'] = series_ticker
                            weather_mkts.append(m)
                            count += 1
                    if count > 0:
                        series_with_data += 1
                        log_debug(f"    {series_ticker} ({info['city']}): {count} markets")
                    elif mkts:
                        log_debug(f"    {series_ticker} ({info['city']}): {len(mkts)} raw but 0 with prices")
                elif resp.status_code == 429:
                    log_debug(f"  Kalshi rate limited on {series_ticker}, waiting 5s...")
                    time.sleep(5)
                    resp = kalshi_get(f"{KALSHI_API}/markets",
                        params={'series_ticker': series_ticker, 'status': 'open', 'limit': 50},
                        timeout=10)
                    if resp.status_code == 200:
                        mkts = resp.json().get('markets', [])
                        count = 0
                        for m in mkts:
                            yb, ya, lp = kalshi_prices(m)
                            has_price = yb > 0 or ya > 0 or lp > 0
                            if has_price:
                                m['yes_bid'] = yb if yb > 0 else lp
                                m['yes_ask'] = ya if ya > 0 else lp
                                m['_city_info'] = info
                                m['_series'] = series_ticker
                                weather_mkts.append(m)
                                count += 1
                        if count > 0:
                            series_with_data += 1
                            log_debug(f"    {series_ticker} ({info['city']}): {count} markets (retry)")
                    else:
                        log_debug(f"  Kalshi still rate limited, skipping remaining weather")
                        break
                else:
                    log_debug(f"    {series_ticker}: HTTP {resp.status_code}")
                time.sleep(1.5)
            except Exception as e:
                log_debug(f"    {series_ticker}: error {e}")
                continue

        log_debug(f"  Kalshi weather: {len(weather_mkts)} markets from {series_with_data}/{len(WEATHER_SERIES)} series")

        if not weather_mkts:
            return []

        # 2. For each weather market, fetch forecast and compare
        import math
        # Cache forecasts per city to avoid duplicate API calls
        forecast_cache = {}
        matched_count = 0

        for mkt in weather_mkts:
            title = (mkt.get('title', '') or '')
            title_low = title.lower()
            city_info = mkt.get('_city_info', {})
            city_key = city_info.get('city', '?')

            # Extract threshold temperature from title
            temp_match = re.search(r'(\d+(?:\.\d+)?)\s*°?(?:f|fahrenheit)?', title_low)
            if not temp_match:
                continue
            threshold = float(temp_match.group(1))
            if threshold < -20 or threshold > 140:
                continue

            # Determine if it's "above" or "below"
            is_over = any(kw in title_low for kw in ['above', 'over', 'higher', 'at least', 'exceed', 'or more', 'high'])
            is_under = any(kw in title_low for kw in ['below', 'under', 'lower', 'at most', 'or less', 'low'])
            if not is_over and not is_under:
                is_over = True  # Default for "highest temp" series

            # Kalshi mid price
            yb = mkt.get('yes_bid', 0) or 0
            ya = mkt.get('yes_ask', 0) or 0
            k_mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0

            # 3. Fetch forecast from Open-Meteo (cached per city, with rate limit respect)
            try:
                if city_key not in forecast_cache:
                    time.sleep(0.5)  # Respect Open-Meteo rate limit
                    f_resp = requests.get(OPEN_METEO_API, params={
                        'latitude': city_info['lat'],
                        'longitude': city_info['lon'],
                        'daily': 'temperature_2m_max,temperature_2m_min',
                        'temperature_unit': 'fahrenheit',
                        'forecast_days': 3,
                        'timezone': 'America/New_York',
                    }, timeout=10)
                    if f_resp.status_code != 200:
                        if f_resp.status_code == 429:
                            time.sleep(3)
                            f_resp = requests.get(OPEN_METEO_API, params={
                                'latitude': city_info['lat'],
                                'longitude': city_info['lon'],
                                'daily': 'temperature_2m_max,temperature_2m_min',
                                'temperature_unit': 'fahrenheit',
                                'forecast_days': 3,
                                'timezone': 'America/New_York',
                            }, timeout=10)
                        if f_resp.status_code != 200:
                            log_debug(f"    {city_key} forecast: HTTP {f_resp.status_code}")
                            forecast_cache[city_key] = None
                            continue
                    fdata = f_resp.json()
                    daily = fdata.get('daily', {})
                    maxes = daily.get('temperature_2m_max', [])
                    if not maxes:
                        log_debug(f"    {city_key} forecast: no max temps in response")
                        forecast_cache[city_key] = None
                        continue
                    # Use tomorrow's forecast (index 1) or today if only 1 day
                    forecast_cache[city_key] = maxes[0]  # Today's high (most relevant for today's markets)
                    log_debug(f"    {city_key} forecast high: {forecast_cache[city_key]:.0f}°F")

                if forecast_cache.get(city_key) is None:
                    continue
                forecast_high = forecast_cache[city_key]

                # Model: assume temperature follows roughly normal distribution
                # with mean = forecast and std dev ≈ 3°F (typical 1-day forecast error)
                std_dev = 3.0
                # P(temp > threshold)
                z = (threshold - forecast_high) / std_dev
                # Normal CDF approximation
                def norm_cdf(x):
                    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

                if is_over:
                    model_prob = 1.0 - norm_cdf(z)
                else:
                    model_prob = norm_cdf(z)

                if model_prob < 0.02 or model_prob > 0.98:
                    continue

                # Compare
                edge = (model_prob - k_mid) * 100

                # Log first few comparisons for debugging
                if len(opportunities) == 0 and matched_count < 3:
                    log_debug(f"    {city_key} >{threshold:.0f}°F: model={model_prob*100:.1f}% kalshi={k_mid*100:.1f}% edge={edge:+.1f}%")
                matched_count += 1

                if abs(edge) < 3:
                    continue

                if edge > 0:
                    # Model says YES is more likely than Kalshi price — buy YES
                    bet_action = 'BUY YES'
                    display_edge = edge
                else:
                    # Model says NO is more likely — buy NO
                    bet_action = 'BUY NO'
                    display_edge = abs(edge)
                    model_prob = 1.0 - model_prob
                    k_mid = 1.0 - k_mid

                opportunities.append({
                    'player': title[:60],
                    'game': f"{city_key.upper()} · Forecast high: {forecast_high:.0f}°F · Threshold: {threshold:.0f}°F",
                    'commence': '',
                    'market': 'Weather',
                    'book': 'Kalshi',
                    'type': 'weather',
                    'edge': round(display_edge, 1),
                    'gross_edge': round(display_edge, 1),
                    'recommendation': f"{bet_action} on Kalshi",
                    'odds': round(implied_to_american(k_mid)) if k_mid > 0 and k_mid < 1 else 0,
                    'label1_name': 'Kalshi Price',
                    'label1_value': f"{(mkt.get('yes_bid',0) or 0)}–{(mkt.get('yes_ask',0) or 0)}¢",
                    'label2_name': 'Model Fair',
                    'label2_value': f"{model_prob*100:.0f}%",
                    'label3_name': 'Edge',
                    'label3_value': f"+{display_edge:.1f}%",
                    'target_prob': round(k_mid * 100, 1),
                    'fair_prob': round(model_prob * 100, 1),
                    'juice_display': f"±{std_dev:.0f}°F",
                    'consensus_books': 0,
                    'kelly_fraction': round(quarter_kelly(model_prob, round(implied_to_american(k_mid))) * 100, 2) if 0 < k_mid < 1 else 0,
                })

            except Exception as e:
                continue

        log_debug(f"  Weather: {matched_count} markets compared, {len(opportunities)} with 3%+ edge")

    except Exception as e:
        log_debug(f"  Weather error: {e}")

    return opportunities


# ============================================================
# ECONOMIC DATA SCANNER
# CME FedWatch (3x) + Polymarket (2x) → weighted consensus vs Kalshi
# Same methodology as sports: leave-one-out weighted consensus
# ============================================================

def _econ_keywords(title):
    """Extract normalized keywords from a market title for matching."""
    t = re.sub(r'[^a-z0-9%\s.]', '', title.lower())
    stop = {'will', 'the', 'be', 'a', 'an', 'in', 'on', 'at', 'to', 'of',
            'for', 'is', 'it', 'by', 'or', 'and', 'this', 'that', 'than', 'more'}
    words = [w for w in t.split() if w not in stop and len(w) > 1]
    return set(words)

def _topic_tag(title):
    """Classify a market title into an economic topic."""
    t = title.lower()
    if any(w in t for w in ['fed', 'fomc', 'rate cut', 'rate hike', 'interest rate',
        'federal reserve', 'federal funds', 'target rate', 'basis point', 'bps']):
        return 'fed'
    if any(w in t for w in ['cpi', 'inflation', 'consumer price', 'price index']):
        return 'cpi'
    if any(w in t for w in ['job', 'payroll', 'nonfarm', 'employment', 'unemployment', 'labor']):
        return 'jobs'
    if any(w in t for w in ['gdp', 'gross domestic', 'economic growth']):
        return 'gdp'
    if any(w in t for w in ['recession', 'economic downturn']):
        return 'recession'
    if any(w in t for w in ['tariff', 'trade war', 'trade deal', 'trade deficit']):
        return 'tariff'
    if any(w in t for w in ['pce', 'personal consumption']):
        return 'pce'
    return None

def _extract_month(title):
    """Extract month reference from title for matching."""
    months = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
              'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
              'january': 1, 'february': 2, 'march': 3, 'april': 4, 'june': 6,
              'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12}
    t = title.lower()
    for name, num in months.items():
        if name in t:
            return num
    return None

def _is_fed_rate_market(title):
    """Check if this Kalshi market is about Fed rate decisions."""
    t = title.lower()
    return any(w in t for w in ['fed', 'fomc', 'rate cut', 'rate hike', 'interest rate',
        'federal reserve', 'federal funds', 'target rate', 'basis point', 'bps'])

def fetch_fedwatch_probs():
    """Fetch CME FedWatch implied probabilities from free aggregator.
    Returns dict: {'hold': 0.94, 'cut_25': 0.06, 'cut_50': 0.0, 'hike': 0.0, 'source': 'growbeansprout'}
    Falls back gracefully if unavailable."""
    try:
        resp = requests.get('https://growbeansprout.com/tools/fedwatch', timeout=10,
            headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code != 200:
            return None
        html = resp.text

        # Parse probability percentages from the page
        # Look for patterns like "98.9% probability" or "X% chance" near "hold"/"cut"/"hike"
        import re as _re

        # Try to find the main probability statement
        # "there is a X% probability that the US Federal Reserve will..."
        m = _re.search(r'(\d+\.?\d*)%\s*probability.*?(?:cut|hold|unchanged|steady|maintain|raise|hike)', html, _re.IGNORECASE | _re.DOTALL)
        if not m:
            # Try alternative pattern
            m = _re.search(r'(?:cut|hold|unchanged|steady|maintain|raise|hike).*?(\d+\.?\d*)%', html, _re.IGNORECASE | _re.DOTALL)

        if not m:
            return None

        prob = float(m.group(1)) / 100.0
        text_after = html[m.start():m.end() + 100].lower()

        # Determine if this probability is for a cut or a hold
        result = {'hold': 0.0, 'cut_25': 0.0, 'cut_50': 0.0, 'hike': 0.0, 'source': 'growbeansprout'}
        if any(w in text_after for w in ['cut', 'lower', 'reduce', 'ease']):
            result['cut_25'] = prob
            result['hold'] = round(1.0 - prob, 4)
        elif any(w in text_after for w in ['hold', 'unchanged', 'steady', 'maintain', 'no change']):
            result['hold'] = prob
            result['cut_25'] = round(1.0 - prob, 4)
        elif any(w in text_after for w in ['hike', 'raise', 'increase']):
            result['hike'] = prob
            result['hold'] = round(1.0 - prob, 4)

        return result

    except Exception:
        return None

def fetch_econ_opps():
    """Find +EV econ bets: CME FedWatch (3x) + Polymarket (2x) consensus vs Kalshi.
    Same methodology as sports scanner: weighted consensus = fair value."""
    opportunities = []

    try:
        # 1. Fetch CME FedWatch probabilities (sharpest signal for Fed markets)
        fedwatch = fetch_fedwatch_probs()
        if fedwatch:
            log_debug(f"  CME FedWatch: hold={fedwatch['hold']*100:.1f}%, cut={fedwatch['cut_25']*100:.1f}% (via {fedwatch['source']})")
        else:
            log_debug("  CME FedWatch: unavailable (will use Polymarket only for Fed markets)")

        # 2. Fetch Kalshi econ markets
        ECON_SERIES = [
            'KXFED', 'FED', 'FEDDECISION',
            'KXCPI', 'CPI', 'KXCPICORE',
            'KXPAYROLLS', 'PAYROLLS',
            'KXGDP', 'GDP',
            'RATECUT', 'KXRATECUT',
        ]

        econ_mkts = []
        log_debug("  Fetching Kalshi economic markets...")

        for series in ECON_SERIES:
            try:
                resp = kalshi_get(f"{KALSHI_API}/markets",
                    params={'series_ticker': series, 'status': 'open', 'limit': 50},
                    timeout=10)
                if resp.status_code == 200:
                    mkts = resp.json().get('markets', [])
                    count = 0
                    for m in mkts:
                        yb, ya, lp = kalshi_prices(m)
                        if yb > 0 or ya > 0 or lp > 0:
                            m['yes_bid'] = yb if yb > 0 else lp
                            m['yes_ask'] = ya if ya > 0 else lp
                            econ_mkts.append(m)
                            count += 1
                    if count > 0:
                        log_debug(f"    {series}: {count} markets")
                elif resp.status_code == 429:
                    log_debug(f"  Kalshi rate limited on {series}, waiting 5s...")
                    time.sleep(5)
                    resp = kalshi_get(f"{KALSHI_API}/markets",
                        params={'series_ticker': series, 'status': 'open', 'limit': 50},
                        timeout=10)
                    if resp.status_code == 200:
                        mkts = resp.json().get('markets', [])
                        count = 0
                        for m in mkts:
                            yb, ya, lp = kalshi_prices(m)
                            if yb > 0 or ya > 0 or lp > 0:
                                m['yes_bid'] = yb if yb > 0 else lp
                                m['yes_ask'] = ya if ya > 0 else lp
                                econ_mkts.append(m)
                                count += 1
                        if count > 0:
                            log_debug(f"    {series}: {count} markets (retry)")
                    else:
                        log_debug(f"  Kalshi still rate limited, skipping remaining econ")
                        break
                time.sleep(1.5)
            except:
                continue

        log_debug(f"  Kalshi: {len(econ_mkts)} economic markets")

        # Debug: show sample Kalshi econ titles
        if econ_mkts:
            samples = [m.get('title', '')[:60] for m in econ_mkts[:3]]
            log_debug(f"    Sample Kalshi titles: {samples}")

        # 3. Fetch Polymarket econ markets
        poly_mkts = []
        poly_unmatched_sample = []
        try:
            resp = requests.get(f"{POLYMARKET_API}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'},
                timeout=15)
            if resp.status_code == 200:
                import json as _json
                for m in resp.json():
                    q = m.get('question', '') or ''
                    topic = _topic_tag(q)
                    if not topic:
                        # Collect samples of unmatched titles for debugging
                        if len(poly_unmatched_sample) < 5:
                            poly_unmatched_sample.append(q[:60])
                        continue
                    outcomes = m.get('outcomes', '[]')
                    prices = m.get('outcomePrices', '[]')
                    if isinstance(outcomes, str):
                        try:
                            outcomes = _json.loads(outcomes)
                            prices = _json.loads(prices)
                        except:
                            continue
                    if len(outcomes) >= 2 and len(prices) >= 2:
                        try:
                            yes_price = float(prices[0])
                        except:
                            continue
                        if 0.03 < yes_price < 0.97:
                            poly_mkts.append({
                                'title': q,
                                'yes_price': yes_price,
                                'topic': topic,
                                'month': _extract_month(q),
                                'keywords': _econ_keywords(q),
                                'volume': float(m.get('volume', 0) or 0),
                            })
        except Exception as e:
            log_debug(f"  Polymarket error: {e}")

        log_debug(f"  Polymarket: {len(poly_mkts)} econ markets")
        if not poly_mkts and poly_unmatched_sample:
            log_debug(f"    Sample unmatched Poly titles: {poly_unmatched_sample[:3]}")

        # 4. For each Kalshi market, build weighted consensus and find +EV
        from difflib import SequenceMatcher
        matched = 0
        fed_matched = 0
        min_edge = 3.0  # Higher threshold than sports (fewer sources, less confidence)
        fed_sample_logged = 0
        no_topic_count = 0
        extreme_price_count = 0
        no_source_count = 0

        for km in econ_mkts:
            k_title = km.get('title', '')
            k_topic = _topic_tag(k_title)
            if not k_topic:
                no_topic_count += 1
                continue

            yb = km.get('yes_bid', 0) or 0
            ya = km.get('yes_ask', 0) or 0
            # Fallback to kalshi_prices if not normalized yet
            if yb == 0 and ya == 0:
                yb, ya, lp = kalshi_prices(km)
                if yb == 0 and ya == 0 and lp > 0:
                    yb = ya = lp
            k_mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0

            # Debug: log first 3 Fed market prices
            if k_topic == 'fed' and fed_sample_logged < 3:
                log_debug(f"    Fed: '{k_title[:55]}' → {k_mid*100:.1f}%")
                fed_sample_logged += 1

            if k_mid <= 0.03 or k_mid >= 0.97:
                extreme_price_count += 1
                continue

            # --- Build consensus fair probability ---
            # Sources with weights (same approach as sports: weighted average)
            sources = []  # list of (prob, weight, name)

            # A) CME FedWatch — only for Fed rate markets, 3x weight
            if fedwatch and _is_fed_rate_market(k_title):
                t = k_title.lower()
                # Match Kalshi market to FedWatch probability
                # "Will Fed cut rates?" → use cut probability
                # "Will Fed hold rates?" → use hold probability
                # "Rate cut in [month]?" → use cut probability
                fw_prob = None
                if any(w in t for w in ['cut', 'lower', 'reduce', 'ease', 'decrease']):
                    fw_prob = fedwatch['cut_25'] + fedwatch.get('cut_50', 0)
                elif any(w in t for w in ['hold', 'unchanged', 'steady', 'maintain', 'no change', 'pause']):
                    fw_prob = fedwatch['hold']
                elif any(w in t for w in ['hike', 'raise', 'increase', 'tighten']):
                    fw_prob = fedwatch.get('hike', 0)
                # For generic "rate decision" markets, try to infer from title
                elif 'rate' in t and not any(w in t for w in ['cut', 'hold', 'hike']):
                    # Check if it's asking about number of cuts
                    nums = re.findall(r'(\d+)\s*(?:cut|rate)', t)
                    if nums:
                        fw_prob = fedwatch['cut_25']

                if fw_prob is not None and 0.01 < fw_prob < 0.99:
                    sources.append((fw_prob, 3, 'FedWatch'))

            # B) Polymarket — fuzzy match, 2x weight for Fed, 1x for others
            k_month = _extract_month(k_title)
            k_keywords = _econ_keywords(k_title)
            best_poly_score = 0
            best_poly = None

            for pm in poly_mkts:
                if pm['topic'] != k_topic:
                    continue
                if k_month and pm['month'] and k_month != pm['month']:
                    continue
                if k_keywords and pm['keywords']:
                    overlap = len(k_keywords & pm['keywords'])
                    keyword_score = overlap / max(min(len(k_keywords), len(pm['keywords'])), 1)
                else:
                    keyword_score = 0
                fuzzy = SequenceMatcher(None, k_title.lower(), pm['title'].lower()).ratio()
                score = keyword_score * 0.5 + fuzzy * 0.5
                if score > best_poly_score:
                    best_poly_score = score
                    best_poly = pm

            if best_poly_score >= 0.35 and best_poly:
                poly_weight = 2 if k_topic == 'fed' else 1
                sources.append((best_poly['yes_price'], poly_weight, 'Polymarket'))

            # Need at least 1 consensus source
            if not sources:
                no_source_count += 1
                continue

            # C) Weighted consensus
            total_weight = sum(w for _, w, _ in sources)
            fair_prob = sum(p * w for p, w, _ in sources) / total_weight

            # Edge: fair value vs Kalshi mid
            # Positive edge = Kalshi is underpriced (buy YES)
            # Negative edge = Kalshi is overpriced (buy NO)
            edge_yes = (fair_prob - k_mid) * 100  # Edge on YES side
            edge_no = (k_mid - fair_prob) * 100   # Edge on NO side (= overpriced YES)

            actionable_edge = max(edge_yes, edge_no)

            if actionable_edge < min_edge:
                continue

            matched += 1
            if k_topic == 'fed':
                fed_matched += 1

            # Determine bet direction
            if edge_yes > edge_no:
                bet_action = 'BUY YES'
                edge = round(edge_yes, 1)
                bet_odds = round(implied_to_american(k_mid))
            else:
                bet_action = 'BUY NO'
                edge = round(edge_no, 1)
                no_implied = 1.0 - k_mid
                bet_odds = round(implied_to_american(no_implied))

            # Build consensus description
            source_desc = ' + '.join(f'{n}({w}x)' for _, w, n in sources)

            opportunities.append({
                'player': k_title[:60],
                'game': f"Fair: {fair_prob*100:.0f}% [{source_desc}] | {k_topic.upper()}",
                'commence': '',
                'market': 'Economic',
                'book': 'Kalshi',
                'type': 'economic',
                'edge': edge,
                'gross_edge': edge,
                'recommendation': f"{bet_action} on Kalshi (consensus {fair_prob*100:.0f}% vs Kalshi {k_mid*100:.0f}%)",
                'odds': bet_odds,
                'label1_name': 'Kalshi',
                'label1_value': f"{k_mid*100:.0f}¢ ({format_american(round(implied_to_american(k_mid)))})",
                'label2_name': 'Consensus Fair',
                'label2_value': f"{fair_prob*100:.0f}¢ [{source_desc}]",
                'label3_name': 'Edge',
                'label3_value': f"+{edge:.1f}% {bet_action}",
                'target_prob': round(k_mid * 100, 1),
                'fair_prob': round(fair_prob * 100, 1),
                'juice_display': f'{len(sources)} source{"s" if len(sources) > 1 else ""}: {source_desc}',
                'consensus_books': len(sources),
                'kelly_fraction': round(quarter_kelly(fair_prob, bet_odds) * 100, 2) if bet_odds != 0 else 0,
            })

        log_debug(f"  Econ: {no_topic_count} no-topic, {extreme_price_count} extreme-price, {no_source_count} no-consensus → {matched} with sources, {len(opportunities)} above {min_edge}%")

    except Exception as e:
        log_debug(f"  Economic error: {e}")

    return opportunities


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
    log_debug(f"Consensus: + {', '.join(BOOK_DISPLAY.get(b, b) for b in CONSENSUS_ONLY)} + Kalshi + Polymarket")
    log_debug(f"Strategy: CO book vs weighted consensus (Pinnacle/Kalshi/Poly 3x) | Min edge: {MIN_EDGE_NET}%")

    # ---- POLYMARKET (separate API, no Kalshi rate limit) ----
    log_debug("--- Polymarket ---")
    poly_sports = fetch_polymarket_sports(log_fn=log_debug)
    poly_props = poly_sports.get('props', {})
    poly_games = poly_sports.get('games', {})

    kalshi_props = {}
    kalshi_games = {}

    log_debug(f"  Exchange consensus: Polymarket {len(poly_games)} games")

    # ---- SPORTS (Odds API) ----
    # 1. Player props (event-level) — +EV and arbs
    log_debug("--- Player Props ---")
    for sport, prop_markets, max_ev in PROP_MARKETS:
        if len(_dead_keys) >= len(API_KEYS):
            log_debug("  All keys exhausted — stopping")
            break
        opps, arbs = fetch_event_props(sport, prop_markets, max_events=max_ev,
            kalshi_props=kalshi_props, poly_props=poly_props)
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
            opps = analyze_game_markets(games, name, poly_games=poly_games, kalshi_games=kalshi_games)
            arbs = find_game_arbs(games, name)
            if opps:
                all_opps.extend(opps)
            if arbs:
                all_opps.extend(arbs)
        time.sleep(0.3)

    # Sort: arbs first, then by edge descending
    all_opps.sort(key=lambda x: (0 if x['type'] == 'arbitrage' else 1, -x['edge']))

    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['scanning'] = False

    active = len(API_KEYS) - len(_dead_keys)
    arb_count = len([o for o in all_opps if o['type'] == 'arbitrage'])
    sports_count = len(all_opps) - arb_count
    log_debug(f"=== DONE: {sports_count} sports +EV, {arb_count} arb ({active}/{len(API_KEYS)} keys active) ===")


# ============================================================
# ROUTES
# ============================================================

@app.route('/')
def index():
    return render_template('arbitrage.html')

@app.route('/api/scan', methods=['POST'])
def trigger_scan():
    if SCAN_KEY:
        provided = request.args.get('key', '')
        if not provided and request.is_json:
            provided = request.json.get('key', '')
        if provided != SCAN_KEY:
            return jsonify({'error': 'Invalid key'}), 403
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

@app.route('/api/kalshi-debug')
def kalshi_debug():
    """Diagnostic: discover what Kalshi actually has available."""
    results = {'series': [], 'sample_markets': [], 'errors': []}

    # 1. Try /series endpoint to list all series
    try:
        resp = kalshi_get(f"{KALSHI_API}/series", timeout=10)
        if resp.status_code == 200:
            series_list = resp.json().get('series', [])
            for s in series_list:
                ticker = s.get('ticker', '')
                title = s.get('title', '')
                cat = s.get('category', '')
                results['series'].append({
                    'ticker': ticker,
                    'title': title,
                    'category': cat,
                })
        else:
            results['errors'].append(f'/series returned HTTP {resp.status_code}')
    except Exception as e:
        results['errors'].append(f'/series error: {str(e)}')

    # 2. Try fetching a page of individual (non-combo) markets
    try:
        resp = kalshi_get(f"{KALSHI_API}/markets",
            params={'status': 'open', 'limit': 100}, timeout=10)
        if resp.status_code == 200:
            mkts = resp.json().get('markets', [])
            for m in mkts[:50]:
                title = m.get('title', '')
                if ',' not in title:  # Non-combo only
                    results['sample_markets'].append({
                        'ticker': m.get('ticker', ''),
                        'series_ticker': m.get('series_ticker', ''),
                        'event_ticker': m.get('event_ticker', ''),
                        'title': title[:100],
                        'subtitle': (m.get('subtitle', '') or '')[:100],
                        'yes_sub_title': (m.get('yes_sub_title', '') or '')[:100],
                        'category': m.get('category', ''),
                        'yes_bid': m.get('yes_bid', 0),
                        'yes_ask': m.get('yes_ask', 0),
                        'status': m.get('status', ''),
                    })
        else:
            results['errors'].append(f'/markets returned HTTP {resp.status_code}')
    except Exception as e:
        results['errors'].append(f'/markets error: {str(e)}')

    # 3. Filter series for weather/econ/sports keywords
    weather_series = [s for s in results['series']
        if any(kw in (s['title'] + s['ticker']).lower()
        for kw in ['temp', 'weather', 'high', 'low', 'rain', 'snow'])]
    econ_series = [s for s in results['series']
        if any(kw in (s['title'] + s['ticker']).lower()
        for kw in ['fed', 'cpi', 'inflation', 'gdp', 'jobs', 'rate', 'unemployment', 'recession', 'payroll'])]
    sports_series = [s for s in results['series']
        if any(kw in (s['title'] + s['ticker'] + s['category']).lower()
        for kw in ['nba', 'nfl', 'nhl', 'mlb', 'basketball', 'football', 'hockey', 'baseball', 'player', 'prop', 'touchdown'])]

    results['weather_series'] = weather_series
    results['econ_series'] = econ_series
    results['sports_series'] = sports_series[:20]  # Limit
    results['total_series'] = len(results['series'])
    results['series'] = results['series'][:20]  # Only return first 20 to keep response manageable

    return jsonify(results)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
