"""
+EV Finder — Sports, Cross-Exchange, Weather, Economic
Sports: CO-legal books vs weighted consensus (Pinnacle/Kalshi/Poly 3x)
Cross-Exchange: Kalshi vs Polymarket price disagreements
Weather: Kalshi temperature contracts vs Open-Meteo ensemble forecast
Economic: Kalshi vs CME FedWatch + Polymarket weighted consensus

MONETIZATION INFRASTRUCTURE:
- SQLite CLV tracking (every flagged opp is logged for performance review)
- Affiliate links per opportunity (env-var configured)
- History/export endpoints for data product potential
"""

from flask import Flask, render_template, jsonify, request, Response
import requests
import re
import time
import sqlite3
import math
import json
import csv
import io
from datetime import datetime, timedelta
from contextlib import contextmanager
import threading
import os

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

SCAN_KEY = os.environ.get('SCAN_KEY', '')

# API keys from env var (comma-separated). Falls back to single ODDS_API_KEY.
_keys_env = os.environ.get('ODDS_API_KEYS', '') or os.environ.get('ODDS_API_KEY', '')
API_KEYS = [k.strip() for k in _keys_env.split(',') if k.strip()]

# Lock for thread safety on key rotation and state
_key_lock = threading.Lock()
_state_lock = threading.Lock()
_cache_lock = threading.Lock()

_key_index = 0
_dead_keys = set()

# Simple in-memory odds cache (avoid repeat API calls within 60s)
_odds_cache = {}
CACHE_TTL = 60  # seconds

# Database for CLV tracking
DB_PATH = os.environ.get('DB_PATH', '/tmp/evfinder.db')

# Anthropic API for LLM-based Kalshi prop parsing (falls back to regex if unavailable)
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
try:
    import anthropic as _anthropic_sdk
    _anthropic_available = True
except ImportError:
    _anthropic_sdk = None
    _anthropic_available = False
LLM_PARSER_ENABLED = bool(ANTHROPIC_API_KEY and _anthropic_available)

# CLV worker knobs
CLV_WORKER_INTERVAL_SEC = int(os.environ.get('CLV_WORKER_INTERVAL_SEC', '1800'))  # 30 min
CLV_WINDOW_HOURS_AHEAD = int(os.environ.get('CLV_WINDOW_HOURS_AHEAD', '2'))

# Default bankroll
DEFAULT_BANKROLL = 3000

MIN_EDGE_NET = 0.1  # Minimum edge % to surface

# ============================================================
# AFFILIATE LINKS (set these env vars to activate; left blank = no link shown)
# ============================================================

AFFILIATE_URLS = {
    'fanduel':     os.environ.get('AFF_FANDUEL', ''),
    'draftkings':  os.environ.get('AFF_DRAFTKINGS', ''),
    'betmgm':      os.environ.get('AFF_BETMGM', ''),
    'betrivers':   os.environ.get('AFF_BETRIVERS', ''),
    'espnbet':     os.environ.get('AFF_ESPNBET', ''),
    'hardrockbet': os.environ.get('AFF_HARDROCK', ''),
    'ballybet':    os.environ.get('AFF_BALLY', ''),
    'kalshi':      os.environ.get('AFF_KALSHI', ''),
    'polymarket':  os.environ.get('AFF_POLYMARKET', ''),
}

def affiliate_url(book_key):
    return AFFILIATE_URLS.get(book_key, '') or ''

# ============================================================
# COLORADO-LEGAL SPORTSBOOKS
# ============================================================

CO_BETTABLE = [
    'fanduel', 'draftkings', 'betmgm', 'betrivers',
    'espnbet', 'hardrockbet', 'ballybet',
]
CONSENSUS_ONLY = ['pinnacle', 'bovada', 'betonlineag']
ALL_BOOKS = CO_BETTABLE + CONSENSUS_ONLY

BOOK_DISPLAY = {
    'fanduel': 'FanDuel', 'draftkings': 'DraftKings', 'betmgm': 'BetMGM',
    'betrivers': 'BetRivers', 'espnbet': 'theScore Bet', 'hardrockbet': 'Hard Rock',
    'ballybet': 'Bally Bet', 'pinnacle': 'Pinnacle', 'bovada': 'Bovada',
    'betonlineag': 'BetOnline', 'kalshi': 'Kalshi', 'polymarket': 'Polymarket',
}
BOOK_KEY_LOOKUP = {v: k for k, v in BOOK_DISPLAY.items()}

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

BOOK_WEIGHT = {'pinnacle': 3, 'kalshi': 3, 'polymarket': 3}
def get_weight(book_key):
    return BOOK_WEIGHT.get(book_key, 1)

# ============================================================
# STATE
# ============================================================

state = {
    'opportunities': [],
    'last_scan': None,
    'scanning': False,
    'debug_info': [],
    'scan_id': None,
}

def log_debug(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    with _state_lock:
        state['debug_info'].append(line)
    print(line, flush=True)

# ============================================================
# DATABASE (CLV tracking)
# ============================================================

def init_db():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id TEXT,
                    scan_time TEXT NOT NULL,
                    commence_time TEXT,
                    sport TEXT,
                    market TEXT,
                    player TEXT,
                    game TEXT,
                    book TEXT,
                    bet_type TEXT,
                    recommendation TEXT,
                    line REAL,
                    odds INTEGER,
                    edge REAL,
                    fair_prob REAL,
                    target_prob REAL,
                    kelly_fraction REAL,
                    consensus_books INTEGER,
                    closing_odds INTEGER,
                    clv REAL,
                    result TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_time ON opportunities(scan_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_commence ON opportunities(commence_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_id ON opportunities(scan_id)")
            # Migrations — safe to re-run, "duplicate column" errors ignored
            for col_sql in [
                "ALTER TABLE opportunities ADD COLUMN sport_key TEXT",
                "ALTER TABLE opportunities ADD COLUMN event_id TEXT",
                "ALTER TABLE opportunities ADD COLUMN clv_captured_at TEXT",
            ]:
                try:
                    conn.execute(col_sql)
                except sqlite3.OperationalError:
                    pass
    except Exception as e:
        print(f"DB init error: {e}", flush=True)

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def log_opportunity(opp, scan_id):
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO opportunities (scan_id, scan_time, commence_time, sport, market,
                    player, game, book, bet_type, recommendation, line, odds, edge,
                    fair_prob, target_prob, kelly_fraction, consensus_books,
                    sport_key, event_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_id,
                datetime.now().isoformat(),
                opp.get('commence', ''),
                opp.get('sport', ''),
                opp.get('market', ''),
                opp.get('player', ''),
                opp.get('game', ''),
                opp.get('book', ''),
                opp.get('type', ''),
                opp.get('recommendation', ''),
                float(opp.get('line', 0) or 0),
                int(opp.get('odds', 0) or 0),
                float(opp.get('edge', 0) or 0),
                float(opp.get('fair_prob', 0) or 0),
                float(opp.get('target_prob', 0) or 0),
                float(opp.get('kelly_fraction', 0) or 0),
                int(opp.get('consensus_books', 0) or 0),
                opp.get('sport_key', ''),
                opp.get('event_id', ''),
            ))
    except Exception as e:
        log_debug(f"DB log error: {e}")

init_db()

# ============================================================
# KEY ROTATION
# ============================================================

def get_api_key():
    global _key_index
    with _key_lock:
        attempts = 0
        while attempts < len(API_KEYS):
            if not API_KEYS:
                return None
            key = API_KEYS[_key_index % len(API_KEYS)]
            _key_index += 1
            if key not in _dead_keys:
                return key
            attempts += 1
        return None

def mark_key_dead(key):
    with _key_lock:
        _dead_keys.add(key)
        remaining = len(API_KEYS) - len(_dead_keys)
    log_debug(f"  ⚠️ Key ...{key[-6:]} exhausted. {remaining} keys left.")

# ============================================================
# KALSHI
# ============================================================

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.environ.get('KALSHI_API_KEY', '')

def kalshi_headers():
    if KALSHI_API_KEY:
        return {'Authorization': f'Bearer {KALSHI_API_KEY}', 'Content-Type': 'application/json'}
    return {'Content-Type': 'application/json'}

def kalshi_get(url, params=None, timeout=15):
    return requests.get(url, params=params, headers=kalshi_headers(), timeout=timeout)

def kalshi_prices(m):
    """Extract yes_bid, yes_ask, last_price from a Kalshi market dict in cents (int).
    Handles both legacy (yes_bid: 50) and new (yes_bid_dollars: '0.5000') formats."""
    yb = m.get('yes_bid', 0) or 0
    ya = m.get('yes_ask', 0) or 0
    lp = m.get('last_price', 0) or 0
    if yb == 0:
        s = m.get('yes_bid_dollars', '') or ''
        if s:
            try: yb = round(float(s) * 100)
            except: pass
    if ya == 0:
        s = m.get('yes_ask_dollars', '') or ''
        if s:
            try: ya = round(float(s) * 100)
            except: pass
    if lp == 0:
        s = m.get('last_price_dollars', '') or ''
        if s:
            try: lp = round(float(s) * 100)
            except: pass
    return int(yb), int(ya), int(lp)

def normalize_player_name(name):
    name = ' '.join(name.strip().split()).lower()
    name = re.sub(r'\s+(jr\.?|sr\.?|ii+|iv|v)$', '', name)
    return name

def kalshi_stat_to_market(stat_str):
    s = stat_str.lower()
    if 'point' in s: return 'player_points'
    elif 'rebound' in s: return 'player_rebounds'
    elif 'assist' in s: return 'player_assists'
    elif 'three' in s or '3-pointer' in s or '3pt' in s: return 'player_threes'
    elif 'shot' in s: return 'player_shots_on_goal'
    return None

def parse_kalshi_prop(market):
    """Parse a Kalshi market dict → (player, market_type, line, over_prob) or None."""
    title = market.get('title', '')
    subtitle = market.get('subtitle', '') or ''
    yes_sub = market.get('yes_sub_title', '') or ''
    event_ticker = market.get('event_ticker', '') or ''
    series_ticker = market.get('series_ticker', '') or ''

    if ',' in title:  # combo/parlay
        return None

    # Use normalized prices
    yb, ya, lp = kalshi_prices(market)
    if yb > 0 and ya > 0:
        mid_prob = ((yb + ya) / 2) / 100.0
    elif yb > 0:
        mid_prob = yb / 100.0
    elif ya > 0:
        mid_prob = ya / 100.0
    elif lp > 0:
        mid_prob = lp / 100.0
    else:
        return None
    if mid_prob <= 0.02 or mid_prob >= 0.98:
        return None

    full_text = f"{title} {subtitle} {yes_sub}"

    def _valid_name(n):
        n = n.strip()
        if len(n) < 4 or len(n) > 40: return False
        if n.count(' ') > 4: return False
        if ',' in n or 'yes' in n.lower() or 'no ' in n.lower(): return False
        return True

    def _stat_from_context():
        ctx = f"{event_ticker} {series_ticker} {subtitle}".lower()
        if 'point' in ctx or 'pts' in ctx: return 'player_points'
        elif 'rebound' in ctx or 'reb' in ctx: return 'player_rebounds'
        elif 'assist' in ctx or 'ast' in ctx: return 'player_assists'
        elif 'three' in ctx or '3pt' in ctx or '3p' in ctx: return 'player_threes'
        elif 'shot' in ctx or 'sog' in ctx: return 'player_shots_on_goal'
        return None

    # Pattern 1: "Will [Player] score/have/record [X]+ [stat]"
    m = re.search(
        r'(?:Will\s+)?(.+?)\s+(?:score|have|record|get|make)\s+(\d+(?:\.\d+)?)\+?\s*'
        r'(points?|rebounds?|assists?|three[- ]?pointers?|3[- ]?pointers?|threes?|shots?)',
        full_text, re.IGNORECASE)
    if m:
        player = m.group(1).strip().rstrip('?')
        line_raw = float(m.group(2))
        stat = m.group(3)
        line = (line_raw - 0.5) if (line_raw == int(line_raw) and '.' not in m.group(2)) else line_raw
        mkt = kalshi_stat_to_market(stat)
        if mkt and _valid_name(player):
            return (player, mkt, line, mid_prob)

    # Pattern 2: "[Player] Over [X] [stat]"
    m = re.search(
        r'(.+?)\s+[Oo]ver\s+(\d+(?:\.\d+)?)\s*(points?|rebounds?|assists?|shots?)',
        full_text, re.IGNORECASE)
    if m:
        player = m.group(1).strip()
        line = float(m.group(2))
        stat = m.group(3)
        mkt = kalshi_stat_to_market(stat)
        if mkt and _valid_name(player):
            return (player, mkt, line, mid_prob)

    # Pattern 3: number+stat anywhere in full_text
    m = re.search(r'(\d+(?:\.\d+)?)\+?\s*(points?|rebounds?|assists?|shots?)', full_text, re.IGNORECASE)
    if m:
        line_raw = float(m.group(1))
        stat = m.group(2)
        idx = full_text.lower().find(m.group(0).lower())
        if idx > 2:
            player = full_text[:idx].strip().rstrip(' -–—')
            player = re.sub(r'^(will|can)\s+', '', player, flags=re.IGNORECASE).strip()
            if _valid_name(player):
                line = (line_raw - 0.5) if (line_raw == int(line_raw) and '.' not in m.group(1)) else line_raw
                mkt = kalshi_stat_to_market(stat)
                if mkt:
                    return (player, mkt, line, mid_prob)

    # Pattern 4: "Player: N+" compact format in title
    m = re.search(r'^(.+?):\s*(\d+(?:\.\d+)?)\+\s*(points?|rebounds?|assists?|shots?)?',
                  title.strip(), re.IGNORECASE)
    if m:
        player = m.group(1).strip()
        line_raw = float(m.group(2))
        stat_word = m.group(3)
        mkt = kalshi_stat_to_market(stat_word) if stat_word else _stat_from_context()
        if mkt and _valid_name(player):
            line = (line_raw - 0.5) if line_raw == int(line_raw) else line_raw
            return (player, mkt, line, mid_prob)

    # Pattern 5: "Player: N+" in yes_sub_title
    m = re.search(r'^(.+?):\s*(\d+(?:\.\d+)?)\+\s*(points?|rebounds?|assists?|shots?)?',
                  yes_sub.strip(), re.IGNORECASE)
    if m:
        player = m.group(1).strip()
        line_raw = float(m.group(2))
        stat_word = m.group(3)
        mkt = kalshi_stat_to_market(stat_word) if stat_word else _stat_from_context()
        if mkt and _valid_name(player):
            line = (line_raw - 0.5) if line_raw == int(line_raw) else line_raw
            return (player, mkt, line, mid_prob)

    return None

# NBA team lookup for game matching
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

POLYMARKET_API = "https://gamma-api.polymarket.com"
OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"

# ============================================================
# LLM-BASED KALSHI PARSER (fallback for regex misses)
# ============================================================

def parse_kalshi_props_llm(unparsed_markets, log_fn=None):
    """Batch-parse Kalshi markets that regex couldn't handle, using Claude Haiku.
    Returns list of (player, market_type, line, over_prob) tuples.
    Costs ~$0.001–0.005 per scan depending on volume."""
    def log(msg):
        if log_fn: log_fn(msg)

    if not unparsed_markets or not LLM_PARSER_ENABLED:
        return []

    results = []
    BATCH_SIZE = 25
    MAX_BATCHES = 4  # cap cost — at most 100 markets per scan sent to LLM

    try:
        client = _anthropic_sdk.Anthropic(api_key=ANTHROPIC_API_KEY)
    except Exception as e:
        log(f"  LLM client init failed: {e}")
        return []

    batches_sent = 0
    for batch_start in range(0, min(len(unparsed_markets), BATCH_SIZE * MAX_BATCHES), BATCH_SIZE):
        batch = unparsed_markets[batch_start:batch_start + BATCH_SIZE]
        batches_sent += 1

        items_text = []
        for idx, m in enumerate(batch):
            title = (m.get('title', '') or '').strip()
            subtitle = (m.get('subtitle', '') or '').strip()
            yes_sub = (m.get('yes_sub_title', '') or '').strip()
            event_ticker = (m.get('event_ticker', '') or '').strip()
            parts = [f"[{idx}] title: {title!r}"]
            if subtitle: parts.append(f"subtitle: {subtitle!r}")
            if yes_sub: parts.append(f"yes_sub: {yes_sub!r}")
            if event_ticker: parts.append(f"event: {event_ticker!r}")
            items_text.append(" | ".join(parts))

        prompt = """You are parsing Kalshi prediction market descriptions to extract sports player props.

For each market below, output a JSON object with:
- idx: the integer index from the market
- player: player's full name (e.g., "LeBron James"), or null if not a player prop
- market: one of ["player_points", "player_rebounds", "player_assists", "player_threes", "player_shots_on_goal"], or null
- line: the threshold as a float for OVER/UNDER semantics

CRITICAL line conversion rule:
- A Kalshi "N+" market (e.g., "25+ points") is an OVER on line (N - 0.5). Output line as (N - 0.5).
- A half-point line (e.g., "25.5+") means OVER 25.5. Output line as N.

Skip (return null for player/market/line) if:
- It's a team/game outcome, not a player prop
- It's a futures/award/season-long market
- It combines multiple players or conditions (commas, "and", "both")
- The market isn't clearly mapping to one of the allowed market types

Return ONLY a JSON array, no prose, no markdown fences. Example output:
[{"idx":0,"player":"LeBron James","market":"player_points","line":24.5},{"idx":1,"player":null,"market":null,"line":null}]

Markets:
""" + "\n".join(items_text)

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip() if response.content else ""
            # Strip potential markdown fences
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)
            parsed_list = json.loads(text)
            if not isinstance(parsed_list, list):
                continue

            for p in parsed_list:
                if not isinstance(p, dict):
                    continue
                idx = p.get('idx')
                player = p.get('player')
                market_type = p.get('market')
                line = p.get('line')
                if idx is None or player is None or market_type is None or line is None:
                    continue
                if not isinstance(idx, int) or idx < 0 or idx >= len(batch):
                    continue
                if market_type not in (
                    'player_points', 'player_rebounds', 'player_assists',
                    'player_threes', 'player_shots_on_goal'):
                    continue
                try:
                    line = float(line)
                except (ValueError, TypeError):
                    continue

                mkt = batch[idx]
                yb, ya, lp = kalshi_prices(mkt)
                if yb > 0 and ya > 0:
                    mid_prob = ((yb + ya) / 2) / 100.0
                elif yb > 0:
                    mid_prob = yb / 100.0
                elif ya > 0:
                    mid_prob = ya / 100.0
                elif lp > 0:
                    mid_prob = lp / 100.0
                else:
                    continue
                if mid_prob <= 0.02 or mid_prob >= 0.98:
                    continue
                player = str(player).strip()
                if len(player) < 4 or len(player) > 40:
                    continue
                results.append((player, market_type, line, mid_prob))

        except json.JSONDecodeError as e:
            log(f"  LLM batch {batches_sent} JSON error: {str(e)[:80]}")
        except Exception as e:
            log(f"  LLM batch {batches_sent} error: {str(e)[:80]}")

    log(f"  LLM: {batches_sent} batch(es), {len(results)} extracted from {min(len(unparsed_markets), BATCH_SIZE * MAX_BATCHES)} candidates")
    return results


# ============================================================
# ODDS MATH
# ============================================================

def american_to_implied(odds):
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)

def devig_pair(prob_a, prob_b):
    total = prob_a + prob_b
    if total <= 0:
        return (0.5, 0.5)
    return (prob_a / total, prob_b / total)

def format_american(odds):
    try:
        rounded = int(round(odds))
        return f"+{rounded}" if rounded > 0 else str(rounded)
    except:
        return "—"

def implied_to_american(prob):
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return -(prob / (1 - prob)) * 100
    return ((1 - prob) / prob) * 100

def clamp_prob(p, lo=0.01, hi=0.99):
    """Keep probabilities in a sane range so downstream math doesn't blow up."""
    if p != p:  # NaN
        return 0.5
    return max(lo, min(hi, p))

def quarter_kelly(fair_prob, american_odds):
    if american_odds == 0:
        return 0
    if american_odds >= 0:
        b = american_odds / 100.0
    else:
        b = 100.0 / abs(american_odds)
    p = clamp_prob(fair_prob)
    q = 1.0 - p
    kelly = (b * p - q) / b
    if kelly <= 0:
        return 0
    return kelly * 0.25


# ============================================================
# ODDS API FETCHING (with caching)
# ============================================================

def _cache_get(key):
    with _cache_lock:
        entry = _odds_cache.get(key)
        if entry:
            data, ts = entry
            if time.time() - ts < CACHE_TTL:
                return data
            else:
                del _odds_cache[key]
    return None

def _cache_set(key, data):
    with _cache_lock:
        _odds_cache[key] = (data, time.time())

def fetch_odds(sport, market):
    cache_key = f"odds:{sport}:{market}"
    cached = _cache_get(cache_key)
    if cached is not None:
        log_debug(f"  {sport}/{market}: [cache hit]")
        return cached

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
            _cache_set(cache_key, data)
            return data
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_odds(sport, market)
        else:
            try: msg = r.json().get('message', r.text[:100])
            except: msg = r.text[:100]
            log_debug(f"  {sport}/{market}: HTTP {r.status_code} - {msg}")
    except Exception as e:
        log_debug(f"  {sport}/{market}: Error - {str(e)[:80]}")
    return None

def fetch_events(sport):
    cache_key = f"events:{sport}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    key = get_api_key()
    if not key:
        return []
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/events"
    try:
        r = requests.get(url, params={'apiKey': key}, timeout=30)
        if r.status_code == 200:
            events = r.json()
            log_debug(f"  {sport}: {len(events)} events (key ...{key[-6:]})")
            _cache_set(cache_key, events)
            return events
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_events(sport)
    except Exception as e:
        log_debug(f"  {sport} events: Error - {str(e)[:60]}")
    return []

def fetch_event_odds(sport, event_id, market):
    cache_key = f"event_odds:{sport}:{event_id}:{market}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

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
            data = r.json()
            _cache_set(cache_key, data)
            return data
        elif r.status_code == 401:
            mark_key_dead(key)
            return fetch_event_odds(sport, event_id, market)
    except:
        pass
    return None

# ============================================================
# KALSHI SPORTS FETCH (populates kalshi_props + kalshi_games)
# ============================================================

def fetch_kalshi_sports(log_fn=None):
    """Fetch Kalshi sports markets (props + game outcomes)."""
    def log(msg):
        if log_fn: log_fn(msg)

    result = {'props': {}, 'games': {}}
    total_parsed = 0
    total_fetched = 0
    games_matched = 0

    try:
        cursor = ''
        all_props = []

        for page in range(20):
            params = {'status': 'open', 'limit': 200}
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

                if ',' in title:
                    continue

                futures_kws = ['championship', 'champion', 'title', 'finals', 'playoff',
                               'season', 'mvp', 'award', 'division', 'conference']
                is_futures = any(kw in title for kw in futures_kws)
                if not is_futures:
                    game_match = re.search(
                        r'(?:will\s+)?(?:the\s+)?(.+?)\s+(?:wins?|beats?|defeats?)\s+(?:the\s+)?(.+)',
                        title, re.IGNORECASE)
                    if game_match:
                        team_str = game_match.group(1).strip().lower()
                        opponent_str = game_match.group(2).strip().lower()
                        has_opponent = any(key in opponent_str for key in NBA_TEAMS.keys())
                        if has_opponent:
                            yb, ya, lp = kalshi_prices(mkt)
                            if yb > 0 or ya > 0 or lp > 0:
                                mid = (((yb + ya) / 2 if yb > 0 and ya > 0 else (yb or ya or lp))) / 100.0
                                if 0.05 < mid < 0.95:
                                    for key, full_name in NBA_TEAMS.items():
                                        if key in team_str:
                                            result['games'][full_name] = mid
                                            games_matched += 1
                                            break

                has_stat = any(kw in full for kw in ['points', 'rebounds', 'assists', 'three-pointer', '3-pointer', 'shots'])
                has_action = any(kw in full for kw in ['score', 'have', 'record', 'over', 'get', 'make'])
                has_compact = bool(re.search(r':\s*\d+\+', full))
                if has_stat and (has_action or '+' in full):
                    all_props.append(mkt)
                elif has_compact:
                    all_props.append(mkt)

            cursor = data.get('cursor', '')
            if not cursor:
                break
            time.sleep(1.0)

        log(f"  Kalshi: scanned {total_fetched} open markets, {len(all_props)} look like player props")

        unparsed = []
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
            else:
                unparsed.append(mkt)

        regex_parsed = total_parsed
        log(f"  Kalshi regex: {regex_parsed} parsed, {len(unparsed)} unparsed")

        # LLM fallback for anything regex couldn't handle
        if LLM_PARSER_ENABLED and unparsed:
            try:
                llm_results = parse_kalshi_props_llm(unparsed, log_fn=log)
                for player, market_type, line, over_prob in llm_results:
                    norm = normalize_player_name(player)
                    if norm not in result["props"]:
                        result["props"][norm] = {}
                    if market_type not in result["props"][norm]:
                        result["props"][norm][market_type] = {}
                    # Only add if regex didn't already get this player/line
                    if line not in result["props"][norm].get(market_type, {}):
                        result["props"][norm][market_type][line] = over_prob
                        total_parsed += 1
                log(f"  Kalshi LLM: recovered {total_parsed - regex_parsed} additional props")
            except Exception as e:
                log(f"  Kalshi LLM error: {e}")
        elif unparsed and not LLM_PARSER_ENABLED:
            log(f"  Kalshi LLM: skipped ({len(unparsed)} unparsed, set ANTHROPIC_API_KEY to enable)")

        log(f"  Kalshi: {total_parsed} total player props for {len(result['props'])} players")
        log(f"  Kalshi: {games_matched} game outcomes matched for {len(result['games'])} teams")

        if total_parsed == 0 and all_props:
            log(f"  Kalshi: 0 parsed from {len(all_props)} candidates — sample titles:")
            for m in all_props[:3]:
                log(f"    ✗ '{m.get('title','?')}'")

    except Exception as e:
        log(f"  Kalshi error: {e}")

    return result

# ============================================================
# POLYMARKET SPORTS FETCH
# ============================================================

def fetch_polymarket_sports(log_fn=None):
    def log(msg):
        if log_fn: log_fn(msg)

    result = {'games': {}, 'props': {}}
    try:
        resp = requests.get(f"{POLYMARKET_API}/markets",
            params={'closed': 'false', 'limit': 500, 'active': 'true', 'tag': 'sports'},
            timeout=15)

        if resp.status_code != 200:
            resp = requests.get(f"{POLYMARKET_API}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'},
                timeout=15)

        if resp.status_code != 200:
            log(f"  Polymarket API: HTTP {resp.status_code}")
            return result

        markets = resp.json() if isinstance(resp.json(), list) else []
        sports_count = 0

        for m in markets:
            q = (m.get('question', '') or '').strip()
            q_low = q.lower()

            outcomes = m.get('outcomes', '[]')
            prices = m.get('outcomePrices', '[]')
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                    prices = json.loads(prices)
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

            is_futures = any(kw in q_low for kw in [
                'championship', 'champion', 'title', 'finals', 'playoff',
                'season', 'mvp', 'award', 'division', 'conference',
                'win the nba', 'win the nfl', 'win the nhl',
            ])
            if is_futures:
                continue

            game_match = re.search(
                r'(?:will|do)\s+(?:the\s+)?(.+?)\s+(?:win|beat|defeat)\s+(?:the\s+)?(.+?)[\?\.]?$',
                q_low, re.IGNORECASE)
            if game_match:
                team_str = game_match.group(1).strip()
                opponent_str = game_match.group(2).strip()
                has_opponent = any(key in opponent_str for key in NBA_TEAMS.keys())
                if has_opponent:
                    for key, full_name in NBA_TEAMS.items():
                        if key in team_str:
                            result['games'][full_name] = yes_price
                            sports_count += 1
                            break
                continue

            prop_match = re.search(
                r'(?:will\s+)?(.+?)\s+(?:score|have|record|get)\s+(\d+(?:\.\d+)?)\+?\s*'
                r'(points?|rebounds?|assists?|three)',
                q_low, re.IGNORECASE)
            if prop_match:
                player = prop_match.group(1).strip()
                line_raw = float(prop_match.group(2))
                stat = prop_match.group(3)
                mkt = kalshi_stat_to_market(stat)
                if mkt:
                    line = (line_raw - 0.5) if line_raw == int(line_raw) else line_raw
                    norm = normalize_player_name(player)
                    if norm not in result['props']:
                        result['props'][norm] = {}
                    if mkt not in result['props'][norm]:
                        result['props'][norm][mkt] = {}
                    result['props'][norm][mkt][line] = yes_price
                    sports_count += 1

        log(f"  Polymarket: {len(markets)} total markets, {sports_count} sports matched")
        log(f"    Games: {len(result['games'])} teams, Props: {len(result['props'])} players")

    except Exception as e:
        log(f"  Polymarket sports error: {e}")

    return result


# ============================================================
# GAME MARKET ANALYSIS
# ============================================================

def analyze_game_markets(games_data, market_name="", poly_games=None, kalshi_games=None):
    if not games_data:
        return []
    opportunities = []

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')
        sport_key_val = game.get('sport_key', '') or ''
        event_id_val = game.get('id', '') or ''

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

        book_devigged = {}
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
            book_devigged[bk] = {k1: clamp_prob(f1), k2: clamp_prob(f2)}

        if len(book_devigged) < 3:
            continue

        home = game.get('home_team', '')
        away = game.get('away_team', '')
        home_key = None
        away_key = None
        for bk in book_devigged:
            for k in book_devigged[bk]:
                name, pt = k
                if name == home: home_key = k
                elif name == away: away_key = k
            if home_key and away_key:
                break

        for exchange_name, exchange_data in [('kalshi', kalshi_games), ('polymarket', poly_games)]:
            if not exchange_data:
                continue
            ex_home = exchange_data.get(home)
            ex_away = exchange_data.get(away)
            if ex_home and home_key and away_key:
                book_devigged[exchange_name] = {
                    home_key: clamp_prob(ex_home),
                    away_key: clamp_prob(1.0 - ex_home),
                }
            elif ex_away and home_key and away_key:
                book_devigged[exchange_name] = {
                    away_key: clamp_prob(ex_away),
                    home_key: clamp_prob(1.0 - ex_away),
                }

        all_keys = set()
        for bk in book_devigged:
            all_keys.update(book_devigged[bk].keys())

        for eval_book in book_devigged:
            if eval_book not in book_pairs:
                continue
            if eval_book not in CO_BETTABLE:
                continue

            for key in all_keys:
                if key not in book_pairs[eval_book]:
                    continue
                if key not in book_devigged.get(eval_book, {}):
                    continue

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
                consensus_fair = clamp_prob(
                    sum(f * w for f, w in zip(other_fairs, other_weights)) / total_weight)
                eval_implied = american_to_implied(book_pairs[eval_book][key])
                eval_fair = book_devigged[eval_book][key]

                net_edge = (consensus_fair - eval_implied) * 100
                gross_edge = (consensus_fair - eval_fair) * 100
                juice_pct = book_juice.get(eval_book, 0)

                if net_edge < MIN_EDGE_NET:
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
                    'player': display_name, 'game': game_info, 'commence': commence,
                    'sport_key': sport_key_val, 'event_id': event_id_val,
                    'market': market_name, 'book': BOOK_DISPLAY.get(eval_book, eval_book),
                    'book_key': eval_book, 'type': 'game_market',
                    'edge': round(net_edge, 1), 'gross_edge': round(gross_edge, 1),
                    'recommendation': f"BET {display_name}", 'odds': odds,
                    'label1_name': f'{BOOK_DISPLAY.get(eval_book, eval_book)} Odds',
                    'label1_value': format_american(odds),
                    'label2_name': f'Fair Odds ({len(other_fairs)} books)',
                    'label2_value': format_american(fair_american),
                    'label3_name': 'Net Edge', 'label3_value': f"+{net_edge:.1f}%",
                    'target_prob': round(eval_implied * 100, 1),
                    'fair_prob': round(consensus_fair * 100, 1),
                    'juice_display': f"{juice_pct}%",
                    'consensus_books': len(other_fairs),
                    'consensus_detail': other_detail,
                    'kelly_fraction': round(kf * 100, 2),
                    'affiliate_url': affiliate_url(eval_book),
                })
    return opportunities


# ============================================================
# PLAYER PROP ANALYSIS
# ============================================================

def analyze_player_props(games_data, market_name="", kalshi_props=None, poly_props=None, market_key=""):
    if not games_data:
        return []
    if not market_key:
        mn = market_name.lower()
        if 'point' in mn: market_key = 'player_points'
        elif 'rebound' in mn: market_key = 'player_rebounds'
        elif 'assist' in mn: market_key = 'player_assists'
        elif 'shot' in mn: market_key = 'player_shots_on_goal'

    opportunities = []
    stats = {'players': 0, 'same_line': 0, 'diff_line': 0, 'too_few_books': 0}

    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')
        sport_key_val = game.get('sport_key', '') or ''
        event_id_val = game.get('id', '') or ''

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
                        players[player] = {
                            'game': game_info, 'commence': commence,
                            'sport_key': sport_key_val, 'event_id': event_id_val,
                            'books': {},
                        }
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

            books_with_both = {bk: bdata for bk, bdata in books.items()
                               if 'over_odds' in bdata and 'under_odds' in bdata}
            if len(books_with_both) < 3:
                stats['too_few_books'] += 1
                continue

            line_groups = {}
            for bk, bdata in books_with_both.items():
                rounded = round(bdata['line'] * 4) / 4
                if rounded not in line_groups:
                    line_groups[rounded] = {}
                line_groups[rounded][bk] = bdata

            for line_val, group_books in line_groups.items():
                if len(group_books) < 3:
                    stats['diff_line'] += 1
                    continue
                stats['same_line'] += 1

                devigged = {}
                juice_map = {}
                for bk, bdata in group_books.items():
                    ov = american_to_implied(bdata['over_odds'])
                    un = american_to_implied(bdata['under_odds'])
                    juice_map[bk] = round((ov + un - 1.0) * 100, 1)
                    fo, fu = devig_pair(ov, un)
                    devigged[bk] = {'over': clamp_prob(fo), 'under': clamp_prob(fu)}

                if kalshi_props and market_key:
                    norm = normalize_player_name(player)
                    if norm in kalshi_props and market_key in kalshi_props[norm]:
                        k_lines = kalshi_props[norm][market_key]
                        if line_val in k_lines:
                            k_over = clamp_prob(k_lines[line_val])
                            devigged['kalshi'] = {'over': k_over, 'under': 1.0 - k_over}

                if poly_props and market_key:
                    norm = normalize_player_name(player)
                    if norm in poly_props and market_key in poly_props[norm]:
                        p_lines = poly_props[norm][market_key]
                        if line_val in p_lines:
                            p_over = clamp_prob(p_lines[line_val])
                            devigged['polymarket'] = {'over': p_over, 'under': 1.0 - p_over}

                exchange_books = [b for b in ['kalshi', 'polymarket'] if b in devigged]
                eval_candidates = list(group_books.keys()) + exchange_books
                for eval_book in eval_candidates:
                    if eval_book not in CO_BETTABLE and eval_book not in ('kalshi', 'polymarket'):
                        continue
                    other_over_fairs = []
                    other_weights = []
                    other_detail = []
                    for other_bk in devigged:
                        if other_bk == eval_book:
                            continue
                        w = get_weight(other_bk)
                        other_over_fairs.append(devigged[other_bk]['over'])
                        other_weights.append(w)
                        if other_bk in group_books:
                            raw_over = format_american(group_books[other_bk].get('over_odds', 0))
                            raw_under = format_american(group_books[other_bk].get('under_odds', 0))
                            vig = juice_map.get(other_bk, 0)
                        elif other_bk in ('kalshi', 'polymarket'):
                            raw_over = f"{devigged[other_bk]['over']*100:.0f}¢"
                            raw_under = f"{devigged[other_bk]['under']*100:.0f}¢"
                            vig = 0
                        else:
                            raw_over = raw_under = '—'
                            vig = 0
                        other_detail.append({
                            'book': BOOK_DISPLAY.get(other_bk, other_bk),
                            'over_prob': round(devigged[other_bk]['over'] * 100, 1),
                            'over_odds': format_american(implied_to_american(devigged[other_bk]['over'])),
                            'under_odds': format_american(implied_to_american(devigged[other_bk]['under'])),
                            'raw_over': raw_over, 'raw_under': raw_under,
                            'vig': vig, 'weight': w,
                        })

                    if len(other_over_fairs) < 2:
                        continue

                    total_weight = sum(other_weights)
                    consensus_over = clamp_prob(
                        sum(f * w for f, w in zip(other_over_fairs, other_weights)) / total_weight)
                    consensus_under = 1.0 - consensus_over

                    if eval_book in ('kalshi', 'polymarket'):
                        eval_over_imp = devigged[eval_book]['over']
                        eval_under_imp = devigged[eval_book]['under']
                        eval_over_fair = eval_over_imp
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
                        if net_edge < MIN_EDGE_NET:
                            continue

                        fair_odds = implied_to_american(consensus_fair)
                        kf = quarter_kelly(consensus_fair, odds)
                        opportunities.append({
                            'player': player, 'game': data['game'],
                            'commence': data.get('commence', ''),
                            'sport_key': data.get('sport_key', ''),
                            'event_id': data.get('event_id', ''),
                            'market': market_name, 'book': BOOK_DISPLAY.get(eval_book, eval_book),
                            'book_key': eval_book, 'type': 'player_prop',
                            'edge': round(net_edge, 1), 'gross_edge': round(gross_edge, 1),
                            'recommendation': f"{side} {line_val}", 'odds': odds,
                            'line': line_val,
                            'label1_name': f'{BOOK_DISPLAY.get(eval_book, eval_book)} Odds',
                            'label1_value': format_american(odds),
                            'label2_name': f'Fair Odds ({len(other_over_fairs)} books)',
                            'label2_value': format_american(fair_odds),
                            'label3_name': 'Net Edge', 'label3_value': f"+{net_edge:.1f}%",
                            'target_prob': round(eval_imp * 100, 1),
                            'fair_prob': round(consensus_fair * 100, 1),
                            'juice_display': f"{juice_pct}%",
                            'consensus_books': len(other_over_fairs),
                            'consensus_detail': other_detail,
                            'kelly_fraction': round(kf * 100, 2),
                            'affiliate_url': affiliate_url(eval_book),
                        })

    log_debug(f"    Players: {stats['players']}, same-line: {stats['same_line']}, "
              f"too few books: {stats['too_few_books']}, diff-line: {stats['diff_line']} "
              f"→ {len(opportunities)} +EV")
    return opportunities


# ============================================================
# ARBITRAGE DETECTION
# ============================================================

def find_game_arbs(games_data, market_name=""):
    if not games_data:
        return []
    arbs = []
    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')

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

        all_keys = set()
        for bk in book_odds:
            all_keys.update(book_odds[bk].keys())
        keys_list = sorted(all_keys, key=str)
        if len(keys_list) < 2:
            continue

        side_a_key = keys_list[0]
        side_b_key = keys_list[1]
        best_a_odds = best_b_odds = None
        best_a_book = best_b_book = None

        for bk in book_odds:
            if bk not in CO_BETTABLE:
                continue
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
            continue

        imp_a = american_to_implied(best_a_odds)
        imp_b = american_to_implied(best_b_odds)
        total = imp_a + imp_b
        if total < 1.0:
            profit_pct = round((1.0 - total) * 100, 2)
            name_a, point_a = side_a_key
            name_b, point_b = side_b_key
            dn_a = f"{name_a} ML" if point_a is None else f"{name_a} {point_a:+.1f}"
            dn_b = f"{name_b} ML" if point_b is None else f"{name_b} {point_b:+.1f}"
            stake_a = round(100 * imp_a / (imp_a + imp_b), 2)
            stake_b = round(100 - stake_a, 2)

            arbs.append({
                'player': f"ARB: {dn_a} + {dn_b}", 'game': game_info, 'commence': commence,
                'market': market_name,
                'book': f"{BOOK_DISPLAY.get(best_a_book, best_a_book)} / {BOOK_DISPLAY.get(best_b_book, best_b_book)}",
                'type': 'arbitrage',
                'edge': profit_pct, 'gross_edge': profit_pct,
                'recommendation': f"Guaranteed {profit_pct}% profit",
                'odds': best_a_odds,
                'label1_name': f'{BOOK_DISPLAY.get(best_a_book, best_a_book)}: {dn_a}',
                'label1_value': format_american(best_a_odds),
                'label2_name': f'{BOOK_DISPLAY.get(best_b_book, best_b_book)}: {dn_b}',
                'label2_value': format_american(best_b_odds),
                'label3_name': 'Guaranteed Profit', 'label3_value': f"+{profit_pct}%",
                'target_prob': round(total * 100, 1), 'fair_prob': 100.0,
                'juice_display': f"{round(total * 100, 1)}% combined",
                'stake_a': stake_a, 'stake_b': stake_b,
                'affiliate_url_a': affiliate_url(best_a_book),
                'affiliate_url_b': affiliate_url(best_b_book),
            })
    return arbs


def find_prop_arbs(games_data, market_name=""):
    if not games_data:
        return []
    arbs = []
    for game in games_data:
        game_info = f"{game.get('away_team', '?')} @ {game.get('home_team', '?')}"
        commence = game.get('commence_time', '')

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
            line_groups = {}
            for bk, bdata in books.items():
                if 'over_odds' not in bdata or 'under_odds' not in bdata:
                    continue
                rounded = round(bdata['line'] * 4) / 4
                if rounded not in line_groups:
                    line_groups[rounded] = {}
                line_groups[rounded][bk] = bdata

            for line_val, group_books in line_groups.items():
                if len(group_books) < 2:
                    continue
                best_over_odds = best_under_odds = None
                best_over_book = best_under_book = None
                for bk, bdata in group_books.items():
                    if bk not in CO_BETTABLE:
                        continue
                    if best_over_odds is None or american_to_implied(bdata['over_odds']) < american_to_implied(best_over_odds):
                        best_over_odds = bdata['over_odds']
                        best_over_book = bk
                    if best_under_odds is None or american_to_implied(bdata['under_odds']) < american_to_implied(best_under_odds):
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
                        'player': f"ARB: {player}", 'game': data['game'],
                        'commence': data.get('commence', ''),
                        'market': market_name,
                        'book': f"{BOOK_DISPLAY.get(best_over_book, best_over_book)} / {BOOK_DISPLAY.get(best_under_book, best_under_book)}",
                        'type': 'arbitrage',
                        'edge': profit_pct, 'gross_edge': profit_pct,
                        'recommendation': f"OVER {line_val} + UNDER {line_val}",
                        'odds': best_over_odds, 'line': line_val,
                        'label1_name': f'{BOOK_DISPLAY.get(best_over_book, best_over_book)}: Over {line_val}',
                        'label1_value': format_american(best_over_odds),
                        'label2_name': f'{BOOK_DISPLAY.get(best_under_book, best_under_book)}: Under {line_val}',
                        'label2_value': format_american(best_under_odds),
                        'label3_name': 'Guaranteed Profit', 'label3_value': f"+{profit_pct}%",
                        'target_prob': round(total * 100, 1), 'fair_prob': 100.0,
                        'juice_display': f"{round(total * 100, 1)}% combined",
                        'stake_over': stake_over, 'stake_under': stake_under,
                        'affiliate_url_a': affiliate_url(best_over_book),
                        'affiliate_url_b': affiliate_url(best_under_book),
                    })
    return arbs


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
# CROSS-EXCHANGE SCANNER (Kalshi vs Polymarket, non-sports)
# ============================================================

def fetch_cross_exchange_opps():
    opportunities = []
    try:
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
                    continue
                yb, ya, lp = kalshi_prices(m)
                if yb <= 0 and ya <= 0 and lp <= 0:
                    continue
                mid = (((yb + ya) / 2 if yb > 0 and ya > 0 else (yb or ya or lp))) / 100.0
                if 0.05 < mid < 0.95:
                    kalshi_markets[title] = {
                        'title': m.get('title', ''), 'mid': mid,
                        'yes_bid': yb, 'yes_ask': ya,
                        'ticker': m.get('ticker', ''),
                        'event_ticker': m.get('event_ticker', ''),
                    }
            cursor = data.get('cursor', '')
            if not cursor:
                break
            time.sleep(0.3)
        log_debug(f"  Kalshi: {len(kalshi_markets)} tradeable non-combo markets")

        log_debug("  Fetching Polymarket markets...")
        poly_markets = {}
        try:
            resp = requests.get(f"{POLYMARKET_API}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'}, timeout=15)
            if resp.status_code == 200:
                for m in resp.json():
                    q = (m.get('question', '') or '').lower().strip()
                    outcomes = m.get('outcomes', '[]')
                    prices = m.get('outcomePrices', '[]')
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                            prices = json.loads(prices)
                        except:
                            continue
                    # Only 2-outcome Yes/No markets for cross-exchange comparison
                    if len(outcomes) != 2:
                        continue
                    if not any(o.lower() in ('yes', 'no') for o in outcomes):
                        continue
                    if len(prices) < 2:
                        continue
                    try:
                        yes_idx = next((i for i, o in enumerate(outcomes) if o.lower() == 'yes'), 0)
                        yes_price = float(prices[yes_idx])
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
        log_debug(f"  Polymarket: {len(poly_markets)} active Y/N markets")

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
            if diff < 3:
                continue

            if k_mid < p_yes:
                edge = (p_yes - k_mid) * 100
                bet_on = 'Kalshi'
                bet_on_key = 'kalshi'
                bet_odds = round(implied_to_american(k_mid))
            else:
                edge = (k_mid - p_yes) * 100
                bet_on = 'Polymarket'
                bet_on_key = 'polymarket'
                bet_odds = round(implied_to_american(p_yes))

            opportunities.append({
                'player': k_data['title'][:60],
                'game': f"Kalshi {k_mid*100:.0f}¢ vs Poly {p_yes*100:.0f}¢",
                'commence': '', 'market': 'Cross-Exchange',
                'book': bet_on, 'book_key': bet_on_key, 'type': 'cross_exchange',
                'edge': round(edge, 1), 'gross_edge': round(edge, 1),
                'recommendation': f"BUY YES on {bet_on}",
                'odds': bet_odds,
                'label1_name': 'Kalshi',
                'label1_value': f"{k_mid*100:.0f}¢ ({format_american(round(implied_to_american(k_mid)))})",
                'label2_name': 'Polymarket',
                'label2_value': f"{p_yes*100:.0f}¢ ({format_american(round(implied_to_american(p_yes)))})",
                'label3_name': 'Spread', 'label3_value': f"+{diff:.1f}%",
                'target_prob': round(min(k_mid, p_yes) * 100, 1),
                'fair_prob': round(max(k_mid, p_yes) * 100, 1),
                'juice_display': '—', 'consensus_books': 2,
                'kelly_fraction': round(quarter_kelly(max(k_mid, p_yes), bet_odds) * 100, 2),
                'affiliate_url': affiliate_url(bet_on_key),
            })
        log_debug(f"  Cross-exchange: {matches} matched, {len(opportunities)} with 3%+ spread")
    except Exception as e:
        log_debug(f"  Cross-exchange error: {e}")
    return opportunities


# ============================================================
# WEATHER SCANNER (ENSEMBLE MODEL — improved)
# ============================================================

def _fetch_ensemble_forecast(lat, lon):
    """Fetch multi-model ensemble forecast. Returns (mean_high, std_dev) or (None, 3.0)."""
    try:
        resp = requests.get(OPEN_METEO_API, params={
            'latitude': lat, 'longitude': lon,
            'daily': 'temperature_2m_max',
            'temperature_unit': 'fahrenheit',
            'forecast_days': 3,
            'models': 'gfs_seamless,ecmwf_ifs04,icon_seamless',
            'timezone': 'America/New_York',
        }, timeout=10)
        if resp.status_code == 429:
            time.sleep(3)
            resp = requests.get(OPEN_METEO_API, params={
                'latitude': lat, 'longitude': lon,
                'daily': 'temperature_2m_max',
                'temperature_unit': 'fahrenheit',
                'forecast_days': 3,
                'models': 'gfs_seamless,ecmwf_ifs04,icon_seamless',
                'timezone': 'America/New_York',
            }, timeout=10)
        if resp.status_code != 200:
            return (None, 3.0)
        data = resp.json()
        daily = data.get('daily', {})
        # Collect today's max from each model series
        temps = []
        for key, vals in daily.items():
            if 'temperature_2m_max' in key and isinstance(vals, list) and vals:
                try:
                    v = float(vals[0])
                    temps.append(v)
                except (ValueError, TypeError):
                    pass
        if len(temps) == 0:
            return (None, 3.0)
        mean = sum(temps) / len(temps)
        if len(temps) >= 2:
            variance = sum((t - mean) ** 2 for t in temps) / len(temps)
            std_dev = max(2.0, math.sqrt(variance))
        else:
            std_dev = 3.0
        return (mean, std_dev)
    except Exception:
        return (None, 3.0)


def fetch_weather_opps():
    opportunities = []
    try:
        WEATHER_SERIES = {
            'KXHIGHNY':  {'city': 'NYC',     'lat': 40.78, 'lon': -73.97},
            'KXHIGHCHI': {'city': 'Chicago', 'lat': 41.88, 'lon': -87.63},
            'KXHIGHMIA': {'city': 'Miami',   'lat': 25.76, 'lon': -80.19},
            'KXHIGHLAX': {'city': 'LA',      'lat': 34.05, 'lon': -118.24},
            'KXHIGHDEN': {'city': 'Denver',  'lat': 39.74, 'lon': -104.98},
        }
        weather_mkts = []
        log_debug("  Fetching Kalshi weather series...")
        for series_ticker, info in WEATHER_SERIES.items():
            try:
                resp = kalshi_get(f"{KALSHI_API}/markets",
                    params={'series_ticker': series_ticker, 'status': 'open', 'limit': 50}, timeout=10)
                if resp.status_code == 429:
                    time.sleep(5)
                    resp = kalshi_get(f"{KALSHI_API}/markets",
                        params={'series_ticker': series_ticker, 'status': 'open', 'limit': 50}, timeout=10)
                if resp.status_code == 200:
                    mkts = resp.json().get('markets', [])
                    count = 0
                    for m in mkts:
                        yb, ya, lp = kalshi_prices(m)
                        if yb > 0 or ya > 0 or lp > 0:
                            m['yes_bid'] = yb if yb > 0 else lp
                            m['yes_ask'] = ya if ya > 0 else lp
                            m['_city_info'] = info
                            m['_series'] = series_ticker
                            weather_mkts.append(m)
                            count += 1
                    if count > 0:
                        log_debug(f"    {series_ticker} ({info['city']}): {count} markets")
                time.sleep(1.5)
            except Exception as e:
                log_debug(f"    {series_ticker}: error {e}")
                continue

        log_debug(f"  Kalshi weather: {len(weather_mkts)} total markets")
        if not weather_mkts:
            return []

        forecast_cache = {}
        matched_count = 0

        for mkt in weather_mkts:
            title = (mkt.get('title', '') or '')
            title_low = title.lower()
            city_info = mkt.get('_city_info', {})
            city_key = city_info.get('city', '?')

            temp_match = re.search(r'(\d+(?:\.\d+)?)\s*°?(?:f|fahrenheit)?', title_low)
            if not temp_match:
                continue
            threshold = float(temp_match.group(1))
            if threshold < -20 or threshold > 140:
                continue

            is_over = any(kw in title_low for kw in ['above', 'over', 'higher', 'at least', 'exceed', 'or more', 'high'])
            is_under = any(kw in title_low for kw in ['below', 'under', 'lower', 'at most', 'or less', 'low'])
            if not is_over and not is_under:
                is_over = True

            yb = mkt.get('yes_bid', 0) or 0
            ya = mkt.get('yes_ask', 0) or 0
            k_mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0

            try:
                if city_key not in forecast_cache:
                    time.sleep(0.5)
                    mean_high, std_dev = _fetch_ensemble_forecast(city_info['lat'], city_info['lon'])
                    forecast_cache[city_key] = (mean_high, std_dev)
                    if mean_high is not None:
                        log_debug(f"    {city_key} ensemble: {mean_high:.1f}°F ±{std_dev:.1f}")

                mean_high, std_dev = forecast_cache[city_key]
                if mean_high is None:
                    continue

                z = (threshold - mean_high) / std_dev
                def norm_cdf(x):
                    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

                if is_over:
                    model_prob = 1.0 - norm_cdf(z)
                else:
                    model_prob = norm_cdf(z)
                if model_prob < 0.02 or model_prob > 0.98:
                    continue

                edge = (model_prob - k_mid) * 100
                matched_count += 1
                if abs(edge) < 3:
                    continue

                if edge > 0:
                    bet_action = 'BUY YES'
                    display_edge = edge
                else:
                    bet_action = 'BUY NO'
                    display_edge = abs(edge)
                    model_prob = 1.0 - model_prob
                    k_mid = 1.0 - k_mid

                opportunities.append({
                    'player': title[:60],
                    'game': f"{city_key.upper()} · Ensemble high: {mean_high:.0f}°F ±{std_dev:.1f} · Threshold: {threshold:.0f}°F",
                    'commence': '', 'market': 'Weather',
                    'book': 'Kalshi', 'book_key': 'kalshi', 'type': 'weather',
                    'edge': round(display_edge, 1), 'gross_edge': round(display_edge, 1),
                    'recommendation': f"{bet_action} on Kalshi",
                    'odds': round(implied_to_american(clamp_prob(k_mid))) if 0 < k_mid < 1 else 0,
                    'label1_name': 'Kalshi Price',
                    'label1_value': f"{(mkt.get('yes_bid',0) or 0)}–{(mkt.get('yes_ask',0) or 0)}¢",
                    'label2_name': 'Ensemble Fair', 'label2_value': f"{model_prob*100:.0f}%",
                    'label3_name': 'Edge', 'label3_value': f"+{display_edge:.1f}%",
                    'target_prob': round(k_mid * 100, 1),
                    'fair_prob': round(model_prob * 100, 1),
                    'juice_display': f"±{std_dev:.1f}°F",
                    'consensus_books': 0,
                    'kelly_fraction': round(quarter_kelly(model_prob, round(implied_to_american(clamp_prob(k_mid)))) * 100, 2) if 0 < k_mid < 1 else 0,
                    'affiliate_url': affiliate_url('kalshi'),
                })
            except Exception:
                continue
        log_debug(f"  Weather: {matched_count} compared, {len(opportunities)} with 3%+ edge")
    except Exception as e:
        log_debug(f"  Weather error: {e}")
    return opportunities


# ============================================================
# ECONOMIC SCANNER (unchanged core, just bundled)
# ============================================================

def _econ_keywords(title):
    t = re.sub(r'[^a-z0-9%\s.]', '', title.lower())
    stop = {'will', 'the', 'be', 'a', 'an', 'in', 'on', 'at', 'to', 'of',
            'for', 'is', 'it', 'by', 'or', 'and', 'this', 'that', 'than', 'more'}
    return set(w for w in t.split() if w not in stop and len(w) > 1)

def _topic_tag(title):
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
    t = title.lower()
    return any(w in t for w in ['fed', 'fomc', 'rate cut', 'rate hike', 'interest rate',
        'federal reserve', 'federal funds', 'target rate', 'basis point', 'bps'])

def fetch_fedwatch_probs():
    try:
        resp = requests.get('https://growbeansprout.com/tools/fedwatch', timeout=10,
            headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code != 200:
            return None
        html = resp.text
        m = re.search(r'(\d+\.?\d*)%\s*probability.*?(?:cut|hold|unchanged|steady|maintain|raise|hike)',
            html, re.IGNORECASE | re.DOTALL)
        if not m:
            m = re.search(r'(?:cut|hold|unchanged|steady|maintain|raise|hike).*?(\d+\.?\d*)%',
                html, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        prob = float(m.group(1)) / 100.0
        text_after = html[m.start():m.end() + 100].lower()
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
    opportunities = []
    try:
        fedwatch = fetch_fedwatch_probs()
        if fedwatch:
            log_debug(f"  CME FedWatch: hold={fedwatch['hold']*100:.1f}%, cut={fedwatch['cut_25']*100:.1f}%")
        else:
            log_debug("  CME FedWatch: unavailable")

        ECON_SERIES = ['KXFED', 'FED', 'FEDDECISION', 'KXCPI', 'CPI', 'KXCPICORE',
                       'KXPAYROLLS', 'PAYROLLS', 'KXGDP', 'GDP', 'RATECUT', 'KXRATECUT']
        econ_mkts = []
        log_debug("  Fetching Kalshi economic markets...")
        for series in ECON_SERIES:
            try:
                resp = kalshi_get(f"{KALSHI_API}/markets",
                    params={'series_ticker': series, 'status': 'open', 'limit': 50}, timeout=10)
                if resp.status_code == 429:
                    time.sleep(5)
                    resp = kalshi_get(f"{KALSHI_API}/markets",
                        params={'series_ticker': series, 'status': 'open', 'limit': 50}, timeout=10)
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
                time.sleep(1.5)
            except:
                continue
        log_debug(f"  Kalshi: {len(econ_mkts)} economic markets")

        poly_mkts = []
        try:
            resp = requests.get(f"{POLYMARKET_API}/markets",
                params={'closed': 'false', 'limit': 500, 'active': 'true'}, timeout=15)
            if resp.status_code == 200:
                for m in resp.json():
                    q = m.get('question', '') or ''
                    topic = _topic_tag(q)
                    if not topic:
                        continue
                    outcomes = m.get('outcomes', '[]')
                    prices = m.get('outcomePrices', '[]')
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                            prices = json.loads(prices)
                        except:
                            continue
                    if len(outcomes) >= 2 and len(prices) >= 2:
                        try:
                            yes_price = float(prices[0])
                        except:
                            continue
                        if 0.03 < yes_price < 0.97:
                            poly_mkts.append({
                                'title': q, 'yes_price': yes_price, 'topic': topic,
                                'month': _extract_month(q), 'keywords': _econ_keywords(q),
                                'volume': float(m.get('volume', 0) or 0),
                            })
        except Exception as e:
            log_debug(f"  Polymarket econ error: {e}")
        log_debug(f"  Polymarket: {len(poly_mkts)} econ markets")

        from difflib import SequenceMatcher
        matched = 0
        min_edge = 3.0

        for km in econ_mkts:
            k_title = km.get('title', '')
            k_topic = _topic_tag(k_title)
            if not k_topic:
                continue
            yb = km.get('yes_bid', 0) or 0
            ya = km.get('yes_ask', 0) or 0
            if yb == 0 and ya == 0:
                yb, ya, lp = kalshi_prices(km)
                if yb == 0 and ya == 0 and lp > 0:
                    yb = ya = lp
            k_mid = ((yb + ya) / 2 if yb > 0 and ya > 0 else yb or ya) / 100.0
            if k_mid <= 0.03 or k_mid >= 0.97:
                continue

            sources = []
            if fedwatch and _is_fed_rate_market(k_title):
                t = k_title.lower()
                fw_prob = None
                if any(w in t for w in ['cut', 'lower', 'reduce', 'ease', 'decrease']):
                    fw_prob = fedwatch['cut_25'] + fedwatch.get('cut_50', 0)
                elif any(w in t for w in ['hold', 'unchanged', 'steady', 'maintain', 'no change', 'pause']):
                    fw_prob = fedwatch['hold']
                elif any(w in t for w in ['hike', 'raise', 'increase', 'tighten']):
                    fw_prob = fedwatch.get('hike', 0)
                elif 'rate' in t:
                    nums = re.findall(r'(\d+)\s*(?:cut|rate)', t)
                    if nums:
                        fw_prob = fedwatch['cut_25']
                if fw_prob is not None and 0.01 < fw_prob < 0.99:
                    sources.append((fw_prob, 3, 'FedWatch'))

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

            if not sources:
                continue
            total_weight = sum(w for _, w, _ in sources)
            fair_prob = clamp_prob(sum(p * w for p, w, _ in sources) / total_weight)

            edge_yes = (fair_prob - k_mid) * 100
            edge_no = (k_mid - fair_prob) * 100
            actionable_edge = max(edge_yes, edge_no)
            if actionable_edge < min_edge:
                continue
            matched += 1

            if edge_yes > edge_no:
                bet_action = 'BUY YES'
                edge = round(edge_yes, 1)
                bet_odds = round(implied_to_american(k_mid))
            else:
                bet_action = 'BUY NO'
                edge = round(edge_no, 1)
                no_implied = 1.0 - k_mid
                bet_odds = round(implied_to_american(no_implied))

            source_desc = ' + '.join(f'{n}({w}x)' for _, w, n in sources)
            opportunities.append({
                'player': k_title[:60],
                'game': f"Fair: {fair_prob*100:.0f}% [{source_desc}] | {k_topic.upper()}",
                'commence': '', 'market': 'Economic',
                'book': 'Kalshi', 'book_key': 'kalshi', 'type': 'economic',
                'edge': edge, 'gross_edge': edge,
                'recommendation': f"{bet_action} (consensus {fair_prob*100:.0f}% vs Kalshi {k_mid*100:.0f}%)",
                'odds': bet_odds,
                'label1_name': 'Kalshi',
                'label1_value': f"{k_mid*100:.0f}¢ ({format_american(round(implied_to_american(k_mid)))})",
                'label2_name': 'Consensus Fair',
                'label2_value': f"{fair_prob*100:.0f}¢ [{source_desc}]",
                'label3_name': 'Edge', 'label3_value': f"+{edge:.1f}% {bet_action}",
                'target_prob': round(k_mid * 100, 1),
                'fair_prob': round(fair_prob * 100, 1),
                'juice_display': f'{len(sources)} source(s)',
                'consensus_books': len(sources),
                'kelly_fraction': round(quarter_kelly(fair_prob, bet_odds) * 100, 2) if bet_odds != 0 else 0,
                'affiliate_url': affiliate_url('kalshi'),
            })
        log_debug(f"  Econ: {matched} matched, {len(opportunities)} above {min_edge}% edge")
    except Exception as e:
        log_debug(f"  Economic error: {e}")
    return opportunities


# ============================================================
# MAIN SCAN
# ============================================================

def scan_markets():
    global _dead_keys
    with _state_lock:
        state['scanning'] = True
        state['debug_info'] = []
    with _key_lock:
        _dead_keys = set()

    scan_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    all_opps = []

    log_debug("=== SCAN STARTED ===")
    log_debug(f"Scan ID: {scan_id}")
    if not API_KEYS:
        log_debug("⚠️  No ODDS_API_KEYS configured in env — sports scanning will be skipped")
    log_debug(f"Bettable: {', '.join(BOOK_DISPLAY.get(b, b) for b in CO_BETTABLE)}")
    log_debug(f"Consensus: + {', '.join(BOOK_DISPLAY.get(b, b) for b in CONSENSUS_ONLY)} + Kalshi + Polymarket")
    log_debug(f"Strategy: CO book vs weighted consensus (Pinnacle/Kalshi/Poly 3x) | Min edge: {MIN_EDGE_NET}%")

    # ---- Exchange data for consensus ----
    log_debug("--- Polymarket sports ---")
    try:
        poly_sports = fetch_polymarket_sports(log_fn=log_debug)
        poly_props = poly_sports.get('props', {})
        poly_games = poly_sports.get('games', {})
    except Exception as e:
        log_debug(f"  Polymarket sports failed: {e}")
        poly_props, poly_games = {}, {}

    log_debug("--- Kalshi sports ---")
    try:
        kalshi_sports = fetch_kalshi_sports(log_fn=log_debug)
        kalshi_props = kalshi_sports.get('props', {})
        kalshi_games = kalshi_sports.get('games', {})
    except Exception as e:
        log_debug(f"  Kalshi sports failed: {e}")
        kalshi_props, kalshi_games = {}, {}

    log_debug(f"  Exchange consensus loaded: "
              f"Kalshi {len(kalshi_props)} players / {len(kalshi_games)} games, "
              f"Poly {len(poly_props)} players / {len(poly_games)} games")

    # ---- Sports: Player Props ----
    if API_KEYS:
        log_debug("--- Player Props ---")
        for sport, prop_markets, max_ev in PROP_MARKETS:
            if len(_dead_keys) >= len(API_KEYS):
                log_debug("  All keys exhausted — stopping props")
                break
            try:
                opps, arbs = fetch_event_props(sport, prop_markets, max_events=max_ev,
                    kalshi_props=kalshi_props, poly_props=poly_props)
                all_opps.extend(opps)
                all_opps.extend(arbs)
            except Exception as e:
                log_debug(f"  {sport} props failed: {e}")

        # ---- Sports: Moneylines ----
        log_debug("--- Moneylines ---")
        for sport, market, name in GAME_MARKETS:
            if len(_dead_keys) >= len(API_KEYS):
                log_debug("  All keys exhausted — stopping moneylines")
                break
            try:
                games = fetch_odds(sport, market)
                if games:
                    opps = analyze_game_markets(games, name,
                        poly_games=poly_games, kalshi_games=kalshi_games)
                    arbs = find_game_arbs(games, name)
                    all_opps.extend(opps)
                    all_opps.extend(arbs)
            except Exception as e:
                log_debug(f"  {sport} {name} failed: {e}")
            time.sleep(0.3)

    # ---- Cross-Exchange ----
    log_debug("--- Cross-Exchange (Kalshi ↔ Polymarket) ---")
    try:
        all_opps.extend(fetch_cross_exchange_opps())
    except Exception as e:
        log_debug(f"  Cross-exchange failed: {e}")

    # ---- Weather ----
    log_debug("--- Weather (Ensemble Model) ---")
    try:
        all_opps.extend(fetch_weather_opps())
    except Exception as e:
        log_debug(f"  Weather failed: {e}")

    # ---- Economic ----
    log_debug("--- Economic ---")
    try:
        all_opps.extend(fetch_econ_opps())
    except Exception as e:
        log_debug(f"  Economic failed: {e}")

    # Sort: arbs first, then by edge descending
    all_opps.sort(key=lambda x: (0 if x['type'] == 'arbitrage' else 1, -x['edge']))

    # Log every opportunity to DB for CLV tracking
    try:
        for opp in all_opps:
            log_opportunity(opp, scan_id)
    except Exception as e:
        log_debug(f"DB logging error: {e}")

    with _state_lock:
        state['opportunities'] = all_opps
        state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        state['scanning'] = False
        state['scan_id'] = scan_id

    active = len(API_KEYS) - len(_dead_keys)
    by_type = {}
    for o in all_opps:
        by_type[o['type']] = by_type.get(o['type'], 0) + 1
    summary = ', '.join(f"{k}:{v}" for k, v in sorted(by_type.items()))
    log_debug(f"=== DONE: {len(all_opps)} total [{summary}] ({active}/{len(API_KEYS)} keys active) ===")


# ============================================================
# CLOSING LINE VALUE (CLV) CAPTURE
# ============================================================

def _market_name_to_api_key(market_name):
    """Map stored market names (e.g., 'NBA Points') to Odds API market keys."""
    if not market_name:
        return None
    mn = market_name.lower()
    if 'point' in mn: return 'player_points'
    if 'rebound' in mn: return 'player_rebounds'
    if 'assist' in mn: return 'player_assists'
    if 'shot' in mn: return 'player_shots_on_goal'
    if 'three' in mn: return 'player_threes'
    if 'moneyline' in mn or ' ml' in mn: return 'h2h'
    return None

def update_clv():
    """Re-fetch odds for bets where the game is approaching tip-off, capturing
    closing line value. Runs periodically in a background thread and on demand
    via /api/update-clv. Only touches sports bets (player_prop, game_market)."""
    updated = 0
    scanned = 0
    try:
        now = datetime.now()
        future_cutoff = (now + timedelta(hours=CLV_WINDOW_HOURS_AHEAD)).isoformat()
        now_iso = now.isoformat()

        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, sport_key, event_id, sport, market, player, game, book,
                       bet_type, recommendation, line, odds, closing_odds
                FROM opportunities
                WHERE commence_time > ? AND commence_time < ?
                  AND bet_type IN ('player_prop', 'game_market')
                  AND odds != 0
                  AND sport_key != ''
                  AND event_id != ''
                ORDER BY commence_time ASC
            """, (now_iso, future_cutoff)).fetchall()

        if not rows:
            return 0

        log_debug(f"CLV: checking {len(rows)} opps with games starting in next {CLV_WINDOW_HOURS_AHEAD}h")

        # Group by (sport_key, event_id, market_api_key) so we fetch each event once
        groups = {}
        for row in rows:
            market_api_key = _market_name_to_api_key(row['market'])
            if not market_api_key:
                continue
            key = (row['sport_key'], row['event_id'], market_api_key)
            groups.setdefault(key, []).append(row)

        for (sport_key, event_id, market_api_key), group_rows in groups.items():
            scanned += len(group_rows)

            # Fetch current odds for this event+market
            event_data = None
            if market_api_key == 'h2h':
                all_games = fetch_odds(sport_key, market_api_key)
                if all_games:
                    event_data = next((g for g in all_games if g.get('id') == event_id), None)
            else:
                ed = fetch_event_odds(sport_key, event_id, market_api_key)
                if isinstance(ed, list):
                    event_data = ed[0] if ed else None
                else:
                    event_data = ed
            if not event_data:
                continue

            for row in group_rows:
                book_name = row['book'] or ''
                # Resolve display name back to API book key
                book_key = BOOK_KEY_LOOKUP.get(book_name, '')
                if not book_key:
                    book_key = book_name.lower().replace(' ', '')

                closing_odds = None
                for bk in event_data.get('bookmakers', []):
                    if bk.get('key') != book_key:
                        continue
                    for mkt in bk.get('markets', []):
                        if mkt.get('key') != market_api_key:
                            continue

                        if market_api_key.startswith('player_'):
                            rec = (row['recommendation'] or '').lower()
                            is_over = 'over' in rec
                            is_under = 'under' in rec
                            row_line = row['line'] or 0
                            for outcome in mkt.get('outcomes', []):
                                if (outcome.get('description', '') or '') != (row['player'] or ''):
                                    continue
                                olp = outcome.get('point')
                                if olp is None or abs(float(olp) - float(row_line)) > 0.01:
                                    continue
                                side = (outcome.get('name', '') or '').lower()
                                if is_over and 'over' not in side: continue
                                if is_under and 'under' not in side: continue
                                closing_odds = outcome.get('price')
                                break
                        else:  # h2h moneyline
                            rec = (row['recommendation'] or '').replace('BET ', '').replace(' ML', '').strip()
                            for outcome in mkt.get('outcomes', []):
                                if outcome.get('name', '') == rec:
                                    closing_odds = outcome.get('price')
                                    break

                        if closing_odds is not None:
                            break
                    if closing_odds is not None:
                        break

                if closing_odds is None:
                    continue

                try:
                    bet_implied = american_to_implied(row['odds'])
                    close_implied = american_to_implied(closing_odds)
                    clv = round((close_implied - bet_implied) * 100, 2)
                    with get_db() as conn2:
                        conn2.execute("""
                            UPDATE opportunities
                            SET closing_odds = ?, clv = ?, clv_captured_at = ?
                            WHERE id = ?
                        """, (int(closing_odds), clv, datetime.now().isoformat(), row['id']))
                    updated += 1
                except Exception as e:
                    log_debug(f"CLV update error for id={row['id']}: {e}")

        log_debug(f"CLV: {updated}/{scanned} rows updated with closing odds")
    except Exception as e:
        log_debug(f"CLV capture error: {e}")
    return updated


def _clv_background_worker():
    """Periodic CLV capture — runs in a daemon thread. Interval controlled by
    CLV_WORKER_INTERVAL_SEC env var (default 30 min)."""
    # Small initial delay so we don't race app startup
    time.sleep(60)
    while True:
        try:
            update_clv()
        except Exception as e:
            print(f"CLV worker error: {e}", flush=True)
        time.sleep(CLV_WORKER_INTERVAL_SEC)


# Fire up the background CLV thread unless explicitly disabled
if not os.environ.get('DISABLE_CLV_WORKER'):
    try:
        threading.Thread(target=_clv_background_worker, daemon=True).start()
    except Exception as e:
        print(f"Could not start CLV worker: {e}", flush=True)


# ============================================================
# ROUTES
# ============================================================

def _auth_check():
    """Returns a Flask response if unauthorized, or None if OK."""
    if not SCAN_KEY:
        return None
    provided = request.args.get('key', '')
    if not provided and request.is_json:
        try:
            provided = (request.json or {}).get('key', '')
        except:
            provided = ''
    if provided != SCAN_KEY:
        return jsonify({'error': 'Invalid key'}), 403
    return None

@app.route('/')
def index():
    return render_template('arbitrage.html')

@app.route('/api/scan', methods=['POST'])
def trigger_scan():
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    with _state_lock:
        if state['scanning']:
            return jsonify({'error': 'Scan in progress'})
    threading.Thread(target=scan_markets, daemon=True).start()
    return jsonify({'success': True})

@app.route('/api/opportunities')
def get_opportunities():
    with _state_lock:
        return jsonify({
            'opportunities': state['opportunities'],
            'last_scan': state['last_scan'],
            'total': len(state['opportunities']),
            'scanning': state['scanning'],
            'debug': state.get('debug_info', []),
            'bankroll': DEFAULT_BANKROLL,
            'scan_id': state.get('scan_id'),
        })

@app.route('/api/key-status')
def key_status():
    auth_err = _auth_check()
    if auth_err:
        return auth_err
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
        except:
            results.append({'key': f'...{key[-6:]}', 'status': 'error'})
    return jsonify({'keys': results, 'total_keys': len(API_KEYS)})

# ============================================================
# HISTORY & CLV ENDPOINTS (new — for performance tracking)
# ============================================================

@app.route('/api/update-clv', methods=['POST', 'GET'])
def trigger_clv_update():
    """Manually trigger a CLV capture run. Useful right before/after game tip-offs."""
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    updated = update_clv()
    return jsonify({'success': True, 'updated': updated})

@app.route('/api/history')
def history():
    """Paginated history of all flagged opportunities."""
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    limit = min(int(request.args.get('limit', 100)), 500)
    offset = int(request.args.get('offset', 0))
    bet_type = request.args.get('type', '')
    book = request.args.get('book', '')
    try:
        with get_db() as conn:
            sql = "SELECT * FROM opportunities WHERE 1=1"
            params = []
            if bet_type:
                sql += " AND bet_type = ?"
                params.append(bet_type)
            if book:
                sql += " AND book = ?"
                params.append(book)
            sql += " ORDER BY scan_time DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            rows = conn.execute(sql, params).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
            return jsonify({
                'total': total,
                'count': len(rows),
                'opportunities': [dict(r) for r in rows],
            })
    except Exception as e:
        return jsonify({'error': str(e), 'opportunities': []})

@app.route('/api/stats')
def stats():
    """Aggregated performance stats by book, market, type — now includes CLV."""
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    try:
        with get_db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
            with_clv = conn.execute("SELECT COUNT(*) FROM opportunities WHERE clv IS NOT NULL").fetchone()[0]
            overall_clv = conn.execute(
                "SELECT AVG(clv) as avg_clv, SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) as beat, COUNT(clv) as n "
                "FROM opportunities WHERE clv IS NOT NULL"
            ).fetchone()
            by_type = conn.execute("""
                SELECT bet_type, COUNT(*) as n, AVG(edge) as avg_edge, AVG(kelly_fraction) as avg_kelly,
                       AVG(clv) as avg_clv, COUNT(clv) as clv_n
                FROM opportunities GROUP BY bet_type
            """).fetchall()
            by_book = conn.execute("""
                SELECT book, COUNT(*) as n, AVG(edge) as avg_edge,
                       AVG(clv) as avg_clv, COUNT(clv) as clv_n
                FROM opportunities GROUP BY book ORDER BY n DESC LIMIT 15
            """).fetchall()
            by_day = conn.execute("""
                SELECT substr(scan_time, 1, 10) as day, COUNT(*) as n, AVG(edge) as avg_edge,
                       AVG(clv) as avg_clv, COUNT(clv) as clv_n
                FROM opportunities GROUP BY day ORDER BY day DESC LIMIT 30
            """).fetchall()
            return jsonify({
                'total': total,
                'with_clv': with_clv,
                'overall_clv': dict(overall_clv) if overall_clv else {},
                'by_type': [dict(r) for r in by_type],
                'by_book': [dict(r) for r in by_book],
                'by_day': [dict(r) for r in by_day],
            })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/export.csv')
def export_csv():
    """Export full history as CSV — useful for Excel analysis or selling as dataset."""
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT * FROM opportunities ORDER BY scan_time DESC").fetchall()
            if not rows:
                return Response('no data\n', mimetype='text/csv')
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            for r in rows:
                writer.writerow(dict(r))
            return Response(output.getvalue(), mimetype='text/csv',
                headers={'Content-Disposition': 'attachment; filename=ev_history.csv'})
    except Exception as e:
        return Response(f'error: {e}\n', mimetype='text/plain')

# ============================================================
# ODDS SEARCH (existing, now authed)
# ============================================================

@app.route('/api/search')
def search_odds():
    auth_err = _auth_check()
    if auth_err:
        return auth_err
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'all')
    sport_filter = request.args.get('sport', 'all')
    if not query or len(query) < 2:
        return jsonify({'error': 'Query too short', 'results': []})
    q_lower = query.lower()
    results = []
    sport_map = {'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab', 'nhl': 'icehockey_nhl'}
    if sport_filter != 'all' and sport_filter in sport_map:
        sports = [(sport_filter, sport_map[sport_filter])]
    else:
        sports = list(sport_map.items())
    prop_markets = {
        'basketball_nba': [('player_points', 'Points'), ('player_rebounds', 'Rebounds')],
        'basketball_ncaab': [('player_points', 'Points'), ('player_rebounds', 'Rebounds')],
        'icehockey_nhl': [('player_points', 'Points'), ('player_shots_on_goal', 'Shots on Goal')],
    }
    for sport_key, sport_api in sports:
        if search_type in ('moneyline', 'all'):
            events = fetch_events(sport_api)
            matching = [ev for ev in events
                        if q_lower in (ev.get('home_team','') or '').lower()
                        or q_lower in (ev.get('away_team','') or '').lower()]
            if matching:
                games = fetch_odds(sport_api, 'h2h')
                if games:
                    for game in games:
                        home = game.get('home_team', '')
                        away = game.get('away_team', '')
                        if q_lower not in home.lower() and q_lower not in away.lower():
                            continue
                        matched_team = home if q_lower in home.lower() else away
                        commence = game.get('commence_time', '')
                        book_odds = []
                        for bk in game.get('bookmakers', []):
                            bk_name = bk.get('title', bk.get('key', ''))
                            bk_key = bk.get('key', '')
                            for mkt in bk.get('markets', []):
                                if mkt['key'] != 'h2h':
                                    continue
                                for outcome in mkt.get('outcomes', []):
                                    if outcome['name'] == matched_team:
                                        book_odds.append({
                                            'book': bk_name, 'book_key': bk_key,
                                            'odds': outcome.get('price', 0),
                                            'bettable': bk_key in CO_BETTABLE,
                                        })
                        if book_odds:
                            book_odds.sort(key=lambda x: x['odds'], reverse=True)
                            results.append({
                                'type': 'moneyline', 'player': matched_team,
                                'game': f"{away} @ {home}", 'sport': sport_key.upper(),
                                'market': 'Moneyline', 'line': None, 'commence': commence,
                                'books': book_odds,
                                'best_bettable': next((b for b in book_odds if b['bettable']), None),
                            })
        if search_type in ('props', 'all'):
            events = fetch_events(sport_api)
            for ev in events[:8]:
                eid = ev.get('id')
                home = ev.get('home_team', '')
                away = ev.get('away_team', '')
                for prop_api_key, prop_name in prop_markets.get(sport_api, []):
                    edata = fetch_event_odds(sport_api, eid, prop_api_key)
                    if not edata or not edata.get('bookmakers'):
                        continue
                    player_found = False
                    for bk in edata.get('bookmakers', []):
                        for mkt in bk.get('markets', []):
                            for outcome in mkt.get('outcomes', []):
                                if q_lower in (outcome.get('description', '') or outcome.get('name', '')).lower():
                                    player_found = True
                                    break
                            if player_found: break
                        if player_found: break
                    if not player_found:
                        continue
                    lines = {}
                    for bk in edata.get('bookmakers', []):
                        bk_name = bk.get('title', bk.get('key', ''))
                        bk_key = bk.get('key', '')
                        is_bettable = bk_key in CO_BETTABLE
                        for mkt in bk.get('markets', []):
                            for outcome in mkt.get('outcomes', []):
                                name = outcome.get('description', '') or outcome.get('name', '')
                                if q_lower not in name.lower():
                                    continue
                                side = outcome.get('name', '').lower()
                                line_val = outcome.get('point', 0)
                                odds = outcome.get('price', 0)
                                key = f"{name}_{line_val}"
                                if key not in lines:
                                    lines[key] = {'player': name, 'line': line_val, 'over': [], 'under': []}
                                entry = {'book': bk_name, 'book_key': bk_key,
                                         'odds': odds, 'bettable': is_bettable}
                                if 'over' in side:
                                    lines[key]['over'].append(entry)
                                elif 'under' in side:
                                    lines[key]['under'].append(entry)
                    for key, line_data in lines.items():
                        for side in ['over', 'under']:
                            line_data[side].sort(key=lambda x: x['odds'], reverse=True)
                        if line_data['over'] or line_data['under']:
                            results.append({
                                'type': 'prop', 'player': line_data['player'],
                                'game': f"{away} @ {home}", 'sport': sport_key.upper(),
                                'market': prop_name, 'line': line_data['line'],
                                'commence': ev.get('commence_time', ''),
                                'over': line_data['over'], 'under': line_data['under'],
                                'best_over_bettable': next((b for b in line_data['over'] if b['bettable']), None),
                                'best_under_bettable': next((b for b in line_data['under'] if b['bettable']), None),
                            })
                    break
    return jsonify({'query': query, 'results': results, 'count': len(results)})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
