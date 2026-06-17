"""
providers.py — Free direct-from-source odds layer (replaces The Odds API)

Sources (all free, no API key):
  pinnacle    guest.api.arcadia.pinnacle.com  (sharp anchor, weight 3)
  draftkings  sportsbook.draftkings.com v5 eventgroups
  fanduel     sbapi.co.sportsbook.fanduel.com (CO subdomain)
  betmgm      sports.co.betmgm.com cds-api
  betrivers   Kambi offering API (rsiusco = RSI Colorado)

Everything is normalized to The Odds API v4 JSON shape:
  [{id, sport_key, commence_time, home_team, away_team,
    bookmakers: [{key, title, markets: [{key, outcomes: [
        {name, price, point?, description?}]}]}]}]
so web_arbitrage.py's analysis code runs unchanged.

Notes:
- These are the books' own public website JSON endpoints. They serve them
  to every browser, but automated access is technically against their ToS.
  Polite pacing (RATE_DELAY) is enforced. If a host starts 403'ing your
  server IP (datacenter IPs sometimes are), run fetch_worker.py from a
  residential connection and POST snapshots to /api/ingest instead.
- Endpoints drift occasionally. Run smoke_test.py after any silence.
"""

import json
import os
import re
import time
import threading
import unicodedata
from datetime import datetime, timezone

import requests

# ============================================================
# CONFIG
# ============================================================

ENABLED = os.environ.get('DIRECT_FEEDS', '1') != '0'

UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36')

RATE_DELAY = float(os.environ.get('FEED_RATE_DELAY', '0.4'))   # s between HTTP calls
SNAPSHOT_TTL = int(os.environ.get('FEED_SNAPSHOT_TTL', '55'))  # s
PROP_EVENT_LIMIT = int(os.environ.get('FEED_PROP_EVENTS', '8'))  # FD event-page calls per sport

# Pinnacle guest key: public, embedded in pinnacle.com's own JS for years.
PINNACLE_KEY = os.environ.get('PINNACLE_GUEST_KEY', 'CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R')
PINNACLE_BASE = 'https://guest.api.arcadia.pinnacle.com/0.1'

DK_BASE = 'https://sportsbook.draftkings.com/sites/US-SB/api/v5'
FD_BASE = 'https://sbapi.co.sportsbook.fanduel.com/api'
FD_AK = os.environ.get('FANDUEL_AK', 'FhMFpcPWXMeyZxOx')  # public app key in FD's own requests
MGM_BASE = 'https://sports.co.betmgm.com'
KAMBI_BASE = 'https://eu-offering-api.kambicdn.com/offering/v2018/rsiusco'

# sport_key -> per-provider identifiers. Lists are tried in order (discovery).
SPORTS = {
    'basketball_nba': {
        'pinnacle_leagues': [487],
        'dk_groups': [42648],
        'fd_pages': ['nba'],
        'mgm_sport': 7, 'mgm_match': ['nba'],
        'kambi_paths': ['basketball/nba'],
        'soccer': False,
        'props': {'player_points': ['points'], 'player_rebounds': ['rebounds'],
                  'player_assists': ['assists']},
    },
    'basketball_ncaab': {
        'pinnacle_leagues': [493],
        'dk_groups': [92483],
        'fd_pages': ['ncaa-basketball', 'college-basketball'],
        'mgm_sport': 7, 'mgm_match': ['ncaa', 'college'],
        'kambi_paths': ['basketball/ncaab', 'basketball/ncaab_(m)'],
        'soccer': False,
        'props': {'player_points': ['points'], 'player_rebounds': ['rebounds']},
    },
    'icehockey_nhl': {
        'pinnacle_leagues': [1456],
        'dk_groups': [42133],
        'fd_pages': ['nhl'],
        'mgm_sport': 12, 'mgm_match': ['nhl'],
        'kambi_paths': ['ice_hockey/nhl'],
        'soccer': False,
        'props': {'player_points': ['points'], 'player_shots_on_goal': ['shots on goal', 'shots']},
    },
    'soccer_fifa_world_cup': {
        'pinnacle_leagues': [],            # discovered dynamically (sport 29)
        'pinnacle_sport': 29,
        'pinnacle_league_match': ['world cup'],
        'pinnacle_league_exclude': ['qualif', 'women', 'u-17', 'u-20', 'u17', 'u20', 'club'],
        'dk_groups': [40361, 104151, 41410, 88670846],   # candidates; discovery keeps first hit
        'fd_pages': ['world-cup', 'fifa-world-cup', 'wc-2026'],
        'mgm_sport': 4, 'mgm_match': ['world cup'],
        'kambi_paths': ['football/fifa_world_cup', 'football/world_cup_2026',
                        'football/world_cup'],
        'soccer': True,
        'props': {},                        # game lines only for v1
    },
    'baseball_mlb': {
        'pinnacle_leagues': [],            # discovered dynamically (sport 3)
        'pinnacle_sport': 3,
        'pinnacle_league_match': ['mlb'],
        'dk_groups': [84240],
        'fd_pages': ['mlb'],
        'mgm_sport': 23, 'mgm_match': ['mlb'],
        'kambi_paths': ['baseball/mlb'],
        'soccer': False,
        'props': {
            'player_strikeouts': ['strikeout', 'strikeouts', 'pitcher strikeouts',
                                  'total strikeouts', 'ks recorded'],
            'player_total_bases': ['total bases', 'total base'],
        },
    },
    'basketball_wnba': {
        'pinnacle_leagues': [],            # discovered dynamically (sport 4)
        'pinnacle_sport': 4,
        'pinnacle_league_match': ['wnba'],
        'dk_groups': [94682],
        'fd_pages': ['wnba'],
        'mgm_sport': 7, 'mgm_match': ['wnba', 'women'],
        'kambi_paths': ['basketball/wnba'],
        'soccer': False,
        'props': {'player_points': ['points'], 'player_rebounds': ['rebounds'],
                  'player_assists': ['assists']},
    },
}

PROVIDER_TITLES = {
    'pinnacle': 'Pinnacle', 'draftkings': 'DraftKings', 'fanduel': 'FanDuel',
    'betmgm': 'BetMGM', 'betrivers': 'BetRivers',
}

# ============================================================
# HTTP
# ============================================================

_session = requests.Session()
_session.headers.update({'User-Agent': UA, 'Accept': 'application/json',
                         'Accept-Language': 'en-US,en;q=0.9'})
_last_call = [0.0]
_http_lock = threading.Lock()


def _get_json(url, headers=None, params=None, timeout=20):
    with _http_lock:
        wait = RATE_DELAY - (time.time() - _last_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()
    try:
        r = _session.get(url, headers=headers or {}, params=params, timeout=timeout)
        if r.status_code != 200:
            return None, r.status_code
        return r.json(), 200
    except Exception as e:
        return None, str(e)[:80]


def _dec_to_american(dec):
    try:
        dec = float(dec)
        if dec <= 1.0:
            return None
        if dec >= 2.0:
            return int(round((dec - 1.0) * 100))
        return int(round(-100.0 / (dec - 1.0)))
    except Exception:
        return None


def _norm_price(p):
    """Accept either decimal or American odds from any book.

    American odds are by definition always >= +100 or <= -100, so any value
    strictly inside (-100, +100) that is > 1.0 must be decimal. The previous
    version assumed American and silently dropped whole-number decimals (2.0,
    4.0) and treated longshot decimals (>=11.0) as invalid American — which is
    why Pinnacle draws and underdogs came through wrong or missing."""
    try:
        p = float(p)
    except Exception:
        return None
    if p >= 100 or p <= -100:
        return int(round(p))            # American (-110, +250, ...)
    if p > 1.0:
        return _dec_to_american(p)      # decimal (1.91, 2.0, 4.0, 13.0, ...)
    return None                          # <= 1.0 is not a valid price


# ============================================================
# TEAM / PLAYER NORMALIZATION
# ============================================================

_MULTIWORD_NICKS = {'sox', 'jackets', 'wings', 'leafs', 'blazers', 'devils',
                    'kings', 'state'}


def _strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def _team_key(name, soccer=False):
    s = _strip_accents(str(name)).lower().strip()
    s = re.sub(r'[^a-z0-9 ]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    if soccer:
        s = re.sub(r'\b(fc|cf|sc|afc|national team)\b', '', s).strip()
        return re.sub(r'\s+', ' ', s)
    toks = s.split()
    if not toks:
        return s
    if len(toks) >= 2 and toks[-1] in _MULTIWORD_NICKS:
        return ' '.join(toks[-2:])
    return toks[-1]


def _full_toks(name, soccer=False):
    """Full normalized token set of a team name (for fuzzy bucket matching)."""
    s = _strip_accents(str(name)).lower()
    s = re.sub(r'[^a-z0-9 ]', '', s)
    if soccer:
        s = re.sub(r'\b(fc|cf|sc|afc|national team)\b', '', s)
    return frozenset(t for t in s.split() if t)


def _sides_match(toks_a, key_a, toks_b, key_b):
    """True if two team references plausibly denote the same team.

    Exact key match, or one full-name token set contains the other
    (handles 'Oklahoma City' vs 'Oklahoma City Thunder',
    'Indiana' vs 'Indiana Pacers')."""
    if key_a == key_b:
        return True
    if toks_a and toks_b and (toks_a <= toks_b or toks_b <= toks_a):
        return True
    return False


def _norm_player(name):
    s = _strip_accents(str(name)).lower()
    s = re.sub(r'[^a-z ]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def _parse_ts(val):
    """Accept ISO strings or epoch ms; return epoch seconds or None."""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return float(val) / (1000.0 if val > 1e12 else 1.0)
        s = str(val).replace('Z', '+00:00')
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ============================================================
# BOOK GAME container produced by every provider:
# {'home': str, 'away': str, 'ts': float,
#  'h2h': [(outcome_name, american)],          # may include 'Draw'
#  'props': {market_key: [(player, line, side, american)]}}  side='over'/'under'
# ============================================================

def _mk_game(home, away, ts):
    return {'home': str(home).strip(), 'away': str(away).strip(), 'ts': ts,
            'h2h': [], 'props': {}}


def _add_prop(g, mkey, player, line, side, price):
    if price is None or line is None or not player:
        return
    g['props'].setdefault(mkey, []).append((str(player).strip(), float(line),
                                            side, price))


# ---------------- PINNACLE ----------------

def _pinnacle_headers():
    return {'x-api-key': PINNACLE_KEY, 'referer': 'https://www.pinnacle.com/',
            'origin': 'https://www.pinnacle.com'}


_pinnacle_league_cache = {}


def _pinnacle_league_ids(sport_key, cfg, log):
    ids = list(cfg.get('pinnacle_leagues') or [])
    if ids:
        return ids
    if sport_key in _pinnacle_league_cache:
        return _pinnacle_league_cache[sport_key]
    sid = cfg.get('pinnacle_sport')
    if not sid:
        return []
    data, code = _get_json(f'{PINNACLE_BASE}/sports/{sid}/leagues',
                           headers=_pinnacle_headers(), params={'all': 'false'})
    found = []
    if isinstance(data, list):
        want = cfg.get('pinnacle_league_match', [])
        block = cfg.get('pinnacle_league_exclude', [])
        for lg in data:
            nm = str(lg.get('name', '')).lower()
            if any(w in nm for w in want) and not any(b in nm for b in block):
                found.append(lg.get('id'))
    else:
        log(f'    pinnacle league discovery failed ({code})')
    _pinnacle_league_cache[sport_key] = found[:4]
    return _pinnacle_league_cache[sport_key]


def fetch_pinnacle(sport_key, log=print):
    cfg = SPORTS[sport_key]
    games = {}
    prop_cfg = cfg.get('props', {})
    for lid in _pinnacle_league_ids(sport_key, cfg, log):
        mups, c1 = _get_json(f'{PINNACLE_BASE}/leagues/{lid}/matchups',
                             headers=_pinnacle_headers())
        mkts, c2 = _get_json(f'{PINNACLE_BASE}/leagues/{lid}/markets/straight',
                             headers=_pinnacle_headers())
        if not isinstance(mups, list) or not isinstance(mkts, list):
            log(f'    pinnacle league {lid}: matchups={c1} markets={c2}')
            continue

        by_id, specials, part_names = {}, {}, {}
        for m in mups:
            mid = m.get('id')
            parts = m.get('participants') or []
            for p in parts:
                if p.get('id') is not None:
                    part_names[p['id']] = p.get('name', '')
            sp = m.get('special') or {}
            if sp:
                parent = (m.get('parent') or {})
                pparts = parent.get('participants') or []
                specials[mid] = {
                    'desc': sp.get('description', ''),
                    'category': str(sp.get('category', '')).lower(),
                    'parent_id': parent.get('id'),
                    'parent_parts': pparts,
                }
                continue
            if str(m.get('type', 'matchup')) != 'matchup' or len(parts) < 2:
                continue
            home = next((p.get('name') for p in parts
                         if p.get('alignment') == 'home'), None)
            away = next((p.get('name') for p in parts
                         if p.get('alignment') == 'away'), None)
            if not home or not away:
                continue
            by_id[mid] = _mk_game(home, away, _parse_ts(m.get('startTime')))

        def _stat_to_mkey(desc):
            d = desc.lower()
            for mkey, toks in prop_cfg.items():
                if any(t in d for t in toks):
                    return mkey
            return None

        for mk in mkts:
            if mk.get('period') not in (0, None):
                continue
            mid = mk.get('matchupId')
            mtype = mk.get('type')
            prices = mk.get('prices') or []
            if mid in by_id and mtype == 'moneyline':
                g = by_id[mid]
                for pr in prices:
                    desig = pr.get('designation')
                    price = _norm_price(pr.get('price'))
                    if price is None:
                        continue
                    if desig == 'home':
                        g['h2h'].append((g['home'], price))
                    elif desig == 'away':
                        g['h2h'].append((g['away'], price))
                    elif desig == 'draw':
                        g['h2h'].append(('Draw', price))
            elif mid in specials and mtype in ('total', 'moneyline'):
                sp = specials[mid]
                if 'player' not in sp['category'] and '(' not in sp['desc']:
                    continue
                mkey = _stat_to_mkey(sp['desc'])
                if not mkey:
                    continue
                pm = re.match(r'^(.*?)\s*\(', sp['desc'])
                player = pm.group(1).strip() if pm else sp['desc']
                pg = by_id.get(sp['parent_id'])
                if pg is None and sp['parent_parts']:
                    h = next((p.get('name') for p in sp['parent_parts']
                              if p.get('alignment') == 'home'), None)
                    a = next((p.get('name') for p in sp['parent_parts']
                              if p.get('alignment') == 'away'), None)
                    if h and a:
                        pg = by_id.setdefault(sp['parent_id'],
                                              _mk_game(h, a, None))
                if pg is None:
                    continue
                for pr in prices:
                    desig = pr.get('designation') or \
                        str(part_names.get(pr.get('participantId'), '')).lower()
                    price = _norm_price(pr.get('price'))
                    line = pr.get('points')
                    if 'over' in str(desig):
                        _add_prop(pg, mkey, player, line, 'over', price)
                    elif 'under' in str(desig):
                        _add_prop(pg, mkey, player, line, 'under', price)

        games.update({k: v for k, v in by_id.items()
                      if v['h2h'] or v['props']})
    out = list(games.values())
    log(f'    pinnacle: {len(out)} games')
    return out


# ---------------- DRAFTKINGS (v5 eventgroups) ----------------

_dk_group_cache = {}


def _dk_root(sport_key, cfg, log):
    if sport_key in _dk_group_cache:
        gid = _dk_group_cache[sport_key]
        data, _ = _get_json(f'{DK_BASE}/eventgroups/{gid}', params={'format': 'json'})
        if data and data.get('eventGroup'):
            return gid, data
    for gid in cfg.get('dk_groups', []):
        data, code = _get_json(f'{DK_BASE}/eventgroups/{gid}',
                               params={'format': 'json'})
        eg = (data or {}).get('eventGroup') or {}
        if eg.get('events'):
            _dk_group_cache[sport_key] = gid
            return gid, data
    log('    draftkings: no live event group found')
    return None, None


def _dk_event_teams(ev):
    for hk, ak in (('teamName2', 'teamName1'),):
        if ev.get(hk) and ev.get(ak):
            return ev[hk], ev[ak]
    t1, t2 = ev.get('team1') or {}, ev.get('team2') or {}
    if t1.get('name') and t2.get('name'):
        return t2['name'], t1['name']            # DK: team1=away, team2=home
    name = ev.get('name', '')
    if ' @ ' in name:
        a, h = name.split(' @ ', 1)
        return h.strip(), a.strip()
    for sep in (' vs ', ' vs. ', ' v '):
        if sep in name:
            h, a = name.split(sep, 1)
            return h.strip(), a.strip()
    return None, None


def _dk_walk_offers(category):
    for d in (category.get('offerSubcategoryDescriptors') or []):
        sub = d.get('offerSubcategory') or {}
        for row in (sub.get('offers') or []):
            for offer in (row or []):
                yield d.get('name', ''), offer


def fetch_draftkings(sport_key, log=print):
    cfg = SPORTS[sport_key]
    gid, data = _dk_root(sport_key, cfg, log)
    if not data:
        return []
    eg = data['eventGroup']
    games = {}
    for ev in eg.get('events', []):
        home, away = _dk_event_teams(ev)
        if not home or not away:
            continue
        games[ev.get('eventId')] = _mk_game(home, away,
                                            _parse_ts(ev.get('startDate')))

    def _ingest_category(cat):
        for sub_name, offer in _dk_walk_offers(cat):
            g = games.get(offer.get('eventId'))
            if g is None:
                continue
            label = str(offer.get('label', '')).lower()
            outs = offer.get('outcomes') or []
            if 'moneyline' in label or (label in ('', 'money line') and
                                        all(o.get('line') is None for o in outs)):
                if 'moneyline' not in label:
                    continue
                for o in outs:
                    price = _norm_price(o.get('oddsAmerican'))
                    nm = o.get('label', '')
                    if price is None or not nm:
                        continue
                    g['h2h'].append(('Draw' if nm.lower() in ('draw', 'tie')
                                     else nm, price))
            else:
                mkey = None
                hay = f'{sub_name} {label}'.lower()
                for mk, toks in cfg.get('props', {}).items():
                    if any(t in hay for t in toks):
                        mkey = mk
                        break
                if not mkey:
                    continue
                for o in outs:
                    side = str(o.get('label', '')).lower()
                    if side not in ('over', 'under'):
                        continue
                    player = o.get('participant') or \
                        ((o.get('participants') or [{}])[0].get('name')) or \
                        re.sub(r'\s*\b(over|under)\b.*$', '', offer.get('label', ''),
                               flags=re.I).strip()
                    _add_prop(g, mkey, player, o.get('line'), side,
                              _norm_price(o.get('oddsAmerican')))

    cats = eg.get('offerCategories') or []
    for cat in cats:
        nm = str(cat.get('name', '')).lower()
        if 'game lines' in nm or nm == 'featured':
            if cat.get('offerSubcategoryDescriptors'):
                _ingest_category(cat)
            else:
                full, _ = _get_json(
                    f'{DK_BASE}/eventgroups/{gid}/categories/{cat.get("offerCategoryId")}',
                    params={'format': 'json'})
                fc = (((full or {}).get('eventGroup') or {})
                      .get('offerCategories') or [])
                for c2 in fc:
                    _ingest_category(c2)
    if cfg.get('props'):
        _PROP_CAT_HINTS = ('player', 'props', 'batter', 'pitcher',
                           'hitting', 'pitching', 'by player')
        for cat in cats:
            nm = str(cat.get('name', '')).lower()
            if not any(h in nm for h in _PROP_CAT_HINTS):
                continue
            full, _ = _get_json(
                f'{DK_BASE}/eventgroups/{gid}/categories/{cat.get("offerCategoryId")}',
                params={'format': 'json'})
            fc = (((full or {}).get('eventGroup') or {})
                  .get('offerCategories') or [])
            for c2 in fc:
                _ingest_category(c2)
    out = [g for g in games.values() if g['h2h'] or g['props']]
    log(f'    draftkings: {len(out)} games')
    return out


# ---------------- FANDUEL ----------------

def _fd_params(extra=None):
    p = {'_ak': FD_AK, 'timezone': 'America/Denver'}
    if extra:
        p.update(extra)
    return p


def _fd_event_teams(name, soccer):
    name = str(name)
    if ' @ ' in name:
        a, h = name.split(' @ ', 1)
        return h.strip(), a.strip()
    for sep in (' v ', ' vs ', ' vs. '):
        if sep in name:
            h, a = name.split(sep, 1)
            return h.strip(), a.strip()
    return None, None


def _fd_ingest_markets(markets, games_by_event, cfg):
    soccer = cfg.get('soccer')
    for m in (markets or {}).values():
        g = games_by_event.get(m.get('eventId'))
        if g is None:
            continue
        mtype = str(m.get('marketType', '')).upper()
        mname = str(m.get('marketName', ''))
        runners = m.get('runners') or []

        def _price(r):
            try:
                return _norm_price(r['winRunnerOdds']['americanDisplayOdds']
                                   ['americanOdds'])
            except Exception:
                try:
                    return _dec_to_american(
                        r['winRunnerOdds']['trueOdds']['decimalOdds']['decimalOdds'])
                except Exception:
                    return None

        if mtype in ('MONEY_LINE', 'MATCH_ODDS') or \
                mname.lower() in ('moneyline', 'match result', 'full time result'):
            for r in runners:
                price = _price(r)
                nm = str(r.get('runnerName', '')).strip()
                if price is None or not nm:
                    continue
                if nm.lower() in ('draw', 'the draw', 'tie'):
                    nm = 'Draw'
                g['h2h'].append((nm, price))
        elif cfg.get('props'):
            pm = re.match(r'^(.+?)\s*[-–]\s*(.+)$', mname)
            if not pm:
                continue
            player, stat = pm.group(1).strip(), pm.group(2).lower()
            mkey = None
            for mk, toks in cfg['props'].items():
                if any(t in stat for t in toks):
                    mkey = mk
                    break
            if not mkey:
                continue
            for r in runners:
                side = str(r.get('runnerName', '')).lower()
                if side not in ('over', 'under'):
                    continue
                _add_prop(g, mkey, player, r.get('handicap'), side, _price(r))


def fetch_fanduel(sport_key, log=print):
    cfg = SPORTS[sport_key]
    data = None
    for slug in cfg.get('fd_pages', []):
        data, code = _get_json(f'{FD_BASE}/content-managed-page',
                               params=_fd_params({'page': 'CUSTOM',
                                                  'customPageId': slug,
                                                  'pbHorizontal': 'false'}))
        if data and (data.get('attachments') or {}).get('events'):
            break
        data = None
    if not data:
        log('    fanduel: no page data')
        return []
    att = data.get('attachments') or {}
    games_by_event = {}
    for ev in (att.get('events') or {}).values():
        home, away = _fd_event_teams(ev.get('name', ''), cfg.get('soccer'))
        if not home or not away:
            continue
        games_by_event[ev.get('eventId')] = _mk_game(
            home, away, _parse_ts(ev.get('openDate')))
    _fd_ingest_markets(att.get('markets'), games_by_event, cfg)

    if cfg.get('props'):
        upcoming = sorted([
            (eid, g) for eid, g in games_by_event.items()
            if g['ts'] and g['ts'] > time.time() - 3600
        ], key=lambda x: x[1]['ts'])[:PROP_EVENT_LIMIT]
        for eid, _g in upcoming:
            ep, _ = _get_json(f'{FD_BASE}/event-page',
                              params=_fd_params({'eventId': eid}))
            if ep:
                _fd_ingest_markets((ep.get('attachments') or {}).get('markets'),
                                   games_by_event, cfg)
    out = [g for g in games_by_event.values() if g['h2h'] or g['props']]
    log(f'    fanduel: {len(out)} games')
    return out


# ---------------- BETMGM ----------------

_mgm_token = [os.environ.get('BETMGM_ACCESS_ID', '')]


def _mgm_access_id(log):
    if _mgm_token[0]:
        return _mgm_token[0]
    try:
        r = _session.get(f'{MGM_BASE}/en/sports', timeout=20)
        m = re.search(r'accessId["\']?\s*[:=]\s*["\']([A-Za-z0-9_\-+=]{16,})',
                      r.text or '')
        if m:
            _mgm_token[0] = m.group(1)
            return _mgm_token[0]
    except Exception:
        pass
    log('    betmgm: could not extract access id '
        '(set BETMGM_ACCESS_ID env var from devtools)')
    return ''


def fetch_betmgm(sport_key, log=print):
    cfg = SPORTS[sport_key]
    token = _mgm_access_id(log)
    if not token:
        return []
    data, code = _get_json(
        f'{MGM_BASE}/cds-api/bettingoffer/fixtures',
        params={'x-bwin-accessid': token, 'lang': 'en-us', 'country': 'US',
                'userCountry': 'US', 'fixtureTypes': 'Standard',
                'state': 'Latest', 'offerMapping': 'Filtered',
                'offerCategories': 'Gridable',
                'sportIds': cfg['mgm_sport'], 'skip': 0, 'take': 200,
                'sortBy': 'Tags'})
    fixtures = (data or {}).get('fixtures') or []
    if not fixtures:
        log(f'    betmgm: no fixtures ({code})')
        return []
    match_toks = cfg.get('mgm_match', [])
    out = []
    for fx in fixtures:
        comp = str(((fx.get('competition') or {}).get('name') or {})
                   .get('value', '')).lower()
        if match_toks and not any(t in comp for t in match_toks):
            continue
        parts = fx.get('participants') or []
        home = away = None
        for p in parts:
            ptype = str((p.get('properties') or {}).get('type', '')).lower()
            nm = (p.get('name') or {}).get('value')
            if ptype == 'hometeam':
                home = nm
            elif ptype == 'awayteam':
                away = nm
        if not home and len(parts) == 2:
            home = (parts[0].get('name') or {}).get('value')
            away = (parts[1].get('name') or {}).get('value')
        if not home or not away:
            continue
        g = _mk_game(home, away, _parse_ts(fx.get('startDate')))
        for game in (fx.get('games') or []):
            gname = str((game.get('name') or {}).get('value', '')).lower()
            if gname not in ('money line', 'moneyline', 'result',
                             'match result', '2way - match'):
                continue
            for res in (game.get('results') or []):
                price = res.get('americanOdds')
                if price is None:
                    price = _dec_to_american(res.get('odds'))
                price = _norm_price(price)
                nm = str((res.get('name') or {}).get('value', '')).strip()
                if price is None or not nm:
                    continue
                low = nm.lower()
                if low in ('x', 'draw', 'tie'):
                    nm = 'Draw'
                g['h2h'].append((nm, price))
            if g['h2h']:
                break
        if g['h2h']:
            out.append(g)
    log(f'    betmgm: {len(out)} games')
    return out


# ---------------- BETRIVERS (Kambi) ----------------

def fetch_betrivers(sport_key, log=print):
    cfg = SPORTS[sport_key]
    data = None
    for path in cfg.get('kambi_paths', []):
        data, code = _get_json(
            f'{KAMBI_BASE}/listView/{path}/all/all/matches.json',
            params={'lang': 'en_US', 'market': 'US', 'useCombined': 'true'})
        if data and data.get('events'):
            break
        data = None
    if not data:
        log('    betrivers: no kambi data')
        return []
    out = []
    for wrap in data.get('events', []):
        ev = wrap.get('event') or {}
        home, away = ev.get('homeName'), ev.get('awayName')
        if not home or not away:
            continue
        g = _mk_game(home, away, _parse_ts(ev.get('start')))
        for bo in (wrap.get('betOffers') or []):
            crit = str((bo.get('criterion') or {}).get('label', '')).lower()
            btype = str((bo.get('betOfferType') or {}).get('name', '')).lower()
            # Many sub-markets (corners, cards, totals, handicaps) reuse the
            # exact OT_ONE/OT_CROSS/OT_TWO 1X2 structure, so structure alone
            # can't distinguish them. Reject anything whose label names a
            # sub-market, then require a positive match-result signal.
            _BLOCK = ('corner', 'card', 'booking', 'total', 'handicap',
                      'over/under', 'both teams', 'shot', 'offside', 'throw',
                      'foul', 'half', 'asian', 'double chance', 'draw no bet',
                      'qualify', 'advance', 'exact', 'margin', 'method',
                      'anytime', 'first ', 'last ', 'race to', 'odd/even',
                      'odd or even', 'penalt', 'clean sheet', 'to nil',
                      'period', 'minute', 'interval', 'winning margin')
            if any(b in crit for b in _BLOCK):
                continue
            is_match_result = (
                'full time' in crit or 'moneyline' in crit or 'money line' in crit
                or '1x2' in crit
                or crit in ('match', 'match result', 'winner', 'to win',
                            'regular time', 'full time result', 'match odds')
                or (btype == 'match' and not crit))
            if not is_match_result:
                continue
            for oc in (bo.get('outcomes') or []):
                price = _dec_to_american((oc.get('odds') or 0) / 1000.0)
                if price is None:
                    continue
                otype = str(oc.get('type', ''))
                if otype == 'OT_ONE':
                    g['h2h'].append((home, price))
                elif otype == 'OT_TWO':
                    g['h2h'].append((away, price))
                elif otype == 'OT_CROSS':
                    g['h2h'].append(('Draw', price))
                else:
                    nm = oc.get('participant') or oc.get('label', '')
                    if nm:
                        g['h2h'].append((str(nm), price))
            if g['h2h']:
                break
        if g['h2h']:
            out.append(g)
    log(f'    betrivers: {len(out)} games')
    return out


PROVIDERS = [
    ('pinnacle', fetch_pinnacle),
    ('draftkings', fetch_draftkings),
    ('fanduel', fetch_fanduel),
    ('betmgm', fetch_betmgm),
    ('betrivers', fetch_betrivers),
]

# Canonical naming priority when books disagree on team strings
_CANON_ORDER = ['pinnacle', 'draftkings', 'fanduel', 'betmgm', 'betrivers']


# ============================================================
# MERGE -> The Odds API v4 shape
# ============================================================

def _merge_to_v4(sport_key, per_book, log=print):
    soccer = SPORTS[sport_key].get('soccer', False)
    merged = {}    # pair-key -> bucket (several keys may alias one bucket)
    buckets = []   # unique buckets, creation order

    def _ts_ok(a, b):
        return a is None or b is None or abs(a - b) <= 8 * 3600

    def _bucket_for(g):
        """Return (bucket, aligned) where aligned=True means g.home
        corresponds to bucket home."""
        hk, ak = _team_key(g['home'], soccer), _team_key(g['away'], soccer)
        if not hk or not ak or hk == ak:
            return None, True
        htoks, atoks = _full_toks(g['home'], soccer), _full_toks(g['away'], soccer)
        pair = frozenset((hk, ak))
        b = merged.get(pair)
        if b is not None and not _ts_ok(g['ts'], b['ts']):
            pair = (pair, int(g['ts'] // (8 * 3600)))
            b = merged.get(pair)
        if b is not None:
            return b, (hk == b['home_key'] or ak == b['away_key'])
        # Fuzzy fallback: city-only vs nickname vs full names.
        # Merge only if exactly one candidate matches (ambiguity -> new bucket).
        cands = []
        for cb in buckets:
            if not _ts_ok(g['ts'], cb['ts']):
                continue
            if (_sides_match(htoks, hk, cb['home_toks'], cb['home_key']) and
                    _sides_match(atoks, ak, cb['away_toks'], cb['away_key'])):
                cands.append((cb, True))
            elif (_sides_match(htoks, hk, cb['away_toks'], cb['away_key']) and
                    _sides_match(atoks, ak, cb['home_toks'], cb['home_key'])):
                cands.append((cb, False))
        if len(cands) == 1:
            b, aligned = cands[0]
            merged[pair] = b          # fast path for this book's later rows
            if aligned:
                b['home_toks'] |= htoks
                b['away_toks'] |= atoks
            else:
                b['home_toks'] |= atoks
                b['away_toks'] |= htoks
            return b, aligned
        b = {'home': g['home'], 'away': g['away'],
             'home_key': hk, 'away_key': ak,
             'home_toks': set(htoks), 'away_toks': set(atoks),
             'ts': g['ts'], 'canon_rank': 99, 'books': {}}
        merged[pair] = b
        buckets.append(b)
        return b, True

    for book, games in per_book.items():
        rank = _CANON_ORDER.index(book) if book in _CANON_ORDER else 99
        for g in games:
            b, aligned = _bucket_for(g)
            if b is None:
                continue
            hk, ak = _team_key(g['home'], soccer), _team_key(g['away'], soccer)
            if rank < b['canon_rank']:
                if aligned:
                    b['home'], b['away'] = g['home'], g['away']
                    b['home_key'], b['away_key'] = hk, ak
                else:
                    b['home'], b['away'] = g['away'], g['home']
                    b['home_key'], b['away_key'] = ak, hk
                b['canon_rank'] = rank
            if b['ts'] is None:
                b['ts'] = g['ts']
            # this game's own keys -> bucket canonical names
            if aligned:
                name_map = {hk: b['home'], ak: b['away']}
            else:
                name_map = {hk: b['away'], ak: b['home']}
            h2h = []
            for nm, price in g['h2h']:
                if nm == 'Draw':
                    h2h.append(('Draw', price))
                    continue
                canon = name_map.get(_team_key(nm, soccer))
                if canon:
                    h2h.append((canon, price))
            entry = b['books'].setdefault(book, {'h2h': {}, 'props': {}})
            for nm, price in h2h:
                entry['h2h'][nm] = price
            for mkey, rows in g['props'].items():
                pm = entry['props'].setdefault(mkey, {})
                for player, line, side, price in rows:
                    rec = pm.setdefault((_norm_player(player), line),
                                        {'player': player})
                    rec[side] = price

    out = []
    now = time.time()
    for b in buckets:
        if b['ts'] and b['ts'] < now - 4 * 3600:
            continue
        ts = b['ts'] or now
        gid = f"{sport_key}|{b['home_key']}|{b['away_key']}|{int(ts // 3600)}"
        bookmakers = []
        for book in sorted(b['books'], key=lambda x: _CANON_ORDER.index(x)
                           if x in _CANON_ORDER else 99):
            entry = b['books'][book]
            markets = []
            if len(entry['h2h']) >= 2:
                markets.append({'key': 'h2h', 'outcomes': [
                    {'name': nm, 'price': pr} for nm, pr in entry['h2h'].items()]})
            for mkey, players in entry['props'].items():
                outs = []
                for (_pk, line), rec in players.items():
                    for side in ('over', 'under'):
                        if side in rec:
                            outs.append({'name': side.capitalize(),
                                         'description': rec['player'],
                                         'point': line, 'price': rec[side]})
                if outs:
                    markets.append({'key': mkey, 'outcomes': outs})
            if markets:
                bookmakers.append({'key': book,
                                   'title': PROVIDER_TITLES.get(book, book),
                                   'markets': markets})
        if not bookmakers:
            continue
        out.append({'id': gid, 'sport_key': sport_key,
                    'commence_time': _iso(ts),
                    'home_team': b['home'], 'away_team': b['away'],
                    'bookmakers': bookmakers})
    out.sort(key=lambda g: g['commence_time'])
    log(f'    merged: {len(out)} games across '
        f'{len([k for k in per_book if per_book[k]])} books')
    return out


# ============================================================
# SNAPSHOT CACHE + PUBLIC DROP-IN API
# ============================================================

_snap_lock = threading.Lock()
_snapshots = {}   # sport_key -> {'ts': epoch, 'games': [...], 'source': 'direct'|'ingest'}


def build_sport(sport_key, log=print, force=False):
    if sport_key not in SPORTS:
        return []
    with _snap_lock:
        cur = _snapshots.get(sport_key)
        if cur and not force and time.time() - cur['ts'] < SNAPSHOT_TTL:
            return cur['games']
    log(f'  [direct feeds] building {sport_key}')
    per_book = {}
    for name, fn in PROVIDERS:
        try:
            per_book[name] = fn(sport_key, log=log)
        except Exception as e:
            log(f'    {name}: error {str(e)[:80]}')
            per_book[name] = []
    games = _merge_to_v4(sport_key, per_book, log=log)
    with _snap_lock:
        # don't clobber a fresher externally-ingested snapshot with nothing
        cur = _snapshots.get(sport_key)
        if games or not cur or cur.get('source') != 'ingest':
            _snapshots[sport_key] = {'ts': time.time(), 'games': games,
                                     'source': 'direct'}
    return games


def _filter_market(games, market):
    out = []
    for g in games:
        bks = []
        for bk in g.get('bookmakers', []):
            ms = [m for m in bk.get('markets', []) if m.get('key') == market]
            if ms:
                bks.append({'key': bk['key'], 'title': bk.get('title', bk['key']),
                            'markets': ms})
        if bks:
            ng = dict(g)
            ng['bookmakers'] = bks
            out.append(ng)
    return out


def fetch_odds(sport, market, log=print):
    """Drop-in for The Odds API GET /v4/sports/{sport}/odds."""
    if not ENABLED or sport not in SPORTS:
        return None
    games = build_sport(sport, log=log)
    return _filter_market(games, market) or None


def fetch_events(sport, log=print):
    """Drop-in for GET /v4/sports/{sport}/events."""
    if not ENABLED or sport not in SPORTS:
        return []
    games = build_sport(sport, log=log)
    return [{'id': g['id'], 'sport_key': sport,
             'commence_time': g['commence_time'],
             'home_team': g['home_team'], 'away_team': g['away_team']}
            for g in games]


def fetch_event_odds(sport, event_id, market, log=print):
    """Drop-in for GET /v4/sports/{sport}/events/{id}/odds."""
    if not ENABLED or sport not in SPORTS:
        return None
    games = build_sport(sport, log=log)
    for g in games:
        if g['id'] == event_id:
            f = _filter_market([g], market)
            return f[0] if f else None
    return None


# ---- external snapshot ingestion (fetch_worker.py mode) ----

def snapshot_payload(sport_keys, log=print):
    """Build a JSON-safe payload of fresh snapshots for the given sports."""
    return {'generated': _iso(time.time()),
            'sports': {s: {'games': build_sport(s, log=log, force=True)}
                       for s in sport_keys if s in SPORTS}}


def ingest_snapshot(payload):
    """Accept a payload from fetch_worker.py; returns sports loaded."""
    sports = (payload or {}).get('sports') or {}
    n = 0
    with _snap_lock:
        for sk, blob in sports.items():
            games = blob.get('games')
            if sk in SPORTS and isinstance(games, list):
                _snapshots[sk] = {'ts': time.time(), 'games': games,
                                  'source': 'ingest'}
                n += 1
    return n


def status():
    with _snap_lock:
        return {sk: {'age_sec': int(time.time() - v['ts']),
                     'games': len(v['games']), 'source': v['source']}
                for sk, v in _snapshots.items()}
