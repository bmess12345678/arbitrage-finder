#!/usr/bin/env python3
"""Offline sanity tests — no network. Exercises the new spreads/totals plumbing:
pair completeness, arb pairing, middle math, and CLV market-name mapping."""
import sys, math
sys.path.insert(0, '.')

import providers
import web_arbitrage as wa

FAIL = []

def check(name, cond, detail=''):
    status = 'PASS' if cond else 'FAIL'
    print(f"[{status}] {name}" + (f" — {detail}" if detail and not cond else ''))
    if not cond:
        FAIL.append(name)

# ---------- 1. providers._finalize_lines ----------
g = providers._mk_game('Denver Nuggets', 'LA Lakers', '2026-07-02T00:00:00Z')
# complete total pair + a stray alt line that must be dropped
providers._add_total(g, 'Over', 220.5, -110)
providers._add_total(g, 'Under', 220.5, -110)
providers._add_total(g, 'Over', 225.5, +150)      # half pair — must not survive
# complete spread pair
providers._add_spread(g, 'Denver Nuggets', -3.5, -108)
providers._add_spread(g, 'LA Lakers', +3.5, -112)
# second complete spread pair with worse juice — only the more balanced pair survives
providers._add_spread(g, 'Denver Nuggets', -4.5, +120)
providers._add_spread(g, 'LA Lakers', +4.5, -160)
providers._finalize_lines(g)
tot_pts = {t[1] for t in g['totals']}
check('finalize: one complete total pair kept', tot_pts == {220.5} and len(g['totals']) == 2,
      f"got {g['totals']}")
sp_pts = {abs(s[1]) for s in g['spreads']}
check('finalize: one main spread pair kept', sp_pts == {3.5} and len(g['spreads']) == 2,
      f"got {g['spreads']}")

# half spread pair only -> dropped entirely
g2 = providers._mk_game('A', 'B', '')
providers._add_spread(g2, 'A', -7.0, -110)
providers._finalize_lines(g2)
check('finalize: lone half-pair dropped', g2['spreads'] == [], f"got {g2['spreads']}")

# period/alt guard
check('period guard blocks 1st half', providers._is_period_or_alt('1st Half Total'))
check('period guard blocks alternate', providers._is_period_or_alt('Alternate Spread'))
check('period guard allows main total', not providers._is_period_or_alt('Total Points'))

# ---------- 2. find_game_arbs: same-line total arb ----------
def mk_v4(markets_by_book):
    return [{
        'home_team': 'Yankees', 'away_team': 'Red Sox',
        'commence_time': '2026-07-02T23:00:00Z',
        'bookmakers': [
            {'key': bk, 'title': bk, 'markets': mkts}
            for bk, mkts in markets_by_book.items()
        ],
    }]

def tot_mkt(over_pt, over_px, under_pt, under_px):
    return {'key': 'totals', 'outcomes': [
        {'name': 'Over', 'point': over_pt, 'price': over_px},
        {'name': 'Under', 'point': under_pt, 'price': under_px},
    ]}

# FD Over 8.5 +105, DK Under 8.5 +105 -> combined implied 97.6% -> real arb
games = mk_v4({
    'fanduel':    [tot_mkt(8.5, +105, 8.5, -125)],
    'draftkings': [tot_mkt(8.5, -125, 8.5, +105)],
})
arbs = wa.find_game_arbs(games, 'MLB Totals')
real_arbs = [a for a in arbs if a['type'] == 'arbitrage']
check('same-line totals arb found', len(real_arbs) == 1, f"got {len(real_arbs)}")
if real_arbs:
    check('arb edge ≈ 2.44%', abs(real_arbs[0]['edge'] - 2.44) < 0.05,
          f"edge={real_arbs[0]['edge']}")

# ---------- 3. mismatched lines must NOT be an arb (the old bug) ----------
games = mk_v4({
    'fanduel':    [tot_mkt(44.5, -105, 44.5, -115)],
    'draftkings': [tot_mkt(45.5, -115, 45.5, -105)],
})
arbs = wa.find_game_arbs(games, 'NFL Totals')
fake = [a for a in arbs if a['type'] == 'arbitrage']
check('Over 44.5 + Over 45.5 never emits fake arb', len(fake) == 0, f"got {fake}")
mids = [a for a in arbs if a['type'] == 'middle']
check('gapped totals emit a middle instead', len(mids) == 1, f"got {len(mids)}")
if mids:
    m = mids[0]
    # Over 44.5 -105 (FD best) + Under 45.5 -105 (DK best):
    imp = 105 / 205
    total = 2 * imp
    r = 1 / total
    check('middle be_pct = overround', abs(m['be_pct'] - (total - 1) * 100) < 0.01,
          f"be={m['be_pct']}")
    check('middle cost_pct correct', abs(m['cost_pct'] - (1 - r) * 100) < 0.01,
          f"cost={m['cost_pct']}")
    check('middle win_pct correct', abs(m['win_pct'] - (2 * r - 1) * 100) < 0.15,
          f"win={m['win_pct']}")
    check('middle window names 45', '45' in m['middle_window'], m['middle_window'])
    check('middle cost under cap', m['cost_pct'] <= wa.MIDDLE_MAX_COST)

# ---------- 4. spread middle ----------
def sp_mkt(fav, fpt, fpx, dog, dpt, dpx):
    return {'key': 'spreads', 'outcomes': [
        {'name': fav, 'point': fpt, 'price': fpx},
        {'name': dog, 'point': dpt, 'price': dpx},
    ]}

games = mk_v4({
    'fanduel':    [sp_mkt('Yankees', -2.5, -105, 'Red Sox', +2.5, -115)],
    'draftkings': [sp_mkt('Yankees', -4.5, -115, 'Red Sox', +4.5, -105)],
})
arbs = wa.find_game_arbs(games, 'NFL Spread')
mids = [a for a in arbs if a['type'] == 'middle']
check('spread middle found (−2.5 / +4.5)', len(mids) >= 1, f"got {len(mids)}")
if mids:
    m = mids[0]
    check('spread window = win by 3/4', '3' in m['middle_window'] and '4' in m['middle_window'],
          m['middle_window'])

# same-team pairing must never arb (Yankees -2.5 with Yankees -4.5)
games_same = mk_v4({
    'fanduel':    [{'key': 'spreads', 'outcomes': [{'name': 'Yankees', 'point': -2.5, 'price': +200}]}],
    'draftkings': [{'key': 'spreads', 'outcomes': [{'name': 'Yankees', 'point': -4.5, 'price': +200}]}],
})
arbs = wa.find_game_arbs(games_same, 'x')
check('same-team spread legs never pair', all(a['type'] != 'arbitrage' for a in arbs))

# ---------- 5. free middle routes to arb ----------
games = mk_v4({
    'fanduel':    [tot_mkt(8.5, +110, 8.5, -130)],
    'draftkings': [tot_mkt(9.5, -130, 9.5, +110)],
})
arbs = wa.find_game_arbs(games, 'MLB Totals')
free = [a for a in arbs if a['type'] == 'arbitrage']
check('combined <100% gapped pair routes to arb (free middle)', len(free) == 1,
      f"types={[a['type'] for a in arbs]}")

# ---------- 6. integer-line totals get no middle (push risk) ----------
games = mk_v4({
    'fanduel':    [tot_mkt(44.0, -105, 44.0, -115)],
    'draftkings': [tot_mkt(46.0, -115, 46.0, -105)],
})
arbs = wa.find_game_arbs(games, 'NFL Totals')
check('integer lines excluded from middles', all(a['type'] != 'middle' for a in arbs))

# ---------- 7. CLV market-name mapping ----------
cases = {
    'MLB Run Line': 'spreads', 'NHL Puck Line': 'spreads', 'NBA Spread': 'spreads',
    'MLB Totals': 'totals', 'World Cup Totals': 'totals',
    'MLB Total Bases': 'player_total_bases',
    'NFL Pass Yards': 'player_pass_yds', 'NFL Receptions': 'player_receptions',
    'NBA Points': 'player_points', 'MLB Moneyline': 'h2h',
    'MLB Pitcher Strikeouts': 'player_strikeouts',
}
for name, want in cases.items():
    got = wa._market_name_to_api_key(name)
    check(f"CLV map: {name} -> {want}", got == want, f"got {got}")

# ---------- 8. exchange overlay never touches point keys ----------
# analyze_game_markets home/away key discovery must skip (name, point) keys
games = mk_v4({
    'fanduel': [
        {'key': 'h2h', 'outcomes': [
            {'name': 'Yankees', 'price': -140}, {'name': 'Red Sox', 'price': +120}]},
        tot_mkt(8.5, -110, 8.5, -110),
        sp_mkt('Yankees', -1.5, +130, 'Red Sox', +1.5, -155),
    ],
    'pinnacle': [
        {'key': 'h2h', 'outcomes': [
            {'name': 'Yankees', 'price': -145}, {'name': 'Red Sox', 'price': +125}]},
        tot_mkt(8.5, -108, 8.5, -112),
        sp_mkt('Yankees', -1.5, +128, 'Red Sox', +1.5, -152),
    ],
})
fake_kalshi = {('yankees', 'red sox'): {'Yankees': 0.60, 'Red Sox': 0.40}}
try:
    opps = wa.analyze_game_markets(games, 'MLB Moneyline', kalshi_games=None, poly_games=None)
    check('analyze_game_markets runs on mixed markets', True)
except Exception as e:
    check('analyze_game_markets runs on mixed markets', False, str(e))

# ---------- 9. middle sort key ----------
sample = [
    {'type': 'player_prop', 'edge': 5.0},
    {'type': 'middle', 'edge': 0.0, 'be_pct': 2.4},
    {'type': 'arbitrage', 'edge': 1.2},
    {'type': 'middle', 'edge': 0.0, 'be_pct': 1.1},
]
def _rank(x):
    t = x.get('type', '')
    if t == 'arbitrage': return (0, -x.get('edge', 0.0))
    if t == 'middle':    return (1, x.get('be_pct', 99.0))
    return (2, -x.get('edge', 0.0))
sample.sort(key=_rank)
order = [s['type'] for s in sample]
check('sort: arb, cheap middle, dear middle, +EV',
      order == ['arbitrage', 'middle', 'middle', 'player_prop']
      and sample[1]['be_pct'] == 1.1, str(order))


# ---------- 16. EXECUTION smoke tests (the timezone-bug lesson: these
# functions must RUN, not just parse — network mocked out) ----------
class _FakeResp:
    def __init__(self, payload, code=200):
        self._p, self.status_code = payload, code
    def json(self):
        return self._p

_real_kget = wa.kalshi_get
def _fake_kget(url, params=None, timeout=15):
    if '/markets/KXTEST' in url:
        return _FakeResp({'market': {'status': 'settled', 'result': 'yes'}})
    return _FakeResp({'markets': [
        {'ticker': 'KXMLBGAME-26AUG21NYYBOS-NYY',
         'yes_sub_title': 'New York Y', 'close_time': '2099-01-01T00:00:00Z',
         'yes_bid': 55, 'yes_ask': 57},
    ], 'cursor': ''})
wa.kalshi_get = _fake_kget
try:
    res = wa.fetch_kalshi_sports(log_fn=lambda m: None)
    check('fetch_kalshi_sports executes without NameError', isinstance(res, dict)
          and 'games' in res)
except NameError as e:
    check('fetch_kalshi_sports executes without NameError', False, str(e))
finally:
    wa.kalshi_get = _real_kget

_real_req_get = wa.requests.get
wa.requests.get = lambda *a, **k: _FakeResp({'events': []})
wa.kalshi_get = _fake_kget
try:
    n = wa.grade_results(max_rows=1)
    check('grade_results executes without NameError', isinstance(n, int))
except NameError as e:
    check('grade_results executes without NameError', False, str(e))
finally:
    wa.requests.get = _real_req_get
    wa.kalshi_get = _real_kget

try:
    wa.kalshi_get = _fake_kget
    out = wa._grade_kalshi_row({'event_id': 'KXTEST-1', 'recommendation': 'Buy YES at ~34c',
                                'target_prob': 34.0})
    check('kalshi settlement grading math', out is not None and out[0] == 'win'
          and abs(out[1] - (100 * 0.66 / 0.34 - wa.kalshi_fee_pct(0.34))) < 0.1,
          str(out))
except NameError as e:
    check('kalshi settlement grading math', False, str(e))
finally:
    wa.kalshi_get = _real_kget

# ---------- 17. Exchange opponent verification (the Browns bug) ----------
poly_bad = {'Cleveland Browns': {'p': 0.535, 'opp': 'New York Jets', 'end': '2027-01-05'}}
poly_good = {'Cleveland Browns': {'p': 0.25, 'opp': 'Jacksonville Jaguars', 'end': '2026-09-13'}}
games_ex = mk_v4({
    'fanduel': [{'key': 'h2h', 'outcomes': [
        {'name': 'Jacksonville Jaguars', 'price': -430},
        {'name': 'Cleveland Browns', 'price': +330}]}],
    'pinnacle': [{'key': 'h2h', 'outcomes': [
        {'name': 'Jacksonville Jaguars', 'price': -380},
        {'name': 'Cleveland Browns', 'price': +288}]}],
})
games_ex[0]['home_team'] = 'Jacksonville Jaguars'
games_ex[0]['away_team'] = 'Cleveland Browns'
opps_bad = wa.analyze_game_markets(games_ex, 'NFL Moneyline', poly_games=poly_bad)
big_fake = [o for o in opps_bad if o.get('edge', 0) > 5]
check('wrong-opponent Poly market NEVER joins consensus', len(big_fake) == 0,
      str([(o['player'], o['edge']) for o in opps_bad]))
opps_good = wa.analyze_game_markets(games_ex, 'NFL Moneyline', poly_games=poly_good)
used_poly = any(any(d.get('book') == 'Polymarket' for d in (o.get('consensus_detail') or []))
                for o in opps_good)
check('right-opponent Poly market DOES join consensus', used_poly)
legacy_float = {'Cleveland Browns': 0.535}
opps_leg = wa.analyze_game_markets(games_ex, 'NFL Moneyline', poly_games=legacy_float)
check('legacy float entries rejected outright',
      all(o.get('edge', 0) < 5 for o in opps_leg))

# ---------- 18. Kalshi opponent parsing from ticker ----------
_real_kget2 = wa.kalshi_get
from datetime import datetime as _dtm, timedelta as _tdl, timezone as _tzn
_soon = (_dtm.now(_tzn.utc) + _tdl(hours=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
def _kget_games(url, params=None, timeout=15):
    return _FakeResp({'markets': [
        {'ticker': 'KXMLBGAME-26AUG211845LAASD-SD', 'close_time': _soon,
         'yes_bid': 60, 'yes_ask': 62},
        {'ticker': 'KXMLBGAME-26AUG211845LAASD-LAA', 'close_time': _soon,
         'yes_bid': 38, 'yes_ask': 40},
    ], 'cursor': ''})
wa.kalshi_get = _kget_games
try:
    kres = wa.fetch_kalshi_sports(log_fn=lambda m: None)
    sd = kres['games'].get('San Diego Padres')
    laa = kres['games'].get('Los Angeles Angels')
    check('kalshi ambiguous code split (LAA+SD)',
          isinstance(sd, dict) and sd['opp'] == 'Los Angeles Angels'
          and isinstance(laa, dict) and laa['opp'] == 'San Diego Padres',
          str(kres['games']))
finally:
    wa.kalshi_get = _real_kget2

# ---------- 19. Derivative translation math ----------
tr = wa.translate_prop_prob
p_same = tr('player_points', 24.5, 0.55, 24.5)
check('translate identity', p_same == 0.55)
p_up = tr('player_points', 24.5, 0.55, 25.5)
check('normal: higher line -> lower over prob', p_up is not None and p_up < 0.55, str(p_up))
p_dn = tr('player_points', 24.5, 0.55, 23.5)
check('normal: lower line -> higher over prob', p_dn is not None and p_dn > 0.55, str(p_dn))
# round trip: translate up then back ≈ original
p_rt = tr('player_points', 25.5, p_up, 24.5) if p_up else None
check('normal round-trip consistent', p_rt is not None and abs(p_rt - 0.55) < 0.02, str(p_rt))
q_up = tr('player_strikeouts', 6.5, 0.60, 7.5)
check('poisson: higher K line -> lower over prob', q_up is not None and q_up < 0.60, str(q_up))
q_rt = tr('player_strikeouts', 7.5, q_up, 6.5) if q_up else None
check('poisson round-trip consistent', q_rt is not None and abs(q_rt - 0.60) < 0.02, str(q_rt))
check('too-far hop refused', tr('player_points', 10.5, 0.55, 20.5) is None)
check('integer poisson line refused', tr('player_strikeouts', 6.0, 0.55, 7.5) is None)
check('unknown stat refused', tr('player_touchdowns_xyz', 1.5, 0.5, 2.5) is None)

# ---------- 20. Prop consensus with translated legs ----------
def prop_game(fd_line, fd_o, fd_u, br_line, br_o, br_u, pin_line, pin_o, pin_u):
    return [{
        'home_team': 'Denver Nuggets', 'away_team': 'LA Lakers',
        'commence_time': '2026-08-22T00:00:00Z', 'id': 'evp1', 'sport_key': 'basketball_nba',
        'bookmakers': [
            {'key': 'fanduel', 'markets': [{'key': 'player_points', 'outcomes': [
                {'description': 'Nikola Jokic', 'name': 'Over', 'point': fd_line, 'price': fd_o},
                {'description': 'Nikola Jokic', 'name': 'Under', 'point': fd_line, 'price': fd_u}]}]},
            {'key': 'betrivers', 'markets': [{'key': 'player_points', 'outcomes': [
                {'description': 'Nikola Jokic', 'name': 'Over', 'point': br_line, 'price': br_o},
                {'description': 'Nikola Jokic', 'name': 'Under', 'point': br_line, 'price': br_u}]}]},
            {'key': 'pinnacle', 'markets': [{'key': 'player_points', 'outcomes': [
                {'description': 'Nikola Jokic', 'name': 'Over', 'point': pin_line, 'price': pin_o},
                {'description': 'Nikola Jokic', 'name': 'Under', 'point': pin_line, 'price': pin_u}]}]},
        ]}]
# FD way off vs BR+Pinnacle at a DIFFERENT line: only translation can catch it
g = prop_game(24.5, +150, -190, 25.5, -125, -105, 25.5, -128, -102)
opps_t = wa.analyze_player_props(g, 'NBA Points', market_key='player_points')
fd_flags = [o for o in opps_t if o['book_key'] == 'fanduel' and o['recommendation'].startswith('OVER')]
check('derived-line consensus flags cross-line mispricing', len(fd_flags) >= 1,
      str([(o['book_key'], o['recommendation'], o['edge']) for o in opps_t]))
if fd_flags:
    check('derived flag labeled', 'derived-line' in fd_flags[0]['label2_name'],
          fd_flags[0]['label2_name'])

# ---------- 21. Exchange divergence guard ----------
# Right game, right opponent, right date — but a wrong-quantity price
# (series market / doubleheader leg / stale quote). Must be rejected.
poly_poison = {'Atlanta Braves': {'p': 0.645, 'opp': 'Milwaukee Brewers',
                                  'end': '2026-08-21', 'src': 'Braves vs. Brewers (series)'}}
poly_sane = {'Atlanta Braves': {'p': 0.47, 'opp': 'Milwaukee Brewers',
                                'end': '2026-08-21', 'src': 'Braves vs. Brewers'}}
games_div = mk_v4({
    'fanduel': [{'key': 'h2h', 'outcomes': [
        {'name': 'Milwaukee Brewers', 'price': -148},
        {'name': 'Atlanta Braves', 'price': +126}]}],
    'pinnacle': [{'key': 'h2h', 'outcomes': [
        {'name': 'Milwaukee Brewers', 'price': -145},
        {'name': 'Atlanta Braves', 'price': +123}]}],
    'betrivers': [{'key': 'h2h', 'outcomes': [
        {'name': 'Milwaukee Brewers', 'price': -147},
        {'name': 'Atlanta Braves', 'price': +123}]}],
})
games_div[0]['home_team'] = 'Milwaukee Brewers'
games_div[0]['away_team'] = 'Atlanta Braves'
o_poison = wa.analyze_game_markets(games_div, 'MLB Moneyline', poly_games=poly_poison)
fd_braves = [o for o in o_poison if o['book_key'] == 'fanduel' and 'Braves' in o['player']]
check('divergent exchange price rejected (no fake 8%+ edge)',
      all(o['edge'] < 4 for o in fd_braves),
      str([(o['player'], o['edge']) for o in fd_braves]))
check('rejected exchange absent from consensus detail',
      all(all(d.get('book') != 'Polymarket' for d in (o.get('consensus_detail') or []))
          for o in o_poison))
o_sane = wa.analyze_game_markets(games_div, 'MLB Moneyline', poly_games=poly_sane)
used = any(any(d.get('book') == 'Polymarket' for d in (o.get('consensus_detail') or []))
           for o in o_sane)
check('plausible exchange price still joins consensus', used)

# ---------- 22. Props: one book + exchange quote now analyzable ----------
one_book_game = [{
    'home_team': 'Las Vegas Aces', 'away_team': 'Seattle Storm',
    'commence_time': '2026-08-22T00:00:00Z', 'id': 'evw1', 'sport_key': 'basketball_wnba',
    'bookmakers': [
        {'key': 'fanduel', 'markets': [{'key': 'player_points', 'outcomes': [
            {'description': 'A. Wilson', 'name': 'Over', 'point': 22.5, 'price': +100},
            {'description': 'A. Wilson', 'name': 'Under', 'point': 22.5, 'price': -130}]}]},
    ]}]
kp = {wa.normalize_player_name('A. Wilson'): {'player_points': {22.5: 0.62}}}
opps_1b = wa.analyze_player_props(one_book_game, 'WNBA Points',
                                  kalshi_props=kp, market_key='player_points')
check('single book vs Kalshi prop flags', 
      any(o['book_key'] == 'fanduel' and o['recommendation'].startswith('OVER')
          and o['edge'] >= 2.0 for o in opps_1b),
      str([(o.get('book_key'), o.get('recommendation'), o.get('edge')) for o in opps_1b]))
no_ex = wa.analyze_player_props(one_book_game, 'WNBA Points', market_key='player_points')
check('single book with NO exchange still skipped', len(no_ex) == 0, str(no_ex))


# ---------- 23. Hardened name normalization ----------
np = wa.normalize_player_name
check('apostrophe-insensitive', np("A'ja Wilson") == np("Aja Wilson"))
check('accent-insensitive', np("Le\u00efla Lacan") == np("Leila Lacan"))
check('suffix+period stripped', np("Ken Griffey Jr.") == np("Ken Griffey"))

# ---------- 24. Kalshi window covers settlement-style close times ----------
_far = (_dtm.now(_tzn.utc) + _tdl(days=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
def _kget_far(url, params=None, timeout=15):
    return _FakeResp({'markets': [
        {'ticker': 'KXMLBGAME-26AUG211910ATLMIL-MIL', 'close_time': _far,
         'yes_bid': 55, 'yes_ask': 57},
    ], 'cursor': ''})
_rk = wa.kalshi_get
wa.kalshi_get = _kget_far
try:
    kr = wa.fetch_kalshi_sports(log_fn=lambda m: None)
    mil = kr['games'].get('Milwaukee Brewers')
    check('close_time = game+3d now inside window',
          isinstance(mil, dict) and mil['opp'] == 'Atlanta Braves', str(kr['games']))
finally:
    wa.kalshi_get = _rk

# ---------- 25. Origination model: game lines -> player points ----------
import time as _t
wa._FORM_CACHE['basketball_wnba'] = {
    'ts': _t.time(),
    'players': {
        wa.normalize_player_name("A'ja Wilson"): {'g': 8, 'pts_pg': 22.0,
                                                  'team': 'las vegas aces'},
        wa.normalize_player_name('Bench Player'): {'g': 2, 'pts_pg': 4.0,
                                                   'team': 'las vegas aces'},
        **{f'filler {i}': {'g': 6, 'pts_pg': 9.0, 'team': 'seattle storm'}
           for i in range(5)},
    },
    'teams': {'las vegas aces': 82.0, 'seattle storm': 79.0},
}
_ritt = wa._implied_team_totals
wa._implied_team_totals = lambda s, h, a: (84.0, 78.0)   # Aces home
def model_event(line, over_px, under_px, extra_book=None):
    bms = [{'key': 'fanduel', 'markets': [{'key': 'player_points', 'outcomes': [
        {'description': "A'ja Wilson", 'name': 'Over', 'point': line, 'price': over_px},
        {'description': "A'ja Wilson", 'name': 'Under', 'point': line, 'price': under_px},
        {'description': 'Bench Player', 'name': 'Over', 'point': 5.5, 'price': -110},
        {'description': 'Bench Player', 'name': 'Under', 'point': 5.5, 'price': -110},
    ]}]}]
    if extra_book:
        bms.append(extra_book)
    return [{'home_team': 'Las Vegas Aces', 'away_team': 'Seattle Storm',
             'commence_time': '2026-08-22T00:00:00Z', 'id': 'evm1',
             'bookmakers': bms}]
try:
    # share 22/82=0.268, mu = 0.268*84 = 22.54, sigma = 7.21
    # line 19.5: p_over = 1-Phi(-0.421) = 0.663 vs -110 imp 0.524 -> +13.9%
    mo = wa.model_prop_opportunities(model_event(19.5, -110, -110),
                                     'basketball_wnba', 'WNBA Points')
    wilson = [o for o in mo if 'Wilson' in o['player']]
    check('model flags stale line vs single book',
          len(wilson) == 1 and wilson[0]['recommendation'] == 'OVER 19.5'
          and 10 < wilson[0]['edge'] < 15,
          str([(o['player'], o['recommendation'], o['edge']) for o in mo]))
    check('model card labeled as game-line derived',
          wilson and 'game-line derived' in wilson[0]['label2_name'])
    check('thin-form player never flagged',
          all('Bench' not in o['player'] for o in mo))

    # sanity guard: mu 22.5 vs line 14.5 -> gap 8 > 30% of line -> skip
    mo2 = wa.model_prop_opportunities(model_event(14.5, +200, -280),
                                      'basketball_wnba', 'WNBA Points')
    check('model-vs-line sanity guard skips wild gaps',
          all('Wilson' not in o['player'] for o in mo2), str(mo2))

    # fairly-priced line -> no flag (p_model 0.545 at 21.5 vs imp 0.524 -> 2.1% < 4)
    mo3 = wa.model_prop_opportunities(model_event(21.5, -110, -110),
                                      'basketball_wnba', 'WNBA Points')
    check('fair line stays quiet', all('Wilson' not in o['player'] for o in mo3),
          str([(o.get('player'), o.get('edge')) for o in mo3]))

    # non-CO book quote ignored
    pin = {'key': 'pinnacle', 'markets': [{'key': 'player_points', 'outcomes': [
        {'description': "A'ja Wilson", 'name': 'Over', 'point': 19.5, 'price': -150},
        {'description': "A'ja Wilson", 'name': 'Under', 'point': 19.5, 'price': +120}]}]}
    mo4 = wa.model_prop_opportunities(model_event(19.5, -110, -110, extra_book=pin),
                                      'basketball_wnba', 'WNBA Points')
    check('model prices only CO-bettable books',
          all(o['book_key'] != 'pinnacle' for o in mo4))
finally:
    wa._implied_team_totals = _ritt

print()
if FAIL:
    print(f"❌ {len(FAIL)} failures: {FAIL}")
    sys.exit(1)
print("✅ all tests passed")
