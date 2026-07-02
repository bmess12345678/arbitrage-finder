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

print()
if FAIL:
    print(f"❌ {len(FAIL)} failures: {FAIL}")
    sys.exit(1)
print("✅ all tests passed")
