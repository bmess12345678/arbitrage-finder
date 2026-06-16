#!/usr/bin/env python3
"""
smoke_test.py — live check of every direct sportsbook feed.

Run this FIRST, from your own machine (residential IP):

    python3 smoke_test.py                # all sports
    python3 smoke_test.py basketball_nba # one sport

For each sport x provider it hits the real endpoint and reports
games found, moneyline rows, and prop rows, then shows what the
merged Odds-API-shaped output looks like. No bets, no writes —
read-only HTTP GETs.

Interpreting results:
  - A column of zeros for ONE provider = that book changed its API
    (or geoblocks you). The app still works off the others.
  - Zeros for ALL providers = network problem (VPN? firewall?).
  - Pinnacle 403 from a US IP is common — it's fine if Render's
    region differs, or just rely on DK/FD/MGM/BR for consensus.
"""
import sys
import time
import traceback

import providers as P

SPORTS = sys.argv[1:] or list(P.SPORTS.keys())

FETCHERS = [
    ('pinnacle',   P.fetch_pinnacle),
    ('draftkings', P.fetch_draftkings),
    ('fanduel',    P.fetch_fanduel),
    ('betmgm',     P.fetch_betmgm),
    ('betrivers',  P.fetch_betrivers),
]


def quiet(*_a, **_k):
    pass


def main():
    print(f"direct feeds enabled: {P.ENABLED}")
    print(f"sports: {', '.join(SPORTS)}\n")
    grand_ok = 0

    for sport in SPORTS:
        if sport not in P.SPORTS:
            print(f"!! unknown sport '{sport}' — valid: {list(P.SPORTS)}")
            continue
        print("=" * 64)
        print(sport)
        print("=" * 64)
        per_book = {}
        for name, fn in FETCHERS:
            t0 = time.time()
            try:
                games = fn(sport, log=quiet) or []
                gl = sum(len(g['h2h']) for g in games)
                pr = sum(len(v) for g in games for v in g['props'].values())
                per_book[name] = games
                flag = 'OK ' if games else '-- '
                if games:
                    grand_ok += 1
                print(f"  {flag}{name:<11} games={len(games):<4} "
                      f"ml_rows={gl:<5} prop_rows={pr:<5} "
                      f"({time.time()-t0:.1f}s)")
            except Exception as e:
                per_book[name] = []
                print(f"  XX {name:<11} ERROR: {type(e).__name__}: {e}")
                if '-v' in sys.argv:
                    traceback.print_exc()

        merged = P._merge_to_v4(sport, per_book, log=quiet)
        multi = [g for g in merged if len(g['bookmakers']) >= 2]
        print(f"  => merged: {len(merged)} games, "
              f"{len(multi)} with 2+ books (these are scannable)")
        for g in merged[:3]:
            books = ','.join(b['key'] for b in g['bookmakers'])
            print(f"     {g['commence_time']}  "
                  f"{g['away_team']} @ {g['home_team']}  [{books}]")
        print()

    print("=" * 64)
    if grand_ok == 0:
        print("RESULT: nothing fetched. Check your connection/VPN before")
        print("deploying — the feeds themselves may be fine from Render.")
    else:
        print(f"RESULT: {grand_ok} provider/sport feeds alive. "
              f"Safe to deploy.")
        print("If Pinnacle failed here it may still work from Render, and")
        print("vice versa — the worker (fetch_worker.py) covers the case")
        print("where Render's datacenter IP gets blocked.")


if __name__ == '__main__':
    main()
