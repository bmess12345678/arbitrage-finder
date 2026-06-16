#!/usr/bin/env python3
"""
fetch_worker.py — run the sportsbook feeds from YOUR machine and push
snapshots to the Render app.

Use this if (and only if) the Render deploy logs show 403/blocked
responses from the books. Sportsbooks sometimes block datacenter IPs;
your home IP looks like a normal customer. The worker:

  1. fetches all providers locally (providers.py),
  2. POSTs the merged snapshot to  {APP_URL}/api/ingest?key=SCAN_KEY
  3. triggers a scan via            {APP_URL}/api/scan?key=SCAN_KEY
  4. sleeps, repeats.

The web app then serves opportunities from the ingested snapshot —
no outbound book traffic from Render at all (set DIRECT_FEEDS=0 there).

Env / flags:
  APP_URL          e.g. https://arbitrage-finder-1.onrender.com  (required)
  SCAN_KEY         same key the app uses for /api/scan           (required)
  WORKER_SPORTS    csv, default: all configured sports
  WORKER_INTERVAL  seconds between cycles, default 300
  --once           single cycle, then exit (good for cron / Task Scheduler)

Example:
  APP_URL=https://arbitrage-finder-1.onrender.com SCAN_KEY=xxx \
      python3 fetch_worker.py
"""
import json
import os
import sys
import time

import requests

import providers as P

APP_URL = (os.environ.get('APP_URL') or '').rstrip('/')
SCAN_KEY = os.environ.get('SCAN_KEY', '')
SPORTS = [s for s in os.environ.get('WORKER_SPORTS', '').split(',') if s] \
    or list(P.SPORTS.keys())
INTERVAL = int(os.environ.get('WORKER_INTERVAL', '300'))
ONCE = '--once' in sys.argv


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def cycle(sess):
    payload = P.snapshot_payload(SPORTS, log=log)
    n_games = sum(len(v['games']) for v in payload['sports'].values())
    body = json.dumps(payload)
    log(f"snapshot: {n_games} games, {len(body)//1024} KB")
    if n_games == 0:
        log("nothing fetched — skipping push (check smoke_test.py)")
        return

    r = sess.post(f"{APP_URL}/api/ingest", params={'key': SCAN_KEY},
                  data=body, headers={'Content-Type': 'application/json'},
                  timeout=60)
    log(f"ingest -> {r.status_code} {r.text[:120]}")
    r.raise_for_status()

    r = sess.post(f"{APP_URL}/api/scan", params={'key': SCAN_KEY},
                  timeout=300)
    log(f"scan   -> {r.status_code} {r.text[:200]}")


def main():
    if not APP_URL or not SCAN_KEY:
        sys.exit("Set APP_URL and SCAN_KEY environment variables first.")
    log(f"worker up: {APP_URL} sports={SPORTS} every {INTERVAL}s")
    sess = requests.Session()
    backoff = INTERVAL
    while True:
        try:
            cycle(sess)
            backoff = INTERVAL
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"cycle failed: {type(e).__name__}: {e}")
            backoff = min(backoff * 2, 3600)
        if ONCE:
            break
        time.sleep(backoff)


if __name__ == '__main__':
    main()
