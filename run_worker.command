#!/bin/bash
# ============================================================
#  Arb Scanner - Home Feed Worker (Mac)
#  Double-click this file (or run it in Terminal). First run
#  asks two questions (app URL and scan key), then it runs
#  forever, pushing fresh sportsbook odds to your Render app.
#  Note: if macOS blocks it, right-click > Open the first time.
# ============================================================
cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
    echo
    echo "Python 3 is not installed. Opening the download page now."
    echo "Install it, then double-click this file again."
    open "https://www.python.org/downloads/"
    read -p "Press enter to close"
    exit 1
fi

echo "Installing/checking the one required package (requests)..."
python3 -m pip install --user --quiet requests

echo
echo "Starting the feed worker. Leave this window open."
echo "Press Ctrl+C or close the window to stop."
echo
python3 fetch_worker.py
read -p "Press enter to close"
