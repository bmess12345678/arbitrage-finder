"""
Premium Arbitrage Finder - Web Version
Runs on Render.com, access from iPhone
NOW WITH NCAA BASKETBALL!
"""

from flask import Flask, render_template, jsonify
import requests
import time
from datetime import datetime
import threading
import os

app = Flask(__name__)

# API key
API_KEY = "19c83d930cc9b8bfcd3da28458f38d76"

# Global state
state = {
    'opportunities': [],
    'pure_arbitrage': [],
    'last_scan': None,
    'scanning': False,
    'total_scanned': 0
}

# Exploitable markets - NOW WITH NCAA!
MARKETS = [
    # NCAA Basketball (MOST EXPLOITABLE - March Madness season!)
    ('basketball_ncaab', 'player_points', 'NCAAB Points'),
    ('basketball_ncaab', 'player_rebounds', 'NCAAB Rebounds'),
    ('basketball_ncaab', 'player_assists', 'NCAAB Assists'),
    
    # NBA
    ('basketball_nba', 'player_points', 'NBA Points'),
    ('basketball_nba', 'player_rebounds', 'NBA Rebounds'),
    ('basketball_nba', 'player_assists', 'NBA Assists'),
    ('basketball_nba', 'player_threes', 'NBA 3-Pointers'),
    
    # NFL
    ('americanfootball_nfl', 'player_pass_tds', 'NFL Pass TDs'),
    ('americanfootball_nfl', 'player_pass_yds', 'NFL Pass Yards'),
    
    # NHL
    ('icehockey_nhl', 'player_points', 'NHL Points'),
]

BOOKMAKERS = ['fanduel', 'espnbet', 'draftkings', 'betmgm', 'williamhill_us']

def fetch_odds(sport, market):
    """Fetch odds from API"""
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params = {
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': market,
        'bookmakers': ','.join(BOOKMAKERS),
        'oddsFormat': 'american'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def analyze_market(games_data):
    """Find outliers and arbitrage"""
    if not games_data:
        return []
    
    opportunities = []
    
    for game in games_data:
        game_info = f"{game['away_team']} @ {game['home_team']}"
        players = {}
        
        for bookmaker in game.get('bookmakers', []):
            book_name = bookmaker['key']
            
            for market in bookmaker.get('markets', []):
                for outcome in market.get('outcomes', []):
                    player = outcome.get('description', outcome.get('name', ''))
                    if not player:
                        continue
                    
                    line = outcome.get('point')
                    odds = outcome.get('price')
                    
                    if line is None or odds is None:
                        continue
                    
                    if player not in players:
                        players[player] = {'game': game_info, 'books': {}}
                    
                    if book_name not in players[player]['books']:
                        players[player]['books'][book_name] = []
                    
                    players[player]['books'][book_name].append({
                        'line': line,
                        'odds': odds
                    })
        
        # Find opportunities
        for player, data in players.items():
            books = data['books']
            
            if len(books) < 3:
                continue
            
            # Consensus from sharp books
            consensus_lines = []
            for book in ['draftkings', 'betmgm', 'williamhill_us']:
                if book in books and books[book]:
                    consensus_lines.append(books[book][0]['line'])
            
            if len(consensus_lines) < 2:
                continue
            
            consensus = sum(consensus_lines) / len(consensus_lines)
            
            # Check FanDuel
            if 'fanduel' in books and books['fanduel']:
                fd_line = books['fanduel'][0]['line']
                fd_odds = books['fanduel'][0]['odds']
                diff = fd_line - consensus
                edge = abs(diff / consensus * 100) if consensus != 0 else 0
                
                if abs(diff) >= 1.0 and edge >= 3:
                    opportunities.append({
                        'player': player,
                        'game': data['game'],
                        'book': 'FanDuel',
                        'line': fd_line,
                        'consensus': round(consensus, 1),
                        'difference': round(diff, 1),
                        'edge': round(edge, 1),
                        'recommendation': 'UNDER' if diff > 0 else 'OVER',
                        'odds': fd_odds
                    })
            
            # Check ESPN Bet
            if 'espnbet' in books and books['espnbet']:
                espn_line = books['espnbet'][0]['line']
                espn_odds = books['espnbet'][0]['odds']
                diff = espn_line - consensus
                edge = abs(diff / consensus * 100) if consensus != 0 else 0
                
                if abs(diff) >= 1.0 and edge >= 3:
                    opportunities.append({
                        'player': player,
                        'game': data['game'],
                        'book': 'ESPN Bet',
                        'line': espn_line,
                        'consensus': round(consensus, 1),
                        'difference': round(diff, 1),
                        'edge': round(edge, 1),
                        'recommendation': 'UNDER' if diff > 0 else 'OVER',
                        'odds': espn_odds
                    })
    
    return opportunities

def scan_markets():
    """Scan all markets"""
    state['scanning'] = True
    all_opps = []
    
    for sport, market, name in MARKETS:
        games = fetch_odds(sport, market)
        if games:
            opps = analyze_market(games)
            all_opps.extend(opps)
        time.sleep(1)
    
    # Sort by edge
    all_opps.sort(key=lambda x: x['edge'], reverse=True)
    
    state['opportunities'] = all_opps
    state['last_scan'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    state['total_scanned'] = len(all_opps)
    state['scanning'] = False

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
        'total': state['total_scanned'],
        'scanning': state['scanning']
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

