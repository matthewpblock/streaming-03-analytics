"""src/streaming/fetch_nba_data.py.

A utility script to fetch live/historical NBA play-by-play data
and transform it into the streaming data contract format for Phase 5.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
import re

from nba_api.stats.endpoints import playbyplayv3
import pandas as pd

# === PATH SETUP ===
ROOT_DIR = Path.cwd()
DATA_DIR = ROOT_DIR / "data"
EVENTS_CSV = DATA_DIR / "events.csv"
PLAYERS_CSV = DATA_DIR / "players.csv"

def fetch_game_data(game_id: str):
    """Fetch play-by-play data for a given NBA game ID.

    Arguments:
        game_id: The NBA game ID to fetch.
    """
    print(f"Fetching Play-by-Play data for Game ID: {game_id}...")

    # Call the nba_api endpoint
    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
    df = pbp.get_data_frames()[0]

    events = []
    players = {}

    # Pull all events from the Play-By-Play data
    all_events = df

    # Simulated timestamp starting from current time to mimic a live stream
    base_time = datetime.now(UTC)

    for idx, row in all_events.iterrows():
        # Skip events with no assigned player (e.g., technical team fouls)
        if pd.isna(row.get('personId')) or row.get('personId') == 0:
            continue

        play_id = f"P{row['actionNumber']:04d}"
        pid = f"PL_{row['personId']}"
        player_name = row.get('playerName')

        team_city = row.get('teamCity', '')
        team_tricode = row.get('teamTricode', '')
        team_name = f"{team_city} {team_tricode}".strip()

        # Handle cases where player or team name is missing for non-player events
        if not player_name:
            player_name = "NBA Official"
        if not team_name:
            team_name = "NBA"

        # Update our player reference lookup table
        if pid not in players:
            players[pid] = {
                "player_id": pid,
                "player_name": player_name,
                "team_name": team_name,
                "position": "Unknown" # Position is not provided in play-by-play
            }

        # Combine descriptions to parse what happened
        desc = str(row.get('description', '')).replace('None', '')

        # 1. Parse Shot Type
        if '3PT' in desc:
            shot_type = '3PT'
        elif 'Free Throw' in desc:
            shot_type = 'FT'
        else:
            shot_type = '2PT'

        # 2. Parse Distance
        distance_ft = row.get('shotDistance')
        if pd.isna(distance_ft):
            dist_match = re.search(r"(\d+)'", desc)
            if dist_match:
                distance_ft = float(dist_match.group(1))
            elif shot_type == 'FT':
                distance_ft = 15.0
            else:
                # Defaults to 2.0 ft for layups/dunks if distance isn't logged
                distance_ft = 2.0
        else:
            distance_ft = float(distance_ft)

        # Clean up action_type to avoid blank validation errors
        action_type = str(row.get('actionType', '')).strip()
        if not action_type or action_type == 'nan':
            action_type = 'Unknown'

        # 3. Parse if the shot was made
        if action_type == 'Made Shot':
            is_made = True
        elif action_type == 'Missed Shot':
            is_made = False
        elif action_type == 'Free Throw':
            is_made = 'MISS' not in desc
        else:
            is_made = False

        # 4. Create an artificial timestamp incremented by a few seconds per play
        timestamp = (base_time + timedelta(seconds=idx*5)).strftime("%Y-%m-%dT%H:%M:%SZ")

        events.append({
            "play_id": play_id,
            "game_id": game_id,
            "timestamp": timestamp,
            "player_id": pid,
            "action_type": action_type,
            "shot_type": shot_type,
            "distance_ft": distance_ft,
            "is_made": is_made,
            "quarter": str(row.get('period', '')),
            "time_remaining": str(row.get('clock', ''))
        })

    # Save to standard CSVs to feed the Kafka Pipeline
    events_df = pd.DataFrame(events)
    players_df = pd.DataFrame(list(players.values()))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    events_df.to_csv(EVENTS_CSV, index=False)
    players_df.to_csv(PLAYERS_CSV, index=False)

    print(f"Successfully generated {len(events_df)} events into 'data/events.csv'")
    print(f"Successfully generated {len(players_df)} players into 'data/players.csv'")

if __name__ == "__main__":
    fetch_game_data("0042500317")
