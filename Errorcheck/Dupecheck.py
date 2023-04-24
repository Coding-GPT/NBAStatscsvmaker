import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO)

schedule_csv = os.path.join(os.path.dirname(__file__), '..', 'NBAschedule17to22.csv')
advanced_csv = os.path.join(os.path.dirname(__file__), '..', 'nba_games_17to22_sadvanced.csv')

# Read the NBAschedule17to22.csv file
with open(schedule_csv, 'r') as f:
    schedule_data = pd.read_csv(f, dtype={'GAME_ID': str})

# Read the nba_games_17to22_sadvanced.csv file
with open(advanced_csv, 'r') as f:
    advanced_data = pd.read_csv(f, dtype={'GAME_ID': str})

# Check for duplicate GAME_ID and TEAM_ID pairs in the advanced data
duplicates = advanced_data[advanced_data.duplicated(subset=['GAME_ID', 'TEAM_ID'], keep=False)]

if not duplicates.empty:
    logging.warning("Found duplicates in the advanced data:")
    logging.warning(duplicates)

# Find the missing GAME_ID and TEAM_ID pairs in the advanced data
missing_ids = pd.concat([schedule_data, advanced_data]).drop_duplicates(subset=['GAME_ID', 'TEAM_ID'], keep=False)

if not missing_ids.empty:
    logging.warning("Found missing GAME_ID and TEAM_ID pairs in the advanced data:")
    logging.warning(missing_ids)
else:
    logging.info("No missing GAME_ID and TEAM_ID pairs found in the advanced data.")
