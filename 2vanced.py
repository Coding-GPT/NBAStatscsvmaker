import time
import gc
import os
import pandas as pd
from nba_api.stats.endpoints import boxscoreadvancedv2
from updategit import update_git
import logging

logging.basicConfig(level=logging.INFO)
logging.info("2vanced.py Running")

sorted_games = pd.DataFrame()
existing_data = pd.DataFrame()

with open('NBAschedule17to22.csv', 'r') as f:
    sorted_games = pd.read_csv(f, dtype={'GAME_ID': str})

sorted_games = sorted_games.sort_values('GAME_ID', ascending=True).reset_index(drop=True)

delay = 0.6
chunk_size = 50
counter = 0
chunks_processed = 0
last_git_update = time.time()


if os.path.exists('nba_games_17to22_sadvanced.csv'):
    existing_data = pd.read_csv('nba_games_17to22_sadvanced.csv', dtype={'GAME_ID': str})
    existing_data = existing_data.sort_values('GAME_ID', ascending=True).reset_index(drop=True)
    existing_game_ids = set(existing_data['GAME_ID'])
else:
    existing_game_ids = set()
if not existing_data.empty:
    last_processed_game_id = existing_data['GAME_ID'].iloc[-1]
    last_processed_index = sorted_games[sorted_games['GAME_ID'] == last_processed_game_id].iloc[0].name
else:
    last_processed_index = 0

start_index = last_processed_index
# Iterate through the sorted_games DataFrame in chunks of the specified size
while start_index < len(sorted_games):
    end_index = min(start_index + chunk_size, len(sorted_games))
    current_chunk = sorted_games.iloc[start_index:end_index].copy()
    current_chunk.loc[:, 'GAME_ID'] = current_chunk['GAME_ID'].apply(str)
    logging.info(f'Processing chunk of {len(current_chunk)} games with {len(current_chunk.index)} game IDs')

    advanced_stats = pd.DataFrame()
    combined_stats = pd.DataFrame()

    # Iterate through the rows of the current_chunk DataFrame
    for index, row in current_chunk.iterrows():
        game_id = row["GAME_ID"]

        # Skip processing if the game_id has already been processed
        if game_id in existing_game_ids:
            continue

        try:
            # Make an API call to get the advanced boxscore data for the current game_id
            response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            boxscore = response.get_data_frames()[1]

            # Filter the boxscore data to include only the rows with TEAM_IDs in the current_chunk
            team_stats = boxscore.loc[boxscore["TEAM_ID"].isin(current_chunk["TEAM_ID"].values)]

            # Add the filtered team_stats to the advanced_stats DataFrame
            advanced_stats = pd.concat([advanced_stats, team_stats], ignore_index=True)
            advanced_stats['GAME_ID'] = advanced_stats['GAME_ID'].apply(str)

        except Exception as e:
            logging.warning(f"Could not retrieve the advanced stats for game: {game_id}")
            logging.warning(f"Error: {e}")

        # Add the processed game_id to the existing_game_ids set
        existing_game_ids.add(game_id)

        counter += 1
        logging.info(f'Processed game {counter} with GAME_ID: {game_id}')
        time.sleep(delay)

    # Merge the advanced_stats DataFrame with the current_chunk DataFrame based on GAME_ID and TEAM_ID
    if 'GAME_ID' in advanced_stats.columns and 'GAME_ID' in current_chunk.columns:
        combined_stats = pd.merge(advanced_stats, current_chunk, on=['GAME_ID', 'TEAM_ID'], suffixes=('', '_y'))
    combined_stats = combined_stats.reset_index(drop=True)
    combined_stats = combined_stats.filter(regex='^(?!.*_y).*$')

    # If the existing_data DataFrame is not empty, concatenate it with the combined_stats DataFrame
    if not existing_data.empty:
        combined_stats = pd.concat([existing_data, combined_stats], ignore_index=True)

    chunks_processed += 1
    start_index += chunk_size
    
    logging.info(combined_stats.head())

    elapsed_time = time.time() - last_git_update

    # Write the existing_data DataFrame to the CSV file every 5 chunks
    if chunks_processed % 1 == 0:
        logging.info(f"Writing existing_data DataFrame to CSV file {combined_stats.head(3)}")
        # If the CSV file does not exist yet, create it and write the data in 'write' mode ('w')
        if not os.path.exists('nba_games_17to22_sadvanced.csv'):
            with open('nba_games_17to22_sadvanced.csv', 'w') as f:
                combined_stats.to_csv(f, index=False)
        # If the CSV file exists, open it in 'append' mode ('a') and write the data
        else:
            with open('nba_games_17to22_sadvanced.csv', 'a') as f:
                combined_stats.to_csv(f, header=False, index=False)
        combined_stats = pd.DataFrame()  # Reset combined_stats DataFrame
        
        if elapsed_time >= (5 * 60):
            update_git('nba_games_17to22_sadvanced.csv')  # Update the git file every 15 min

        last_git_update = time.time() # Reset the last_git_update time to the current time

    logging.info(f"Wrote game IDs {start_index + 1} to {end_index}")
    gc.collect()
