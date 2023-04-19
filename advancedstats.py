print("advancedstats.py Running")
import pandas as pd
import time
from nba_api.stats.endpoints import boxscoreadvancedv2

delay = 0.6
counter = 0

# Assuming you have the sorted_games dataframe with GAME_ID and TEAM_ID columns created earlier in the script
# Import the CSV file as the sorted_games dataframe
loaded_games = pd.read_csv('nba_games_2017_to_2022.csv',
                           dtype={'GAME_ID': str})

# Get the top 20 rows of the sorted_games dataframe
#top_20_games = loaded_games.head(501)
##print(top_20_games.head())
# Initialize an empty dataframe to store advanced stats
advanced_stats = pd.DataFrame()

counter = 0
#loop through each row in the sorted_games dataframe
for index, row in loaded_games.iterrows():
  game_id = row["GAME_ID"]
  team_id = row["TEAM_ID"]

  try:
    response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
    boxscore = response.get_data_frames()[
      1]  # Access the second dataframe for team stats

    team_stats = boxscore.loc[boxscore["TEAM_ID"] == team_id]

    advanced_stats = pd.concat([advanced_stats, team_stats], ignore_index=True)

  except Exception as e:
    print("Could not retrieve the advanced stats for game:", game_id)
    print("Error:", e)

  # Increment and print the counter
  counter += 1
  print(f'Processed game {counter} with GAME_ID: {game_id}')

  if counter % 100 == 0:
    time.sleep(10)
  # Sleep to respect the rate limit
  time.sleep(delay)
print(f"Current chunk: {current_chunk}")

combined_stats = pd.merge(loaded_games,
                          advanced_stats,
                          on=['GAME_ID', 'TEAM_ID'])

# Print a list of all columns in combined_stats
print("Columns in combined_stats:", combined_stats.columns.tolist())
# Select the desired columns
selected_columns = [
  'SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION_x', 'GAME_ID', 'GAME_DATE',
  'MATCHUP', 'WL', 'PTS', 'MIN', 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING',
  'DEF_RATING', 'E_NET_RATING', 'NET_RATING', 'AST_PCT', 'AST_TOV',
  'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'E_TM_TOV_PCT', 'TM_TOV_PCT',
  'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT', 'E_PACE', 'PACE', 'PACE_PER40',
  'POSS', 'PIE'
]

final_stats = combined_stats.loc[:, selected_columns]

# Write final stats to a CSV file
final_stats.to_csv('1nba_games_2017to2022_advanced.csv', index=False)
