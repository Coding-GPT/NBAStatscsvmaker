print("superadvanced.py Running")
import time
import pandas as pd
from nba_api.stats.endpoints import boxscoreadvancedv2

# Import the CSV file as the sorted_games dataframe
sorted_games = pd.read_csv('NBAschedule17to22.csv', dtype={'GAME_ID': str})

# Sort the sorted_games dataframe by GAME_ID in ascending order and reset the index
sorted_games = sorted_games.sort_values('GAME_ID',
                                        ascending=True).reset_index(drop=True)


#print(sorted_games.head())
print(sorted_games['GAME_ID'].dtype)
# Define a delay in seconds to respect the rate limit
delay = 0.6  # You can adjust this value to stay below the rate limit
#
chunk_size = 50  # This is the number of rows to process at a time
counter = 0
# Read existing data to avoid duplicate requests
advanced_stats = pd.DataFrame()
combined_stats = pd.DataFrame()

try:
  existing_data = pd.read_csv('nba_games_17to22_sadvanced.csv',
                              dtype={'GAME_ID': str})
  existing_game_ids = set(existing_data['GAME_ID'])
except FileNotFoundError:
  existing_game_ids = set()

for start_index in range(0, len(sorted_games), chunk_size):
  end_index = min(start_index + chunk_size, len(sorted_games))
  current_chunk = sorted_games.iloc[start_index:end_index]
  print(
    f'Processing chunk of {len(current_chunk)} games with {len(current_chunk.index)} game IDs'
  )
  print(current_chunk.head(2))

  advanced_stats = pd.DataFrame()
  combined_stats = pd.DataFrame()

  for index, row in current_chunk.iterrows():
    game_id = row["GAME_ID"]
    team_id = row["TEAM_ID"]

    if game_id in existing_game_ids:
      continue

    try:
      response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
      boxscore = response.get_data_frames()[1]
      team_stats = boxscore.loc[boxscore["TEAM_ID"] == team_id]
      advanced_stats = pd.concat([advanced_stats, team_stats],
                                 ignore_index=True)
      advanced_stats['GAME_ID'] = advanced_stats['GAME_ID'].apply(str)
      print('Advstats post concat', advanced_stats.tail(1))

    except Exception as e:
      print("Could not retrieve the advanced stats for game:", game_id)
      print("Error:", e)

    counter += 1
    print(f'Processed game {counter} with GAME_ID: {game_id}')
    time.sleep(delay)

  #print(advanced_stats.dtypes)
  #print("ADVSTat dtype", advanced_stats['GAME_ID'].dtype)
  print("CC dtype", current_chunk['GAME_ID'].dtype)
  #print(f'Advancedstats', advanced_stats.tail(3))
  # Merge the advanced_stats and current_chunk dataframes with suffixes
  #print('current-chunk', current_chunk.tail(3))
  combined_stats = pd.merge(advanced_stats, current_chunk, on=['GAME_ID', 'TEAM_ID'], suffixes=('', '_y'))

  # Remove the duplicated columns with the '_y' suffix
  combined_stats = combined_stats.reset_index(drop=True)
  combined_stats = combined_stats.filter(regex='^(?!.*_y).*$')

  # Append the combined_stats dataframe to the existing CSV file
  with open('nba_games_17to22_sadvanced.csv', 'a') as f:
    combined_stats.to_csv(f, header=f.tell() == 0, index=False)

  print(f"Processed game IDs {start_index + 1} to {end_index}")
