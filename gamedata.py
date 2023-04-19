print("gamedata.py Running")
import pandas as pd
import time
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, boxscoreadvancedv2
from nba_api.stats.library.parameters import SeasonType

# Get list of NBA teams
nba_teams = teams.get_teams()

# Define the seasons to loop through
seasons = ['2017-18', '2018-19', '2019-20', '2020-21', '2021-22']

# Create an empty list to store the game data
game_data = []

# Loop through each season
for season in seasons:

    # Loop through each team and get game data for the current season
    for team in nba_teams:
        team_id = team['id']
        game_finder = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=team_id,
            season_nullable=season,
            season_type_nullable=SeasonType.regular)
        games = game_finder.get_data_frames()[0]
        game_data.append(games)
        print(f"{team['full_name']} - {season}")
        time.sleep(.6)

# Concatenate all the data frames in game_data into a single data frame
all_games = pd.concat(game_data, axis=0, ignore_index=True)

# Select columns to keep
cols_to_keep = [
  'SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'GAME_ID', 'GAME_DATE',
  'MATCHUP', 'WL', 'PTS'
]

# Filter data frame to keep only the selected columns
filtered_games = all_games.loc[:, cols_to_keep]


# Sort the data frame by GAME_DATE column in ascending order
sorted_games = filtered_games.sort_values(by='GAME_ID', ascending=True)
sorted_games['GAME_ID'] = sorted_games['GAME_ID'].astype(str)

# Write data frame to a CSV file
sorted_games.to_csv('nba_games_2017_to_2022.csv', index=False)

print(sorted_games.head(3))

# Assuming you have the sorted_games dataframe with GAME_ID and TEAM_ID columns created earlier in the script
# Import the CSV file as the sorted_games dataframe
loaded_games = pd.read_csv('nba_games_2020_filtered_sorted.csv',dtype={'GAME_ID': str})

# Get the top 20 rows of the sorted_games dataframe
top_20_games = loaded_games.head(5)
print(top_20_games.head())
# Initialize an empty dataframe to store advanced stats
advanced_stats = pd.DataFrame()

# Loop through each row in the sorted_games dataframe
for index, row in top_20_games.iterrows():
  game_id = row["GAME_ID"]
  team_id = row["TEAM_ID"]

  # Retrieve the advanced stats using the NBA API
  response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
  boxscore = response.get_data_frames()[1]
  print(boxscore, "Boxscore")
  # Filter the boxscore dataframe to keep only the rows that match the team_id
  team_stats = boxscore.loc[boxscore["TEAM_ID"] == team_id]

  # Append the team_stats dataframe to the advanced_stats dataframe
  advanced_stats = pd.concat([advanced_stats, team_stats], ignore_index=True)
time.sleep(.6)
combined_stats = pd.merge(top_20_games, advanced_stats, on=['GAME_ID', 'TEAM_ID'])

# Print a list of all columns in combined_stats
print("Columns in combined_stats:", combined_stats.columns.tolist())
# Select the desired columns
selected_columns = [
    'SEASON_ID', 'TEAM_ID','TEAM_ABBREVIATION_x', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'PTS',
    'MIN', 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING', 'DEF_RATING', 'E_NET_RATING', 'NET_RATING',
    'AST_PCT', 'AST_TOV', 'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'E_TM_TOV_PCT', 'TM_TOV_PCT',
    'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT', 'E_PACE', 'PACE', 'PACE_PER40', 'POSS', 'PIE'
]

final_stats = combined_stats.loc[:, selected_columns]

# Write final stats to a CSV file
#final_stats.to_csv('nba_games_20172_advanced.csv', index=False)