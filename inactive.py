

# Get the top 20 rows of the sorted_games dataframe
top_20_games = sorted_games.head(20)
# Initialize an empty dataframe to store advanced stats
advanced_stats = pd.DataFrame()

# Loop through each row in the sorted_games dataframe
for index, row in top_20_games.iterrows():
  game_id = row["GAME_ID"]
  team_id = row["TEAM_ID"]

  # Retrieve the advanced stats using the NBA API
  response = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
  boxscore = response.get_data_frames()[1]

  # Filter the boxscore dataframe to keep only the rows that match the team_id
  team_stats = boxscore.loc[boxscore["TEAM_ID"] == team_id]

  # Append the team_stats dataframe to the advanced_stats dataframe
  advanced_stats = pd.concat([advanced_stats, team_stats], ignore_index=True)

# Rename the combined dataframe to "advanced_stats"
advanced_stats.rename(columns={
  "GAME_ID": "GAME_ID",
  "TEAM_ID": "TEAM_ID"
},
                      inplace=True)

print(advanced_stats.head())
# Write advanced stats to a CSV file
advanced_stats.to_csv('nba_games_2020_advanced.csv', index=False)