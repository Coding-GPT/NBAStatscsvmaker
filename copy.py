import requests
import os

# Define the file you want to copy
file_to_copy = "nba_games_17to22_sadvanced.csv"

# Define the URL of the second Replit's API
replit_api_url = "https://your-second-replit-name.repl.co"

# Authenticate with the Replit API using the API key
auth_header = {"Authorization": f"Bearer {os.environ['REPLIT_API_KEY']}"}

# Read the file contents
with open(file_to_copy, "r") as f:
    file_contents = f.read()

# Send a POST request to the second Replit's API to write the file
response = requests.post(
    f"{replit_api_url}/v0/repl/data/path/to/your/file.csv",
    headers=auth_header,
    data=file_contents
)

# Print the response status code to confirm that the file was copied successfully
print(response.status_code)
