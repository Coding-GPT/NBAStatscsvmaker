import os

def update_git(filename):
    commit_message = f"Update {filename}"
    git_token = os.environ['GIT_TOKEN']
    git_user = "Coding-GPT"  # Replace with your GitHub username
    git_email = "jacksilverberg2020@gmail.com"  # Replace with your GitHub email

    os.system(f"git config --global user.name '{git_user}'")
    os.system(f"git config --global user.email '{git_email}'")
    os.system(f"git remote set-url origin https://{git_user}:{git_token}@github.com/{git_user}/NBAStatscsvmaker.git")
    os.system(f"git add {filename}")
    os.system(f"git commit -m '{commit_message}'")
    os.system("git push")
update_git('nba_games_17to22_sadvanced.csv')