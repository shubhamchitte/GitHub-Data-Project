# pip install pandas PyGithub github

import pandas as pd
from github import Github
from github.GithubException import RateLimitExceededException
import csv


def extract_repo_info(owner_name, repo):
    repo_name = repo.name
    try:
        languages = repo.get_languages()
    except GithubException as e:
        print(f"Error fetching languages for {owner_name}/{repo.name}: {e}")
        languages = "N/A"
    bytes = repo.size
    return [owner_name, repo_name, languages, bytes]

def get_owner_repos_info(owner_name, access_tokens, csv_file):
    access_token_count = len(access_tokens)
    headers = ['Owner', 'Repository', 'Languages', 'Size (bytes)']
    # We will simultaneously write the info in the csv as we process 
    with open(csv_file, 'a+', newline='', encoding='utf-8') as f:
        f.seek(0)
        if not f.read(1):
            writer = csv.writer(f)
            writer.writerow(headers) 

    current_token_index = 0
    for _ in range(access_token_count):
        access_token = access_tokens[current_token_index]

        try:
            g = Github(access_token)
            owner = g.get_user(owner_name)

            # Check if the provided name corresponds to an organization
            if owner.type == "Organization":
                owner = g.get_organization(owner_name)

            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for repo in owner.get_repos():
                    repo_info = extract_repo_info(owner_name, repo)
                    writer.writerow(repo_info)
                    print(f"Retrieved info for {owner_name}/{repo.name} using token {current_token_index + 1}")

        except RateLimitExceededException:
            print(f"Rate limit exceeded for access token {current_token_index + 1}. Switching to next token...")
            pass  # Continue to the next token
        finally:
            current_token_index = (current_token_index + 1) % access_token_count

if __name__ == "__main__":
    orgs_file = r"F:\Shubham\OneDrive - Indian School of Business\p_f_ghub_list.csv"  # Path to CSV file containing organization names and user accounts
    orgs_list = pd.read_csv(orgs_file, header=None)[0].tolist()  # Convert CSV to list

    access_tokens = ["token_1", "token_2", "token_3"] # Enter your GitHub access tokens (classic) 
    csv_file = r'path_to_csv'  # CSV file to store repository information

    for owner_name in orgs_list:
        get_owner_repos_info(owner_name, access_tokens, csv_file)
