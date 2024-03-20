import pandas as pd
from github import Github
from github.GithubException import RateLimitExceededException, GithubException
import csv

def extract_repo_info(owner_name, repo):
    repo_name = repo.name
    stars_count = repo.stargazers_count
    watchers_count = repo.watchers_count
    forks_count = repo.forks_count
    created_at = repo.created_at
    org_created_at = repo.organization.created_at if repo.organization else None
    topics = repo.get_topics()
    topics_count = len(topics)
    
    return [owner_name, repo_name, stars_count, watchers_count, forks_count, created_at, org_created_at, topics, topics_count]

def get_owner_repos_info(owner_name, access_tokens, csv_file):
    access_token_count = len(access_tokens)
    headers = ['Owner', 'Repository', 'Stars', 'Watchers', 'Forks', 'Repo Created At', 'Org Created At', 'Topics', 'Topics Count']

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
                    try:
                        repo_info = extract_repo_info(owner_name, repo)
                        writer.writerow(repo_info)
                        print(f"Retrieved info for {owner_name}/{repo.name} using token {current_token_index + 1}")
                    except GithubException as e:
                        print(f"Error retrieving info for {owner_name}/{repo.name}: {e}")
                        continue

        except RateLimitExceededException:
            print(f"Rate limit exceeded for access token {current_token_index + 1}. Switching to next token...")
            pass  # Continue to the next token
        except GithubException as e:
            print(f"Github API error: {e}")
            continue
        finally:
            current_token_index = (current_token_index + 1) % access_token_count

# Example usage
orgs_file = r"path_to_csv"  # Path to CSV file containing organization names and user accounts
orgs_list = pd.read_csv(orgs_file, header=None)[0].tolist()  # Convert CSV to list. Assuming csv has no headers

access_tokens = ["token_1", "token_2", "token_3"] # Enter you tokens (classic)

csv_file = r'path_to_csv'  # CSV file to store repository information

for owner_name in orgs_list:
    get_owner_repos_info(owner_name, access_tokens, csv_file)
