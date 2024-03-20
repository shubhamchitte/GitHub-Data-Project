import requests
import json
import gzip
import io
import csv
from datetime import datetime, timedelta
from IPython.display import clear_output
import time
from http.client import IncompleteRead
import json.decoder


# Define the start and end dates for the iteration
start_date = datetime(2023, 4, 2)
end_date = datetime(2024, 2, 29)


# Define the folder path to save CSV files
folder_path = r'folder_path'

# Define the fieldnames for each CSV file
commit_fieldnames = ['actor_login', 'repo_name', 'actor_display', 'commit_time', 'event', 'commit', 'payload_action',
           'commit_author_name', 'commit_author_email', 'commit_message']

issue_fieldnames = ['actor_login', 'repo_name', 'actor_display', 'commit_time', 'event', 'issue_number', 'issue_title',
          'issue_state', 'issue_created_at', 'issue_updated_at', 'issue_closed_at', 'issue_user_login']

pull_request_fieldnames = ['actor_login', 'repo_name', 'actor_display', 'commit_time', 'event', 'pull_request_number',
              'pull_request_title', 'pull_request_state', 'pull_request_created_at', 'pull_request_updated_at',
              'pull_request_closed_at', 'pull_request_merged_at', 'pull_request_user_login']


# Function to make requests with retries and exponential backoff

def make_request_with_retry(url, max_retries=3, base_delay=2, backoff_factor=2):
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 404:  # Check for 404 error
                print(f"404 Not Found error occurred for {url}. Skipping to the next hour.")
                return None
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            return response
        except (requests.exceptions.RequestException, requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"Error occurred on attempt {attempt}: {e}")
            if attempt < max_retries:
                delay = base_delay * backoff_factor ** attempt
                print(f"Retrying after {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Skipping to the next hour.")
                return None  # Return None if maximum retries reached



def write_to_csv(data, filename, fieldnames):
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', escapechar='\\')
        if csvfile.tell() == 0:
            writer.writeheader()
        writer.writerow(data)

    
    
# Define a list of predefined account names

#The data on the archives is quite big so I filtered data based on the organizations names I was interested in (like microsft, bticoin, etc..). 
#If you just want all data between the mentioned period you can ignore this part

predefined_accounts = []

with open(r"path_to_csv_containing_orgs", 'r', encoding='utf-8-sig') as csv_file:
    reader = csv.reader(csv_file)
    for row in reader:
        if row: # Check if the row is not empty
            account = row[0].lstrip('ï»¿') # Remove BOM character if present
            predefined_accounts.append(account)
#print(predefined_accounts) 

        
# Loop through each date
current_date = start_date
while current_date <= end_date:
    print(current_date)
    for hour in range(24):
        print(hour)
        url = f'http://data.gharchive.org/{current_date.strftime("%Y-%m-%d")}-{hour}.json.gz'
        response = make_request_with_retry(url)

        if response is not None and response.status_code == 200:
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                        except json.decoder.JSONDecodeError:
                            try:
                                # Try decoding the line using Latin-1 encoding
                                decoded_line = line.decode('latin-1')
                                data = json.loads(decoded_line)
                            except json.decoder.JSONDecodeError:
                                # Handle the JSON decoding error gracefully
                                print("Error decoding JSON:", line)
                                continue

                        # Check if the repo belongs to one of the predefined accounts (This is my filter since I am only interested in data from specific orgs)
                        repo_name = data.get('repo', {}).get('name', '')
                        account_name = repo_name.split('/')[0]
                        if account_name in predefined_accounts:
                            if 'commits' in data.get('payload', {}):
                                for commit in data['payload']['commits']:
                                    commit_data = {
                                        'actor_login': data.get('actor', {}).get('login', ''),
                                        'repo_name': data.get('repo', {}).get('name', ''),
                                        'commit_time': data.get('created_at', ''),
                                        'actor_display': data.get('actor', {}).get('display_login', ''),
                                        'event': data.get('type', ''),
                                        'commit': commit.get('sha', ''),
                                        'payload_action': data.get('payload', {}).get('action', ''),
                                        'commit_author_name': commit.get('author', {}).get('name', ''),
                                        'commit_author_email': commit.get('author', {}).get('email', ''),
                                        'commit_message': commit.get('message', '')
                                    }
                                    write_to_csv(commit_data, folder_path + 'commits.csv', commit_fieldnames)

                            if 'pull_request' in data.get('payload', {}):
                                pull_request = data['payload']['pull_request']
                                pull_request_data = {
                                    'actor_login': data.get('actor', {}).get('login', ''),
                                    'repo_name': data.get('repo', {}).get('name', ''),
                                    'actor_display': data.get('actor', {}).get('display_login', ''),
                                    'commit_time': data.get('created_at', ''),
                                    'event': data.get('type', ''),
                                    'pull_request_number': pull_request.get('number', ''),
                                    'pull_request_title': pull_request.get('title', ''),
                                    'pull_request_state': pull_request.get('state', ''),
                                    'pull_request_created_at': pull_request.get('created_at', ''),
                                    'pull_request_updated_at': pull_request.get('updated_at', ''),
                                    'pull_request_closed_at': pull_request.get('closed_at', ''),
                                    'pull_request_merged_at': pull_request.get('merged_at', ''),
                                    'pull_request_user_login': pull_request.get('user', {}).get('login', '') if pull_request.get('user') else None,
                                }
                                write_to_csv(pull_request_data, folder_path + 'pull_requests.csv', pull_request_fieldnames)

                            if 'issue' in data.get('payload', {}):
                                issue = data['payload']['issue']
                                issue_data = {
                                    'actor_login': data.get('actor', {}).get('login', ''),
                                    'repo_name': data.get('repo', {}).get('name', ''),
                                    'actor_display': data.get('actor', {}).get('display_login', ''),
                                    'commit_time': data.get('created_at', ''),
                                    'event': data.get('type', ''),
                                    'issue_number': issue.get('number', ''),
                                    'issue_title': issue.get('title', ''),
                                    'issue_state': issue.get('state', ''),
                                    'issue_created_at': issue.get('created_at', ''),
                                    'issue_updated_at': issue.get('updated_at', ''),
                                    'issue_closed_at': issue.get('closed_at', ''),
                                    'issue_user_login': issue.get('user', {}).get('login', '') if issue.get('user') else None,
                                }
                                write_to_csv(issue_data, folder_path + 'issues.csv', issue_fieldnames)
            except EOFError as e:
                print(f"EOFError occurred for {url}. Skipping to the next hour.")
                continue
            except requests.exceptions.ChunkedEncodingError as e:
                print(f"ChunkedEncodingError occurred for {url}. Skipping to the next hour.")
                continue
        else:
            print(f"Failed to retrieve data for {current_date.strftime('%Y-%m-%d')} hour {hour}. Skipping to the next hour.")

    print(current_date)
    clear_output()
    current_date += timedelta(days=1)
