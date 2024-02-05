#add_category_by_scraping.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import threading
import os

category_dict = {
    'domestic': '国内',
    'world': '国際',
    'business': '経済',
    'entertainment': 'エンタメ',
    'sports': 'スポーツ',
    'it': 'IT',
    'science': '科学',
    'life': 'ライフ',
    'local': '地域',
}

def sanitize_filename(url):
    filename = re.sub(r'[\/:*?"<>|]', '_', url)
    return filename + '.html'

def file_exists(file_path):
    return os.path.exists(file_path)

def parse_html_for_category(html_content):
    print("--- parse_html_for_category ---")
    soup = BeautifulSoup(html_content, 'html.parser')
    scripts = soup.find_all('script')
    category_found = False
    for script in scripts:
        if 'currentCategory' in script.text:
            print('Discover currentCategory.')
            match = re.search(r'"currentCategory":"(\w+)"', script.text)
            if match:
                print(f'match.group(1): {match.group(1)}')
                category_key = match.group(1)
                if category_key in category_dict:
                    print(f'category_key: {category_key}')
                    headline_match = re.search(r'"headline":"([^"]+)"', script.text)
                    if headline_match:
                        title = headline_match.group(1)
                        print(f'headline_match.group(1): {title}')
                    else:
                        title = 'None'
                    category_found = True
                    return category_dict[category_key]
                else:
                    print(f"category_dictに存在しないkey({category_key})です。")
            continue
        elif 'id_type' in script.text:
            category_info = re.search(r'"id_type":"(\w+)"', script.text)
            if category_info:
                category_key = category_info.group(1)
                if category_key in category_dict:
                    category_found = True
                    return category_dict[category_key]
            continue
    if not category_found:
        metatags = soup.find_all('meta')
        for meta in metatags:
            if 'name' in meta.attrs and meta.attrs['name'] == 'description':
                content = meta.attrs.get('content', '').lower() # convert to lower case
                if 'スポーツ' in content:
                    print("Discover categories among meta tags: return 'スポーツ'")
                    return 'スポーツ'
        print("######### Category not found in HTML content. #########")
        return "category_not_found"

def get_category_from_archive(url, max_retries=3, wait_seconds=12, max_wait_seconds=60):
    time.sleep(9)
    print(f"--------------- Analyzing {url} ---------------")
    now = time.time()
    print(f"Current time: {now}")
    retries = 0
    html_dir = '../html/archive_files/'
    file_name = sanitize_filename(url)
    file_path = f'{html_dir}{file_name}'
    print(file_path)
    html_content = ''
    if file_exists(file_path):
        print(f'File already exists: {file_path}')
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
    else:
        while retries < max_retries:
            try:
                archive_url = f'https://web.archive.org/web/{url}'
                print(f"Trying requests.get({archive_url})")
                response = requests.get(archive_url, timeout=18)
                response.raise_for_status()
                print("Request successful. Status code: 200")
                # Save HTML only for 200 status code
                os.makedirs(html_dir, exist_ok=True)
                with open(file_path, 'w', encoding='utf-8') as f:
                    print(f'open: {file_path}')
                    f.write(response.text)
                print(f'Saved HTML file: {file_name}')
                html_content = response.text
                break
            except requests.exceptions.Timeout as e:
                print(f"Timeout with URL {url}: {e}")
                retries += 1
                print(f"Retrying... ({retries}/{max_retries})")
                time.sleep(wait_seconds)
            except requests.exceptions.ConnectionError as e:
                print(f"ConnectionError with URL {url}: {e}")
                retries += 1
                print(f"Retrying... ({retries}/{max_retries})")
                time.sleep(wait_seconds)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"404 Not Found error for URL {url}: {e}")
                    return "404_not_found"
                elif e.response.status_code == 403:
                    print(f"403 Forbidden error for URL {url}: {e}")
                    return "403_forbidden"
                else:
                    print(f"HTTP error with URL {url}: {e}")
                    retries += 1
                    print(f"Retrying... ({retries}/{max_retries})")
                    time.sleep(wait_seconds)
                    continue
            except requests.exceptions.TooManyRedirects as e:
                print(f"Too many redirects for URL {url}: {e}")
                return "too_many_redirects"
            except requests.exceptions.RequestException as e:
                print(f"Error with URL {url}: {e}")
                retries += 1
                print(f"Retrying... ({retries}/{max_retries})")
                time.sleep(wait_seconds)
                wait_seconds = min(wait_seconds * 2, max_wait_seconds)
    # Parse HTML content to find categories
    if html_content:
        return parse_html_for_category(html_content)
    print("Retry_limit_exceeded.")
    return "retry_limit_exceeded"

def listen_for_exit_command():
    global exit_command_issued
    global exit_thread
    while not exit_thread:
        input("Press Enter to stop the process... ")
        exit_command_issued = True
        break


start = time.time()
exit_command_issued = False
exit_thread = False

output_dir = "../../csv/add_category/complete/"
os.makedirs(output_dir, exist_ok=True)
output_file_complete = "../../csv/add_category/complete/category_nan_row_comp_v1.csv"
output_file_partial = "../../csv/add_category/partial/category_nan_row_partial.csv"

# Branch processing depending on the presence or absence of output_file_partial
if os.path.exists(output_file_partial):
    print(f"Loading partial data from {output_file_partial}")
    df = pd.read_csv(output_file_partial, dtype={'user': str})
else:
    print("Loading original data")
    df = pd.read_csv('../../csv/original/category_nan_row_unique_urls.csv', dtype={'user': str})
    # Create empty column if 'category' column does not exist
    if 'category' not in df.columns:
        df['category'] = None

exit_command_issued = False
exit_listener = threading.Thread(target=listen_for_exit_command)
exit_listener.start()

retry_limit_exceeded = False
exit_command_detected = False

error_urls = []
categories = []
for index, row in df.iterrows():
    if exit_command_issued:
        print("Exit command issued. Saving partial data...")
        # Check length of current categories until all rows are assigned categories
        while len(categories) < len(df):
            categories.append(None)
        df['category'] = categories
        df.to_csv(output_file_partial, index=False)
        print(f"{output_file_partial} was saved: ")
        exit_command_detected = True
        break
    # Skip rows that already have a category assigned
    if pd.notna(row['category']):
        categories.append(row['category'])
        continue
    # Get the category by calling get_category_from_archive function
    category = get_category_from_archive(row['url'])
    if category in ["retry_limit_exceeded", "request_exception", "category_not_found"]:
        print("Error occurred. URL will be reprocessed later.")
        categories.append(None)
        error_urls.append((row['url'], row['title']))
        continue
    categories.append(category)
df['category'] = categories

# If processing is completed, save complete data and error URL data
if not exit_command_detected:
    if retry_limit_exceeded:
        df.to_csv(output_file_partial, index=False)
    else:
        print("All data processed successfully. Saving data...")
        df.to_csv(output_file_complete, index=False)
if error_urls:
    error_df = pd.DataFrame(error_urls, columns=['url', 'title'])
    error_output_file = "../../csv/add_category/error/error_urls.csv"
    error_df.to_csv(error_output_file, index=False)
    print(f"Error URLs saved to {error_output_file}")

# Tell the user that the process is complete
print("Processing completed.")
input("Press Enter to wait for thread to terminate...")
exit_thread = True
exit_listener.join() 

end = time.time()
print(f"Elapsed time: {end - start} seconds.")
