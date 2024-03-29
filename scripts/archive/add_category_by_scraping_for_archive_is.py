""".DS_Store"""
import re
import threading
import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException,TimeoutException

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
    """
    URLからファイル名を生成し、不適切な文字をアンダースコアに置換します。

    Parameters:
    url (str): ファイル名を生成するためのURL。

    Returns:
    str: サニタイズされたファイル名。
    """
    filename = re.sub(r'[\/:*?"<>|]', '_', url)
    return filename + '.html'

def file_exists(file_path):
    """
    指定されたファイルが存在するかどうかを確認します。
    """
    return os.path.exists(file_path)

def parse_html_for_category(html_content, archive_url):
    """.DS_Store"""
    print("--- parse_html_for_category ---")
    soup = BeautifulSoup(html_content, 'html.parser')

    url_patterns = [
        re.compile(re.escape(archive_url) + r'/https://news\.yahoo\.co\.jp/categories/(\w+)'),
        re.compile(re.escape(archive_url.replace('https://archive.is/', 'https://archive.is/o/')) + \
        r'/https://news\.yahoo\.co\.jp/ranking/access/news/(\w+)')
    ]
    for pattern in url_patterns:
        for link in soup.find_all('a', href=pattern):
            match = pattern.search(link['href'])
            if match:
                print(f"Matched pattern: {pattern}.")
                category_key = match.group(1)
                if category_key in category_dict:
                    print(f"Category found in link: {category_dict[category_key]}")
                    return category_dict[category_key]

    pattern = re.compile(r'アクセスランキング\（([^）]+)）')
    match = pattern.search(html_content)
    if match:
        print("Matched 'アクセスランキング' pattern.")
        print(f"Access ranking category found: {match.group(1)}")
        return match.group(1)

    print("Category not found in HTML content.")
    return "category_not_found"



def get_url_from_archiveis_selenium(original_url):
    """
    Seleniumを使用してarchive.isからURLを取得します。
    """
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    # Automatically download Chrome WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(original_url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        pattern = re.compile(r'https://archive\.is/\w{5}$')
        links = soup.find_all('a', href=pattern)
        for link in links:
            if link['href'] != original_url:
                print(f"link found: {link['href']}")
                return link['href']
        # Check if the string '結果はありません' exists in pattern
        if soup.find(string='結果はありません'):
            print("find '結果はありません' in pattern.")
            return None
        print("###### Failed to detect 'url' in 'get_url_from_archiveis_selenium()' ######")
        return None
    except WebDriverException as e:
        print(f"WebDriver error with URL {original_url}: {e}")
        driver.quit()
        return None
    except TimeoutException as e:
        print(f"Timeout error with URL {original_url}: {e}")
        driver.quit()
        return None

session = requests.Session()
session.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/58.0.3029.110 Safari/537.3'
    )
})

def get_url_from_saved_html(file_path):
    """
    保存されたHTMLファイルからarchive.isのURLを取得します。
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    soup = BeautifulSoup(html_content, 'html.parser')
    pattern = re.compile(r'https://archive\.is/\w{5}$')
    links = soup.find_all('a', href=pattern)
    for link in links:
        if link['href'].startswith('https://archive.is/'):
            return link['href']
    print("No valid archive URL found in saved file.")
    return None

def get_category_from_archive(url, max_retries=3, wait_seconds=12, max_wait_seconds=60):
    """
    archive.isからURLを取得し、HTMLを解析してカテゴリを取得します。
    """
    time.sleep(3)
    print(f"\n--------------- Analyzing {url} ---------------")
    now = time.time()
    print(f"Current time: {now}")
    retries = 0
    html_dir = '../../html/archive_files/'
    file_name = sanitize_filename(url)
    file_path = f'{html_dir}{file_name}'
    html_content = ''
    if file_exists(file_path):
        print(f'File already exists: {file_path}')
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        # Get archive_url from saved HTML file
        archive_url = get_url_from_saved_html(file_path)
        print(f"archive_url: {archive_url}")
        if not archive_url:
            print("No archive_url found in saved file. Skipping...")
            return "404_not_found"
    else:
        print(f'File does not exist: {file_path}')
        # Use Selenium to get the page in archive.is and save the HTML)
        while retries < max_retries:
            try:
                original_url = 'https://archive.is/' + url
                print(f'original_url: {original_url}')
                archive_url = get_url_from_archiveis_selenium(original_url)
                print(f"archive_url: {archive_url}")
                if not archive_url:
                    print("No archive_url found. Skipping...")
                    return "404_not_found"
                # Get page HTML using Selenium
                options = Options()
                options.headless = True
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                driver.get(archive_url)
                html_content = driver.page_source
                driver.quit()
                # Save HTML only for 200 status code
                os.makedirs(html_dir, exist_ok=True)
                with open(file_path, 'w', encoding='utf-8') as f:
                    print(f'open: {file_path}')
                    f.write(html_content)
                print(f'Saved HTML file: {file_name}')
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
        return parse_html_for_category(html_content, archive_url)
    print("Retry_limit_exceeded.")
    return "retry_limit_exceeded"

def listen_for_exit_command():
    """
    Waits for the user to press Enter, then sets the global variable EXIT_COMMAND_ISSUED to True.
    """
    print("Exit listener thread started.")
    while not EXIT_THREAD:
        print("Waiting for the exit command...")
        input("Press Enter to stop the process... ")
        break


start = time.time()
OUTPUT_DIR = "../../csv/add_category"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE_COMPLETE = "../../csv/add_category/complete/category_nan_row_comp_v2.csv"
OUTPUT_FILE_PARTIAL = "../../csv/add_category/partial/category_nan_row_partial_v2.csv"

# Branch processing depending on the presence or absence of OUTPUT_FILE_PARTIAL
if os.path.exists(OUTPUT_FILE_PARTIAL):
    print(f"Loading partial data from {OUTPUT_FILE_PARTIAL}")
    df = pd.read_csv(OUTPUT_FILE_PARTIAL, dtype={'user': str})
else:
    print("Loading original data")
    df = pd.read_csv('../../csv/original/category_nan_row_v2.csv', dtype={'user': str})
    df['category'] = None

EXIT_THREAD = False
EXIT_COMMAND_ISSUED = False
RETRY_LIMIT_EXCEEDED = False
EXIT_COMMAND_DETECTED = False
exit_listener = threading.Thread(target=listen_for_exit_command)
exit_listener.start()

error_urls = []
categories = []
for index, row in df.iterrows():
    if EXIT_COMMAND_ISSUED:
        print("Exit command issued. Saving partial data...")
        # Check length of current categories until all rows are assigned categories
        while len(categories) < len(df):
            categories.append(None)
        df['category'] = categories
        df.to_csv(OUTPUT_FILE_PARTIAL, index=False)
        print(f"{OUTPUT_FILE_PARTIAL} was saved: ")
        EXIT_COMMAND_DETECTED = True
        break
    # Skip rows that already have a category assigned
    if pd.notna(row['category']):
        categories.append(row['category'])
        continue
    # Get category by get_category_from_archive function
    category = get_category_from_archive(row['url'])
    if category in ["retry_limit_exceeded", "request_exception", "category_not_found"]:
        print("Error occurred. URL will be reprocessed later.")
        categories.append(None)
        error_urls.append((row['url'], row['title']))
        continue
    categories.append(category)
df['category'] = categories

# If processing is completed, save complete data and error URL data
if not EXIT_COMMAND_DETECTED:
    if RETRY_LIMIT_EXCEEDED:
        df.to_csv(OUTPUT_FILE_PARTIAL, index=False)
    else:
        df.to_csv(OUTPUT_FILE_COMPLETE, index=False)
        print("All data processed successfully. Saving data...")
    if error_urls:
        error_df = pd.DataFrame(error_urls, columns=['url', 'title'])
        ERROR_OUTPUT_FILE = "../../csv/add_category/error/error_urls_v2.csv"
        error_df.to_csv(ERROR_OUTPUT_FILE, index=False)
        print(f"Error URLs saved to {ERROR_OUTPUT_FILE}")
    EXIT_THREAD = True

# Tell the user that the process is complete
print("Processing completed.")
input("Press Enter to wait for thread to terminate...")
exit_listener.join()
end = time.time()
print(f"Elapsed time: {end - start} seconds.")
