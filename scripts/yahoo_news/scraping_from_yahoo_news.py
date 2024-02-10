"""
ニュース記事のスクレイピングと保存を行うスクリプト
"""
import datetime
import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

def get_output_file(concat_dir, today_date):
    """
    出力ファイル名を取得する
    """
    if os.path.exists(concat_dir) and os.listdir(concat_dir):
        files = os.listdir(concat_dir)
        pattern = re.compile(rf'yahoo_news_concat_{today_date}_v(\d+)\.csv$')
        latest_version = 0
        latest_file = ''
        for file in files:
            match = pattern.search(file)
            if match:
                version = int(match.group(1))
                if version > latest_version:
                    latest_version = version
                    latest_file = file
        next_version = latest_version + 1
        output_file = f'../../data/csv/yahoo_news/daily/\
                        yahoo_news_articles_{today_date}_v{next_version}.csv'
        if latest_file:
            latest_df = pd.read_csv(os.path.join(concat_dir, latest_file))
            print(f'latest_df: {latest_df}')
            skip_urls = set(latest_df['url'].dropna().unique())
    else:
        print("First scraping today or directory does not exist or is empty.")
        output_file = f'../../data/csv/yahoo_news/daily/yahoo_news_articles_{today_date}_v1.csv'
        skip_urls = set()

    return output_file, skip_urls

def scrape_news(category, url, skip_urls, all_urls_set, max_retries, retry_interval):
    """
    ニュース記事をスクレイピングする
    """
    new_articles_count = 0
    for _ in range(max_retries):
        try:
            print(f'scrape_news... {category}: {url}')
            response = requests.get(url, timeout=(12, 18))
            print(f'response.status_code: {response.status_code}')
            soup = BeautifulSoup(response.content, 'xml')
            if "The specified URL did not exist." in str(soup):
                print(f"Error page detected, skipping remaining page for ({category}) url:{url}")
                return [], new_articles_count
            articles = []
            for item in soup.find_all("item"):
                title = item.find("title").get_text(strip=True)
                link_tag = item.find("link")
                link = link_tag.string.strip() if link_tag.string else ""
                if link in skip_urls or link in all_urls_set:
                    print(f"Skipping {link} as it's already scraped.")
                    continue
                all_urls_set.add(link)
                new_articles_count += 1
                print(f"{category}: {title}")
                print(f"{link}")
                articles.append((category, title, link))
            return articles, new_articles_count
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}, retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

    print("Failed scrape_news after max retries.")
    return [], new_articles_count

def clean_text(text):
    """
    テキストをクリーニングする
    """
    text = text.replace('\u2028', '').replace('\u2029', '')  # ユニコード行区切り文字を削除
    text = text.replace('"', '""')  # 引用符をエスケープ
    return text

def scrape_article_content(link, max_retries, skipped_articles):
    """
    記事のコンテンツをスクレイピングする
    """
    attempts = 0
    while attempts < max_retries:
        try:
            print(f"{link}")
            response = requests.get(link, timeout=(12, 18))
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraph = soup.find('p', class_=lambda x: x and 'highLightSearchTarget' in x)
            if paragraph:
                clean_paragraph = clean_text(paragraph.get_text(strip=True))
                return clean_paragraph, skipped_articles
            else:
                return "", skipped_articles
        except requests.RequestException as e:
            attempts += 1
            print(f"Error occurred: {e}, retrying {attempts}/{max_retries}")
            time.sleep(6)
    print(f"Failed to scrape_article_content after {max_retries} attempts, skipping URL: {link}")
    skipped_articles += 1
    return "", skipped_articles

def main():
    """
    メイン関数
    """
    today_date = datetime.datetime.now().strftime('%Y%m%d')
    concat_dir = '../../data/csv/yahoo_news/concat/'
    skip_urls = set()
    os.makedirs('../../data/csv/yahoo_news/daily/', exist_ok=True)
    os.makedirs('../../data/csv/yahoo_news/backup/', exist_ok=True)
    output_file, skip_urls = get_output_file(concat_dir, today_date)

    print(f'output_file: {output_file}')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    rss_categories = 'https://news.yahoo.co.jp/rss/categories/'
    rss_topics = 'https://news.yahoo.co.jp/rss/topics/'
    categories = {
        '国内': 'domestic',
        '国際': 'world',
        '経済': 'business',
        'エンタメ': 'entertainment',
        'スポーツ': 'sports',
        'IT': 'it',
        '科学': 'science',
        'ライフ': 'life',
        '地域': 'local'
    }
    category_urls = {cat_name: f'{rss_categories}{cat_value}.xml' \
                    for cat_name, cat_value in categories.items()}
    topic_categories = {k: v for k, v in categories.items() if k != 'ライフ'}
    topic_urls = {category: f'{rss_topics}{topic_categories[category]}.xml' \
                        for category in topic_categories}
    print(category_urls)
    print(topic_urls)

    all_urls_set = set()
    skipped_articles = 0
    new_articles_count = 0
    max_retries = 6
    retry_interval = 12  # seconds

    print('Start scraping.')
    start = time.time()
    all_articles = []

    for category, url in category_urls.items():
        print(f'{category}: {url}')
        articles, count = scrape_news(category, url, skip_urls, \
                            all_urls_set, max_retries, retry_interval)
        new_articles_count += count
        if articles:
            print(f'Found {len(articles)} articles.')
            all_articles.extend(articles)
        time.sleep(0.5)

    for category, url in topic_urls.items():
        print(f'{category}: {url}')
        articles, count = scrape_news(category, url, skip_urls, \
                            all_urls_set, max_retries, retry_interval)
        new_articles_count += count
        if articles:
            print(f'Found {len(articles)} articles.')
            all_articles.extend(articles)
        time.sleep(0.5)

    print(f'------ Complete. Found {len(all_articles)} articles. ------')

    all_articles_data = []
    for article in all_articles:
        category, title, link = article
        print(f'{category} {title} ...')
        content, skipped_articles = scrape_article_content(link, max_retries, skipped_articles)
        print(f'Found {len(content)} characters.')
        title = clean_text(title)
        all_articles_data.append([category, title, link, content])
        time.sleep(0.5)

    df = pd.DataFrame(all_articles_data, columns=['category', 'title', 'url', 'content'])
    print(f"df['url'].duplicated().sum(): {df['url'].duplicated().sum()}")
    df.to_csv(output_file, index=False, encoding='utf-8')
    print('End scraping.')
    print(f'Number of new articles scraped: {new_articles_count}')
    print(f'Number of skipped articles: {skipped_articles}')
    end = time.time()
    print(f'Time elapsed: {end - start} seconds.')
    print(f'Current time: {datetime.datetime.now()}')

if __name__ == "__main__":
    main()
