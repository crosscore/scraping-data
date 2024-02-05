#remove_text_outliers.py
import pandas as pd
import os
import numpy as np
import datetime
import re

backup_directory = '../csv/yahoo_news/backup/'
concat_directory = '../csv/yahoo_news/concat/'
os.makedirs(backup_directory, exist_ok=True)

def delete_old_files(directory, days=1):
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(days=days)
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            file_modified = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_modified < cutoff:
                os.remove(filepath)
                print(f"Deleted: {filepath}")

def delete_except_latest_files(directory, number_of_files_to_keep):
    pattern = re.compile(r'(\d{8})_v(\d+)\.csv$')
    files_with_date_version = []
    for file in os.listdir(directory):
        match = pattern.search(file)
        if match:
            date = match.group(1)
            version = int(match.group(2))
            files_with_date_version.append((date, version, file))
    if not files_with_date_version:
        return
    # Sort files by date and version
    sorted_files = sorted(files_with_date_version, key=lambda x: (x[0], x[1]), reverse=True)
    # Keep the specified number of latest files and delete the rest
    latest_files = set(file[2] for file in sorted_files[:number_of_files_to_keep])
    for file in files_with_date_version:
        if file[2] not in latest_files:
            os.remove(os.path.join(directory, file[2]))
            print(f"Deleted: {os.path.join(directory, file[2])}")

def remove_outliers(df, column):
    cleaned_dfs = []
    for category in df['category'].unique():
        df_category = df[df['category'] == category]
        lengths = df_category[column].str.len()
        mean_len = lengths.mean()
        std_len = lengths.std()
        if category in ['科学', 'IT', '国際']:
            threshold_multiplier = 8
        else:
            threshold_multiplier = 5
        upper_threshold = mean_len + threshold_multiplier * std_len
        lower_threshold = mean_len - threshold_multiplier * std_len
        cleaned_df = df_category[(df_category[column].str.len() <= upper_threshold) & (df_category[column].str.len() >= lower_threshold)]
        cleaned_dfs.append(cleaned_df)
        print(f"Adjusted the threshold for removing outliers: {category}")
    cleaned_df_combined = pd.concat(cleaned_dfs).reset_index(drop=True)
    return cleaned_df_combined

def apply_remove_outliers(df):
    df['combined_text'] = df['title'] + ' ' + df['content']
    df = remove_outliers(df, 'combined_text')
    df.drop('combined_text', axis=1, inplace=True)
    return df

def find_latest_csv():
    files = os.listdir(concat_directory)
    pattern = re.compile(r'(\d{8})_v(\d+)\.csv$')
    latest_file = ''
    latest_date = '00000000'
    latest_version = 0
    for file in files:
        match = pattern.search(file)
        if match:
            date = match.group(1)
            version = int(match.group(2))
            if date > latest_date or (date == latest_date and version > latest_version):
                latest_date = date
                latest_version = version
                latest_file = file
    return os.path.join(concat_directory, latest_file) if latest_file else None

print("============ exec remove_text_outliers.py ============")
latest_csv = find_latest_csv()
print(f"latest_csv: {latest_csv}")
if latest_csv and os.path.exists(latest_csv):
    df = pd.read_csv(latest_csv)
    before_num = df['title'].nunique()
    # Delete outliers
    df = apply_remove_outliers(df)
    # Save processed data
    df.to_csv(latest_csv, index=False)
    backup_csv = os.path.basename(latest_csv)
    backup_path = os.path.join(backup_directory, backup_csv)
    try:
        df.to_csv(backup_path, index=False)
        print(f"Backup CSV saved: {backup_path}")
    except Exception as e:
        print(f"Error saving backup CSV: {e}")
    delete_except_latest_files(concat_directory, 1)
    delete_except_latest_files(backup_directory, 3)
else:
    print("The latest CSV file was not found.")
    before_num = 0

print(f"before: {before_num}")
print(f"after: {df['title'].nunique()}")
print('---------')
print(f"df['category'].value_counts(dropna=False):\n{df['category'].value_counts(dropna=False)}")
