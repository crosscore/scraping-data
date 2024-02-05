#concat_only_unique_urls.py
import pandas as pd
import os
import datetime
import glob
import re

category_list = ['国内', '国際', '経済', 'エンタメ', 'スポーツ', 'IT', '科学', 'ライフ', '地域']

new_files = glob.glob('../csv/yahoo_news/daily/*.csv')
if new_files:
    new_file = new_files[0]
    # Extract date and version part from file name
    file_part = re.search(r'(\d{8}_v\d+)\.csv$', new_file)
    if file_part:
        file_part = file_part.group(1)
        output_file = f'../csv/yahoo_news/concat/yahoo_news_concat_{file_part}.csv'
    else:
        print("Unable to extract date and version from filename of new_file.")
else:
    print("new_file not found.")
original_files = glob.glob('../csv/yahoo_news/concat/*.csv')
original_file = original_files[0]

print("============ exec concat_only_unique_urls.py ============")
print(f'input_old: {original_file}')
print(f'input_new: {new_file}')
print(f'output_file: {output_file}')

if original_file and os.path.exists(original_file):
    df_original = pd.read_csv(original_file)
else:
    df_original = pd.DataFrame()
print(f"df_original['category'].value_counts(dropna=False):\n{df_original['category'].value_counts(dropna=False)}")

before_num = df_original['url'].nunique()
df_new = pd.read_csv(new_file)
df_concat = pd.concat([df_original, df_new])

df_concat.drop_duplicates(subset=['title'], inplace=True)
df_concat.drop_duplicates(subset=['content'], inplace=True)
df_concat.drop_duplicates(subset=['url'], inplace=True)
df_concat.dropna()

# Leave only str lines (deals with float-only lines)
df_concat = df_concat[(df_concat['title'].apply(lambda x: isinstance(x, str))) & (df_concat['content'].apply(lambda x: isinstance(x, str)))]

# Processing categories
df_concat['category'] = pd.Categorical(df_concat['category'], categories=category_list, ordered=True)
df_concat.sort_values(by='category', inplace=True)
df_concat.to_csv(output_file, index=False)

# Check the data
#df = df_concat.copy()[~df_concat['url'].str.contains('/pickup/')]
df = df_concat.copy()
print('---------')
print(f'df.isnull().sum():\n{df.isnull().sum()}')
print('---------')
print(f'df.describe():\n{df.describe()}')
print('---------')
print(f"df['category'].unique():\n{df['category'].unique()}")
print('---------')
print(f"df['category'].value_counts(dropna=False):\n{df['category'].value_counts(dropna=False)}")
print('---------')
for column in df.columns:
    print(f"df['{column}'].duplicated().sum(): {df[column].duplicated().sum()}")
print('---------')
#Delete all .csv files in '../csv/yahoo_news/daily/' folder
for file in os.listdir('../csv/yahoo_news/daily/'):
    os.remove(os.path.join('../csv/yahoo_news/daily/', file))
    print(f"remove: {file}")
print('---------')
print(f"befor : {before_num}")
print(f"after : {df_concat['url'].nunique()}")
