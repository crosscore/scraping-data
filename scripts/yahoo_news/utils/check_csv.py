import pandas as pd
import glob
import re

file_list = glob.glob('../../csv/yahoo_news/concat/*_v*.csv')
# Select latest files based on version number
latest_file = max(file_list, key=lambda x: int(re.search(r'_v(\d+).csv', x).group(1)))
print(latest_file)
df = pd.read_csv(latest_file, encoding='utf-8')

#Delete the row that contains the string '/pickup/' in the 'url' column of df
#df = df.copy()[~df['url'].str.contains('/pickup/')]

print('---------')
print(f'df.isnull().sum():\n{df.isnull().sum()}')
print('---------')
print(f'df.describe():\n{df.describe()}')
print('---------')
print(f"df['category'].unique():\n{df['category'].unique()}")
print('---------')
print(f"df['category'].value_counts(dropna=False):\n{df['category'].value_counts(dropna=False)}")
print('---------')
print(f"df[df['title'].duplicated()]['title']:\n{df[df['title'].duplicated()]['title']}")
print('---------')
for column in df.columns:
    print(f"{column}の重複数: {df[column].duplicated().sum()}")