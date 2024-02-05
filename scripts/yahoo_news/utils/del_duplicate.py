import pandas as pd
import glob

files = glob.glob("../../../data/csv/yahoo_news/concat/*.csv")
# output_pathはファイル名の前にdel_を付ける
output_path = files[0].replace('yahoo_news_concat', 'del_yahoo_news_concat')
print(f'output_path: {output_path}')

df = pd.read_csv(files[0], encoding='utf-8')

print(df['category'].value_counts())

#　Outputs the number of rows where the 'url' column of df is duplicated
print(df['url'].duplicated().sum())

#　Remove duplicate 'url' in df
df = df.drop_duplicates(subset=['url', 'content'])
print(df['url'].duplicated().sum())

print(df['category'].value_counts())

df.to_csv(output_path, index=False)