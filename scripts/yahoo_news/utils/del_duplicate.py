import pandas as pd
import glob

files = glob.glob("../../csv/yahoo_news/concat/*.csv")
df = pd.read_csv(files[0])

print(df['category'].value_counts())

#　Outputs the number of rows where the 'url' column of df is duplicated
print(df['url'].duplicated().sum())

#　Remove duplicate 'url' in df
df = df.drop_duplicates(subset='url')
print(df['url'].duplicated().sum())

print(df['category'].value_counts())

df.to_csv('../../csv/yahoo_news/concat/yahoo_news_concat_20231207_v8.csv', index=False)