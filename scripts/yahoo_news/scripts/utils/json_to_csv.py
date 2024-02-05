import pandas as pd

df = pd.read_json('your_file.json')

df_extracted = df['Item'].apply(pd.Series)[['url', 'title', 'body']].applymap(lambda x: x['S'])

df_extracted.to_csv('output.csv', index=False)