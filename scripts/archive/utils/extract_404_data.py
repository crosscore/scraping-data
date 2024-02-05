import pandas as pd

csv_file = '../../csv/add_category/device_with_category_v3.csv'
df = pd.read_csv(csv_file, encoding='utf-8', dtype={'user': str})

df = df[df['category'] == '404_not_found']
#df = df[df['category'] == 'processing_skipped']
print(df.info())

df.to_csv('../../csv/original/all_404_not_found_v3.csv', index=False, encoding='utf-8')
