import pandas as pd
import numpy as np
import glob

csv_files = glob.glob('../../csv/add_category/complete/*.csv')
#csv_files = glob.glob('../csv/original/all_404_not_found_v3.csv')
print(f'csv_files: {csv_files[0]}')
df = pd.read_csv(csv_files[0], encoding='utf-8', dtype={'user': str})
print('---------')
print(f'df.isnull().sum():\n{df.isnull().sum()}')
print('---------')
print(f'df.info():\n{df.info()}')
#Output the number of rows where the value of the url column is unique
print(f'df["url"].nunique():\n{df["url"].nunique()}')

#Extract only rows with unique values in the url column of df
df_unique = df.drop_duplicates(subset=['url'])
print(df_unique)

# Replace '404_not_found' and '403_forbidden' with NaN in 'category' column
df_unique['category'] = df_unique['category'].replace(['404_not_found', '403_forbidden'], np.nan)

print(f'df["category"].nunique():\n{df_unique["category"].value_counts(dropna=False)}')

#df_uniqueのcategory列の値がNaNの行のみをcsvとして出力
df_nan = df_unique[df_unique['category'].isnull()]
df_nan.to_csv('../../csv/original/category_nan_row_v2.csv', index=False)
