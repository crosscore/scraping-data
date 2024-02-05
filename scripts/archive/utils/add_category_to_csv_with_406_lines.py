import pandas as pd

nan_row_df = pd.read_csv('../../csv/add_category/complete/category_nan_row_unique_urls_filled.csv', dtype={'user': str})
nan_row_filled_df = pd.read_csv('../../csv/original/category_nan_row.csv')

# Join dataframes based on 'url' column
combined_df = nan_row_df.merge(nan_row_filled_df[['url', 'category']], on='url', how='left', suffixes=('', '_filled'))

# Update the 'category' column with the value of the 'category_filled' column
combined_df['category'] = combined_df['category_filled'].combine_first(combined_df['category'])
print(combined_df)

combined_df.to_csv('../../csv/add_category/complete/final/with_category_406_lines.csv', index=False)
print(f'combined_df["category"].value_counts(dropna=False):\n{combined_df["category"].value_counts(dropna=False)}')