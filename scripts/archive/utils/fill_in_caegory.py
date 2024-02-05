import pandas as pd
import numpy as np

unique_urls_df = pd.read_csv('../../csv/add_category/complete/category_nan_row_unique_urls.csv', dtype={'user': str})
comp_v1_df = pd.read_csv('../../csv/add_category/complete/category_nan_row_comp_v1.csv', dtype={'user': str})
comp_v2_df = pd.read_csv('../../csv/add_category/complete/category_nan_row_comp_v2.csv', dtype={'user': str})

# Exclude categories from updating
comp_v1_df = comp_v1_df[~comp_v1_df['category'].isin(['403_forbidden', '404_not_found', np.nan])]
comp_v2_df = comp_v2_df[~comp_v2_df['category'].isin(['403_forbidden', '404_not_found', np.nan])]

# Update rows where the category column of unique_urls_df is NaN
for df in [comp_v1_df, comp_v2_df]:
    for index, row in df.iterrows():
        if pd.isna(unique_urls_df.at[index, 'category']):
            unique_urls_df.at[index, 'category'] = row['category']

print(unique_urls_df)

print(f"unique_urls_df['category'].value_counts(): {unique_urls_df['category'].value_counts(dropna=False)}")

unique_urls_df.to_csv('../../csv/add_category/complete/category_nan_row_unique_urls_filled.csv', index=False)
