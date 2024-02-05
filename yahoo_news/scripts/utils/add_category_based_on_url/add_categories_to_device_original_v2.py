import pandas as pd

# Function to update the 'category' column in a target dataframe based on a source dataframe
def update_category(source_df, target_df):
    # Filter out rows where 'category' is '404_not_found' or NaN in the source dataframe
    filtered_source_df = source_df[source_df['category'].notna() & (source_df['category'] != '404_not_found')]

    # Create a dictionary from the source dataframe for updating the category
    category_dict = dict(filtered_source_df[['url', 'category']].values)

    # If 'category' column doesn't exist in the target dataframe, create it
    if 'category' not in target_df.columns:
        target_df['category'] = pd.NA

    # Update the 'category' column in the target dataframe
    target_df['category'] = target_df['url'].map(category_dict).fillna(target_df['category'])

    return target_df

# Load the data
all_device_df = pd.read_csv('../../../csv/original/all_device.csv', dtype={'user': str})
device_with_category_v3_df = pd.read_csv('../../../csv/add_category/device_with_category_v3.csv', dtype={'user': str})
with_category_406_lines_df = pd.read_csv('../../../csv/add_category/complete/final/with_category_406_lines.csv', dtype={'user': str})

# Update the 'category' column in all_device_df based on the other two dataframes
all_device_df_updated = update_category(device_with_category_v3_df, all_device_df)
print(all_device_df_updated['category'].value_counts(dropna=False))
all_device_df_final = update_category(with_category_406_lines_df, all_device_df_updated)
print(all_device_df_final['category'].value_counts(dropna=False))
print(all_device_df_final)
# Save the final dataframe
all_device_df_final.to_csv('../../../csv/original/all_device_add_category.csv', index=False)

all_device_df_final_dropna = all_device_df_final.dropna()
print(all_device_df_final_dropna['category'].value_counts(dropna=False))
print(all_device_df_final_dropna)
all_device_df_final_dropna.to_csv('../../../csv/original/all_device_add_category_dropna.csv', index=False)

# Extract only the rows where the 'category' column value is pd.NA
all_device_df_final_na = all_device_df_final[all_device_df_final['category'].isna()]
print(all_device_df_final_na)
all_device_df_final_na.to_csv('../../../csv/original/all_device_add_category_na.csv', index=False)
print(all_device_df_final_na['url'].nunique())
