import pandas as pd

device_original = pd.read_csv('../../../csv/original/device_original_formatted.csv', dtype={'user':str})
device_with_category = pd.read_csv('../../../csv/add_category/device_with_category_v3.csv', dtype={'user':str})

# Add 'category' column to device_original
device_original['category'] = ''

# Update 'category' value of rows with matching 'url' column value
for i, row in device_original.iterrows():
    url = row['url']
    category = device_with_category[device_with_category['url'] == url]['category']
    if not category.empty:
        device_original.at[i, 'category'] = category.iloc[0]

device_original.to_csv('../../../csv/complete/device_original_with_category.csv', index=False)
print(device_original)
