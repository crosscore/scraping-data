import pandas as pd
import numpy as np

file_path = '../../csv/complete/device_original_with_category_plus_predict.csv'
df = pd.read_csv(file_path, dtype={'user': str})

# Delete the row where the value of 'category' in df is '404_not_found'
df_test = df[df['category'] != '404_not_found']
df_test = df_test.dropna()
print(df_test['category'].unique())

print(df_test['category'].value_counts(dropna=False))

# Calculate number of matches for 'category' and 'predicted_category' columns
matches = (df_test['category'] == df_test['predicted_category']).sum()
match_rate = matches / len(df) * 100
print('------')
print(f"一致率: {match_rate:.2f}%")
print('------')

df = df.dropna()
# Replace rows with '404_not_found' in 'category' column with 'predicted_category' value
df['modified_category'] = np.where(df['category'] == '404_not_found', df['predicted_category'], df['category'])

print(df['modified_category'].value_counts(dropna=False))

# Remove 'predicted_category' column from df
df = df.drop(columns=['predicted_category'])

#user,action,device_id,start_viewing_date,stop_viewing_date,url,title,content,text,category,modified_category
df = df[['user', 'action', 'device_id', 'start_viewing_date', 'stop_viewing_date', 'url', 'title', 'content', 'text', 'category', 'modified_category']]

df.to_csv('../../csv/complete/device_original_with_category_plus_predict_modified.csv', index=False)
