import pandas as pd

df = pd.read_csv("../../csv/original/device_original.csv", dtype={'user': str})
print(df)

# Remove '+09:00' included in each column
df['start_viewing_date'] = df['start_viewing_date'].str.replace('+09:00', '')
df['stop_viewing_date'] = df['stop_viewing_date'].str.replace('+09:00', '')
df['eliminate_date'] = df['eliminate_date'].str.replace('+09:00', '')
df['base_date'] = df['base_date'].str.replace('+09:00', '')

#user,action,device_id,url,start_viewing_date,stop_viewing_date,
df = df[['user', 'action', 'device_id', 'start_viewing_date', 'stop_viewing_date', 'url', 'title', 'content']]

#　Extract only rows where action is view
df = df[df['action'] == 'view']

#Delete rows where 'start_viewing_date' and 'stop_viewing_date' columns of df are nan * Total number of nulls: 3, 14
df = df.dropna(subset=['start_viewing_date', 'stop_viewing_date'])

df.to_csv("../../csv/original/device_original_formatted.csv", index=False)
