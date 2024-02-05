import pandas as pd

df = pd.read_csv("../../csv/man_power/all_404_not_found_v3_man.csv", dtype={'user': str})

#　Convert user' column to string and fill with 4 digits with zeros
df['user'] = df['user'].astype(str).str.zfill(4)
print(df)

df.to_csv("../../csv/man_power/all_404_not_found_v3_man2.csv", index=False)
