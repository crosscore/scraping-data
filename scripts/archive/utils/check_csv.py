import pandas as pd
import numpy as np

df = pd.read_csv('../../csv/original/all_device_add_category.csv', encoding='utf-8', dtype={'user': str})
print(df)

print(f"df['url'].nunique():\n{df['url'].nunique()}")
print(f"df['category'].value_counts(dropna=False):\n{df['category'].value_counts(dropna=False)}")
