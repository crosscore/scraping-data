import pandas as pd
import os

new_category_dict = {
    'ニュース': ['国内', '国際', '経済'],
    'スポーツ': ['スポーツ'],
    'その他': ['エンタメ', 'IT・科学', 'ライフ', '地域']
}
inverted_dict = {v: k for k, values in new_category_dict.items() for v in values}

output_path = './csv/change_category/yahoo_news_change_1116.csv'
os.makedirs(os.path.dirname(output_path), exist_ok=True)

original_df = pd.read_csv('./csv/concat/yahoo_news_concat_1116_v3.csv', dtype={'user': str})
original_df['category'] = original_df['category'].map(inverted_dict)

print(original_df)
original_df.to_csv(output_path, index=False)
print(original_df['category'].unique())
