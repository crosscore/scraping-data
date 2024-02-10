"""
CSVファイルから各カテゴリ毎に最新の行を指定数だけ取得し、新たなCSVファイルに出力する
"""
import glob
import pandas as pd

def extract_latest_rows_by_category(input_file, output_file, max_rows_per_category=5000):
    """
    各カテゴリ毎に最新の行を指定数だけ取得し、新たなCSVファイルに出力する
    """
    try:
        df = pd.read_csv(input_file)
        categories = df['category'].unique()
        new_dfs = []

        for category in categories:
            df_category = df[df['category'] == category]
            df_latest = df_category.tail(max_rows_per_category)
            new_dfs.append(df_latest)
        result_df = pd.concat(new_dfs)
        # result_dfのカテゴリ毎の行数を確認
        print(result_df['category'].value_counts(dropna=False))
        result_df.to_csv(output_file, index=False)
        print(f"File saved: {output_file}")

    except pd.errors.EmptyDataError:
        print("Error: The input CSV file is empty.")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")

INPUT_FILE = glob.glob('../../../data/csv/yahoo_news/backup/*.csv')
OUTPUT_FILE = '../../../data/csv/yahoo_news/concat/output.csv'
extract_latest_rows_by_category(INPUT_FILE[-1], OUTPUT_FILE)
