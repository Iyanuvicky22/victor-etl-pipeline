"""
Goal: Extraction, Transformation and Loading of IMDb movies data.

Author: Arowosegbe Victor Iyanuoluwa\n
Email: Iyanuvicky@gmail.com\n
Github: https://github.com/Iyanuvicky22
"""
import time
from src.web_scraper import *
from src.data_transform import *
from src.db_loader import *


if __name__ == '__main__':
    start = time.time()

    # Scrape Dataset
    df_1 = get_1000_movies(pages=10)
    df_2 = get_imdb_movies(urls=URLS)

    # Transform Dataset
    df_joined = join_dfs(df_1000=df_1, df_imdb=df_2)
    trans_df = transform_df(data=df_joined)

    # Load Dataset
    load_data(trans_df)

    # Save Transformed Dataset into Parquet format.
    trans_df.to_parquet('data/processed_data.parquet')

    end = time.time()

    time_taken = end - start
    print(f'\n\nTotal time taken for ETL:-> {round(time_taken, 2)} seconds.\n')
