"""
Goal: Extraction, Transformation and Loading of IMDb movies data.

Author: Arowosegbe Victor Iyanuoluwa\n
Email: Iyanuvicky@gmail.com\n
Github: https://github.com/Iyanuvicky22
"""
import time
from etl_pipe.web_scraper import *
from etl_pipe.data_transform import *
from etl_pipe.db_loader import *
from logger.logger import etl_logger

import sys
sys.path.append("../")

logger = etl_logger()


def etl_pipeline():
    """
    IMDb ETL for movies scraped from two APIs, trandformed
    """

    start = time.time()
    logger.info("ETL Pipeline Started!")

    # Scrape Dataset
    df_1 = get_1000_movies(pages=10)
    df_2 = get_imdb_movies(urls=URLS)

    # Transform Dataset
    df_joined = join_dfs(df_1000=df_1, df_imdb=df_2)
    trans_df = transform_df(data=df_joined)

    # trans_df.to_csv('31-03-25 Call.csv')

    # Load Dataset
    load_data(trans_df)
    logger.info("ETL Pipeline Successful")

    end = time.time()

    time_taken = end - start
    logger.info(f"Time taken for ETL is {round(time_taken, 2)} secs")
    logger.info(f"{len(trans_df)} unique movies and series loaded into database")


if __name__ == '__main__':
    logger.info('ETL Process Starting')
    etl_pipeline()
    logger.info('ETL Ended')
