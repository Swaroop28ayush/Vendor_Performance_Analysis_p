import os
from sqlalchemy import create_engine
from tqdm import tqdm
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.simplefilter("ignore")
import logging
import time
logging.basicConfig(
    filename = "D://credit_//data//logs//ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)


def ingest_db(df,table_name,engine):
    df.to_sql(table_name, con=engine, if_exists = 'replace', index=False)

def load_raw_data():
    # ✅ Same DB used for all tables
    db_path = os.path.join('D:/credit_/data', 'inventory.db')
    engine = create_engine(f'sqlite:///{db_path}')
    
    folder_path = r'D:\credit_\data'
    start = time.time()
    # 🚀 First insert all files except sales.csv
    for file in os.listdir(folder_path):
        if file.endswith('.csv') and file != 'sales.csv':
            file_path = os.path.join(folder_path, file)
            print(f'Reading {file}.......')
            df = pd.read_csv(file_path)
            logging.info(f'Ingesting {file} in db..')
            print(f'{file} shape: {df.shape}')
            print(f'Ingesting {file} into DB......⏳')
            ingest_db(df, file[:-4], engine)
            print(f'✅ Finished {file}\n')
    end = time.time()
    total_time = (end-start)/60
    logging.info('--------------Ingestion completed Except Sales File----------------')
    logging.info(f'\n Total Time Taken: {total_time} minutes')
    # ✅ Now insert sales.csv in chunks into same DB
    sales_file = os.path.join(folder_path, 'sales.csv')
    table_name = 'sales'
    chunksize = 1000
    first_chunk = True
    time2S = time.time()
    # ⏳ Optional: show progress
    total_rows = sum(1 for _ in open(sales_file)) - 1  # subtract header
    
    print("Ingesting sales.csv into existing inventory.db...\n")
    with tqdm(total=total_rows, desc="Ingesting sales.csv") as pbar:
        for chunk in pd.read_csv(sales_file, chunksize=chunksize):
            if first_chunk:
                chunk.to_sql(table_name, engine, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk.to_sql(table_name, engine, if_exists='append', index=False)
            pbar.update(len(chunk))
    time2E = time.time() 
    Ttime = (time2E-time2S)/60
    logging.info(f'✅ All data including sales.csv ingested into inventory.db-- Total Time for this = {Ttime} minutes.')
    print("✅ All data including sales.csv ingested into inventory.db")

if __name__ == '__main__':
    load_raw_data()