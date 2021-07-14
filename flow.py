import prefect
from prefect import task, Flow

from datetime import datetime
import requests
import pandas as pd
import glob



base_url  = 'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json'
url_param = '?page=0&size=1000'
folder_path = './data_raw'
data_csv_list = glob.glob(f"{folder_path}/*")

@task
def get_data():
    logger = prefect.context.get("logger")

    target_url = base_url + url_param
    response = requests.get(target_url)

    if response: 
        logger.info("PASS: Got the data from API call")  
    return response.json()

@task 
def check_data(data, expect_len = 14):
    logger = prefect.context.get("logger")
    check_len = lambda x: len(x) > 10
    check_result = list(map(check_len, data))
    if all(check_result) == False:
        raise Exception("At least one station has missing variable, length doesn't match with expectation.")
    logger.info(f"PASS: Checked {len(data)} stations, all station has {expect_len} information")
    data = pd.DataFrame(data) # turn data into pandas DataFrame object 
    return data 

@task 
def wranggle_data(data):
    logger = prefect.context.get("logger")
    now = datetime.now()
    data['full_pct'] = data['sbi'].astype(int) / data['tot'].astype(int)
    data['created_at'] = now
    data['mday'] = pd.to_datetime(data['mday'], format='%Y%m%d%H%M%S', errors='ignore')
    data['date'] = data['mday'].dt.date
    data['time'] = data['mday'].dt.time
    logger.info(f'PASS: Add new columns to the data')
    return data

@task
def save_data(data):
    logger = prefect.context.get("logger")
    now = datetime.now() 
    dt_string = now.strftime("%Y_%m_%d")
    file_path = f"./snapshot_{dt_string}.csv"
    if file_path not in data_csv_list:
        final_df = data
    else:
        logger.info("Merging new records to data csv")
        current_df = pd.read_csv(file_path, index_col=False)
        final_df = pd.concat([current_df, data], ignore_index=True) 
    final_df.to_csv(file_path, index=False) # export the object to csv 
    logger.info(f"PASS: Data is saved to folder")
    return None

with Flow("youbike-data-fetch-flow") as flow:
    data = get_data()
    data = check_data(data)
    data = wranggle_data(data)
    save_data(data)

