
import prefect
from prefect import Flow, task, Parameter
from datetime import datetime
import requests
from requests.exceptions import HTTPError
import pandas as pd
import glob

logger = prefect.context.get("logger") 

@task
def fetch_data_request(station_limit):
    """
    Request data from data.ntpc.gov.tw endpoint 
    """
    url = f'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json?page=0&size={station_limit}'
    try:
        response = requests.get(url)
        response.raise_for_status() 
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred in data fetching: {http_err}')
        raise
    logger.info("[ok] Successfully Got the data from API")
    return response.json()

@task 
def check_data(data, expect_len = 14):
    """
    Check if any values are missing from the data source 
    """
    missing_data_flag = False
    for station in data: 
        if len(station) <= expect_len:
            logger.info(f'This station has missing data {station}')
            missing_data_flag = True 
    if missing_data_flag:
        raise Exception("At least one station has missing data, length doesn't match with expectation.")
    else:
        logger.info(f"Checked {len(data)} stations, all station has {expect_len} information")
        return None 

@task 
def wranggle_data(data):
    """
    Create dataframe with new columns 
    """
    data = pd.DataFrame(data) # turn data into pandas DataFrame object 
    now = datetime.now()
    data['full_pct'] = data['sbi'].astype(int) / data['tot'].astype(int)
    data['created_at'] = now
    data['mday'] = pd.to_datetime(data['mday'], format='%Y%m%d%H%M%S', errors='ignore')
    data['date'] = data['mday'].dt.date
    data['time'] = data['mday'].dt.time
    logger.info(f'Added new columns to dataframe')
    return data

@task
def save_data(data, folder_path='./data_raw'):
    """
    Save data as new csv or merge data into existing csv  
    """
    now = datetime.now() 
    date_string = now.strftime("%Y_%m_%d")
    file_path = f"{folder_path}/snapshot_{date_string}.csv"
    if file_path in glob.glob(f"{folder_path}/*"):
        logger.info("Merging new records to data csv")
        current_df = pd.read_csv(file_path, index_col=False)
        final_df = pd.concat([current_df, data], ignore_index=True) 
    else: 
        final_df = data 
    final_df.to_csv(file_path, index=False) # export the object to csv 
    logger.info(f"Data is saved in folder {folder_path}")
    return file_path

with Flow("ubike-data-fetch-flow") as flow:
    station_limit = 1000
    url  = f'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json?page=0&size={station_limit}'
    url = Parameter('API url', url)
    data = fetch_data_request(url)
    check_data(data)
    data = wranggle_data(data)
    save_data(data)
