import prefect
from prefect import Flow, task, Parameter
from datetime import datetime
import requests
from requests.exceptions import HTTPError
import pandas as pd
import glob

logger = prefect.context.get("logger") # should I declare global logger?

@task
def fetch_data_request(url):
    """
    Send request to the API url to fetch the data 
    """
    try:
        response = requests.get(url)
        response.raise_for_status() 
    except HTTPError as http_err:
        print(f'HTTP error occurred in data fetching: {http_err}')  
    logger.info("[ok] Successfully Got the data from API")
    return response

@task 
def parse_response(response):
    """Turn response into json"""
    return response.json()

@task 
def check_data(data, expect_len = 14):
    """
    Check the if the data has missing rows 
    """
    check_len = lambda x: len(x) >= expect_len
    pass_check = list(map(check_len, data))
    if False in pass_check: 
        logger.info("Some stations didn't pass the check:")
        for i, status in enumerate(pass_check):
            if status == False: 
                logger.info(f'Failed Station: {data[i]}')
        raise Exception("At least one station has missing data, length doesn't match with expectation.")
    logger.info(f"[ok] Checked {len(data)} stations, all station has {expect_len} information")
    return True

@task 
def wranggle_data(data):
    """
    Create dataframe with new columns 
    """
    data = pd.DataFrame(data) # turn data into pandas DataFrame object 
    logger = prefect.context.get("logger")
    now = datetime.now()
    data['full_pct'] = data['sbi'].astype(int) / data['tot'].astype(int)
    data['created_at'] = now
    data['mday'] = pd.to_datetime(data['mday'], format='%Y%m%d%H%M%S', errors='ignore')
    data['date'] = data['mday'].dt.date
    data['time'] = data['mday'].dt.time
    logger.info(f'[ok] Added new columns to dataframe')
    return data

@task
def save_data(data, folder_path='./data_raw'):
    """Save the data"""
    logger = prefect.context.get("logger")
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
    logger.info(f"PASS: Data is saved to folder")
    return file_path

with Flow("ubike-data-fetch-flow") as flow:
    station_limit = 1000
    url  = f'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json?page=0&size={station_limit}'
    url = Parameter('API url', url)

    response = fetch_data_request(url)
    data = parse_response(response)
    check_data(data)
    data = wranggle_data(data)
    save_data(data)
