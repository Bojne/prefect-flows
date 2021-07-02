import prefect
from prefect import task, Flow

from datetime import datetime
import requests
import pandas as pd


base_url  = 'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json'
url_param = '?page=0&size=200'
folder_path = './data'

@task
def get_data():
    logger = prefect.context.get("logger")

    # should this be a input variable?
    target_url = base_url + url_param
    response = requests.get(target_url)

    if response: 
        logger.info("PASS: Got the data from API call")  
    return response.json()

@task 
def check_data(data, expect_len = 14):
    logger = prefect.context.get("logger")
    check_len = lambda x: len(x) > 1
    check_result = list(map(check_len, data))
    if all(check_result) == False:
        raise Exception("At least one station has missing variable, length doesn't match with expectation.")
    logger.info(f"PASS: Checked {len(data)} stations, all station has {expect_len} information")
    return None 

@task
def save_data(data):
    logger = prefect.context.get("logger")
    now = datetime.now() 
    dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
    df = pd.DataFrame(data) # turn data into pandas DataFrame object 
    df.to_csv(f"{folder_path}/data{dt_string}.csv") # export the object to csv 
    logger.info(f"PASS: Data is saved to {folder_path} folder")
    return None

with Flow("bike-flow") as flow:
    data = get_data()
    check_data(data)
    save_data(data)

# Register the flow under the "ubike-example" project
flow.register(project_name="ubike-example")
