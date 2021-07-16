from flow import *
import pytest


def test_flow_state():
    state = flow.run()
    assert state.is_successful()

def test_fetch_fail():
    url = 'https://data.ntpc.gov.tw/api/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A/json?page=0&size=2'
    res = fetch_data_request.run(url)
    assert (res.status_code == 200)

def test_fetch_success():
    url = 'https://data.ntpc.gov.tw/random'
    res = fetch_data_request.run(url)
    assert (res.status_code != 200)

def test_check_data():
    data = [{'a': '1'}, {'a':'2'}, {'a':'3'}]
    
    # test working case 
    assert check_data.run(data=data, expect_len = 1)    

    # test failing case 
    with pytest.raises(Exception) as e_info:
        check_data.run(data=data, expect_len = 2)    

def test_wranggle_data():
    mock_data = [{'sbi': 1, 'tot': 2, 'mday': 202107011300}]
    data = wranggle_data.run(mock_data)
    new_cols = ['mday', 'date', 'time', 'created_at', 'full_pct'] 
    for c in new_cols: 
        assert c in data.columns

