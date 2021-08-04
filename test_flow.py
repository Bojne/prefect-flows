from flow import *
import pytest
import tempfile
import json
from unittest import mock
import requests
from datetime import datetime


def test_fetch_calls_requests_correctly_and_returns_data():
    # construct a fake Response model
    fake_response = requests.models.Response()
    fake_response.status_code = 200
    fake_response.headers['Content-Type'] = "application/json"
    fake_response._content = json.dumps({"foo": "bar"}).encode('utf-8')
    # requests.get will return our fake Response model
    fake_request_get = mock.MagicMock(return_value=fake_response)
    with mock.patch("requests.get", fake_request_get) as patched_get:
        res = fetch_data_request.run(1)
        assert res == fake_response.json()

def test_flow_state():
    state = flow.run()
    assert state.is_successful()

def test_fetch_rasises_on_non_200_response():
    # construct a fake Response model
    fake_response = requests.models.Response()
    fake_response.status_code = 404
    # requests.get will return our fake Response model
    fake_request_get = mock.MagicMock(return_value=fake_response)
    with mock.patch("requests.get", fake_request_get) as patched_get:
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_data_request.run(1)

def test_save_data_saves_new_data():
    mock_data = pd.DataFrame([{'sbi': 1, 'tot': 2, 'mday': 202107011300}])
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = save_data.run(mock_data, temp_dir)
        assert file_path in glob.glob(f"{temp_dir}/*")

def test_save_data_updates_existing_data():
    mock_data = pd.DataFrame([{'sbi': 1, 'tot': 2, 'mday': 202107011300}])
    with tempfile.TemporaryDirectory() as temp_dir:
        now = datetime.now()
        date_string = now.strftime("%Y_%m_%d")
        mock_data.to_csv(f"{temp_dir}/snapshot_{date_string}.csv")
        # run our task
        file_path = save_data.run(mock_data, folder_path=temp_dir)

        # check the file has been updated
        result = pd.read_csv(file_path)
        assert result.shape == (2,4)
