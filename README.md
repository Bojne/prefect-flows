# Prefect Flow Example 
A Prefect flow example that fetch and save bike data from [New Taipei City Open API](https://data.ntpc.gov.tw/datasets/71CD1490-A2DF-4198-BEF1-318479775E8A).

## Prerequisites
Have Python and pip / conda installed.

## Installation 

Installing required packages with Python virtual environment is the best practice to set thing up. You can do it via `pip` or `conda`:
### Python Virtual Environment
```
pip install virtualenv
python -m venv prefect-flow-example
source activate prefect-flow-example/bin/activate
```

### Conda Virtual Environment
```
conda create -n prefect-flow-example python=3.7
source activate prefect-flow-example
```

To install the required packages:
```
pip install -r requirements.txt
```
## Usage 
#### Run the Flow 
```
prefect run -p flow.py  
```


### Contribution 
- Create an issue for suggestions 
- Fork this repo and submit a PR for contribution

### Project Layout 

TYPE|OBJECT|DESCRIPTION
---|---|---
📁|[script](./script)| Python Code for data flow  
📄|[requirements.txt](./requirements.txt)|Python packages required for local development of Prefect Flows in this repository