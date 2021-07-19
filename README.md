# Prefect Flow Example 
Fetching Bike Data from Government Open API

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
Package installation
```

To install the required packages:
```
pip install -r requirements.txt
```
### Using Prefect to do ETL 

### UI Screenshot 


### Project Layout 

TYPE|OBJECT|DESCRIPTION
---|---|---
📁|[script](./script)| Python Code for data flow  
📄|[requirements.txt](./requirements.txt)|Python packages required for local development of Prefect Flows in this repository