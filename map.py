import streamlit as st
import pandas as pd 
import numpy as np
from datetime import datetime
import glob
import altair as alt
import matplotlib.pyplot as plt


raw_data_path = 'data_raw'
data_files = glob.glob(f"./{raw_data_path}/*")
data_files.sort()

st.header("Prefect Flow Demo 🚲")


@st.cache
def get_data(file_path):
    df = pd.read_csv(file_path)
    df['mday'] = pd.to_datetime(df['mday'])
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['time'] = pd.to_datetime(df['time'])
    df['hour'] = df['mday'].dt.hour
    return df 

# Read the data
# file_path = st.selectbox('Select CSV', data_files)

file_path = data_files[-1]
df = get_data(file_path)
all_stations = df['snaen'].unique()
select_stations = st.multiselect('Choose stations', list(all_stations), ['Dapeng Community', 'Xizhi Railway Station'])
df = df[df['snaen'].isin(select_stations)]

def plot_data(df,select_stations):
    df = df.groupby(['hour','snaen']).mean()['bemp'].unstack() 
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.plot(
        df.index,
        df[select_stations]
    )
    ax.set_title('Bike Demand in a day')
    ax.set_xlabel("Hours")
    ax.set_ylabel("Number of empty spot in each station")
    ax.legend(select_stations)

    return fig

st.write(plot_data(df,select_stations))
df = df.rename(columns={"lng": "lon"})
st.subheader('Station Location')
st.map(df)


