import streamlit as st
import pandas as pd 
import numpy as np
from datetime import datetime
import glob

raw_data_path = 'data_raw'

data_files = glob.glob(f"./{raw_data_path}/*")
data_files.sort()

st.header("Prefect Flow Demo: Ubike Station Availability Map")

# Read the datas 
file_path = st.selectbox('Select CSV', data_files)
df = pd.read_csv(file_path)
df.set_index('sareaen')

all_area = list(df['sareaen'].unique())
areas = all_area
if st.checkbox('Select Areas'):
    areas = st.multiselect('Select Area', all_area, all_area)
    df = df[df['sareaen'].isin(areas)]  

if st.checkbox('Select Minimal Capacity'):
    # min_bikes = st.select_slider('Slide to select', 0,1, (0.1,0.9))
    values = st.slider('Select a range of values', 0.0, 1.0, (0.25, 0.75))
    st.write('Full percentage range', values)
    df = df[df['full_pct'] > values[0]]
    df = df[df['full_pct'] < values[1]]

# Format Data 
df = df.rename(columns={"lng": "lon", 'sno': 'station id', 'tot': 'bike capcity', 'sbi': 'available bikes'})
cols = ['station id', 'bike capcity', 'available bikes', 'mday', 'sareaen', 'snaen', 'aren', 'bemp', 'act']

if st.checkbox('Select Time Range'):
    hour_to_filter = st.slider('Hours', 0, 23, 17)
    start_time = st.slider(
        "Date Selector",
        value=datetime(2020, 1, 1, 9, 30),
        format="MM/DD/YY")
    st.write("Selected Date", start_time)
if st.checkbox('Show Raw Data'):
    st.subheader('Raw data')
    st.write(df.sort_index())


st.subheader('Distirbution')
hist_values = np.histogram(df['full_pct'], bins=30, range=(0,1))[0]
st.bar_chart(hist_values)

st.write('U-Bike Map')
st.map(df)


