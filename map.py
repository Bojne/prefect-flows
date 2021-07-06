import streamlit as st
import pandas as pd 
import numpy as np
from datetime import datetime
import glob

raw_data_path = 'data_raw'
clean_folder_path = 'data'

data_files = glob.glob(f"./{raw_data_path}/*")
data_files.sort()

st.header("Prefect Flow Demo: Ubike Station Availability Map")
# st.image('./ubike.jpeg')



# Read the datas 
file_path = st.selectbox('Select CSV', data_files)
df = pd.read_csv(file_path)

default_areas = ['Xindian Dist.', 'Sanchong Dist.',
       'Xinzhuang Dist.', 'Luzhou Dist.', 'Zhonghe Dist.', 'Yonghe Dist.',
       'Banqiao Dist.', 'Taishan Dist.', 'Tucheng Dist.', 'Shulin Dist.',
       ]

df.set_index('sareaen')
areas = st.multiselect('Select Area', list(df['sareaen'].unique()), default_areas)

min_bikes = st.select_slider('Slide to select', options=(range(df['sbi'].min(), df['sbi'].max())))

# Filter Data 
df = df[df['sareaen'].isin(areas)]
df = df[df['sbi'] > min_bikes]

# Format Data 
df = df.rename(columns={"lng": "lon", 'sno': 'station id', 'tot': 'bike capcity', 'sbi': 'available bikes'})
cols = ['station id', 'bike capcity', 'available bikes', 'mday', 'sareaen', 'snaen', 'aren', 'bemp', 'act']

hour_to_filter = st.slider('Hours', 0, 23, 17)
start_time = st.slider(
    "Date Selector",
    value=datetime(2020, 1, 1, 9, 30),
    format="MM/DD/YY")
st.write("Selected Date", start_time)


if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(df[cols].sort_index())

st.write('U-Bike Map')
st.map(df)


