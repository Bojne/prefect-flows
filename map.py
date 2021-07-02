import streamlit as st
import pandas as pd 
import numpy as np 
import glob

data_files = glob.glob("./data/*")
st.header("UBike Availability Map")
st.image('./ubike.jpeg')
file_path = st.selectbox('Select CSV', data_files)
df = pd.read_csv(file_path)
df.set_index('sareaen')
areas = st.multiselect('Select Area', df['sareaen'].unique())
if len(areas) == 0:
    areas = ['Xindian Dist.', 'Sanchong Dist.',
       'Xinzhuang Dist.', 'Luzhou Dist.', 'Zhonghe Dist.', 'Yonghe Dist.',
       'Banqiao Dist.', 'Taishan Dist.', 'Tucheng Dist.', 'Shulin Dist.',
       ]
df = df[df['sareaen'].isin(areas)]
min_bikes = st.select_slider('Slide to select', options=(range(df['sbi'].min(), df['sbi'].max())))

df = df[df['sbi'] > min_bikes]
df = df.rename(columns={"lng": "lon"})
map_data = df[['lat', 'lon', 'sbi']]

cols = ['sno', 'tot', 'sbi', 'mday', 'sareaen', 'snaen', 'aren', 'bemp', 'act']
st.write('U-Bike Map')
st.map(map_data)
st.write(df[cols].sort_index())

# https://streamlit-cheat-sheet.herokuapp.com/