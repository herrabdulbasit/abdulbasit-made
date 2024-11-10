import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import requests
import opendatasets as od
import json
from fuzzywuzzy import process

class data_pipeline():
    UNEMPLOYMENT = 1
    CRIME_RATE = 2

    _data_sets = {
        1: { "file_path": 'unemployment-by-county-us/output.csv', "url": "https://www.kaggle.com/datasets/jayrav13/unemployment-by-county-us" },
        2: { "file_path": "us-crime-dataset/US_Crime_DataSet.csv", "url": "https://www.kaggle.com/datasets/mrayushagrawal/us-crime-dataset" }
    }

    _valid_months = ["January", "February", "March", "April", "May", "June", 
                "July", "August", "September", "October", "November", "December"]

    def __init__(self):
        self._api_token = {"username":"abdulbasit26","key":"33537bb663496e170eb9a3ee983c3365"}

    def initialize_pipeline(self):
        self.set_kaggle_api_token()
        self.init_unemployment_df()
        self.init_crime_rate_df()
        self.transform_unemployment_df()
        self.load_data(self.crime_r_df, 'crimes')
        self.load_data(self.unemp_df, 'unemployments')

    # EXTRACT

    def init_unemployment_df(self):
        self.download_data_sets(
            data_pipeline._data_sets[data_pipeline.UNEMPLOYMENT]['file_path'],
            data_pipeline._data_sets[data_pipeline.UNEMPLOYMENT]['url']
            )
        self.unemp_df = pd.read_csv(data_pipeline._data_sets[data_pipeline.UNEMPLOYMENT]['file_path'])

    def init_crime_rate_df(self):
        self.download_data_sets(
            data_pipeline._data_sets[data_pipeline.CRIME_RATE]['file_path'],
            data_pipeline._data_sets[data_pipeline.CRIME_RATE]['url']
            )
        self.crime_r_df = pd.read_csv(data_pipeline._data_sets[data_pipeline.CRIME_RATE]['file_path'], engine='python')

    # TRANSFORM

    def transform_unemployment_df(self):
        self.unemp_df = self.unemp_df[(self.unemp_df['Year'] >= 1990) & (self.unemp_df['Year'] <= 2010)].reset_index()
        self.unemp_df['Month'] = self.unemp_df['Month'].apply(
            lambda x: process.extractOne(x, data_pipeline._valid_months)[0] if x not in data_pipeline._valid_months else x
            )
        self.unemp_df['Month_Number'] = self.unemp_df['Month'].apply(lambda x: data_pipeline._valid_months.index(x) + 1 if x in data_pipeline._valid_months else None)
        self.unemp_df['Month'] = self.unemp_df['Month'].str.title()
        self.unemp_df['Year'] = pd.to_numeric(self.unemp_df['Year'], errors='coerce')
        self.unemp_df['Rate'] = pd.to_numeric(self.unemp_df['Rate'], errors='coerce')
        self.unemp_df['Unemployment_Level'] = pd.cut(self.unemp_df['Rate'], bins=3, labels=['Low', 'Medium', 'High'])
    
    def transform_crime_rate_df(self):
        self.crime_r_df = self.crime_r_df[(self.crime_r_df['Year'] >= 1990) & (self.crime_r_df['Year'] <= 2010)].reset_index()
        self.crime_r_df['Month'] = self.crime_r_df['Month'].apply(lambda x: process.extractOne(x, data_pipeline._valid_months)[0] if x not in data_pipeline._valid_months else x)
        self.crime_r_df['Month_Number'] = self.crime_r_df['Month'].apply(lambda x: data_pipeline._valid_months.index(x) + 1 if x in data_pipeline._valid_months else None)
        self.crime_r_df['Year'] = pd.to_numeric(self.crime_r_df['Year'], errors='coerce')
        self.crime_r_df['Crime Type'].fillna("Unknown", inplace=True)
        crime_counts = self.crime_r_df.groupby(['Year', 'State']).size().rename("Crime Count")
        self.crime_r_df = self.crime_r_df.merge(crime_counts, on=['Year', 'State'])

    # LOAD

    def load_data(self, df, table_name):
        current_directory = os.getcwd()
        data_folder_path = os.path.join(os.path.abspath(os.path.join(current_directory, os.pardir)), 'data')
        if not os.path.exists(data_folder_path):
            os.makedirs(data_folder_path)
        db_path = os.path.join(data_folder_path, 'crime_unemployment_analysis.sqlite')

        if not os.path.isfile(db_path):
            open(db_path, 'w').close()
        conn = sqlite3.connect(db_path)
        conn.close()
        engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

    # HELPER FUNCTIONS

    def set_kaggle_api_token(self):
        with open('kaggle.json', 'w+') as file:
            json.dump(self._api_token, file)

    def download_data_sets(self, dataset_file_path, ds_url):
        if os.path.isfile(dataset_file_path):
            print("Dataset was already downloaded.")
        else:
            od.download(ds_url)

def main():
    data_pipeline().initialize_pipeline()

if __name__ == '__main__':
    main()
