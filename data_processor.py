import pandas as pd
import os
import json


class DataProcessor:
    def __init__(self, config_file):
        with open(config_file) as f:
            config = json.load(f)
        self.data_file = config['data']['file']
        self.date_columns = config['data']['date_columns']
        self.date_format = config['data']['date_format']
        self.state_abbreviations = config['state_abbreviations']
        self.companies_file = config['data']['companies']
        self.softball_file = config['data']["softball"]

    def load_data(self):
        self.data = pd.read_csv(self.data_file)
        self.softball = pd.read_csv(self.softball_file,sep="\t")

    def standardize_names(self):
        self.data['standard_name'] = self.data.apply(
            lambda row: f"{row['first_name'].title()} {row['last_name'].title().split()[-1]}", axis=1)
        self.data = self.data.rename(columns={'standard_name': 'name'})

    def convert_dates(self):
        self.data[self.date_columns] = self.data[self.date_columns].apply(pd.to_datetime)
        self.data[self.date_columns] = self.data[self.date_columns].apply(lambda x: x.dt.strftime(self.date_format))

    def map_states(self):
        self.softball['us_state'] = self.data['state'].map(self.state_abbreviations)

    def combine_data(self):
        combined_data = pd.DataFrame()
        for filename in os.listdir('.'):
            if filename.endswith('.csv'):
                data = pd.read_csv(filename)
                data['source'] = filename
                combined_data = combined_data.append(data, ignore_index=True)
        self.data = combined_data

    def map_companies(self):
        companies = pd.read_csv(self.companies_file)
        company_names = dict(zip(companies['id'], companies['name']))
        # self.data['company_id'] = self.data['company_id'].map(company_names)
        self.data['company_id'] = self.data['company_id'].apply(lambda x: str(x) if pd.notnull(x) else None)
        self.data['company_id'] = self.data['company_id'].astype(str).map(company_names).astype(str)

    def filter_records(self):
        self.data = self.data.dropna(subset=['dob'])  # drop rows with missing dob values
        good_data = self.data[self.data['dob'].str.contains(r'\d{2}/\d{2}/\d{4}')].reset_index(drop=True)
        bad_data = self.data[~self.data['dob'].str.contains(r'\d{2}/\d{2}/\d{4}')].reset_index(drop=True)
        good_data.to_csv('output.csv', index=False)
        bad_data.to_csv('bad_records.csv', index=False)

