import sqlite3
import pandas as pd


class DataLoader:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def _create_connection(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.config['database'])

    def _create_tables(self):
        self._create_connection()
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                date_of_birth DATE NOT NULL,
                company_id INTEGER NOT NULL,
                last_active DATE NOT NULL,
                score INTEGER NOT NULL,
                joined_league INTEGER NOT NULL,
                us_state TEXT NOT NULL,
                source_file TEXT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES companies (id)
            )
        ''')

    def _load_companies(self):
        companies = pd.read_csv(self.config['companies_csv'])
        self._create_connection()
        for index, row in companies.iterrows():
            self.conn.execute('INSERT INTO companies (id, name) VALUES (?, ?)', (row['id'], row['name']))
        self.conn.commit()

    def _load_users(self):
        data = pd.read_csv(self.config['input_csv'])
        companies = pd.read_csv(self.config['companies_csv'])
        self._create_connection()
        for index, row in data.iterrows():
            company_id = companies.loc[companies['name'] == row['company_id'], 'id'].iloc[0]
            self.conn.execute(
                'INSERT INTO users (name, date_of_birth, company_id, last_active, score, joined_league, us_state, source_file) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (row['name'], row['date_of_birth'], company_id, row['last_active'], row['score'], row['joined_league'],
                 row['us_state'], row['source_file']))
        self.conn.commit()

    def load_data(self):
        self._create_tables()
        self._load_companies()
        self._load_users()
        self.conn.close()
