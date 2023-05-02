import unittest
import pandas as pd
from datetime import datetime
from data_processor import DataProcessor


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.config_file = 'config.json'
        self.dp = DataProcessor(self.config_file)

    def test_load_data(self):
        self.dp.load_data()
        self.assertIsInstance(self.dp.data, pd.DataFrame)
        self.assertGreater(len(self.dp.data), 0)

    def test_standardize_names(self):
        self.dp.load_data()
        self.dp.standardize_names()
        self.assertTrue('name' in self.dp.data.columns)
        self.assertEqual(self.dp.data.iloc[0]['name'], 'Robert Mclaughlin')

    def test_convert_dates(self):
        self.dp.load_data()
        self.dp.convert_dates()
        self.assertTrue(all(isinstance(date, str) for date in self.dp.data['dob']))

    def test_map_states(self):
        self.dp.load_data()
        self.dp.map_states()
        self.assertTrue(all(state is None or len(state) == 2 for state in self.dp.data['state']))

    def test_combine_data(self):
        self.dp.combine_data()
        self.assertIsInstance(self.dp.data, pd.DataFrame)
        self.assertGreater(len(self.dp.data), 0)

    def test_map_companies(self):
        self.dp.load_data()
        self.dp.map_companies()
        self.assertTrue(all(isinstance(name, str) for name in self.dp.data['company_id']))

    def test_filter_records(self):
        self.dp.load_data()
        self.dp.filter_records()
        self.assertIsInstance(self.dp.data, pd.DataFrame)
        self.assertGreater(len(self.dp.data), 0)
        self.assertTrue(all(isinstance(date, str) and datetime.strptime(date, '%Y/%m/%d') for date in self.dp.data['dob']))


if __name__ == '__main__':
    unittest.main()
