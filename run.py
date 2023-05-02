from data_processor import DataProcessor

# create an instance of the DataProcessor class
processor = DataProcessor('config.json')

# load the data from the CSV file
processor.load_data()

# standardize the names
processor.standardize_names()

# convert the dates to a specific format
processor.convert_dates()

# map state abbreviations
processor.map_states()

# combine data from multiple CSV files
processor.combine_data()

# map company names
processor.map_companies()

# filter records with invalid date of birth
processor.filter_records()
