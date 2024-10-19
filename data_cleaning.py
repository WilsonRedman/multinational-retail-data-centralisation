import data_extraction
import pandas as pd
from dateutil.parser import parse

class DataCleaning:

    def __init__(self):
        self.extractor = data_extraction.DataExtractor()
        self.user_data = self.extractor.read_rds_table("legacy_users")

    def custom_parse(self, date):
        try:
            return parse(date)
        except:
            return pd.NaT
    
    def clean_user_data(self):
        self.user_data.replace("NULL", None, inplace=True)

        self.user_data.join_date = self.user_data.join_date.apply(self.custom_parse)
        self.user_data.join_date = pd.to_datetime(self.user_data.join_date, infer_datetime_format=True, errors="coerce")

        self.user_data.dropna(inplace=True)

        return self.user_data


if __name__ == "__main__":
    ## Code testing the functionality
    cleaner = DataCleaning()
    print(cleaner.clean_user_data())