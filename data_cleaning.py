from dateutil.parser import parse
import data_extraction, database_utils, api_keys
import re, pandas as pd

class DataCleaning:

    def __init__(self):
        '''
        Constructor for the DataCleaning class, creates instances for connecting to a database
        and extracting data, both needed when extracting data from a database.
        '''

        self.extractor = data_extraction.DataExtractor()
        self.dbConnector = database_utils.DatabaseConnector()

    def custom_parse(self, date):
        '''
        Prevents errors in parsing dates, instead returning NaT

        Args:
            date (string): A date string that needs to be converted into a standard format
        
        Returns:
            date / NaT
        '''
        try:
            return parse(date)
        except:
            return pd.NaT
    
    def clean_user_data(self):
        '''
        Cleans user data from a "legacy_users" table in the connected DatabaseConnector instance

        Returns:
            DataFrame
        '''

        user_data = self.extractor.read_rds_table("legacy_users", self.dbConnector)
        user_data.replace("NULL", pd.NA, inplace=True)

        user_data.join_date = user_data.join_date.apply(self.custom_parse)
        user_data.join_date = pd.to_datetime(user_data.join_date, errors="coerce")

        user_data.drop(["index"], axis=1, inplace=True)

        user_data.dropna(inplace=True, ignore_index=True)

        return user_data
    
    def integer_pass(self, card_number):
        try:
            return int(card_number)
        except:
            return pd.NA
    
    def clean_card_data(self):
        '''
        Cleans card data from a pdf

        Returns:
            DataFrame
        '''

        card_data = self.extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        card_data.replace("Null", pd.NA, inplace=True)

        card_data.drop_duplicates(subset=["card_number"], inplace=True)

        card_data.card_number = card_data.card_number.apply(self.integer_pass)

        card_data.date_payment_confirmed = card_data.date_payment_confirmed.apply(self.custom_parse)
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed, errors="coerce")

        card_data.dropna(inplace=True, ignore_index=True)

        return card_data
    
    def clean_store_data(self):
        '''
        Cleans store data retrieved from APIs

        Returns:
            DataFrame
        '''

        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        response = self.extractor.list_number_of_stores(url, api_keys.storeHeader)

        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
        store_data = self.extractor.retrieve_stores_data(url, api_keys.storeHeader, response)
        store_data.drop(["index", "lat"], axis=1, inplace=True)

        store_data.replace("Null", pd.NA, inplace=True)

        store_data.opening_date = store_data.opening_date.apply(self.custom_parse)
        store_data.opening_date = pd.to_datetime(store_data.opening_date, errors="coerce")

        store_data.staff_numbers = store_data.staff_numbers.str.replace(r"[^\d]", "", regex=True)
        
        store_data.dropna(inplace=True, ignore_index=True)
        
        return store_data
    
    def convert_product_weights(self, weight):
        '''
        Converts weight string to their float values in kg

        Args:
            weight (string): May be in kg, ml, g, or oz, and may contain multiplication

        Returns:
            float
        '''

        weight = re.sub(r"[^\w]", "", weight)

        try:
            multiplier = 1
            if "kg" in weight:
                value = weight[:-2]
            elif "ml" in weight:
                value = weight[:-2]
                multiplier = 1/1000
            elif "g" in weight:
                value = weight[:-1]
                multiplier = 1/1000
            elif "oz" in weight:
                value = weight[:-2]
                multiplier = 0.0283495
            
            if "x" in value:
                newVal = 1.0
                for multiple in value.split("x"):
                    newVal *= float(multiple)
                return newVal * multiplier
            else:
                return float(value) * multiplier
        
        except:
            return pd.NA
        
    def clean_products_data(self):
        '''
        Cleans product data retrieved from a csv file in an s3 bucket

        Returns:
            DataFrame
        '''

        link = "s3://data-handling-public/products.csv"
        products_data = self.extractor.extract_from_s3(link)

        products_data.replace("Null", pd.NA, inplace=True)
        products_data.dropna(inplace=True)

        products_data.weight = products_data.weight.apply(self.convert_product_weights)
        products_data.weight = pd.to_numeric(products_data.weight)

        products_data.dropna(inplace=True, ignore_index=True)

        return products_data
    
    def clean_orders_data(self):
        '''
        Cleans user data from a "orders_table" table in the connected DatabaseConnector instance

        Returns:
            DataFrame
        '''

        orders_data = self.extractor.read_rds_table("orders_table", self.dbConnector)

        orders_data.drop(["level_0", "index", "first_name", "last_name", "1"], axis=1, inplace=True)
        
        return orders_data
    
    def clean_date_data(self):
        '''
        Cleans date data from a json file

        Returns:
            DataFrame
        '''

        url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        date_data = self.extractor.extract_json(url)

        date_data.replace("Null", pd.NA, inplace=True)
        date_data[["month", "day", "year"]] = date_data[["month", "day", "year"]].apply(pd.to_numeric, errors="coerce")

        date_data.dropna(inplace=True, ignore_index=True)

        date_data[["month", "day", "year"]] = date_data[["month", "day", "year"]].astype(int)

        return date_data

if __name__ == "__main__":
    ## Code testing the functionality
    cleaner = DataCleaning()
    print(cleaner.clean_orders_data())