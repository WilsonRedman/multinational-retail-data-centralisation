from dateutil.parser import parse
import re, pandas as pd

class DataCleaning:

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
    
    def clean_user_data(self, user_data):
        '''
        Cleans user data

        Args:
            user_data (DataFrame)

        Returns:
            DataFrame
        '''

        user_data.replace("NULL", pd.NA, inplace=True)

        user_data.join_date = user_data.join_date.apply(self.custom_parse)
        user_data.join_date = pd.to_datetime(user_data.join_date, errors="coerce")

        user_data.drop(["index"], axis=1, inplace=True)

        user_data.dropna(inplace=True, ignore_index=True)

        return user_data
    
    def integer_pass(self, card_number):
        try:
            int(card_number)
            return card_number
        except:
            card_number = re.sub(r"\D+", "", card_number)
            return card_number
    
    def clean_card_data(self, card_data):
        '''
        Cleans card data from a pdf

        Args:
            card_data (DataFrame)

        Returns:
            DataFrame
        '''

        card_data.replace("Null", pd.NA, inplace=True)

        card_data.drop_duplicates(subset=["card_number"], inplace=True)

        card_data.card_number = card_data.card_number.apply(self.integer_pass)

        card_data.date_payment_confirmed = card_data.date_payment_confirmed.apply(self.custom_parse)
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed, errors="coerce")

        card_data.dropna(subset = ["card_number","date_payment_confirmed"], inplace=True, ignore_index=True)

        return card_data
    
    def clean_store_data(self, store_data):
        '''
        Cleans store data

        Args:
            store_data (DataFrame)

        Returns:
            DataFrame
        '''

        store_data.drop(["index"], axis=1, inplace=True)

        store_data.replace("Null", pd.NA, inplace=True)

        store_data.opening_date = store_data.opening_date.apply(self.custom_parse)
        store_data.opening_date = pd.to_datetime(store_data.opening_date, errors="coerce")

        store_data.staff_numbers = store_data.staff_numbers.str.replace(r"[^\d]", "", regex=True)
        
        store_data.dropna(subset = ["store_code", "opening_date", "country_code"],inplace=True, ignore_index=True)
        
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
        
    def clean_products_data(self, products_data):
        '''
        Cleans product data

        Args:
            products_data (DataFrame)

        Returns:
            DataFrame
        '''

        products_data.replace("Null", pd.NA, inplace=True)
        products_data.dropna(inplace=True)

        products_data.weight = products_data.weight.apply(self.convert_product_weights)
        products_data.weight = pd.to_numeric(products_data.weight)

        products_data.dropna(inplace=True, ignore_index=True)

        return products_data
    
    def clean_orders_data(self, orders_data):
        '''
        Cleans order data

        Args:
            orders_data (DataFrame)

        Returns:
            DataFrame
        '''

        orders_data.drop(["level_0", "index", "first_name", "last_name", "1"], axis=1, inplace=True)
        
        return orders_data
    
    def clean_date_data(self, date_data):
        '''
        Cleans date data

        Args:
            date_data (DataFrame)

        Returns:
            DataFrame
        '''

        date_data.replace("Null", pd.NA, inplace=True)
        date_data[["month", "day", "year"]] = date_data[["month", "day", "year"]].apply(pd.to_numeric, errors="coerce")

        date_data.dropna(inplace=True, ignore_index=True)

        date_data[["month", "day", "year"]] = date_data[["month", "day", "year"]].astype(int)

        return date_data

if __name__ == "__main__":
    ## Code testing the functionality
    cleaner = DataCleaning()
    print(cleaner.clean_card_data())