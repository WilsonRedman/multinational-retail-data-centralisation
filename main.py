import data_cleaning, data_extraction, database_utils, api_keys

def upload_user_data():
    '''
    Cleans and uploads user data from a "legacy_users" table in the connected DatabaseConnector instance
    '''

    user_data = extractor.read_rds_table("legacy_users", dbConnector)
    cleaned_data = cleaner.clean_user_data(user_data)

    dbConnector.upload_to_db(cleaned_data, "dim_users")


def upload_card_data():
    '''
    Cleans and uploads card data from a pdf
    '''

    card_data = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    cleaned_data = cleaner.clean_card_data(card_data)

    dbConnector.upload_to_db(cleaned_data, "dim_card_details")


def upload_store_data():
    '''
    Cleans and uploads store data retrieved from APIs
    '''

    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    response = extractor.list_number_of_stores(url, api_keys.storeHeader)

    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    store_data = extractor.retrieve_stores_data(url, api_keys.storeHeader, response)

    cleaned_data = cleaner.clean_store_data(store_data)

    dbConnector.upload_to_db(cleaned_data, "dim_store_details")


def upload_products_data():
    '''
    Cleans and uploads product data retrieved from a csv file in an s3 bucket
    '''

    link = "s3://data-handling-public/products.csv"
    products_data = extractor.extract_from_s3(link)
    cleaned_data = cleaner.clean_products_data(products_data)

    dbConnector.upload_to_db(cleaned_data, "dim_products")


def upload_orders_data():
    '''
    Cleans and uploads order data from a "orders_table" table in the connected DatabaseConnector instance
    '''

    orders_data = extractor.read_rds_table("orders_table", dbConnector)
    cleaned_data = cleaner.clean_orders_data(orders_data)

    dbConnector.upload_to_db(cleaned_data, "orders_table")


def upload_date_data():
    '''
    Cleans and uploads date data from a json file
    '''

    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    date_data = extractor.extract_json(url)
    cleaned_data = cleaner.clean_date_data(date_data)

    dbConnector.upload_to_db(cleaned_data, "dim_date_times")



if __name__ == "__main__":
    dbConnector = database_utils.DatabaseConnector()
    extractor = data_extraction.DataExtractor()
    cleaner = data_cleaning.DataCleaning()

    upload_date_data()