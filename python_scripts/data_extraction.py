#Extract and clean data from multiple sources
class DataExtractor:
   
    import pandas as pd
    #__init__ magic method
    def __init__(self, dataframes = {}, link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", tables = [], store_numbers= list(range(0, 452)), store_dataframes = {}):
        from database_utils import DatabaseConnector
        self.dataframes = dataframes
        self.link = link 
        self.tables = tables
        self.store_numbers = store_numbers
        self.store_dataframes = store_dataframes
       

    #Read the data in a table, and return the table name(s)  
    def read_rds_data(self):
        from database_utils import DatabaseConnector
        db_connector = DatabaseConnector()
        db_connector.read_db_creds()
        db_connector.init_db_engine()
        self.tables = db_connector.list_db_tables()
        return self.tables

    #Read the table names from read_rds_data in list format, and return pandas dataframe corresponding to the table name given
    def read_rds_table(self, db_connector):   
        import pandas as pd
         # Initialize a dictionary to store the dataframes
        self.dataframes = {}
        # Iterate through the table names and read each table into a dataframe
        for table_name in self.tables:
            query = f"SELECT * FROM {table_name}"
            dataframe = pd.read_sql(query, db_connector.engine)
            # Store the dataframe in the dictionary using the table name as the key
            self.dataframes[table_name] = dataframe    
        return self.dataframes


    #Read link to pdf file and convert into a pandas dataframe
    def retrieve_pdf_data(self):
        import tabula
        import pandas as pd
        upload_link = self.link 
        #Read all pages in the pdf
        pdf_data = tabula.read_pdf(upload_link, pages ='all')
        #Convert data in pdf to pandas dataframe
        pdf_dataframe = pd.concat(pdf_data, ignore_index= True)
        return pdf_dataframe

    #Method for api GET request to link with the number of stores 
    def list_number_of_stores():
        import requests
        import json
        #URL for the GET request
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        #x-api-key to access data
        api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        #initialise header- dict with x-api-key
        headers = {
            "x-api-key": api_key
        }
        #GET response from link
        response = requests.get(url, headers=headers)
        #Check whether the request was successful
        if response.status_code == 200:
            print("request success")
            print(response.json())
        else:
            print(f"Error, {response.status_code}")

    #api GET request to receive data regarding stores and convert to pandas dataframe
    def retrieve_stores_data(self):
        import requests 
        import pandas as pd
        #variable for retrieving store numbers- list containing values from 1 to 451
        store_number = self.store_numbers
        #empty dict to store dataframes
        self.store_dataframes = {}
        #URL for the GET request
        store_info_url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        #Initialise header dict with x-api-key
        headers = {
            "x-api-key" : api_key
        }
        #GET response from url- iterates through the store numbers and returns a dataframe for each
        for number in self.store_numbers:
            #URL for the GET request
            store_info_url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{number}"
            #initialise response from API
            response = requests.get(store_info_url, headers=headers)
            #Check whether response is successful
            if response.status_code == 200:
                #Convert response to pandas dataframe
                store_data = pd.DataFrame([response.json()])
                #Store the dataframe in a dict corresponding to the store index number
                self.store_dataframes[number] = store_data
            else:
                print(f"Error, {response.status_code}")
        #Combine these into single dataframe
        all_store_data = pd.concat(self.store_dataframes)   
        return all_store_data

    #Method to extract products data from an AWS S3 bucket
    def extract_from_s3(self):
        import boto3
        import pandas as pd
        s3 = boto3.client('s3')
        #Download file locally
        s3.download_file('data-handling-public', 'products.csv', '/Users/isaachall/Desktop/MRDC/products.csv')
        #Extract local csv file into pandas dataframe
        products_csv = pd.read_csv('/Users/isaachall/Desktop/MRDC/products.csv')
        products_df = pd.DataFrame(products_csv)
        return products_df

    #Extract json data from an s3 bucket and convert to pandas dataframe
    def extract_json_from_s3(self):
        import boto3
        import pandas as pd
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', '/Users/isaachall/Desktop/MRDC/date_details.json')
        date_details = pd.read_json('/Users/isaachall/Desktop/MRDC/date_details.json')
        return date_details
        

