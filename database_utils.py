
#Connect with and upload data to database
class DatabaseConnector:
    #Define self parameters
    def __init__(self , table_names = [], yaml_name = '/Users/isaachall/Desktop/MRDC/db_creds.yaml'):
        import yaml
        self.yaml_name = yaml_name
        #Database credentials- we will connect and extract data from here
        self.db_creds = open(yaml_name, 'r')
        self.data_loaded = yaml.safe_load(self.db_creds)
        self.DATABASE_TYPE = self.data_loaded["RDS_DATABASE"]
        self.DBAPI = 'psycopg2'
        self.USER = self.data_loaded['RDS_USER']
        self.PASSWORD = self.data_loaded['RDS_PASSWORD']
        self.HOST = self.data_loaded['RDS_HOST']
        self.PORT = self.data_loaded['RDS_PORT']
        self.DATABASE = self.data_loaded['RDS_DB_NAME']
        #Table names of the database we are connecting to 
        self.table_names = table_names
      
    #Read a YAML file and convert it to a dictionary
    def read_db_creds(self):
        import yaml
        yaml_name = self.yaml_name
        #Read the YAML file and load the contents to a variable
        with open(yaml_name, 'r') as db_creds:
            self.data_loaded = yaml.safe_load(db_creds)
        
    #Read the dict created by read_db_creds and connect to the database corresponding to the credentials provided
    def init_db_engine(self):
        from sqlalchemy import create_engine
        #Connect to the database using credentials from the YAML file set to our variables
        self.engine = create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        connection = self.engine.connect()
    
    #List the table headers from a database    
    def list_db_tables(self):
        from sqlalchemy import inspect
        inspector = inspect(self.engine)
        self.table_names = inspector.get_table_names()
        return self.table_names
    
    #Take a pandas dataframe as an argument, and upload to our database schema 'sales_data'
    def upload_to_db(self):
        import pandas as pd
        from sqlalchemy import create_engine
        from data_cleaning import DataCleaning
        from data_extraction import DataExtractor
        import yaml
        #User specifies what data is being uploaded- determines which method to use to clean the data
        filetype = input("Please specify the data to be cleaned and uploaded (type user_data, card_data, store_data, products_data, orders_data or date-events_data): ")
        if filetype == 'user_data':
            upload = DataCleaning()
            data_upload = upload.clean_user_data()
        elif filetype == 'card_data':
            upload = DataCleaning()
            data_upload = upload.clean_card_data()
        elif filetype == 'store_data': 
            upload = DataCleaning()
            data_upload = upload.clean_store_data()
        elif filetype == 'products_data':
            products_df = DataExtractor().extract_from_s3()
            data_upload = DataCleaning.clean_products_data(products_df)
        elif filetype == 'orders_data':
            upload = DataCleaning()
            data_upload = upload.clean_orders_data()
        elif filetype == 'date-events_data':
            upload = DataCleaning()
            data_upload = upload.clean_date_details_data()
        #Throws an error if the user enters invalid data type    
        else: 
            ValueError   
        #Connect to the sales_data database and specify table name to be uploaded
        table_name = input("Please input the table name to be uploaded: ")
        DB_TYPE = 'postgresql'
        UPDBAPI = 'psycopg2'
        DBHOST = 'localhost'
        USER = 'postgres'
        PASSWORD2 = 'I1s9a9a7c1997!'
        DATABASE = 'sales_data'
        PORT = 5432
        
        sales_engine = create_engine(f"{DB_TYPE}+{UPDBAPI}://{USER}:{PASSWORD2}@{DBHOST}:{PORT}/{DATABASE}")
        sales_connect = sales_engine.connect()
        #Upload data to sales_data database
        try:
            data_upload.to_sql(f"{table_name}", sales_engine, if_exists='replace')
            sales_connect.close()
        except Exception as e:
            print(f"Error uploading data to database: {e}")
        


#Assign the class method to a variable and call the method upload_to_db in order to upload a table
upload_db = DatabaseConnector()
upload_db.upload_to_db()

