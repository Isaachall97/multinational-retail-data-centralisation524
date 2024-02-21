#Clean data from each of the data sources
class DataCleaning:
    
    #Clean data related to users
    def clean_user_data(self):
        import pandas as pd
        import numpy as np
        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor

        # Initialize DatabaseConnector
        db_connector = DatabaseConnector()
        db_connector.read_db_creds()
        db_connector.init_db_engine()

        # Initialize DataExtractor
        data_extractor = DataExtractor()
        # Use DatabaseConnector to list tables and store them in DataExtractor
        data_extractor.tables = db_connector.list_db_tables()
        print(db_connector.list_db_tables())

        # Use the corrected read_rds_table method to read tables into pandas DataFrames
        dataframes = data_extractor.read_rds_table(db_connector)
        #Call an instance of the dataframes variable, where the table name is 'legacy_users' - this contains info on the users 
        table_df = dataframes[f'{input("Please enter a table name from above: ")}']
        #Drop null values
        dropna_df = table_df.dropna()
        #Convert all columns containing dates to standard YYYY-MM-DD format
        
        for dob in dropna_df['date_of_birth']:
            try:
                dropna_df['date_of_birth'] = pd.to_datetime(dropna_df['date_of_birth'], format='mixed')
            except ValueError:
                dropna_df['date_of_birth'] = 0
        
        for jdate in dropna_df['join_date']:
            try:
                dropna_df['join_date'] = pd.to_datetime(dropna_df['join_date'], format='mixed')
            except ValueError:
                dropna_df['join_date'] = 0
       
        # Capitalize first and last names
        dropna_df['first_name'] = dropna_df['first_name'].str.capitalize()
        dropna_df['last_name'] = dropna_df['last_name'].str.capitalize()

        # Remove duplicate user_uuid values
        dropna_df = dropna_df.drop_duplicates(subset=['user_uuid'])

        # Capitalize country codes and ensure they are two letters long
        dropna_df['country_code'] = dropna_df['country_code'].str.upper()

        # Format phone numbers (example format: "+XX (XXX) XXX XXXX")
        dropna_df['phone_number'] = dropna_df['phone_number'].str.replace(r'^\+?(\d{2})(\d{3})(\d{3})(\d{4})$', r'+\1 (\2) \3 \4')
        
        return dropna_df

    #Cleans data relating to card details
    def clean_card_data(self):
        from data_extraction import DataExtractor
        import numpy as np
        import pandas as pd

        #Call methods from DataExtractor() to get pandas dataframe 'card_data'
        data_to_clean = DataExtractor()
        card_data = data_to_clean.retrieve_pdf_data()

        # Remove rows with card number lengths outside the typical range (13 to 19 digits)
        card_data['card_number_length'] = card_data['card_number'].apply(lambda x: len(str(x).replace(" ", "").replace("-", "")))
        card_data = card_data[(card_data['card_number_length'] >= 13) & (card_data['card_number_length'] <= 19)]

        # Convert 'expiry_date' to datetime, assuming earlier steps removed or corrected anomalies
        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], format='%m/%y', errors='coerce')

        # Convert 'date_payment_confirmed' to datetime
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='coerce')

        #Remove any further null values
        card_data = card_data.dropna()

        #Return cleaned dataframe                
        return card_data

    #Clean data relating to store details
    def clean_store_data(self):
        from data_extraction import DataExtractor
        import numpy as np
        import pandas as pd

        #Call an instance of DataExtractor() to initialise pandas dataframe 'store_data'
        all_data = DataExtractor()
        store_data = all_data.retrieve_stores_data()
        
        # Handle missing values (Example: Drop rows where 'address' is missing)
        store_data.dropna(subset=['address'], inplace=True)
        #  fill missing numeric values with a placeholder like 0 or mean
        longitude_mean = pd.to_numeric(store_data['longitude'], errors='coerce').mean()
        store_data['longitude'] = pd.to_numeric(store_data['longitude'], errors='coerce').fillna(longitude_mean)
        #  Data type conversion
        store_data['longitude'] = pd.to_numeric(store_data['longitude'], errors='coerce')
        store_data['latitude'] = pd.to_numeric(store_data['latitude'], errors='coerce')
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce', downcast='integer')

        # Check for and handle missing values, for example:
        store_data.dropna(subset=['address', 'locality', 'store_code'], inplace=True)

        # Convert 'opening_date' to datetime format
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors='coerce')

        #Ensure store type is capitalised
        store_data['store_type'] = store_data['store_type'].str.title()

        #   Convert 'staff_numbers' to integer (after handling missing values)
        store_data['staff_numbers'] = store_data['staff_numbers'].fillna(0).astype(int)

        # Check for duplicate 'store_code'
        duplicates = store_data['store_code'].duplicated().sum()
        # If duplicates exist, consider removing or investigating them
        if duplicates > 0:
            store_data = store_data.drop_duplicates(subset=['store_code'])

        # Standardize 'store_type'
        store_data['store_type'] = store_data['store_type'].str.strip().str.capitalize()

        # Check for erroneous 'latitude' and 'longitude' values
        # Example: Filter out invalid latitude and longitude values
        store_data = store_data[(store_data['latitude'].between(-90, 90)) & (store_data['longitude'].between(-180, 180))]

        #Return the cleaned dataframe
        return store_data
    
    #Convert all product weights in products_data to kg
    def convert_product_weights(self):
        from data_extraction import DataExtractor
        import pandas as pd

        #call DataExtractor() method to initialise pandas dataframe containing product data
        products = DataExtractor()
        products_df = products.extract_from_s3()

        # Define a helper function for converting each weight value
        def convert_weight(row):
            weight = row['weight']
            if pd.isna(weight):
                return None
            cleaned_weight = ''.join(filter(lambda x: x.isdigit() or x in ['g', 'k', '.'], weight.lower()))
            if 'kg' in cleaned_weight:
                return float(cleaned_weight.replace('kg', ''))
            elif 'g' in cleaned_weight:
                return float(cleaned_weight.replace('g', '')) / 1000
            else:
                return float(cleaned_weight) / 1000  # Assuming 'g' if no unit

        # Apply the conversion function to each row of the DataFrame
        products_df['converted_weight'] = products_df.apply(convert_weight, axis=1)
        return products_df
    
        #Further clean products_df before uploading 
    def clean_products_data(products_df):
        from data_extraction import DataExtractor
        import pandas as pd
        
        #Drop any columns with no entries
        products_df.dropna(inplace=True)
        #Standardise text columns
        products_df['product_name'] = products_df['product_name'].str.lower().str.strip()
        products_df['category'] = products_df['category'].str.lower().str.strip()
        # Convert price to numeric, removing the currency symbol and handling non-numeric values
        products_df['product_price'] = products_df['product_price'].str.replace('£', '', regex=False)
        products_df['product_price'] = pd.to_numeric(products_df['product_price'], errors='coerce')
        # Convert date_added to datetime
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='mixed', errors='coerce')
        #Drop any duplicate data
        products_df.drop_duplicates(inplace=True)
        #Ensure all ID's are unique
        assert products_df['uuid'].is_unique
        assert products_df['EAN'].is_unique
        assert products_df['product_code'].is_unique
        #Return the cleaned dataframe
        return products_df

    #Method to clean the data from orders_table 
    def clean_orders_data(self):
        from data_extraction import DataExtractor
        from database_utils import DatabaseConnector
        import pandas as pd
        # Initialize DatabaseConnector
        db_connector = DatabaseConnector()
        db_connector.read_db_creds()
        db_connector.init_db_engine()
        # Initialize DataExtractor
        data_extractor = DataExtractor()
        # Use DatabaseConnector to list tables and store them in DataExtractor    
        data_extractor.tables = db_connector.list_db_tables()
        # Use the corrected read_rds_table method to read tables into pandas DataFrames
        dataframes = data_extractor.read_rds_table(db_connector)
        #Call an instance of the dataframes variable, where the table name is 'orders_table' - this contains info on the orders 
        table_df = dataframes['orders_table']    
        # Remove unnecessary columns
        columns_to_remove = ['first_name', 'last_name', '1', 'level_0']
        table_df = table_df.drop(columns=columns_to_remove)

        # Check for and remove duplicate rows
        table_df = table_df.drop_duplicates()

        return table_df

    def clean_date_details_data(self):
        from data_extraction import DataExtractor
        import pandas as pd

        #Call DataExtractor() method to read data into pandas dataframe
        date = DataExtractor()
        date_details = date.extract_json_from_s3()
        
        #Ensure 'month' is numeric and between 1 and 12
        date_details['month'] = pd.to_numeric(date_details['month'], errors='coerce')  # Convert to numeric, non-numeric becomes NaN
        date_details = date_details.dropna(subset=['month'])  # Drop rows where 'month' became NaN
        date_details = date_details[(date_details['month'] >= 1) & (date_details['month'] <= 12)]

        #Ensure 'year' is numeric and between 0000 and 2025
        date_details['year'] = pd.to_numeric(date_details['year'], errors='coerce')  # Convert to numeric
        date_details = date_details.dropna(subset=['year'])  # Drop rows where 'year' became NaN
        date_details = date_details[(date_details['year'] >= 0) & (date_details['year'] < 2025)]

        #Ensure 'day' is numeric and between 1 and 31
        date_details['day'] = pd.to_numeric(date_details['day'], errors='coerce')  # Convert to numeric
        date_details = date_details.dropna(subset=['day'])  # Drop rows where 'day' became NaN
        date_details = date_details[(date_details['day'] >= 1) & (date_details['day'] <= 31)]

        #Time period first letter uppercase
        date_details['time_period'] = date_details['time_period'].str.capitalize()

        #Define the allowed values for 'time_period'
        allowed_time_periods = ['Morning', 'Midday', 'Evening', 'Late_hours']

        #Filter the DataFrame to only include rows where 'time_period' matches one of the allowed values
        date_details = date_details[date_details['time_period'].isin(allowed_time_periods)]

        #Remove duplicates based on 'date_uuid'
        date_details = date_details.drop_duplicates(subset=['date_uuid'])

        return date_details

