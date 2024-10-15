# Step 1: import libraries
import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
import numpy as np

#step 2:look for API information. Go to the documentation  and get how to use it
url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&x_cg_demo_api_key=CG-nd8o5FRUJDeFT64BPc6Yrd8A'
headers = {
  'Accepts': 'application/json',
  
}
#Step3:  fetch data, use  the method that documentacion API describe to use and replace the try sesion
def fetch_data_from_api(url):
    try:
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        print(f"The information from the API is: {data}")  # To check the structure
        print("Fetched data type:", type(data))
        dn = pd.json_normalize(data) #create dataframe with data
        dn = dn.replace({np.nan: None})
        print(f"The informationnormalized  from the API is: {dn}")  
        column_names = dn.columns.tolist()# get the name of the columns
        column_type=dn.dtypes  # get the type of the columns
        # Create a list to hold SQL-compatible data types
        column_type_sql = []
        # Map pandas data types to SQL data types
        for dtype, column_name in zip(column_type, column_names):
            if dtype == 'int64':
                max_value = dn[column_name].max()
                if max_value > 2147483647:  # Larger than INT range
                    column_type_sql.append('BIGINT')  # Use BIGINT for large numbers
                else: 
                    column_type_sql.append('INT')
            elif dtype == 'object':
                # loop through column values ​​to find floats
                contains_float = any(isinstance(val, float) for val in dn[column_name].dropna())
                print("Does it have float values?",column_names,contains_float)
                if contains_float:
                    column_type_sql.append('DECIMAL(20, 2)')
                else:
                    column_type_sql.append('VARCHAR(255)')
            elif dtype == 'float64':
                column_type_sql.append('DECIMAL(20, 2)')
            elif dtype == 'bool':
                column_type_sql.append('BOOLEAN')
            else:
                column_type_sql.append('TEXT')  # Default fallback for any other types
             # Print the results
        print("Column Names:", column_names)
        print("Column Types (SQL-compatible):", column_type_sql)
        print(f"dataframe:", dn)
        return dn, column_names, column_type_sql  # Return the DataFrame, column names, and SQL types

     # Parse JSON data from the API
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None



 # Step 4: Create the MySQL insert query string
def create_insert_query(column_names, column_type_sql):
    table_name = 'postss'  # Change this to your actual table name
    sanitized_column_names = [name.replace('.', '_') for name in column_names]
    columns_str = ', '.join(sanitized_column_names)  # Join the column names into a string
    placeholders = ', '.join(['%s'] * len(sanitized_column_names))  # Create placeholders for values
    # Combine column names and types into a column definition for SQL
    columns_definition = ", ".join([f"{name.replace('.', '_')} {dtype}" for name, dtype in zip(column_names, column_type_sql)])
    print("Generated column:",columns_definition)
    
    try:
        # Create the SQL CREATE TABLE query
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"  
        print("Generated table Query:", create_table_query) 
        # insert data in the TABLE on sql
        insert_query = f"""INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE 
        {', '.join([f'{column_names} = VALUES({column_names})' for column_names in sanitized_column_names])}; """
        print("Generated Query:", insert_query)
        return create_table_query, insert_query
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred line 71: {http_err}")
    except Exception as err:
        print(f"An error occurred line 73: {err}")
    return None

# Step 5: Connect to MySQL
def create_connection(host_name, user_name, user_password, db_name, dn, column_names,  column_type_sql):
    try:
        # Establish the database connection
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
         # Insert API data into MySQL table
        if  db_connection.is_connected():
            print("Connection to MySQL DB successful")
            db_info = db_connection.get_server_info()
            cursor = db_connection.cursor() 
            print(f"Server version: {db_info}")
            # Create the table first
            create_table_query, insert_query = create_insert_query(column_names,  column_type_sql )
            try:
                cursor.execute(create_table_query)  # Create the table
                db_connection.commit()  # Commit the transaction
                print("Table created successfully.")
                # Insert API data into MySQL table

             # Preparar los datos para inserción
                print("data to insert")               
                # Insertar los datos de la API en la tabla MySQL
                for index, row in dn.iterrows():
                    cursor.execute(insert_query,tuple(row) ) # Convertir cada fila a tupla

                print("Data inserted successfully.")

                db_connection.commit()  # Commit the transaction for the insert

                # Close the cursor
                cursor.close()
                db_connection.close()                 
            except mysql.connector.Error as e:
                print(f"Error: {e}")
            except ValueError as ve:
                print(f"ValueError: {ve} - Invalid 'id' encountered.")
            except Error as e:
                print(f"Error: '{e}' occurred while inserting data")
        else:    
            print("Failed to connect to the MySQL database.")
            return None
        return db_connection
               
    except Error as e:
        print(f"Error occurred: {e}")
        return None
      
    
if __name__ == "__main__":
    # API Fetch
    print("Fetching data from API...")
    data = fetch_data_from_api(url)

    if data:
        # MySQL Connection
        print("Connecting to MySQL...")

       
  
        db_connection = create_connection(
                '127.0.0.1',
                'root',   #  MySQL username
                'ikkDUD56*',  # MySQL password
                'retail_sales',#  database name
                dn=data[0], column_names=data[1], column_type_sql= data[2]
                
        )
        
        if db_connection:
             # Insert data into MySQL
            print("Inserting data into MySQL...")
            print(db_connection)
            # Close connection
            print("Data inserted and MySQL connection closed.")
            