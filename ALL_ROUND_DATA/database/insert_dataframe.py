import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from all_round_combined_and_merge_data import final_df


server = '192.168.0.27'
database = 'voter'
username = 'sa'
password = 'NXT@LKJHGFDSA'
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()
connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
engine = create_engine(connection_url)

# -----------------------------------------------------------------------------#




#-----------------------------------------------------------------------------#
table_name = 'ALL_STATE_EP_PP_BC_COM_ROUND_DATA'

batch_size = 10000  
first_batch = True
total_rows = len(final_df)
inserted_rows = 0

for start in range(0, total_rows, batch_size):
    end = start + batch_size
    df_batch = final_df.iloc[start:end]
    
    if first_batch:
        df_batch.to_sql(table_name, engine, if_exists='replace', index=False)
        first_batch = False
    else:
        df_batch.to_sql(table_name, engine, if_exists='append', index=False)
    
    inserted_rows += len(df_batch)
    print(f"Inserted batch from row {start} to {end}. Total inserted rows: {inserted_rows}/{total_rows}")

print("Dataframe Inseted in Database Successfully ...... :)")

#-----------------------------------------------------------------------------#
engine.dispose()
cursor.close()
connection.close()


