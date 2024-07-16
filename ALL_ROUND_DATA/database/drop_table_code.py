import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib

server = '192.168.0.27'
database = 'voter'
username = 'sa'
password = 'NXT@LKJHGFDSA'
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={
    server};DATABASE={database};UID={username};PWD={password}"

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()
connection_url = f"mssql+pyodbc:///?odbc_connect={
    urllib.parse.quote_plus(connection_string)}"
engine = create_engine(connection_url)

# -----------------------------------------------------------------------------#



#-----------------------------------------------------------------------------#
# table_name = 'ALL_STATE_AC_PC_REGION_SEQ_WISE_DATA'
# table_name = 'ALL_STATE_EP_PP_ROUNDS_DATA'
# table_name = 'ALL_STATE_COMBINED_EP_PP_DATA'
table_name = 'ALL_STATE_EP_PP_BC_ROUNDS_DATA'

drop_query = f"DROP TABLE {table_name}"
cursor.execute(drop_query)
print("Dataframe dropped successfully from database ..... :)")
connection.commit()

#-----------------------------------------------------------------------------#
engine.dispose()
cursor.close()
connection.close()


