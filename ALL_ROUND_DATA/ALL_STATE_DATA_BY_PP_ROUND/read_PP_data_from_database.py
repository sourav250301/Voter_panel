import pyodbc
import pandas as pd
from ALL_STATE_DATA_BY_PP_ROUND import queries_PP
pd.set_option('display.max_columns', None)

# Function to fetch all query variables from query_Ep module
def fetch_all_queries_from_module(module):
    queries = []
    for name in dir(module):
        if not name.startswith('__'):
            query_str = getattr(module, name)
            if isinstance(query_str, str):  # Ensure it's a string (query)
                queries.append(query_str.strip())
    return queries

# Main function to execute queries and save results in a DataFrame
def execute_queries_and_save_to_dataframe(server, database, username, password):
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    results = []
    try:
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            queries = fetch_all_queries_from_module(queries_PP)
            for query in queries:
                cursor.execute(query)
                data = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                df = pd.DataFrame.from_records(data, columns=columns)
                results.append(df)
                
             # Additional SQL query directly executed against database
            sql_query = 'SELECT * FROM PY_TEST_INDIA_DATA'
            cursor.execute(sql_query)
            data = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df_sql = pd.DataFrame.from_records(data, columns=columns)
            df_sql.rename(columns={'PC_REGION': 'REGION'}, inplace=True)
            df_sql['AC_ID'] = df_sql['State_Code'] + '-AC-' + df_sql['AC_NO'].astype(str)
            df2_reduced = df_sql[['AC_ID', 'PC_ID', 'REGION']]

        if results:
            combined_df = pd.concat(results, ignore_index=True)
            final_df = pd.merge(combined_df, df2_reduced, how='left', on=['AC_ID', 'PC_ID'])            
            return final_df
        else:
            return None
    except pyodbc.Error as e:
        print(f"Database error occurred: {e}")
        return None
    
 
server = '192.168.0.27'
database = 'voter'
username = 'sa'
password = 'NXT@LKJHGFDSA'

dataframe = execute_queries_and_save_to_dataframe(server, database, username, password)


