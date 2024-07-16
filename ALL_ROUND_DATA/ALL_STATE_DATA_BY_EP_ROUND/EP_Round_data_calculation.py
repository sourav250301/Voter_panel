import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.Function_for_calculation import main_processing_function
from ALL_STATE_DATA_BY_EP_ROUND.read_EP_data_from_database import dataframe
pd.set_option('display.max_columns', None)

# csv_file_path="D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\EP_Round\\EP_Round_all_data.csv"
# EP_Round_df=pd.read_csv(csv_file_path,low_memory=False)

print("EP Round data calculation start -------------------------------- :)")
if dataframe is not None:
    print("EP data Retrieve successfully in EP_ROUND_DATA_Calculation.py file")
    df=main_processing_function(dataframe)
else :
    print("Failed to retrieve data in EP_ROUND_DATA_Calculation.py")
df['ROUND']='EP'

# --------------------------------------------------------------------------------------------#
print(" ")
print("Total No of Rows is in EP Round Data before clean :-", df.shape[0])

df.drop(columns=['DN','PART_NO','Start_Time','DTMF_REP','RES2','CASTE','GENDER','AGE','RES_ACWISE_TOTAL','RANK_PER_ACWISE','RES_PCWISE_TOTAL','RANK_PER_PCWISE','RES_SEQWISE_TOTAL','RANK_PER_SEQ_WISE_AC','RES_SEQWISE_TOTAL_PC','RANK_PER_SEQ_WISE_PC'],inplace=True)
df.drop_duplicates(subset=['AC_ID','PC_ID','N_PARTY','RES1'],inplace=True)

#--------------------------------------------------------------------------------------------------#

print(" ")
print('After clean final dataframe is ................')
data=df[(df['AC_ID'] == 'OD-AC-10') & (df['RES1']=="BJP")].head(10)
print(data.to_string(index=False))
print(" ")
print("Total No of Rows is in EP Round Data:-", df.shape[0])
print("EP Round data calculation completed -------------------------------- :)")

#--------------------------------------------------------------------------------------------------#
EP_Round_final_df=df