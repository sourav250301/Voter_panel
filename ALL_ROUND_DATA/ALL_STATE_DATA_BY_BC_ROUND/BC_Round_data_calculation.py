import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.Function_for_calculation import main_processing_function
from ALL_STATE_DATA_BY_BC_ROUND.read_BC_data_from_database import dataframe 
pd.set_option('display.max_columns', None)

print("BC Round data calculation start -------------------------------- :)")

if dataframe is not None:
    print("BC data Retrieve successfully in BC_ROUND_DATA_Calculation.py file")
    df=main_processing_function(dataframe)
else :
    print("Failed to retrieve data in BC_ROUND_DATA_Calculation.py")

df['ROUND']='BC'

# --------------------------------------------------------------------------------------------#
print(" ")
print("Total No of Rows is in BC Round Data before clean :-", df.shape[0])

df.drop(columns=['DN','PART_NO','Start_Time','DTMF_REP','RES2','CASTE','GENDER','AGE','RES_ACWISE_TOTAL','RANK_PER_ACWISE','RES_PCWISE_TOTAL','RANK_PER_PCWISE','RES_SEQWISE_TOTAL','RANK_PER_SEQ_WISE_AC','RES_SEQWISE_TOTAL_PC','RANK_PER_SEQ_WISE_PC'],inplace=True)
df.drop_duplicates(subset=['AC_ID','PC_ID','N_PARTY','RES1'],inplace=True)

#--------------------------------------------------------------------------------------------------#

print(" ")
print('After clean final dataframe is ................')
data=df[(df['AC_ID'] == 'OD-AC-10') & (df['RES1']=="BJP")].head(10)
print(data.to_string(index=False))
print(" ")
print("Total No of Rows is in BC Round Data:-", df.shape[0])

# print("BC Round data calculation completed -------------------------------- :)")
print('Script Executed Succesfully')
#--------------------------------------------------------------------------------------------------#
BC_Round_final_df=df