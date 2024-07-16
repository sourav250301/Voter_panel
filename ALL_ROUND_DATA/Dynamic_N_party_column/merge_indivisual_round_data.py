import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Dynamic_N_party_column.BC_Round_data import BC_Round_N_party_data as df1
from Dynamic_N_party_column.EP_Round_data import EP_Round_N_party_data as df2
from Dynamic_N_party_column.PP_Round_data import PP_Round_N_party_data as df3
pd.set_option("display.max_columns",None)

final_merge_N_party_data = pd.concat([df1, df2, df3],ignore_index=True)

print(" ")
print("total no of rows in final_merge_N_party_data :- ",final_merge_N_party_data.shape[0])
data=final_merge_N_party_data[(final_merge_N_party_data['AC_ID']=="OD-AC-10")]
print(data.to_string(index=False))



# ------------------------------- Execution Time ---------------------------------------------------- #
# 11 minutes