import sys
import os
import pandas as pd
pd.set_option('display.max_columns', None)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Dynamic_N_party_column.merge_indivisual_round_data import final_merge_N_party_data as df1
from Dynamic_N_party_column.combined_Round_data import COM_N_Party_data as df2

final_all_round_N_Party_combined_data=pd.concat([df1,df2],ignore_index=True)
