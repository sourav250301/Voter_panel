import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from DECREASE_FROM_MAX_PERCETAGE_AC_WISE.merge_indivisual_round import final_merge_Decrease_max_per_ac_wise_df as df1
from DECREASE_FROM_MAX_PERCETAGE_AC_WISE.for_COM_Round import COM_decrease_max_per_ac_wise_df as df2

decreas_max_per_ac_wise_final_df=pd.concat([df1, df2],ignore_index=True)