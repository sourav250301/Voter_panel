import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from DECREASE_FROM_MAX_PERCETAGE_AC_WISE.For_BC_Round import BC_decrease_max_per_ac_wise_df as df1
from DECREASE_FROM_MAX_PERCETAGE_AC_WISE.For_EP_Round import EP_decrease_max_per_ac_wise_df as df2
from DECREASE_FROM_MAX_PERCETAGE_AC_WISE.For_PP_Round import PP_decrease_max_per_ac_wise_df as df3

final_merge_Decrease_max_per_ac_wise_df = pd.concat([df1,df2,df3],ignore_index=True)

