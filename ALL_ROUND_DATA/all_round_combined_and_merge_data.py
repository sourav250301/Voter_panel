from COMBINE_EP_AND_PP_RAW_DATA_AND_CALCULATION.calculate_VS_and_VM_status import calculate_wm_vs_status_df 
from merge_EP_PP_AND_BC_ROUND_DATA import final_merging_df
import pandas as pd

print(calculate_wm_vs_status_df.shape[0])
print(final_merging_df.shape[0])


df=pd.concat([calculate_wm_vs_status_df,final_merging_df],ignore_index=True)
df.drop(columns=['RES_DATEWISE'],inplace=True)
# ------------------------------------------------------------------------------------------------ #
final_df=df
print("shape of final df is : ",final_df.shape[0])


print(final_df.columns)
print(" ")
print(final_df['ROUND'].unique())