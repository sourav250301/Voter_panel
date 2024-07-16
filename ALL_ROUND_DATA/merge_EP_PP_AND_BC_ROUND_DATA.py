import pandas as pd

from ALL_STATE_DATA_BY_BC_ROUND.BC_Round_data_calculation import BC_Round_final_df as df1
from ALL_STATE_DATA_BY_EP_ROUND.EP_Round_data_calculation import EP_Round_final_df as df2
from ALL_STATE_DATA_BY_PP_ROUND.PP_Round_data_calculation import PP_Round_final_df as df3

final_merged_df = pd.concat([df1, df2,df3], ignore_index=True)

#  ------------------------------------------------------------------------------------------------#

def calculate_vs_status(percentage):
    if 0 <= percentage <= 40 :
        return "VS-40%"
    elif 40 < percentage <= 42 :
        return "VS+40%"
    elif 42 < percentage <=45 :
        return "VS+42%"
    elif 45 < percentage <=50 :
        return "VS+45%"
    else:
        return "VS+50%"

def calculate_wm_status(margin):
    if 0<= margin <=5 :
        return "W-M 0-5%"
    elif 5< margin <=10 :
        return "W-M 6-10%" 
    elif 10 < margin <=15 :
        return "W-M 11-15%"
    else :
        return "W-M +15%"
    

final_merged_df['VS_STATUS_AC']=final_merged_df['MAX_PER_ACWISE'].apply(calculate_vs_status)
final_merged_df['VS_STATUS_PC']=final_merged_df['MAX_PER_PCWISE'].apply(calculate_vs_status)

final_merged_df['WM_STATUS_AC']=final_merged_df['MARGIN_ACWISE'].apply(calculate_wm_status)
final_merged_df['WM_STATUS_PC']=final_merged_df['MARGIN_PCWISE'].apply(calculate_wm_status)




# ------------------------------------------------------------------------------------------------ #
print(final_merged_df.columns)  
data=final_merged_df[(final_merged_df['AC_ID']=='HR-AC-13') & (final_merged_df['ROUND']=="EP")].head(10)
print(data.to_string(index=False))
print(" ")
print(final_merged_df.shape[0])
    
# final_merged_df.to_csv("D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\indivisual_res_based_all_round_merge_data.csv",index=False)
# print("csv generated successfully ....... :)")
# ------------------------------------------------------------------------------------------------------------ #
final_merging_df=final_merged_df
