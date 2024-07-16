from .ALL_Round_data_calculation import COM_Round_final_df as df
import pandas as pd
pd.set_option('display.max_columns', None)

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

df['VS_STATUS_AC']=df['MAX_PER_ACWISE'].apply(calculate_vs_status)
df['VS_STATUS_PC']=df['MAX_PER_PCWISE'].apply(calculate_vs_status)

df['WM_STATUS_AC']=df['MARGIN_ACWISE'].apply(calculate_wm_status)
df['WM_STATUS_PC']=df['MARGIN_PCWISE'].apply(calculate_wm_status)




data=df[(df['AC_ID']=="OD-AC-10")& (df['ROUND']=="COM")&(df['RES1']=="BJP")]
print(data.to_string(index=False))


# -------------------------------------------------------------------------------------------------------------------- #
calculate_wm_vs_status_df=df
    