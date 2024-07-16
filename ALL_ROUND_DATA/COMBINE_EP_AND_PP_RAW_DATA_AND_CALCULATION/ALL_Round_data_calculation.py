import pandas as pd
import numpy as np
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.Function_for_calculation import main_processing_function
from COMBINE_EP_AND_PP_RAW_DATA_AND_CALCULATION.read_ALLROUND_data_from_database import dataframe
pd.set_option('display.max_columns', None)

# csv_file_path="D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\all_round_combined_original_data.csv"
# COM_Round_df=pd.read_csv(csv_file_path,low_memory=False)

print("COM Round data calculation start -------------------------------- :)")
if dataframe is not None:
    print("ALL Round data retrives successively in ALL_Round_data_calculation.py file.")
    df=main_processing_function(dataframe)
else:
    print("Failed to retrieve all round data in all_Round_data_calculation.py file.")
    
df['ROUND']='COM'
# --------------------------------------------------------------------------------------------#
print(" ")
print("Total No of Rows is in COM Round Data before clean :-", df.shape[0])

df.drop(columns=['DN','PART_NO','Start_Time','DTMF_REP','RES2','CASTE','GENDER','AGE','RES_ACWISE_TOTAL','RANK_PER_ACWISE','RES_PCWISE_TOTAL','RANK_PER_PCWISE','RES_SEQWISE_TOTAL','RANK_PER_SEQ_WISE_AC','RES_SEQWISE_TOTAL_PC','RANK_PER_SEQ_WISE_PC'],inplace=True)
df.drop_duplicates(subset=['AC_ID','PC_ID','N_PARTY','RES1'],inplace=True)

#--------------------------------------------------------------------------------------------------#

print(" ")
print('After clean final dataframe is ................')
data=df[(df['AC_ID'] == 'OD-AC-10') & (df['RES1']=="BJP")].head(10)
print(data.to_string(index=False))
print(" ")
print("Total No of Rows is in COM Round Data:-", df.shape[0])

print("BC Round data calculation completed -------------------------------- :)")
#--------------------------------------------------------------------------------------------------#
COM_Round_final_df=df
# ----------------------------------------------------------------------------------------------------------------#
# def calculate_response(df):
#     print("res calculate start..")
#     df['RES_ACWISE'] = df.groupby(['AC_ID', 'RES1'])['DN'].transform('count')
    
#     df['RES_PCWISE'] = df.groupby(['PC_ID', 'RES1'])['DN'].transform('count')
    
#     df['RES_SEQWISE_AC'] = df.groupby(['AC_ID', 'N_PARTY', 'RES1'])   ['DN'].transform('count')
    
#     df['RES_SEQWISE_PC'] = df.groupby(['PC_ID', 'N_PARTY', 'RES1'])['DN'].transform('count')
    
#     if 'Start_Time' in df.columns:
#         # df['Start_Time'] = pd.to_datetime(df['Start_Time'], format='%d-%m-%Y')
#         df['RES_DATEWISE'] = df.groupby(['AC_ID', 'Start_Time', 'RES1'])['DN'].transform('count')
        
#     df['RES_REGIONWISE_AC'] = df.groupby(['AC_ID', 'REGION', 'RES1'])['DN'].transform('count')
  
#     print("response calculate completed...")
#     data=df[(df['AC_ID'] == 'OD-AC-100') & (df['RES1']=="BJP")].head(10)
#     print(data.to_string(index=False))
    
#     return df

# def calculate_pc_metrics(df):
#     print(" ")
#     print("pc wise calculation start.....")
#     # Calculate PC-wise total responses
#     pcwise_totals = df.groupby('PC_ID')['DN'].transform('count')
#     df['RES_PCWISE_TOTAL'] = pcwise_totals
    
#     # Calculate PC-wise percentage
#     df['PC_WISE_PERCENTAGE'] = ((df['RES_PCWISE'] / df['RES_PCWISE_TOTAL']) * 100).round(2)
    
#     pc_wise_grouped=df.groupby(['PC_ID','RES1']).agg(
#         RES_PCWISE_COUNT=('RES_PCWISE','sum'),
#         MAX_PER_PCWISE=('PC_WISE_PERCENTAGE','max')
#     ).reset_index()

#     max_pcwise_counts = pc_wise_grouped.loc[pc_wise_grouped.groupby('PC_ID')['RES_PCWISE_COUNT'].idxmax()]
#     max_pcwise_counts['DRAW'] = pc_wise_grouped.duplicated(['PC_ID', 'RES_PCWISE_COUNT'], keep=False)
#     max_pcwise_counts['WINNER_PCWISE'] = np.where(max_pcwise_counts['DRAW'], 'DRAW', max_pcwise_counts['RES1'])

#     df = df.merge(max_pcwise_counts[['PC_ID', 'WINNER_PCWISE', 'MAX_PER_PCWISE']], on='PC_ID', how='left')

#     # Calculate second maximum percentage PC-wise
#     df['RANK_PER_PCWISE'] = df.groupby('PC_ID')['PC_WISE_PERCENTAGE'].rank(method='dense', ascending=False)
#     second_highest_df = df[df['RANK_PER_PCWISE']==2]
#     df['2ND_PER_PCWISE'] = df['PC_ID'].map(second_highest_df.set_index('PC_ID')['PC_WISE_PERCENTAGE'].to_dict())
#     df['RUNNER_UP_PCWISE'] = df['PC_ID'].map(second_highest_df.set_index('PC_ID')['RES1'].to_dict())

#     # Calculate margin PC-wise
#     df['MARGIN_PCWISE'] = (df['MAX_PER_PCWISE'] - df['2ND_PER_PCWISE']).round(2)
    
#     df['MARGIN_GROUP_PCWISE'] = df['MARGIN_PCWISE'].apply(categorize_margin)
    
#     # Summarize column PC-wise
#     df['SUMMERY_PCWISE'] = np.where(df['WINNER_PCWISE'] == 'DRAW', 'DRAW', df['WINNER_PCWISE'] + ' ' + df['MARGIN_PCWISE'].astype(str) + ' (' + df['RUNNER_UP_PCWISE'] + ')')

#     print("PC wise calculation completed....")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))
#     return df

# def calculate_ac_metrics(df):
#     print(" ")
#     print("ac wise calculation start....")
#     # calulcate AC wise percentage
#     acwise_totals = df.groupby('AC_ID')['DN'].transform('count')
#     df['RES_ACWISE_TOTAL'] = acwise_totals
#     df['AC_WISE_PERCENTAGE'] = ((df['RES_ACWISE'] / df['RES_ACWISE_TOTAL']) * 100).round(2)

#     # Aggregate to get max count and percentage and handle draw check
#     acwise_grouped = df.groupby(['AC_ID', 'RES1']).agg(
#         RES_ACWISE_COUNT=('RES_ACWISE', 'sum'),
#         MAX_PER_ACWISE=('AC_WISE_PERCENTAGE', 'max')
#     ).reset_index()

#     max_acwise_counts = acwise_grouped.loc[acwise_grouped.groupby('AC_ID')['RES_ACWISE_COUNT'].idxmax()]
#     max_acwise_counts['DRAW'] = acwise_grouped.duplicated(['AC_ID', 'RES_ACWISE_COUNT'], keep=False)

#     max_acwise_counts['WINNER_ACWISE'] = np.where(max_acwise_counts['DRAW'], 'DRAW', max_acwise_counts['RES1'])

#     df = df.merge(max_acwise_counts[['AC_ID', 'WINNER_ACWISE', 'MAX_PER_ACWISE']], on='AC_ID', how='left')

#     # Determine the runner-up AC-wise
#     df['RANK_PER_ACWISE'] = df.groupby('AC_ID')['AC_WISE_PERCENTAGE'].rank(method='dense', ascending=False)
#     second_max_df = df[df['RANK_PER_ACWISE'] == 2]
#     df['2ND_PER_ACWISE'] = df['AC_ID'].map(second_max_df.set_index('AC_ID')['AC_WISE_PERCENTAGE'].to_dict())
#     df['RUNNER_UP_ACWISE'] = df['AC_ID'].map(second_max_df.set_index('AC_ID')['RES1'].to_dict())

#     # Calculate the margin and categorize it
#     df['MARGIN_ACWISE'] = (df['MAX_PER_ACWISE'] - df['2ND_PER_ACWISE']).round(2)
#     df['MARGIN_GROUP_ACWISE'] = df['MARGIN_ACWISE'].apply(categorize_margin)

#     # Summarize AC-wise
#     df['SUMMERY_ACWISE'] = np.where(df['WINNER_ACWISE'] == 'DRAW', 'DRAW', df['WINNER_ACWISE'] + ' ' + df['MARGIN_ACWISE'].astype(str) + ' (' + df['RUNNER_UP_ACWISE'] + ')')

#     print("AC wise calculation completed....")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))

#     return df
    
# def calculate_seq_ac_wise(df):
#     print(" ")
#     print("SEQ-wise AC calculation Start....")
#     seqwise_totals = df.groupby(['AC_ID', 'N_PARTY'])['DN'].transform('count')
#     df['RES_SEQWISE_TOTAL'] = seqwise_totals
#     df['SEQ_WISE_PERCENTAGE_AC'] = ((df['RES_SEQWISE_AC'] / df['RES_SEQWISE_TOTAL']) * 100).round(2)

#     seqwise_grouped = df.groupby(['AC_ID', 'N_PARTY', 'RES1']).agg(
#         RES_SEQWISE_COUNT=('RES_SEQWISE_AC', 'sum'),
#         MAX_PER_SEQWISE_AC=('SEQ_WISE_PERCENTAGE_AC', 'max')
#     ).reset_index()
    
#     max_seqwise_counts = seqwise_grouped.loc[seqwise_grouped.groupby(['AC_ID', 'N_PARTY'])['RES_SEQWISE_COUNT'].idxmax()]
#     max_seqwise_counts['DRAW'] = seqwise_grouped.duplicated(['AC_ID', 'N_PARTY', 'RES_SEQWISE_COUNT'], keep=False)
    
#     max_seqwise_counts['WINNER_SEQWISE_AC'] = np.where(max_seqwise_counts['DRAW'], 'DRAW', max_seqwise_counts['RES1'])

#     df = df.merge(max_seqwise_counts[['AC_ID', 'N_PARTY', 'WINNER_SEQWISE_AC', 'MAX_PER_SEQWISE_AC']], on=['AC_ID', 'N_PARTY'], how='left')

#     # Determine the runner-up SEQ-wise and second max percentage
#     df['RANK_PER_SEQ_WISE_AC'] = df.groupby(['AC_ID', 'N_PARTY'])['SEQ_WISE_PERCENTAGE_AC'].rank(method='dense', ascending=False)
#     second_max_df = df[df['RANK_PER_SEQ_WISE_AC'] == 2]
#     df['2ND_PER_SEQWISE_AC'] = df.set_index(['AC_ID', 'N_PARTY']).index.map(second_max_df.set_index(['AC_ID', 'N_PARTY'])['SEQ_WISE_PERCENTAGE_AC'].to_dict())
#     df['RUNNER_UP_SEQWISE_AC'] = df.set_index(['AC_ID', 'N_PARTY']).index.map(second_max_df.set_index(['AC_ID', 'N_PARTY'])['RES1'].to_dict())
    
#     # calculate margin SEQ wise for ac
#     df['MARGIN_SEQWISE_AC'] = (df['MAX_PER_SEQWISE_AC'] - df['2ND_PER_SEQWISE_AC']).round(2)

#     # margin group column SEQ wise for ac
#     df['MARGIN_GROUP_SEQWISE_AC'] = df['MARGIN_SEQWISE_AC'].apply(categorize_margin)

#     # summery column SEQ wise for ac
#     df['SUMMERY_SEQWISE_AC'] = np.where(df['WINNER_SEQWISE_AC'] == 'DRAW', 'DRAW', df['WINNER_SEQWISE_AC'] + ' ' + df['MARGIN_SEQWISE_AC'].astype(str) + ' (' + df['RUNNER_UP_SEQWISE_AC'] + ')')

#     print("SEQ-wise AC calculation completed....")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))

#     return df

# def calculate_seq_pc_wise(df):
#     print(" ")
#     print("SEQ_wise PC calculation start...........")
    
#     # calulcate SEQ wise percentage for pc
#     seq_wise_totals_pc = df.groupby(['PC_ID', 'N_PARTY'])['DN'].transform('count')
#     df['RES_SEQWISE_TOTAL_PC'] = seq_wise_totals_pc
#     df['SEQ_WISE_PERCENTAGE_PC'] = ((df['RES_SEQWISE_PC']/df['RES_SEQWISE_TOTAL_PC']) * 100).round(2)
    
#     seqwise_grouped = df.groupby(['PC_ID', 'N_PARTY', 'RES1']).agg(
#         RES_SEQWISE_COUNT=('RES_SEQWISE_PC', 'sum'),
#         MAX_PER_SEQWISE_PC=('SEQ_WISE_PERCENTAGE_PC', 'max')
#     ).reset_index()
    
#     max_seqwise_counts = seqwise_grouped.loc[seqwise_grouped.groupby(['PC_ID', 'N_PARTY'])['RES_SEQWISE_COUNT'].idxmax()]
#     max_seqwise_counts['DRAW'] = seqwise_grouped.duplicated(['PC_ID', 'N_PARTY', 'RES_SEQWISE_COUNT'], keep=False)

#     max_seqwise_counts['WINNER_SEQWISE_PC'] = np.where(max_seqwise_counts['DRAW'], 'DRAW', max_seqwise_counts['RES1'])

#     df = df.merge(max_seqwise_counts[['PC_ID', 'N_PARTY', 'WINNER_SEQWISE_PC', 'MAX_PER_SEQWISE_PC']], on=['PC_ID', 'N_PARTY'], how='left')

#     # Determine the runner-up SEQ-wise and second max percentage
#     df['RANK_PER_SEQ_WISE_PC'] = df.groupby(['PC_ID', 'N_PARTY'])['SEQ_WISE_PERCENTAGE_PC'].rank(method='dense', ascending=False)
#     second_max_df = df[df['RANK_PER_SEQ_WISE_PC'] == 2]
#     df['2ND_PER_SEQWISE_PC'] = df.set_index(['PC_ID', 'N_PARTY']).index.map(second_max_df.set_index(['PC_ID', 'N_PARTY'])['SEQ_WISE_PERCENTAGE_PC'].to_dict())
#     df['RUNNER_UP_SEQWISE_PC'] = df.set_index(['PC_ID', 'N_PARTY']).index.map(second_max_df.set_index(['PC_ID', 'N_PARTY'])['RES1'].to_dict())

#     # calculate margin SEQ wise for pc
#     df['MARGIN_SEQWISE_PC'] = (
#         df['MAX_PER_SEQWISE_PC'] - df['2ND_PER_SEQWISE_PC']).round(2)

#     # margin group column SEQ wise for pc
#     df['MARGIN_GROUP_SEQWISE_PC'] = df['MARGIN_SEQWISE_PC'].apply(
#         categorize_margin)

#     # summery column SEQ wise for ac
#     df['SUMMERY_SEQWISE_PC'] = np.where(df['WINNER_SEQWISE_PC'] == 'DRAW', 'DRAW', df['WINNER_SEQWISE_PC'] + ' ' + df['MARGIN_SEQWISE_PC'].astype(str) + ' (' + df['RUNNER_UP_SEQWISE_PC'] + ')')

#     print("SEQ-wise PC calculation completed....")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))
    
#     return df

# def caculate_party_name_datewise(df):
#     print(" ")
#     print("Start calculation maximum response datewise by AC_ID")
#     max_res_datewise = df.groupby(['AC_ID', 'Start_Time'])['RES_DATEWISE'].transform('max')
#     calculate_max_datewise = df['RES_DATEWISE'] == max_res_datewise
#     date_wise_map_ac = df[calculate_max_datewise].set_index(['AC_ID', 'Start_Time'])['RES1'].to_dict()
#     df['WINNER_DATEWISE_AC'] = df.set_index(['AC_ID', 'Start_Time']).index.map(date_wise_map_ac)
    
#     print("maximum response datewise by AC_ID calculation completed.........")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))
    
#     return df

# def calculate_region_wise(df):
#     print(" ")
#     print("Start Calculation Region wise.........")
    
#     max_res_region_wise=df.groupby(['AC_ID','REGION'])['RES_REGIONWISE_AC'].transform('max')
#     calculate_max_region_wise=df['RES_REGIONWISE_AC'] == max_res_region_wise
#     region_wise_map_ac=df[calculate_max_region_wise].set_index(['AC_ID','REGION'])['RES1'].to_dict()
#     df['WINNER_REGIONWISE_AC']=df.set_index(['AC_ID','REGION']).index.map(region_wise_map_ac)   
    
#     print("Calculation Region wise completed.......")
#     data=df[(df['AC_ID'] == 'OD-AC-100')].head(10)
#     print(data.to_string(index=False))
    
#     return df

# def categorize_margin(margin):
#         if 0 <= margin <= 5.00:
#             return '0-5%'
#         elif 5.10 <= margin <= 10.00:
#             return '6-10%'
#         elif 10.10 <= margin <= 20.00:
#             return '11-20%'
#         elif 20.10 <= margin <= 30.00:
#             return '21-30%'
#         else:
#             return 'Above 30%'
        
# def main_processing_function(df):
#     df=calculate_response(df)
#     df = calculate_ac_metrics(df)
#     df = calculate_pc_metrics(df)
#     df=calculate_seq_ac_wise(df)
#     df=calculate_seq_pc_wise(df)
#     df=caculate_party_name_datewise(df)
#     df=calculate_region_wise(df)
#     return df


# df=main_processing_function(df)
# # ----------------------------------------------------------------------------------------------------------------#

# df.drop(columns=['DN','PART_NO','Start_Time','DTMF_REP','RES2','CASTE','GENDER','AGE','RES_ACWISE_TOTAL','RANK_PER_ACWISE','RES_PCWISE_TOTAL','RANK_PER_PCWISE','RES_SEQWISE_TOTAL','RANK_PER_SEQ_WISE_AC','RES_SEQWISE_TOTAL_PC','RANK_PER_SEQ_WISE_PC'],inplace=True)
# # ----------------------------------------------------------------------------------------------------------------#

# df.drop_duplicates(subset=['AC_ID','PC_ID','N_PARTY','RES1'],inplace=True)
# # ----------------------------------------------------------------------------------------------------------------#

# print(" ")
# print("Final distnict table data .................")
# data=df[(df['AC_ID'] == 'OD-AC-100') & (df['RES1']=="BJP")].head(10)
# print(data.to_string(index=False))
# print(df.shape[0])
# # print(df.columns)
# # data=df.head(10)    
# # print(data.to_string(index=False))
# # df.to_csv('D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\all_round_combined_data.csv',index=False)
# # print("CSV generated successfully :)")

# final_df=df


