import sys
import os
import pandas as pd
pd.set_option('display.max_columns', None)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


from ALL_STATE_DATA_BY_BC_ROUND.BC_Round_data_calculation import BC_Round_final_df as df 

def create_party_columns_grouped(df):
    result_df = pd.DataFrame(index=df.index)
    
    grouped = df.groupby('AC_ID')
    
    for ac_id, group_df in grouped:
        unique_parties = group_df['N_PARTY'].unique()
        
        for party in unique_parties:
            column_name = f"{party}_SEQ"  
            
            filtered_data = group_df[group_df['N_PARTY'] == party]
            result_df.loc[filtered_data.index, column_name] = filtered_data['SUMMERY_SEQWISE_AC'].values
    df_with_columns = pd.concat([df, result_df], axis=1)
    df_with_columns.drop(columns=[ 'AC_NO', 'PC_NO', 'N_PARTY', 'RES1',
       'REGION', 'RES_ACWISE', 'RES_PCWISE', 'RES_SEQWISE_AC',
       'RES_SEQWISE_PC', 'RES_DATEWISE', 'RES_REGIONWISE_AC',
       'AC_WISE_PERCENTAGE', 'WINNER_ACWISE', 'MAX_PER_ACWISE',
       '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE', 'MARGIN_ACWISE',
       'MARGIN_GROUP_ACWISE', 'PC_WISE_PERCENTAGE',
       'WINNER_PCWISE', 'MAX_PER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE',
       'MARGIN_PCWISE', 'MARGIN_GROUP_PCWISE', 'SUMMERY_PCWISE',
       'SEQ_WISE_PERCENTAGE_AC', 'WINNER_SEQWISE_AC', 'MAX_PER_SEQWISE_AC',
       '2ND_PER_SEQWISE_AC', 'RUNNER_UP_SEQWISE_AC', 'MARGIN_SEQWISE_AC',
       'MARGIN_GROUP_SEQWISE_AC','SUMMERY_SEQWISE_AC',
       'SEQ_WISE_PERCENTAGE_PC', 'WINNER_SEQWISE_PC', 'MAX_PER_SEQWISE_PC',
       '2ND_PER_SEQWISE_PC', 'RUNNER_UP_SEQWISE_PC', 'MARGIN_SEQWISE_PC',
       'MARGIN_GROUP_SEQWISE_PC', 'SUMMERY_SEQWISE_PC', 'WINNER_DATEWISE_AC',
       'WINNER_REGIONWISE_AC','ROUND'], inplace=True)

    dynamic_columns = result_df.columns.tolist()
    
    df_with_columns = df_with_columns.drop_duplicates(subset=['AC_ID'] + dynamic_columns)
    
    return df_with_columns

if df is not None:
   df=create_party_columns_grouped(df)

BC_Round_N_party_data=df  

print("total no of Rows is :- ", df.shape[0])   
data=df[(df['AC_ID']=="OD-AC-10")].head(5)
print(data.to_string(index=False))
