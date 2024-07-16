import pandas as pd
import numpy as np
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
pd.set_option('display.max_columns', None)

from ALL_STATE_DATA_BY_EP_ROUND.EP_Round_data_calculation import EP_Round_final_df as df

for i in range(1, 11):
    column_name = f'AC_WISE_DECREASE_BY_{i}'
    df[column_name] = df['AC_WISE_PERCENTAGE']
    max_ac_wise_percentage = df.groupby('AC_ID')['AC_WISE_PERCENTAGE'].transform('max')
    df.loc[df['AC_WISE_PERCENTAGE'] == max_ac_wise_percentage, column_name] -= i

def calculate_winner_by_decrease(df, column_name):
    acwise_grouped = df.groupby(['AC_ID', 'RES1']).agg(
        RES_ACWISE_COUNT=('RES_ACWISE', 'sum'),
        MAX_PER_ACWISE=(column_name, 'max')
    ).reset_index()
    
    max_acwise_counts = acwise_grouped.loc[acwise_grouped.groupby('AC_ID')['MAX_PER_ACWISE'].idxmax()]
    max_acwise_counts['DRAW'] = acwise_grouped.duplicated(['AC_ID', 'MAX_PER_ACWISE'], keep=False)
    max_acwise_counts[f'WINNER_{column_name}'] = np.where(max_acwise_counts['DRAW'], 'DRAW', max_acwise_counts['RES1'])
    
    
    return max_acwise_counts[['AC_ID', f'WINNER_{column_name}']]


for i in range(1, 11):
    column_name = f'AC_WISE_DECREASE_BY_{i}'
    max_column_name = f'MAX_ACWISE_DECREASE_BY_{i}'
    max_acwise_values = df.groupby('AC_ID')[column_name].transform('max')
    df[max_column_name] = max_acwise_values

    max_acwise_counts = calculate_winner_by_decrease(df, column_name)
    df = df.merge(max_acwise_counts[['AC_ID', f'WINNER_{column_name}']], on='AC_ID', how='left')

 # Determine the runner-up AC-wise
    df['RANK_PER_ACWISE'] = df.groupby('AC_ID')[column_name].rank(method='dense', ascending=False)
    second_max_df = df[df['RANK_PER_ACWISE'] == 2]
    df[f'2ND_PER_ACWISE_{column_name}'] = df['AC_ID'].map(second_max_df.set_index('AC_ID')[column_name].to_dict())
    df[f'RUNNER_UP_ACWISE_{column_name}'] = df['AC_ID'].map(second_max_df.set_index('AC_ID')['RES1'].to_dict())

    df[f'MARGIN_ACWISE_DECREASE_BY_{i}'] = (df[max_column_name] - df[f'2ND_PER_ACWISE_{column_name}']).round(2)
    summary_column_name = f'SUMMERY_AC_WISE_DECREASE_BY_{i}'
    df[summary_column_name] = (
        df[f'WINNER_{column_name}'].astype(str) + 
        ' ' +  # Add a space separator if needed
        df[f'MARGIN_ACWISE_DECREASE_BY_{i}'].astype(str) + 
        ' ' +  # Add a space separator if needed
        df[f'RUNNER_UP_ACWISE_{column_name}'].astype(str)
    )

    columns_to_drop = [
        f'2ND_PER_ACWISE_{column_name}',
        f'MAX_ACWISE_DECREASE_BY_{i}',
        f'RUNNER_UP_ACWISE_{column_name}', 
        f'MARGIN_ACWISE_DECREASE_BY_{i}'  ,
        'RANK_PER_ACWISE'  
    ]
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
# -------------------------------- Drop columns ---------------------------------- #
columns_to_drop = [f'AC_WISE_DECREASE_BY_{i}' for i in range(1, 11)]
df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
df.drop(columns=['N_PARTY', 'RES1','RES_ACWISE', 'RES_PCWISE', 'RES_SEQWISE_AC',
       'RES_SEQWISE_PC', 'RES_DATEWISE', 'RES_REGIONWISE_AC',
       'AC_WISE_PERCENTAGE','MAX_PER_ACWISE',
       '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE','PC_WISE_PERCENTAGE', 'MAX_PER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE',
        'SEQ_WISE_PERCENTAGE_AC', 'WINNER_SEQWISE_AC', 'MAX_PER_SEQWISE_AC',
       '2ND_PER_SEQWISE_AC', 'RUNNER_UP_SEQWISE_AC', 'MARGIN_SEQWISE_AC',
       'MARGIN_GROUP_SEQWISE_AC', 'SUMMERY_SEQWISE_AC',
       'SEQ_WISE_PERCENTAGE_PC', 'WINNER_SEQWISE_PC', 'MAX_PER_SEQWISE_PC',
       '2ND_PER_SEQWISE_PC', 'RUNNER_UP_SEQWISE_PC', 'MARGIN_SEQWISE_PC',
       'MARGIN_GROUP_SEQWISE_PC', 'SUMMERY_SEQWISE_PC', 'WINNER_DATEWISE_AC',
       'WINNER_REGIONWISE_AC',],inplace=True)

df.drop_duplicates(subset=['AC_ID'],inplace=True)

# ----------------------------------- print section --------------------------------------------------------------------------------------------- #
print(" ")
print(df.shape[0])
print(" ")
print(df.columns)
print(" ")
data=df[(df['AC_ID'] == 'OD-AC-10')].head(10)
print(data.to_string(index=False))

# -------------------------------------------------------------------------------------------------------------------------------- #
EP_decrease_max_per_ac_wise_df=df


# ------------------------------------------ Execution Time ----------------------------------------------------------- #
# 3.09 minutes