import pandas as pd

csv_file_path="D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\all_round_combined_data.csv"
df=pd.read_csv(csv_file_path,low_memory=False)
pd.set_option('display.max_columns', None)

df['ROUND']= "COM"

# print(df.columns)
# print(" ")
# data=df.head(10)
# print(data.to_string(index=False))
# print(" ")
# print(df.shape[0])

# -------------------------------------------------------------------------------------------------------------------------------- #
final_combine_df=df
