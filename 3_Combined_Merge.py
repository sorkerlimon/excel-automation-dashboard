from pathlib import Path
import pandas as pd
import time
import re
import os

path = Path(__file__).parent.absolute()  
name = ['L1\L1_merge.csv', 'L2\L2_merge.csv', 'L3\L3_merge.csv','L4\L4_merge.csv', 'L5\L5_merge.csv', 'L6\L6_merge.csv']

x = time.time()

print("\n\t\t*****Started Preprocessing*****\n" )

df1 = pd.read_csv(name[0]).sort_values(['Date', 'Cell Name', 'Time'])

df2 = pd.read_csv(name[1]).sort_values(['Date', 'Cell Name', 'Time'])
df2.drop(['NE Name', 'Cell', 'Period(min)'], axis=1, inplace=True)

df3 = pd.read_csv(name[2]).sort_values(['Date', 'Cell Name', 'Time'])
df3.drop(['NE Name', 'Cell', 'Period(min)'], axis=1, inplace=True)

df4 = pd.read_csv(name[3]).sort_values(['Date', 'Cell Name', 'Time'])
df4.drop(['NE Name', 'Cell', 'Period(min)'], axis=1, inplace=True)

df5 = pd.read_csv(name[4]).sort_values(['Date', 'Cell Name', 'Time'])
df5.drop(['NE Name', 'Cell', 'Period(min)'], axis=1, inplace=True)

df6 = pd.read_csv(name[5]).sort_values(['Date', 'Cell Name', 'Time'])
df6.drop(['NE Name', 'Cell', 'Period(min)'], axis=1, inplace=True)

print("\n\t\t*****Finished Preprocessing*****\n" )

print("\n\t\t*****Merging Started*****\n" )

merged_df1 = pd.merge(df1, df2, on=['Date', 'Cell Name', 'Time'], how='outer', suffixes=('', '.9'))
del df1
del df2
print("\n\t\t***** DONE  :   20% *****\n" )

merged_df2 = pd.merge(merged_df1, df3, on=['Date', 'Cell Name', 'Time'], how='outer', suffixes=('', '.9'))
del merged_df1
del df3
print("\n\t\t***** DONE  :   40% *****\n" )

merged_df3 = pd.merge(merged_df2, df4, on=['Date', 'Cell Name', 'Time'], how='outer', suffixes=('', '.9'))
del merged_df2
del df4
print("\n\t\t***** DONE  :   60% *****\n" )

merged_df4 = pd.merge(merged_df3, df5, on=['Date', 'Cell Name', 'Time'], how='outer', suffixes=('', '.9'))
del merged_df3
del df5
print("\n\t\t***** DONE  :   80% *****\n" )

merged_df5 = pd.merge(merged_df4, df6, on=['Date', 'Cell Name', 'Time'], how='outer', suffixes=('', '.9'))
print("\n\t\t***** DONE  :   100% *****\n" )


print("Shape of Final Merge     :    ", merged_df5.shape)

for col in merged_df5.columns:
    if re.search(r'\.\d+$', col) and (col.endswith('.9')):
        merged_df5.drop(col, axis=1, inplace=True)

merged_df5.fillna('', inplace=True)
merged_df5.to_csv(os.path.join(path, 'Combine', 'super_merge.csv'))

endtime = time.time()

print((endtime-x)/60)