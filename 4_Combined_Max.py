from pathlib import Path
import os
import pandas as pd
import time
import re
starttime = time.time()
path = Path(__file__).parent.absolute()  
name = []
for folder in os.listdir(path):  
    if os.path.isdir(folder) and not folder.startswith("env") and not folder.startswith("Demo") and folder.startswith("L"):
        csv_name = f"{path}\\{folder}\\{folder}_merge.csv"
        df = pd.read_csv(csv_name)
        
        if folder == 'L1':
            x = f"{path}\\{folder}\\{folder}_max.csv"
            name.append(x)
            filtered_df = df.groupby(df.columns[4]).apply(lambda x: x.loc[x.iloc[:, 6].idxmax()])
            del df
            filtered_df.to_csv(os.path.join(path, folder, f"{folder}_max.csv"), index=False)
            new_df = filtered_df.loc[:, ['Date', 'Cell', 'Time']]
            del filtered_df
            print("Done     :", x)

        else:
            x = f"{path}\\{folder}\\{folder}_max.csv"
            name.append(x)
            filtered_df = pd.merge(new_df, df, on=['Date', 'Cell', 'Time'], how='inner')
            del df
            filtered_df.to_csv(os.path.join(path, folder, f"{folder}_max.csv"), index=False)
            del filtered_df
            print("Done     :", x)  


if len(name) >1:
    # merged_df = pd.concat([pd.read_csv(f) for f in names], axis=1)
    # merged_df.to_csv(os.path.join(path, 'Combine', 'max_super_merge.csv'), index=False)
    df1 = pd.read_csv(name[0]).sort_values(['Date', 'Cell Name'])

    df2 = pd.read_csv(name[1]).sort_values(['Date', 'Cell Name'])
    df2.drop(['NE Name', 'Cell', 'Period(min)', 'Time'], axis=1, inplace=True)

    df3 = pd.read_csv(name[2]).sort_values(['Date', 'Cell Name'])
    df3.drop(['NE Name', 'Cell', 'Period(min)', 'Time'], axis=1, inplace=True)

    df4 = pd.read_csv(name[3]).sort_values(['Date', 'Cell Name'])
    df4.drop(['NE Name', 'Cell', 'Period(min)', 'Time'], axis=1, inplace=True)

    df5 = pd.read_csv(name[4]).sort_values(['Date', 'Cell Name'])
    df5.drop(['NE Name', 'Cell', 'Period(min)', 'Time'], axis=1, inplace=True)

    df6 = pd.read_csv(name[5]).sort_values(['Date', 'Cell Name'])
    df6.drop(['NE Name', 'Cell', 'Period(min)', 'Time'], axis=1, inplace=True)

    merged_df1 = pd.merge(df1, df2, on=['Date', 'Cell Name'], how='outer', suffixes=('', '.9'))
    del df1
    del df2

    merged_df2 = pd.merge(merged_df1, df3, on=['Date', 'Cell Name'], how='outer', suffixes=('', '.9'))
    del merged_df1
    del df3

    merged_df3 = pd.merge(merged_df2, df4, on=['Date', 'Cell Name'], how='outer', suffixes=('', '.9'))
    del merged_df2
    del df4

    merged_df4 = pd.merge(merged_df3, df5, on=['Date', 'Cell Name'], how='outer', suffixes=('', '.9'))
    del merged_df3
    del df5

    merged_df5 = pd.merge(merged_df4, df6, on=['Date', 'Cell Name'], how='outer', suffixes=('', '.9'))

    print("Shape of Final Merge     :    ", merged_df5.shape)

    for col in merged_df5.columns:
        if re.search(r'\.\d+$', col) and (col.endswith('.9')):
            merged_df5.drop(col, axis=1, inplace=True)

    merged_df5.fillna('', inplace=True)
    merged_df5.to_csv(os.path.join(path, 'Max.csv'), index=False)


endtime = time.time()

print((endtime-starttime)/60)