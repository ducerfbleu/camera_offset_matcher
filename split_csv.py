import multiprocessing
import pandas as pd
import sys
from copy import deepcopy
from datetime import datetime
import numpy as np
from dtw import dtw
import subprocess
import argparse 
from geotagre import match_timestamps_idx, insert_new_match, conduct_dtw, run_command
import os

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--csv", help="input csv with time series", required= True)
parser.add_argument("-tc", "--ts_col", help="time series column to refer", required = True)
parser.add_argument("-o", "--out_dir", help="dir to output the results", default = ".")
parser.add_argument("-t", "--thres", help="time threshold to split, in seconds", type=int, required = True)
parser.add_argument("-f", "--dt_format", help="format of time series to parse, defaults to %Y-%m-%d %H:%M:%S.%f", default = "%Y-%m-%d %H:%M:%S.%f" )

args = parser.parse_args()

try:
    PATH_CSV = args.csv
    print(f"path to csv data:{PATH_CSV}")
    df_csv = pd.read_csv(PATH_CSV)
    COL_DT = args.ts_col
    OUT_DIR = args.out_dir
    DIFF_THRES = args.thres
    DT_FORMAT = args.dt_format
    BOOL_IDX = pd.to_datetime(
    df_csv[COL_DT],
    format = DT_FORMAT
    ).diff().dt.total_seconds()>DIFF_THRES

except Exception as e:
    print(f"Error {e}")
    sys.exit(1)


base_fn = os.path.basename(PATH_CSV).split(".")[0]
df_csv[COL_DT] = pd.to_datetime(
    df_csv[COL_DT],
    format = DT_FORMAT
)
# order time series in sequence first
df_csv = df_csv.sort_values( by = COL_DT, 
            ascending = True
            ).reset_index()

BOOL_IDX = df_csv[COL_DT].diff().dt.total_seconds()>DIFF_THRES

split_indices = np.where(BOOL_IDX)[0].tolist()

df = deepcopy(df_csv)
split_dfs = []

split_dfs.append(df.iloc[:split_indices[0]])

print(f"found the breaks, the indices that breaks of {DIFF_THRES} seconds or more are {split_indices}")

for i in range(len(split_indices)):
    start_index = split_indices[i]
    if i < len(split_indices) - 1:
        end_index = split_indices[i+1]
        split_dfs.append(df.iloc[start_index:end_index])
    else:  # For the last split, go until the end of the DataFrame
        split_dfs.append(df.iloc[start_index:])

idx = 0
for dataframe_ in split_dfs:
    idx_str = str(idx).zfill(2)
    fin_path = os.path.join(OUT_DIR,base_fn)+"_"+idx_str+".csv"
    dataframe_.to_csv(fin_path, index = False)
    print(f"saved file in {fin_path}")
    idx += 1

print(len(split_dfs))
