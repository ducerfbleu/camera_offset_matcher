 
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
import glob 

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--base_name", help="base csv name", required= True)
parser.add_argument("-o", "--out_dir", help="dir to output the results", default = ".")
parser.add_argument("-m", "--match_methods", help="list of match methods to parse, defaults to 'ascending, descending, dtw'", default = "ascending, descending, dtw")


try:
    args = parser.parse_args()
    BASE_NAME = args.base_name
    OUT_DIR = args.out_dir
    raw_list = args.match_methods.split(',')
    ls_methods = [item.strip() for item in raw_list]

except Exception as e:
    print(f"Error {e}")
    sys.exit(1)


for method_ in ls_methods:
    glob_pattern = os.path.join(OUT_DIR, BASE_NAME)+"*[0-9][0-9]_MATCHED_"+method_+".csv"
    # print(glob_pattern)
    ls_splits_ = glob.glob(glob_pattern)
    # print(ls_splits_)
    ls_splits_sorted_ = sorted(ls_splits_)

    df_splits_sorted_ = [pd.read_csv(path) for path in ls_splits_sorted_]
    full_df = pd.concat(df_splits_sorted_, ignore_index=True)
    path_full_df = os.path.join(OUT_DIR, BASE_NAME)+"_FULL_MATCHED_"+method_+".csv"
    print(f"Concatenated {ls_splits_sorted_}. Saving results to {path_full_df}.")
    full_df.to_csv(path_full_df, index= False)
    
