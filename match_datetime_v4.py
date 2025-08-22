import multiprocessing
import pandas as pd
import sys
from copy import deepcopy
from datetime import datetime
import numpy as np
from dtw import dtw
import subprocess
import argparse 
from geotagre import match_timestamps_idx, insert_new_match, conduct_dtw, run_command, insert_new_match_v2, conduct_dtw_v2
import os
from dtw import stepPattern

def confirm_proceed(PHRASE = "Do you want to proceed? (y/n):"):
    while True:
        response = input(PHRASE).lower()
        if response in ['y', 'yes']: 
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

parser = argparse.ArgumentParser(description="Match datetime with GPS")
parser.add_argument("-gps", "--gps_csv", help="Path to gps csv file")
parser.add_argument("-exif", "--img_exif", help="Path to image exif csv")
parser.add_argument("-n", "--num_cpu", help="Number of cpu for processing")
parser.add_argument("-o", "--output_csv", help="output csv name", default = "OUTPUT" )
parser.add_argument("-od", "--output_dir", help="output dir name")
args = parser.parse_args()

try:
    gps_data_path = args.gps_csv
    print(f"path to gps data:{gps_data_path}")
    gps_df = pd.read_csv(gps_data_path)
    camera_path = args.img_exif
    print(f"path to camera data: {camera_path}")
    camera_df = pd.read_csv(camera_path)
    num_cpu = int(args.num_cpu)
    outfile_name = args.output_csv
    outfile_dir = args.output_dir

except Exception as e:
    print(f"Error {e}")
    sys.exit(1)

print("Direct matching in ascending order of camera date time...")
TF_=True
ls_matching_ = match_timestamps_idx(gps_df, camera_df, ASCENDING_TF=TF_, 
                                    colname_camera = "updated_datetime"
                                   )#"DateTimeOriginal")
df_out_ascending, avg_time_diff = insert_new_match_v2(gps_df, camera_df,  ls_matching_, ASCENDING_TF = TF_, 
                                         camera_dt_col = "updated_datetime",#DateTimeOriginal",
                                        file_path_col = 'SourceFile'
                                        )

df_out_ascending.to_csv(os.path.join(outfile_dir, outfile_name)+"_ascending.csv", index=False)
print("Direct matching in descending order of camera date time...")
TF_=False    
ls_matching_ = match_timestamps_idx(gps_df, camera_df, ASCENDING_TF=TF_, 
                                    colname_camera = "updated_datetime"
                                   )#"DateTimeOriginal")
df_out_descending, avg_time_diff = insert_new_match_v2(gps_df, camera_df,  ls_matching_, ASCENDING_TF = TF_, 
                                         camera_dt_col = "updated_datetime", #"DateTimeOriginal",
                                        file_path_col = 'SourceFile'
                                        )
df_out_descending.to_csv(os.path.join(outfile_dir, outfile_name)+"_descending.csv", index=False)

try:
    print("Attempting Dynamic Time Wrapping...")
    df_out_dtw = conduct_dtw_v2(gps_df, 
        camera_df,
        gps_dt_col = 'datetime',
        camera_dt_col = 'updated_datetime',
        STEP_PATTERN = stepPattern.asymmetric)
    
    avg_time_diff = np.mean(df_out_dtw['time_diff_sec']).item() 
    print(f"Dynamic Time Wrapping yielded {avg_time_diff:.3f} seconds of mean time difference. Adding...")
    df_out_dtw.to_csv(os.path.join(outfile_dir, outfile_name)+"_dtw.csv", index=False)

    
except Exception as e:
    print(f"Error: {e}! Dynamic Time Wrapping Failed.")
print('Done matching camera datetime with GPS.')
print(f"Outputed data Frame to: {outfile_dir}.")

# if confirm_proceed("Proceed to geotagging image files? (y/n)"):
#     pass
# else:
#     print("Aborting the script. Bye bye")
#     sys.exit(1)
