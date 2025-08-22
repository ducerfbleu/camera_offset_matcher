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
parser.add_argument("-match", "--matched_csv", help="matched csv with full info")
parser.add_argument("-dir", "--image_dir", help="image directory to update, if not supplied, uses the file list.")
parser.add_argument("-n", "--num_cpu", help="cpu cores to use for the job.", default = 8)

args = parser.parse_args()
NUM_CPU = int(args.num_cpu)

try:
    matched_data_path = args.matched_csv
    print(f"path to matched csv data:{matched_data_path}")
    fin_result = pd.read_csv(matched_data_path)

except Exception as e:
    print(f"Error {e}")
    sys.exit(1)

gps_lat_col = 'latitude'
gps_long_col = 'longitude'
gps_alti_col = 'altitude'
gps_az_col = 'az'
camera_dt_col = 'adjusted_camera_datetime'
file_path_col = 'SourceFile'

exif_mapping = dict()
exif_mapping[gps_lat_col] = 'GPSLatitude'
exif_mapping[gps_long_col] = 'GPSLongitude'
exif_mapping[gps_alti_col] = 'GPSAltitude'
exif_mapping[gps_az_col] = 'GPSImgDirection'
exif_mapping[camera_dt_col] = 'DateTimeOriginal'
exif_mapping[file_path_col] = 'SourceFile'

fin_trim = fin_result[list(exif_mapping.keys())].rename(columns=exif_mapping)

fin_trim = fin_trim.dropna( subset = [file_path_col])


print(
    fin_trim
)

print('Geotagging files...')

ls_cli_commands = list()

for index, row in fin_trim.iterrows():
    cli_command = ["exiftool"]
    for field_ in list(exif_mapping.values()):
        if field_=='SourceFile':
            pass 
        elif field_=='DateTimeOriginal':
            cli_command += [f"-{field_}={row[field_]}"]
        elif field_=='GPSImgDirection':
            cli_command += [f"-{field_}={row[field_]}", f"-{field_}Ref=T"]
        else:
            cli_command += [f"-{field_}={row[field_]}", f"-{field_}Ref={row[field_]}"]        

    if args.image_dir is not None:
        filename = os.path.basename(row['SourceFile'])
        cli_command += [f"{os.path.join(args.image_dir, filename)}"]
    else:
        cli_command += [f"{row['SourceFile']}"]
    
    ls_cli_commands += [cli_command]

with multiprocessing.Pool(processes = NUM_CPU) as pool:
    results = pool.map(run_command, ls_cli_commands)

print('Done. Check if geotagging went well in $IMG_DIR. e.g., exiftool -GPS* -Date* [image_name.jpg]')