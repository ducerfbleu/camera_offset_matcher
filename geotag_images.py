import multiprocessing
import pandas as pd
import sys
from copy import deepcopy
from datetime import datetime, timedelta
import numpy as np
from dtw import dtw
import subprocess
import argparse 
from geotagre import match_timestamps_idx, insert_new_match, conduct_dtw, run_command, run_command_shell, run_insert_prev_dt
import os

parser = argparse.ArgumentParser(add_help = True)
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
gps_dt_col = 'datetime'
camera_dt_col = 'adjusted_camera_datetime'
file_path_col = 'SourceFile'

exif_mapping = dict()
exif_mapping[gps_lat_col] = 'GPSLatitude'
exif_mapping[gps_long_col] = 'GPSLongitude'
exif_mapping[gps_alti_col] = 'GPSAltitude'
exif_mapping[gps_az_col] = 'GPSImgDirection'
exif_mapping[gps_dt_col] = 'DateTimeOriginal'
exif_mapping[file_path_col] = 'SourceFile'

fin_trim = fin_result[list(exif_mapping.keys())].rename(columns=exif_mapping)

fin_trim = fin_trim.dropna( subset = [file_path_col])


print(
    fin_trim
)

print('Geotagging files...')

ls_cli_commands = list()
ls_cli_dtbackup = list()

def prep_dtbackup_cli(file_name):
    command = f'PrevDateTime=$(exiftool -b -DateTimeOriginal "{file_name}") && exiftool -UserComment=\'{{"PrevDateTime":"$PrevDateTime"}}\' "{file_name}"'
    return command


for index, row in fin_trim.iterrows():
    cli_command = ["exiftool"]
    for field_ in list(exif_mapping.values()):
        if field_=='SourceFile':
            pass 
        elif field_=='DateTimeOriginal':
            dt_str = row[field_]
            dt_obj = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
            dt_obj_rd = dt_obj + timedelta(seconds = 1) if dt_obj.microsecond >= 500000 else dt_obj
            dt_exif = dt_obj_rd.strftime('%Y:%m:%d %H:%M:%S')
            cli_command += [f"-{field_}={dt_exif}"]
        elif field_=='GPSImgDirection':
            cli_command += [f"-{field_}={row[field_]}", f"-{field_}Ref=T"]
        else:
            cli_command += [f"-{field_}={row[field_]}", f"-{field_}Ref={row[field_]}"]        

    if args.image_dir is not None:
        filename = os.path.basename(row['SourceFile'])
        file_path = os.path.join(args.image_dir, filename)
        cli_command += [f"{file_path}"]
    else:
        file_path = row['SourceFile']
        cli_command += [f"{file_path}"]
    
    ls_cli_commands += [cli_command]
    ls_cli_dtbackup += [file_path]
# print(os.getcwd())
# print(os.listdir())
# print(ls_cli_dtbackup[0])
# print(ls_cli_dtbackup[1])
# print(ls_cli_dtbackup[2])

# print(ls_cli_commands[0])
# print(ls_cli_commands[1])
# print(ls_cli_commands[2])

print("Start backing up Previous DateTimeOriginal to UserComment...")
with multiprocessing.Pool(processes = NUM_CPU) as pool:
    results = pool.map(run_insert_prev_dt, ls_cli_dtbackup)
print("Done. Run 'exiftool -UserComment <filename>' to confirm.")

print("Start geotagging from matched CSV...")
with multiprocessing.Pool(processes = NUM_CPU) as pool:
    results = pool.map(run_command, ls_cli_commands)
print('Done. Check if geotagging went well in $IMG_DIR. e.g., exiftool -GPS* -Date* [image_name.jpg]')


# test this method

# import subprocess

# image_path = "wbs1_05212025_copy/sony_cp1/DSC02204.JPG"

# # 1. Get the DateTimeOriginal using the first command
# try:
#     result = subprocess.run(
#         ['exiftool', '-b', '-DateTimeOriginal', image_path],
#         capture_output=True,
#         text=True,
#         check=True
#     )
#     prev_date_time = result.stdout.strip()
# except subprocess.CalledProcessError as e:
#     print(f"Error getting DateTimeOriginal: {e}")
#     # Handle the error, e.g., exit or return

# # 2. Use the captured date/time in the second command
# user_comment_json = f'{{"PrevDateTime":"{prev_date_time}"}}'

# try:
#     subprocess.run(
#         ['exiftool', f'-UserComment={user_comment_json}', image_path],
#         check=True
#     )
#     print("Command executed successfully!")
# except subprocess.CalledProcessError as e:
#     print(f"Error setting UserComment: {e}")
    