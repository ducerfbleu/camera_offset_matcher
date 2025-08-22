# functions for re-geotagging

import pandas as pd
import sys
from copy import deepcopy
from datetime import datetime
import numpy as np
from dtw import dtw
import json
import sys
import argparse
import datetime
from copy import deepcopy
import glob
import os
import pandas as pd
import subprocess
from dtw import stepPattern

# functions for parse_gpslog_and_reformat_dt.py:

def parse_gps_log(csv_path, line_starts):
    try:
        new_line = int(line_starts)
        lines_to_skip = new_line - 1
        df = pd.read_csv(csv_path, skiprows = lines_to_skip)
        df.columns = df.columns.str.strip()
        return df

    except Exception as e:
        print(f'Error: {e}')


def epoch_ms_to_datetime(epoch_ms, target_utc = datetime.timezone.utc):

    if not isinstance(epoch_ms, (int, float)):
        print("input must be int or float.")
        return None
    
    try:
        epoch_sec = epoch_ms/1000.0
        dt_object = datetime.datetime.fromtimestamp(
            epoch_sec,
            tz = target_utc # is this a good practice? What does UTC have to be based on? 
        ) 
        return dt_object
    except OverflowError:
        print(f"Error date out of range")
        return None
    except Exception as e:
        print(f"Unknown error")
        return None
    
# multiprocessing related

def run_command(command):
    result = subprocess.run(command, capture_output = True, text= True)
    return result.stdout

### ftns to extract datetime from gps csv and camera exif

def extract_datetime_to_dict(
    df_gps_log, 
    df_image_exif, 
    gps_dt_col = 'datetime', 
    camera_dt_col = 'DateTimeOriginal',
    camera_dt_format = "%Y:%m:%d %H:%M:%S",
    gps_dt_format = "%Y-%m-%d %H:%M:%S.%f"
):
    json_dat = dict()
    df1 = pd.to_datetime(
    df_gps_log[gps_dt_col]
    )
    df2 = pd.to_datetime(
    df_image_exif[camera_dt_col],
        format = camera_dt_format
    )
    df1 = df1.sort_values().tolist()
    df2 = df2.sort_values().tolist()

    df1_f = [dt.strftime(gps_dt_format) for dt in df1]
    df2_f = [dt.strftime(camera_dt_format) for dt in df2]
    json_dat['GPS_time'] = df1_f[0]
    json_dat['camera_time'] = df2_f[0]
    return json_dat

### ftns to handle offset ###

def calc_offset(json_dat, camera_dt_format = "%Y-%m-%d %H:%M:%S" ):
    # get gps time and camera time from json file
    gps_time = datetime.datetime.strptime(json_dat['GPS_time'], "%Y-%m-%d %H:%M:%S.%f")
    camera_time = datetime.datetime.strptime(json_dat['camera_time'], camera_dt_format)
    offset = gps_time - camera_time
    return offset

def resolv_offset(df_exif, dt_col, offset, new_col = 'updated_datetime', camera_dt_format ="%Y:%m:%d %H:%M:%S"):
    # get offset
    df_exif = deepcopy(df_exif)
    ls_dates_to_update = df_exif[dt_col]
    ls_updated_dates = []
    for focal_date in ls_dates_to_update:
        focal_date_obj = datetime.datetime.strptime(focal_date, camera_dt_format)
        updated_focal_date_obj = focal_date_obj + offset
        updated_focal_date_obj = updated_focal_date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
        updated_date = updated_focal_date_obj
        ls_updated_dates += [updated_date]
    
    df_exif[new_col] = ls_updated_dates
    return df_exif

### ftns to handle matching ###

def match_timestamps_idx(gps_df, camera_df, 
                         ASCENDING_TF = True, 
                         colname_gps = 'datetime', 
                         colname_camera = 'updated_dates'):
    df1 = deepcopy(gps_df)
    df2 = deepcopy(camera_df)

    df1['datetime'] = pd.to_datetime(
            gps_df[colname_gps]
            )

    df2['datetime'] = pd.to_datetime(
            camera_df[colname_camera], #format='%Y:%m:%d %H:%M:%S'
            )

    # sort by timestamp
    df1 = df1.sort_values(by = 'datetime', 
            ascending = ASCENDING_TF #True
            ).reset_index(
            drop = True)
    df2 = df2.sort_values( by = 'datetime', 
            ascending = ASCENDING_TF
            ).reset_index(
            drop = True)
    print(df2.head())

    matched_data = []
    # print(len(df2))
    # print(len(df1))
    gps_df_copy = deepcopy(df1)
   
    # gps_df_copy = gps_df_copy.reset_index(names ='id')
    ls_matching_gps_idx = []
    ls_timediff = []
    # ls_image_filename = []
    # gps_df_copy['image_filename'] = ''
    # gps_df_copy['adjusted_camera_datetime'] = ''
    for _, camera_row in df2.iterrows():
        camera_time = camera_row['datetime']
        closest_gps_ordered = (gps_df_copy['datetime'] - camera_time).abs().argsort().to_list()
        candidate_idx_ordered = [x for x in closest_gps_ordered if x not in ls_matching_gps_idx]
        closest_gps_idx = candidate_idx_ordered[0]
        closest_gps_time = gps_df_copy.iloc[closest_gps_ordered[0]]['datetime']
        ls_matching_gps_idx += [closest_gps_idx] #[matching_gps_id]
        ls_timediff += [np.abs(camera_time - closest_gps_time)]
  
    return ls_matching_gps_idx

def get_timediff(col1, col2):
    return np.abs(pd.to_datetime(col1) - \
                  pd.to_datetime(col2)).total_seconds()

def insert_new_match(df_gps, 
                     df_camera, 
                     ls_matching_idx,
                     ASCENDING_TF, 
                     camera_dt_col = 'updated_dates', 
                     new_dt_col = 'adjusted_camera_datetime',
                     file_path_col = 'file_path',
                     # file_name_col = 'file_name',
                     gps_dt_col = 'datetime'):
    df_gps_copy = deepcopy(df_gps)
    df_gps_copy = df_gps_copy.sort_values( by = gps_dt_col, 
            ascending = True
            ).reset_index(
            drop = True)
    df_camera_copy = deepcopy(df_camera)
    df_camera_copy = df_camera_copy.sort_values( by = camera_dt_col, 
            ascending = ASCENDING_TF
            ).reset_index(
            drop = True)
    camera_datetime = pd.to_datetime(
        df_camera_copy[camera_dt_col]#, format='%Y:%m:%d %H:%M:%S'
    )
    file_path = df_camera_copy[file_path_col]
    # file_name = df_camera[file_name_col]
    df_gps_copy[new_dt_col] = camera_datetime.set_axis(
        ls_matching_idx
    )
    df_gps_copy[file_path_col] = file_path.set_axis(ls_matching_idx)
    # df_gps_copy[file_name_col] = file_name.set_axis(ls_matching_idx)    
    timediff = df_gps_copy.iloc[ls_matching_idx].apply(lambda row:\
                                    get_timediff(
                                    row[gps_dt_col],
                                    row[new_dt_col]
                                    ),
                                    axis = 1)
    df_gps_copy['time_diff_sec'] = timediff.set_axis(ls_matching_idx) 
    avg_time_diff = np.mean(df_gps_copy['time_diff_sec'])
    print(f"Average time difference:{avg_time_diff: .3f} seconds.")
    return df_gps_copy, avg_time_diff

def insert_new_match_v2(df_gps, 
                     df_camera, 
                     ls_matching_idx,
                     ASCENDING_TF, 
                     camera_dt_col = 'updated_dates', 
                     new_dt_col = 'adjusted_camera_datetime',
                     file_path_col = 'file_path',
                     # file_name_col = 'file_name',
                     gps_dt_col = 'datetime'):
    # 8/16/2025 changes: changed rel path to abs path, changes set_axis to reindex to allow for duplicate indices 
    df_gps_copy = deepcopy(df_gps)
    df_gps_copy = df_gps_copy.sort_values( by = gps_dt_col, 
            ascending = ASCENDING_TF #True
            ).reset_index(
            drop = True)
    df_camera_copy = deepcopy(df_camera)
    df_camera_copy = df_camera_copy.sort_values( by = camera_dt_col, 
            ascending = ASCENDING_TF
            ).reset_index(
            drop = True)
    camera_datetime = pd.to_datetime(
        df_camera_copy[camera_dt_col]#, format='%Y:%m:%d %H:%M:%S'
    )
    file_path = pd.Series([os.path.abspath(path) for path in df_camera_copy[file_path_col]])
    # file_name = df_camera[file_name_col]
    df_gps_copy[new_dt_col] = camera_datetime.reindex(
        ls_matching_idx
    )
    df_gps_copy[file_path_col] = file_path.reindex(ls_matching_idx)
    # df_gps_copy[file_name_col] = file_name.set_axis(ls_matching_idx)    
    timediff = df_gps_copy.iloc[ls_matching_idx].apply(lambda row:\
                                    get_timediff(
                                    row[gps_dt_col],
                                    row[new_dt_col]
                                    ),
                                    axis = 1)
    df_gps_copy['time_diff_sec'] = timediff.reindex(ls_matching_idx) 
    avg_time_diff = np.mean(df_gps_copy['time_diff_sec'])
    print(f"Average time difference:{avg_time_diff: .3f} seconds.")
    return df_gps_copy, avg_time_diff

def conduct_dtw(df_gps, 
                df_camera,
                gps_dt_col = 'datetime',
                camera_dt_col = 'updated_dates',
                ASCENDING_TF = True):
    """ previous version of dynamic time wrapping.
    Fails pretty often. See conduct_dtw_v2.
    """
    df_gps = deepcopy(df_gps)
    gps_time = pd.to_datetime(
        df_gps[gps_dt_col]
    )
    df_camera = deepcopy(df_camera)
    df_camera = df_camera.sort_values( by = camera_dt_col, 
            ascending = ASCENDING_TF
            ).reset_index(
            drop = True)
    camera_time = pd.to_datetime(
        df_camera[camera_dt_col]
    )
    x = np.array(
        [
            t.timestamp() for t in gps_time.tolist()
        ]

    )
    y = np.array(
        [
            t.timestamp() for t in camera_time.tolist()
        ]
    )
    alignment = dtw(x, y, keep_internals = True)
    ls_matching_idx = []
    for idx_ in list(set(alignment.index2.tolist())):
        ls_matching_idx += [
            np.min(
                np.where(
                    alignment.index2 == idx_
                )
            ).tolist()
        ]
    return ls_matching_idx

def get_ts_cumsum(data_frame, COLNAME, need_parsing = False, parse_format = "%Y:%m:%d %H:%M:%S"):
    if need_parsing:
        ts_ = pd.Series([datetime.datetime.strptime(dt, parse_format) for dt in data_frame[COLNAME]])
        pd_cumsum = ts_.diff().dt.total_seconds().cumsum()
    else:
        pd_cumsum = pd.to_datetime(data_frame[COLNAME]).diff().dt.total_seconds().cumsum()
    
    pd_cumsum[0] = 0
    return pd_cumsum

def conduct_dtw_v2(df_gps, 
                df_camera,
                gps_dt_col = 'datetime',
                camera_dt_col = 'updated_dates',
                STEP_PATTERN = stepPattern.asymmetric):

    """
    asymmetric pattern allows for selecting unique camera datetime, which is useful in this case. 
    It maybe worthwhile to test other methods. 
    """
    df_gps_copy = deepcopy(df_gps).rename(columns = {'index':'gps_index'})
    print(df_gps_copy)
    #df_gps_copy.reset_index(inplace = True)
    #is_duplicated = df_gps_copy.index.duplicated()
    #print(df_gps_copy[is_duplicated])
    # df_gps_copy = df_gps_copy.sort_values( by = gps_dt_col, 
    #         ascending = True
    #         ).reset_index(
    #         drop = True
    #         )
    df_camera_copy = deepcopy(df_camera).rename(columns = {'index': 'camera_index'})
    print(df_camera_copy)
    #df_camera_copy.reset_index(inplace = True)
    #is_duplicated = df_camera_copy.index.duplicated()
    #print(df_camera_copy[is_duplicated])
    # df_camera_copy = df_camera_copy.sort_values( by = camera_dt_col, 
    #         ascending = True
    #         ).reset_index(
    #         drop = True)

    gps_cumsum = get_ts_cumsum(df_gps_copy, gps_dt_col)
    camera_cumsum = get_ts_cumsum(df_camera_copy, camera_dt_col)
    
    dtw_align = dtw(camera_cumsum, gps_cumsum, keep_internals = True, step_pattern = STEP_PATTERN)
    print(len(dtw_align.index2.tolist()))
    print(len(dtw_align.index1.tolist()))
    df_matched_ = pd.concat(
    [df_gps_copy.loc[dtw_align.index2.tolist()].reset_index(drop=True), 
     df_camera_copy.loc[dtw_align.index1.tolist()].reset_index(drop=True)], 
    axis = 1)
    df_matched_['time_diff_sec'] = (pd.to_datetime(
    df_matched_['datetime']
    ) - pd.to_datetime(
    df_matched_['updated_datetime']
    )
    ).dt.total_seconds()
    return df_matched_

