# Camera-offset Matcher
GPS log parsing & camera date time exact matching script.

Dependency:
- `exiftool` (perl script, can install through conda; `conda install -c conda-forge exiftool`)
- `dtw-python` (install through conda, using `conda install conda-forge::dtw-python`)
- `pandas` (install through conda, with `conda install -c conda-forge::pandas`)
- `geotagre.py` (python file for custom functions, must be in the same directory as the command scripts)
- `reconsile_offset_v3.py` (match_datetime.sh internally calls this script)
- `match_datetime_v4.py` (macth_datetime.sh internally calls this script)

## 0. clone git & prep the environment

### clone git repo

```bash

git clone https://github.com/vwhvpwvk/camera_offset_matcher.git

```

### creating the environment

```bash
module load miniconda3
conda create -n py311_gps python==3.11

```

### activate the environment & install dependencies

```bash
source activate py311_gps
conda install -c conda-forge pandas dtw-python
conda install -c conda-forge exiftool
```
Now you are ready to go with running the scripts!

## 1. parse_gpslog_and_reformat_dt.py

dependency: pandas

### Usage: 

```bash
python camera_offset_matcher/parse_gpslog_and_reformat_dt.py [-h] -f FILENAME -l LINE_BEGINS [-tc TARGET_COLUMN]

this is a code to get datetime from unix epoch.

options:
  -h, --help            show this help message and exit
  -f FILENAME, --filename FILENAME
                        The name of the file to process
  -l LINE_BEGINS, --line_begins LINE_BEGINS
                        Specify line number that the script reads. The script removes every
                        line before the specified line number.
  -tc TARGET_COLUMN, --target_column TARGET_COLUMN
                        column name of datetime data with epoch ms
```
### example usage:

```bash

python camera_offset_matcher/parse_gpslog_and_reformat_dt.py -f gps_data.csv -l 3

```

### explanation of the example:
- The script takes [gps_data.csv], remove all lines before line [3].
- It reads from line 3, and removing all trailing white spaces in the column. 
- It takes column with epoch milisecond (defaults to column name 'timestamp') and converts to "%Y-%m-%d %H:%M:%S.%f" format.
- It adds a new column, "datetime", with the new format: "%Y-%m-%d %H:%M:%S.%f".
- It outputs the data frame to a new filename that adds "_clean_{todays_date}.csv" to the input base filename.

## 2. match_datetime.sh

### Usage: 

```bash 
bash camera_offset_matcher/match_datetime.sh -g <.csv file path> -i <directory>
             where command is one of the following:
                   g (gps_csv)                  - parsed gps csv log file---output of parse_gpslog_and_reformat.py
                   i (image_dir)                - Enter path to camera image files
Please note that this script depends on the following:
 1. exiftool
 2. geotagre.py
 3. reconsile_offset_v3.py
 4. match_datetime_v3.py
```

### example usage:

```bash

bash camera_offset_matcher/match_datetime.sh -g gps_data_clean.csv -i image/directory

```

### explanation of the example:

- The script takes [gps_data_clean.csv], and extract the first gps datetime value for calculating offset
- It takes [image/directory], and extract DateTimeOriginal and SourceFile name using exiftool, saving it to csv file. 
- It takes [exif csv file] and extract the first camera datetime value for calculating offset.
- It gets the offset by : gps datetime - camera_datetime.
- It applies the offset to make a new adjusted column in the [exif csv file].
- It finds the best exact match of each gps time points to each adjusted camera date time.
- It uses total three different algorithm, forward match, backward match, and dynamic time wrapping to conduct the exact match.
- Dynamic time wrapping is supposed to address dynamic offset, however, it doesn't work always. In case it fails, the algorithm defaults to forward / backward match.


## 2-a. match_datetime_split.sh

split csv data based on breaks in datetime. Default threshold for breaks is 300 seconds (5 minutes).

### Usage: 

```bash 
bash camera_offset_matcher/match_datetime_split.sh -g <.csv file path> -i <directory> -o <directory>
             where command is one of the following:
                   g (gps_csv)                  - parsed gps csv log file---output of parse_gpslog_and_reformat.py
                   i (image_dir)                - Enter path to camera image files
                   o (output_dir)               - Enter path of new output directory
Please note that this script depends on the following:
 1. exiftool
 2. geotagre.py
 3. reconsile_offset_v3.py
 4. match_datetime_v3.py
```

### example usage:

```bash

bash camera_offset_matcher/match_datetime.sh -g gps_data_clean.csv -i image/directory -o output_directory

```

### explanation of the example:

- The script takes [gps_data_clean.csv], and extract the first gps datetime value for calculating offset
- It takes [image/directory], and extract DateTimeOriginal and SourceFile name using exiftool, saving it to csv file. 
- It takes [exif csv file] and extract the first camera datetime value for calculating offset.
- It gets the offset by : gps datetime - camera_datetime.
- It applies the offset to make a new adjusted column in the [exif csv file].
- It finds the best exact match of each gps time points to each adjusted camera date time.
- It uses total three different algorithm, forward match, backward match, and dynamic time wrapping to conduct the exact match.
- Dynamic time wrapping is supposed to address dynamic offset, however, it doesn't work always. In case it fails, the algorithm defaults to forward / backward match.

## 3. geotag_images.py

### example usage:

```bash
python camera_offset_matcher/geotag_images.py -match matched_results.csv -dir image/directory -n 12

```



