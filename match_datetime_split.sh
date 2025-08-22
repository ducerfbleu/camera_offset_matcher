#!/bin/bash
set -e # exit when non-zero exit status
usage(){
	echo "Usage: $(basename "$0") -g <.csv file path> -i <directory>"
	echo "             where command is one of the following: "
	echo "                   g (gps_csv)                  - parsed gps csv log file---output of parse_gpslog_and_reformat.py"
	echo "                   i (image_dir)                - Enter path to camera image files"
    echo "                   o (output_dir)               - Enter dir to output matching results"
    echo "Please note that this script depends on the following:"
    echo " 1. exiftool"
    echo " 2. geotagre.py"
    echo " 3. reconsile_offset_v3.py"
    echo " 4. match_datetime_v3.py"
   	exit 1

}

while getopts ":h:g:i:o:" opt; do
	case "$opt" in 
		h)
			usage
			exit 1
			;;

		g) GPS_LOG=$OPTARG ;;
		i) IMG_DIR=$OPTARG ;;
        o) OUT_DIR=$OPTARG ;;
		\\?) echo "invalid option has been entered: $OPTARG" >&2; exit1 ;;
	esac
done

if [[ -z $GPS_LOG || -z $IMG_DIR || -z $OUT_DIR ]]; then
    echo "Error: -i (image_dir) and -g (gps_csv) -o (output_dir) are mendatory arguments."; 
    usage ;
    exit 1;
fi

CURR_DIR=$(dirname $(realpath "$0"))
echo "** gps_csv: $GPS_LOG"
echo "** image_path: $IMG_DIR"
IMG_DIR_ABS=$(realpath ${IMG_DIR})
IMG_DIR_BASE=$(basename ${IMG_DIR_ABS})
IMG_DIR_DIR=$(basename $(dirname ${IMG_DIR_ABS}))
echo "$IMG_DIR_ABS"
echo "$IMG_DIR_DIR"
OUT_NAME="${IMG_DIR_DIR}_${IMG_DIR_BASE}"
OUT_IMG_EXIF=${OUT_NAME}_exif.csv
echo "** image exif output: $OUT_IMG_EXIF"

printf "\n*******\n"
echo "Processing all images within $IMG_DIR..."
exiftool -DateTimeOriginal -csv -c "%%.6f" $IMG_DIR/*.JPG > $OUT_IMG_EXIF

printf "\n*******\n"

### here splitting algorithm goes in #####
### 1. make a directory #####

mkdir -p $OUT_DIR

#### 2. split gps data & image data into different csv files
# save stdout to variables
GPS_SPLIT_OUT=$(python $CURR_DIR/split_csv.py -c $GPS_LOG -tc datetime -o $OUT_DIR -t 300) 
EXIF_SPLIT_OUT=$(python $CURR_DIR/split_csv.py -c $OUT_IMG_EXIF -tc DateTimeOriginal -o $OUT_DIR -t 300 -f "%Y:%m:%d %H:%M:%S")

echo -e "$GPS_SPLIT_OUT"
echo -e "$EXIF_SPLIT_OUT"

BASE_GPS_LOG=$(basename ${GPS_LOG})
BASENAME_GPS_LOG=${BASE_GPS_LOG%.*}
BASE_IMG_EXIF=$(basename ${OUT_IMG_EXIF})
BASENAME_IMG_EXIF=${BASE_IMG_EXIF%.*}

NUM_GPS_SPLIT="${GPS_SPLIT_OUT##*$'\n'}"
NUM_EXIF_SPLIT="${EXIF_SPLIT_OUT##*$'\n'}"

if [ "$NUM_GPS_SPLIT" -ne "$NUM_EXIF_SPLIT" ]; then
    echo "Error: number of splits are different. Please use manual method for splitting." >&2
    exit 1
fi

echo "Number of split matches ($NUM_GPS_SPLIT)...Proceeding with split-geotagging..."

for num in $(seq 0 $(("NUM_GPS_SPLIT"-1))); do
    IDX=$(printf "%02d" $num)
    # echo ${BASENAME_GPS_LOG}_${IDX}.csv
    echo "Correcting datetime offsets in image exif...${BASENAME_IMG_EXIF}_${IDX}.csv"
    python $CURR_DIR/reconsile_offset_v3.py -gps ${OUT_DIR}/${BASENAME_GPS_LOG}_${IDX}.csv -exif ${OUT_DIR}/${BASENAME_IMG_EXIF}_${IDX}.csv -o $OUT_DIR -df "%Y-%m-%d %H:%M:%S"
    echo "Done."
    # echo ${BASENAME_IMG_EXIF}_${IDX}.csv 
    echo "Matching camera datetime to GPS datetime for geotagging..."
    python $CURR_DIR/match_datetime_v4.py -gps ${OUT_DIR}/${BASENAME_GPS_LOG}_${IDX}.csv -exif ${OUT_DIR}/${BASENAME_IMG_EXIF}_${IDX}_corrected_dt.csv -n 8 -o ${BASENAME_IMG_EXIF}_${IDX}_MATCHED -od ${OUT_DIR}
    echo "Done. matching results are in ${OUT_DIR}."
done

python $CURR_DIR/merge_dataframes.py -b ${BASENAME_IMG_EXIF} -o ${OUT_DIR}

