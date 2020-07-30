#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]
then
  echo "Download files <<Color: (yellow, green, fhv)>> <<Years: 2019,2020, etc (2009-2020)>>"
else
  for i in $(echo $2 | sed "s/,/ /g")
  do
    aws s3 cp --recursive --exclude="*" --include="$1_tripdata_$i*.csv" s3://nyc-tlc/trip\ data/ /mnt/nyctaxi/$1/year=$i/
  done
fi

