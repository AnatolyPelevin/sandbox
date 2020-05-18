#!/bin/bash

set -ex

FOLDER=$1
TABLE_NAME=$2
TABLE_LOCATION="$FOLDER/$TABLE_NAME"
TMP_LOCATION="$FOLDER/tmp"
DESTINATION_LOCATION="$FOLDER/$TABLE_NAME"

CORRECT_PARTITION="$TABLE_LOCATION/dt=2020-04-27"

START_DATE_ADD_FIELD=${3:-'2020-04-27'}
END_DATE_ADD_FIELD=${4:-'2020-05-14'}

DELETE_FILES_FROM_SOURCE=${5:-'false'}

LOG_FILE_NAME="add_field_$(date +"%F-%T").log"
touch $LOG_FILE_NAME

spark-shell -i migrationAddField.scala \
            --conf spark.driver.args="$TABLE_LOCATION,$TMP_LOCATION,$DESTINATION_LOCATION,$DELETE_FILES_FROM_SOURCE,$START_DATE_ADD_FIELD,$END_DATE_ADD_FIELD" \
            --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  \
            --conf spark.executor.memoryOverhead=1536  \
            --conf spark.executor.memory=2G  \
            --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  \
            --conf spark.driver.memoryOverhead=1536  \
            --conf spark.driver.memory=2G  \
            --conf spark.dynamicAllocation.maxExecutors=10  \
            --conf spark.dynamicAllocation.enabled=true  \
            --conf spark.dynamicAllocation.initialExecutors=10  \
            --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME