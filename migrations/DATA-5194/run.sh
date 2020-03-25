#!/bin/bash

set -ex

FOLDER="/envs/production/SMS/out-data"
TABLE_LOCATION="$FOLDER/smg_statistic_log"
TMP_LOCATION="$FOLDER/tmp"
DESTINATION_LOCATION="$FOLDER/smg_statistic_log"
DELETE_FILES_FROM_SOURCE="true"
START_DATE="2019-10-06"
END_DATE="2019-10-17"

LOG_FILE_NAME="$(date +"%F-%T").log"
touch $LOG_FILE_NAME

spark-shell -i migration.scala --conf spark.driver.args="$TABLE_LOCATION,$TMP_LOCATION,$DESTINATION_LOCATION,$DELETE_FILES_FROM_SOURCE,$START_DATE,$END_DATE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME
