#!/bin/bash

set -ex

START_TIME="$(date +"%F %T")"

SOURCE_DIR='/user/youval.dar/sonus_with_account_info'
TARGET_DIR='/user/youval.dar/sonus_with_account_info'

TMP_DIR='/tmp/compression_tmp_sonus_4306'

DATE_START=${1:-'2015-12-31'}
DATE_END=${2:-'2019-12-03'}

DATA_TYPE='textfile'

LOG_FILE_NAME="$(date +"%F-%T").log"

touch $LOG_FILE_NAME

echo "Source dir: $SOURCE_DIR" &>> $LOG_FILE_NAME
echo "Target dir: $TARGET_DIR" &>> $LOG_FILE_NAME

echo "Start date: $DATE_START" &>> $LOG_FILE_NAME
echo "End date: $DATE_END" &>> $LOG_FILE_NAME
echo "Data type: $DATA_TYPE" &>> $LOG_FILE_NAME

echo "Size of dir before compression ================================================" >> $LOG_FILE_NAME

hadoop fs -du -h $SOURCE_DIR &>> $LOG_FILE_NAME

hadoop fs -du -s -h $SOURCE_DIR &>> $LOG_FILE_NAME

hadoop fs -rm -r -f $TMP_DIR &>> $LOG_FILE_NAME

hadoop fs -mkdir $TMP_DIR &>> $LOG_FILE_NAME

echo "$TMP_DIR is created" &>> $LOG_FILE_NAME

time spark-shell -i compression_DATA-4306.scala --conf spark.driver.args="$SOURCE_DIR,$TARGET_DIR,$TMP_DIR,$DATE_START,$DATE_END,$DATA_TYPE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

hadoop fs -rm -r $TMP_DIR &>> $LOG_FILE_NAME

echo "$TMP_DIR is removed" &>> $LOG_FILE_NAME

echo "Size of dir after compression ================================================" >> $LOG_FILE_NAME

hadoop fs -du -h $TARGET_DIR &>> $LOG_FILE_NAME

hadoop fs -du -s -h $TARGET_DIR &>> $LOG_FILE_NAME

echo "Compression is done" &>> $LOG_FILE_NAME

END_TIME="$(date +"%F %T")"

echo "Start time: $START_TIME" &>> $LOG_FILE_NAME
echo "End time: $END_TIME" &>> $LOG_FILE_NAME