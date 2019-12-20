#!/bin/bash

set -ex

START_TIME="$(date +"%F %T")"

SOURCE_DB='clp_production'
TARGET_DB='hdg_production'

DATE_START=${1:-'2011-01-01'}
DATE_END=${2:-'2019-12-20'}

ABENTRYLOG_TABLE='abentrylog'
CLOG_TABLE='clog'
EMAILLOG_TABLE='emaillog'
EVENTSLOG_TABLE='eventslog'
MOBILELOG_TABLE='mobilelog'
PACKAGESSTATUS_TABLE='packagesstatus'
SESSIONLOG_TABLE='sessionlog'
SIPAGENT_TABLE='sipagent'

LOG_FILE_NAME="$(date +"%F-%T").log"

touch $LOG_FILE_NAME

echo "Source DB: $SOURCE_DB" &>> $LOG_FILE_NAME
echo "Target DB: $TARGET_DB" &>> $LOG_FILE_NAME

echo "Start date: $DATE_START" &>> $LOG_FILE_NAME
echo "End date: $DATE_END" &>> $LOG_FILE_NAME

echo "Start migration of $ABENTRYLOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$ABENTRYLOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $ABENTRYLOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $CLOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$CLOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $CLOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $EMAILLOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$EMAILLOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $EMAILLOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $EVENTSLOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$EVENTSLOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $EVENTSLOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $MOBILELOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$MOBILELOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $MOBILELOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $PACKAGESSTATUS_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$PACKAGESSTATUS_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $PACKAGESSTATUS_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $SESSIONLOG_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$SESSIONLOG_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $SESSIONLOG_TABLE is done" &>> $LOG_FILE_NAME


echo "Start migration of $SIPAGENT_TABLE" &>> $LOG_FILE_NAME

time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$SIPAGENT_TABLE" --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.executor.memoryOverhead=1536  --conf spark.executor.memory=2G  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2048M  --conf spark.driver.memoryOverhead=1536  --conf spark.driver.memory=2G  --conf spark.dynamicAllocation.maxExecutors=10  --conf spark.dynamicAllocation.enabled=true  --conf spark.dynamicAllocation.initialExecutors=10  --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME

echo "Migration of $SIPAGENT_TABLE is done" &>> $LOG_FILE_NAME

END_TIME="$(date +"%F %T")"

echo "Start time: $START_TIME" &>> $LOG_FILE_NAME
echo "End time: $END_TIME" &>> $LOG_FILE_NAME