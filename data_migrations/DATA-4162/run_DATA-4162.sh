#!/bin/bash

START_TIME="$(date +"%F %T")"
SOURCE_DB='clp_production'
TARGET_DB='hdg_production'

DATE_START=${1:-'2003-08-01'}
DATE_END=${2:-'2019-12-01'}

LOG_FILE_NAME="$(date +"%F-%T")_clp_hdg_migration.log"

declare -a TABLES

TABLES=('abentrylog' 'clog' 'emaillog' 'eventslog' 'mobilelog' 'packagesstatus' 'sessionlog' 'sipagent')

for i in "${TABLES[@]}"
do
	echo "Start migration of $i" 2>&1 | tee -a $LOG_FILE_NAME

	time spark-shell -i data_migration_DATA-4306.scala --conf spark.driver.args="$SOURCE_DB,$TARGET_DB,$DATE_START,$DATE_END,$i"  \
	                                                   --conf spark.executor.memory=2G  \
	                                                   --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1024M  \
	                                                   --conf spark.executor.memoryOverhead=2536M  \
	                                                   --conf spark.driver.memory=2G  \
	                                                   --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=512M  \
	                                                   --conf spark.driver.memoryOverhead=2536M  \
	                                                   --conf spark.dynamicAllocation.enabled=true  \
	                                                   --conf spark.dynamicAllocation.maxExecutors=10 &>> $LOG_FILE_NAME

	echo "Migration of $i is done" 2>&1 | tee -a $LOG_FILE_NAME
done

END_TIME="$(date +"%F %T")"

echo "Start time: $START_TIME" 2>&1 | tee -a $LOG_FILE_NAME
echo "End time: $END_TIME" 2>&1 | tee -a $LOG_FILE_NAME