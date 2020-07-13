#!/bin/bash

LOG_FILE_NAME="$(date +"%F_%H-%M-%S")_clp_hdg_migration.log"
START_TIME="$(date +"%F %T")"

while getopts e:t:d: flag
do
    case "${flag}" in
		e) ENVIRONMENT=${OPTARG};;
		t) TABLES=${OPTARG};;
		d) DATES=${OPTARG};;
    esac
done

echo "environment: $ENVIRONMENT";
echo "tables: $TABLES";
echo "dates: $DATES";


time spark-shell -i migration.scala --conf spark.driver.args="$ENVIRONMENT,$TABLES,$DATES"  \
	                                  --conf spark.executor.memory=2G  \
	                                  --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1024M  \
	                                  --conf spark.executor.memoryOverhead=2536M  \
                                    --conf spark.driver.memory=2G  \
	                                  --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=512M  \
	                                  --conf spark.driver.memoryOverhead=2536M  \
	                                  --conf spark.dynamicAllocation.enabled=true  \
	                                  --conf spark.dynamicAllocation.maxExecutors=10 2>&1 | tee -a  $LOG_FILE_NAME

END_TIME="$(date +"%F %T")"

echo "Start time: $START_TIME" 2>&1 | tee -a $LOG_FILE_NAME
echo "End time: $END_TIME" 2>&1 | tee -a $LOG_FILE_NAME