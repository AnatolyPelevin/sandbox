#!/bin/bash

set -ex

FOLDER="/envs/production/SFDC/out-data/SFDC_ETL_postgres"
TABLE_LOCATION="$FOLDER/account_hierarchy__c"
TMP_LOCATION="$FOLDER/tmp"
DESTINATION_LOCATION="$FOLDER/account_hierarchy__c"

CORRECT_PARTITION="$TABLE_LOCATION/dt=2020-04-27"

DELETE_FILES_FROM_SOURCE="false"

START_DATE_DT_REMOVE="2018-09-25"
END_DATE_DT_REMOVE="2020-01-14"

START_DATE_ADD_FIELD="2020-04-28"
END_DATE_ADD_FIELD="2020-05-07"

LOG_FILE_NAME="$(date +"%F-%T").log"
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

spark-shell -i migrationDtRemove.scala \
            --conf spark.driver.args="$TABLE_LOCATION,$TMP_LOCATION,$DESTINATION_LOCATION,$DELETE_FILES_FROM_SOURCE,$START_DATE_DT_REMOVE,$END_DATE_DT_REMOVE" \
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