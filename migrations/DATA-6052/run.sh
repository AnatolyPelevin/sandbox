#!/bin/bash

set -ex

VERTICA_URL={{ VERTICA_URL }}
VERTICA_USER={{ VERTICA_USER }}
VERTICA_PASSWORD={{ VERTICA_PASSWORD }}
VERTICA_DB={{ VERTICA_DB }}

LOG_FILE_NAME="DATA-6052$(date +"%F-%T").log"
touch $LOG_FILE_NAME

spark-shell -i migration.scala \
            --packages "com.vertica:vertica-jdbc:7.1.2-0" \
            --conf spark.driver.args="$VERTICA_URL,$VERTICA_USER,$VERTICA_PASSWORD,$VERTICA_DB" \
            --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=3072  \
            --conf spark.executor.memoryOverhead=6144  \
            --conf spark.executor.memory=9216  \
            --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=3072  \
            --conf spark.driver.memoryOverhead=4096  \
            --conf spark.driver.memory=4096  \
            --conf spark.dynamicAllocation.maxExecutors=4  \
            --conf spark.dynamicAllocation.enabled=true  \
            --conf spark.dynamicAllocation.initialExecutors=4  \
            --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME
