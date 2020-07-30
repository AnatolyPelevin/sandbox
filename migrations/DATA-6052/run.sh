#!/bin/bash

set -ex

VERTICA_URL="jdbc:vertica://{{ INTRANET_VERTICA_HOST }}:{{ INTRANET_VERTICA_PORT }}/{{ INTRANET_VERTICA_DB }}"
VERTICA_USER="{{ UMD_VERTICA_USER_NAME }}"
VERTICA_PASSWORD="{{ UMD_VERTICA_USER_PASSWORD }}"
VERTICA_DB="umd_production"
DT="2020-06-28"

LOG_FILE_NAME="DATA-6052-$(date +"%F-%T").log"
touch $LOG_FILE_NAME

spark-shell -i migration.scala \
            --jars " hadoop fs -ls /envs/production/UMD/workspace/oozie-apps/lib/vertica-jdbc-7.1.2-0.jar" \
            --conf spark.driver.args="$VERTICA_URL,$VERTICA_USER,$VERTICA_PASSWORD,$VERTICA_DB,$DT" \
            --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1536  \
            --conf spark.executor.memoryOverhead=2048  \
            --conf spark.executor.memory=2048  \
            --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=1536  \
            --conf spark.driver.memoryOverhead=2048  \
            --conf spark.driver.memory=2048  \
            --conf spark.dynamicAllocation.maxExecutors=8  \
            --conf spark.dynamicAllocation.enabled=true  \
            --conf spark.dynamicAllocation.initialExecutors=8  \
            --conf spark.dynamicAllocation.minExecutors=1 &>> $LOG_FILE_NAME
