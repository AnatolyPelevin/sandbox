#!/bin/bash

set -ex

VERTICA_URL="jdbc:vertica://{{ INTRANET_VERTICA_HOST }}:{{ INTRANET_VERTICA_PORT }}/{{ INTRANET_VERTICA_DB }}"
VERTICA_USER="{{ UMD_VERTICA_USER_NAME }}"
VERTICA_PASSWORD="{{ UMD_VERTICA_USER_PASSWORD }}"
VERTICA_DB="umd_production"

# Production data to be copied:
# account_licenses_snapshotted
#2020-07-04
#2020-07-05
#2020-07-06
#2020-07-08
#2020-07-14
#2020-07-20

# umd6_contract_licenses_snapshotted
#2020-07-04
#2020-07-05
#2020-07-06


LOG_FILE_NAME="DATA-6086-$(date +"%F-%T").log"
touch $LOG_FILE_NAME

declare -a DTS

DTS=('2020-07-04' '2020-07-05' '2020-07-06' '2020-07-08' '2020-07-14' '2020-07-20')

for i in "${DTS[@]}"
do
spark-shell -i copy_to_vertica.scala \
            --jars "hdfs://nameservice1/envs/production/UMD/workspace/oozie-apps/lib/vertica-jdbc-7.1.2-0.jar" \
            --conf spark.driver.args="$VERTICA_URL,$VERTICA_USER,$VERTICA_PASSWORD,$VERTICA_DB,$i,account_licenses_snapshotted" \
            --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
            --conf spark.executor.memoryOverhead=2048M  \
            --conf spark.executor.memory=2048M  \
            --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
            --conf spark.driver.memoryOverhead=2048M  \
            --conf spark.driver.memory=2048M  \
            --conf spark.dynamicAllocation.maxExecutors=8  \
            --conf spark.dynamicAllocation.enabled=true  \
            --conf spark.dynamicAllocation.initialExecutors=8  \
            --conf spark.dynamicAllocation.minExecutors=1 2>&1 | tee -a  $LOG_FILE_NAME
done

declare -a DTS

DTS=('2020-07-04' '2020-07-05' '2020-07-06')

for i in "${DTS[@]}"
do
spark-shell -i copy_to_vertica.scala \
            --jars "hdfs://nameservice1/envs/production/UMD/workspace/oozie-apps/lib/vertica-jdbc-7.1.2-0.jar" \
            --conf spark.driver.args="$VERTICA_URL,$VERTICA_USER,$VERTICA_PASSWORD,$VERTICA_DB,$i,umd6_contract_licenses_snapshotted" \
            --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
            --conf spark.executor.memoryOverhead=2048M  \
            --conf spark.executor.memory=2048M  \
            --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
            --conf spark.driver.memoryOverhead=2048M  \
            --conf spark.driver.memory=2048M  \
            --conf spark.dynamicAllocation.maxExecutors=8  \
            --conf spark.dynamicAllocation.enabled=true  \
            --conf spark.dynamicAllocation.initialExecutors=8  \
            --conf spark.dynamicAllocation.minExecutors=1 2>&1 | tee -a  $LOG_FILE_NAME
done
