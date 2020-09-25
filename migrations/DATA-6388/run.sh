#!/bin/bash

set -ex

LATESTDATE="YYYY-MM-DD"
OLDESTDATE="YYYY-MM-DD"
IMPALA_INVALIDATE_SERVICE_URL=""

LOG_FILE_NAME="DATA-6388-$(date +"%F-%T").log"
touch $LOG_FILE_NAME

spark-shell -i migration.scala \
                    --conf spark.driver.args="$LATESTDATE,$OLDESTDATE" \
                    --conf spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
                    --conf spark.executor.memoryOverhead=2048M  \
                    --conf spark.executor.memory=2048M  \
                    --conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=1536M  \
                    --conf spark.driver.memoryOverhead=2048M  \
                    --conf spark.driver.memory=2048M  \
                    --conf spark.dynamicAllocation.maxExecutors=8  \
                    --conf spark.dynamicAllocation.enabled=true  \
                    --conf spark.dynamicAllocation.initialExecutors=8  \
                    --conf spark.dynamicAllocation.minExecutors=1 2>&1>> $LOG_FILE_NAME

curl -X POST $IMPALA_INVALIDATE_SERVICE_URL/invalidate/sfdc_production/dfr_pivot_w_formulas

