#!/bin/bash

set -ex

START_DATE=$1
END_DATE=$2
targetPartition=$3
hiveSchemaName=$4
hiveTableName=$5
outDataPath=$6
iisUrl=$7
beelineUrl=$8

loopEndDate=$(date -I -d "$START_DATE - 1 day")
d=$(date -I -d "$END_DATE")

while [ "$d" != "$loopEndDate" ]; do
  echo "Processing date: $d"
  hdfs dfs -mkdir $outDataPath/$hiveTableName/dt=$d
  hadoop fs -cp $outDataPath/$hiveTableName/dt=$targetPartition/* $outDataPath/$hiveTableName/dt=$d
  d=$(date -I -d "$d - 1 day")
done
echo "Run msck refresh table"
beeline -u $beelineUrl --hivevar hive_schema_name=$hiveSchemaName --hivevar hive_table_name=$hiveTableName -f refresh_hive_table.sql
echo "Send request to invalidate metadata"
curl -X POST $iisUrl/invalidate/$hiveSchemaName/$hiveTableName
