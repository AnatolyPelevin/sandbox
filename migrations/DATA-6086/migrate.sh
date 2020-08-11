#!/bin/bash

set -ex

START_DATE=$1
END_DATE=$2
sourcePartition=$3
hiveSchemaName=$4
hiveFolderName=$5
outDataPath=$6
iisUrl=$7
beelineUrl=$8
hiveTableName=$9

d=$(date -I -d "$START_DATE")
loopEndDate=$(date -I -d "$END_DATE + 1 day")

while [ "$d" != "$loopEndDate" ]; do
  echo "Processing date: $d"
  hadoop fs -mkdir $outDataPath/$hiveFolderName/dt=$d
  hadoop fs -cp $outDataPath/$hiveFolderName/dt=$sourcePartition/* $outDataPath/$hiveFolderName/dt=$d
  d=$(date -I -d "$d + 1 day")
done
echo "Run refresh table"
beeline -u $beelineUrl --hivevar hive_schema_name=$hiveSchemaName --hivevar hive_table_name=$hiveTableName -f refresh_hive_table.sql
echo "Send request to invalidate metadata"
curl -X POST $iisUrl/invalidate/$hiveSchemaName/$hiveTableName
