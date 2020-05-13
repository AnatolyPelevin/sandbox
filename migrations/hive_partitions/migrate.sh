#!/bin/bash

prod_env="PROD"
stage_env="STAGE"

START_DATE=$2
END_DATE=$3
TARGET_PARTITION=$4

if [[ "$1" == "$prod_env" ]] ; then
hive_schema_name="sfdc_production"
outDataPath="hdfs://nameservice1/envs/production/SFDC/out-data/SFDC_ETL_postgres"
impalaUrl="http://impala_invalidate:A53d21e6f22@sjc01-c01-hdc05.c01.ringcentral.com:8080"
beelineUrl="jdbc:hive2://hiveserver.ringcentral.com:10000/default;principal=hive/_HOST@RINGCENTRAL.COM"
elif [[ "$1" == "$stage_env" ]] ; then
hive_schema_name="sfdc_release"
outDataPath="hdfs://nameservice1/envs/release/SFDC/out-data/SFDC_ETL_postgres"
impalaUrl="http://impala_invalidate:A53d21e6f22@sjc06-c01-hdc13.ops.ringcentral.com:8080"
beelineUrl="jdbc:hive2://hiveserver.ops.ringcentral.com:10000/default;principal=hive/_HOST@RINGCENTRAL.COM"
else
hive_schema_name="sfdc_dev_hf_1_1_4"
outDataPath="hdfs://nameservice1/envs/dev/SFDC/hf-1.1.4/out-data/SFDC_ETL_postgres"
impalaUrl="http://admin:password@bda01-t01-hdc03.lab.nordigy.ru:8080"
beelineUrl="jdbc:hive2://bda01-t01-hdc02:10000/default;principal=hive/_HOST@LAB.NORDIGY.RU"
fi
hive_table_name="demand_funnel__c"

loopEndDate=$(date -I -d "$START_DATE - 1 day")
d=$(date -I -d "$END_DATE")
while [ "$d" != "$loopEndDate" ]; do
  echo "Processing date: $d"
  hdfs dfs -mkdir $outDataPath/$hive_table_name/dt=$d
  hadoop fs -cp $outDataPath/$hive_table_name/dt=$TARGET_PARTITION/* $outDataPath/$hive_table_name/dt=$d
  d=$(date -I -d "$d - 1 day")
done
echo "Run msck refresh table"
beeline -u $beelineUrl --hivevar hive_schema_name=$hive_schema_name --hivevar hive_table_name=$hive_table_name -f hive.sql
echo "Send request to invalidate metadata"
curl -X POST $impalaUrl/invalidate/$hive_schema_name/$hive_table_name
