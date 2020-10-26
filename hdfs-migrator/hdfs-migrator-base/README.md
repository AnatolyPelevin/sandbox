#HDFS Migration tool

Example for start migration on lab for branch FB_DATA_6343
```bash
        spark-submit --class com.ringcentral.analytics.etl.HdfsMigrator \
                     --master yarn-client hdfs-migrator-base-1.0.0-jar-with-dependencies.jar  \
                     --hive-db-name=lookup_tables_dev_fb_data_6343 \
                     --etldb-database=ERD_DEV_FB_DATA_6343 \
                     --etldb-connection-string='jdbc:postgresql://bda01-t01-hdm01.lab.nordigy.ru:5432/apps?currentSchema=ERD_DEV_FB_DATA_6343&zeroDateTimeBehavior=convertToNull' \
                     --etldb-user=${etl_user} \
                     --etldb-password=${etl_password} \
                     --application-name=hdfs-migrator \
                     --etl-log-table=ETL_LOGS \
                     --table-config-table-name=ORACLE_ETL_TABLES_CONF \
                     --out-data-path='hdfs://nameservice1/envs/dev/ERD/FB-DATA-6343/out-data/ERD_ETL_oracle'
```
