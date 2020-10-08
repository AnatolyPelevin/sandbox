#!/bin/bash
set -xe

# date settings
START_DATE="2020-09-26" # exclusive
END_DATE="2020-09-28"   # inclusive
COPY_DATE="2020-09-29"

beelineUrl="jdbc:hive2://hiveserver.ringcentral.com:10000/default;principal=hive/_HOST@RINGCENTRAL.COM"

table_billing="usf.billing_location_history"
table_sip="usf.sip_location_history"
table_phone="usf.usf_phone_codes_history"

COPY_YEAR=$(date -d "$COPY_DATE" '+%Y')
COPY_MONTH=$(date -d "$COPY_DATE" '+%-m')
COPY_DAY=$(date -d "$COPY_DATE" '+%-d')

d=$(date -I -d "$END_DATE")
loopEndDate=$(date -I -d "$START_DATE")
while [ "$d" != "$loopEndDate" ]; do
  echo "Processing date: $d"
  
  YEAR=$(date -d "$d" '+%Y')
  MONTH=$(date -d "$d" '+%-m')
  DAY=$(date -d "$d" '+%-d')
  
  QUERY_BILLING="insert overwrite table $table_billing partition (year=$YEAR,month=$MONTH,day=$DAY) select userid,billing_state,billing_country from $table_billing where year=$COPY_YEAR and month=$COPY_MONTH and day=$COPY_DAY;"
  QUERY_SIP="insert overwrite table $table_sip partition (year=$YEAR,month=$MONTH,day=$DAY) select instanceid,country,state from $table_sip where year=$COPY_YEAR and month=$COPY_MONTH and day=$COPY_DAY;"
  QUERY_PHONE_CODES="insert overwrite table $table_phone partition (year=$YEAR,month=$MONTH,day=$DAY) select phone_code,country,state from $table_phone where year=$COPY_YEAR and month=$COPY_MONTH and day=$COPY_DAY;"
  
  beeline -u $beelineUrl -e "$QUERY_BILLING"
  beeline -u $beelineUrl -e "$QUERY_SIP"
  beeline -u $beelineUrl -e "$QUERY_PHONE_CODES"
  
  d=$(date -I -d "$d - 1 day")
done