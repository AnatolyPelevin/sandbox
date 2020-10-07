#!/bin/bash
set -xe

# date settings
DUPLICATE_DATE="2020-09-26" # date to remove duplicates (for billing_location and sip_location)
COPY_DATE="2020-09-29"      # date to copy from (for usf_phone_codes_history only)

beelineUrl="jdbc:hive2://hiveserver.ringcentral.com:10000/default;principal=hive/_HOST@RINGCENTRAL.COM"

table_billing="usf.billing_location_history"
table_sip="usf.sip_location_history"
table_phone="usf.usf_phone_codes_history"

YEAR=$(date -d "$DUPLICATE_DATE" '+%Y')
MONTH=$(date -d "$DUPLICATE_DATE" '+%-m')
DAY=$(date -d "$DUPLICATE_DATE" '+%-d')

COPY_YEAR=$(date -d "$COPY_DATE" '+%Y')
COPY_MONTH=$(date -d "$COPY_DATE" '+%-m')
COPY_DAY=$(date -d "$COPY_DATE" '+%-d')

QUERY_BILLING="insert overwrite table $table_billing partition (year=$YEAR,month=$MONTH,day=$DAY) select distinct userid,billing_state,billing_country from $table_billing where year=$YEAR and month=$MONTH and day=$DAY;"
QUERY_SIP="insert overwrite table $table_sip partition (year=$YEAR,month=$MONTH,day=$DAY) select distinct instanceid,country,state from $table_sip where year=$YEAR and month=$MONTH and day=$DAY;"
QUERY_PHONE_CODES="insert overwrite table $table_phone partition (year=$YEAR,month=$MONTH,day=$DAY) select phone_code,country,state from $table_phone where year=$COPY_YEAR and month=$COPY_MONTH and day=$COPY_DAY;"

beeline -u $beelineUrl -e "$QUERY_BILLING"
beeline -u $beelineUrl -e "$QUERY_SIP"
beeline -u $beelineUrl -e "$QUERY_PHONE_CODES"

