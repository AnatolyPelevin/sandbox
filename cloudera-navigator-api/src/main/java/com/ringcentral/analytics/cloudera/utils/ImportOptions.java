package com.ringcentral.analytics.cloudera.utils;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;

public class ImportOptions {
    private static final OptionParser PARSER = createOptionParser();
    /** temp location for created files with tables' info */
    private static final String TMP_LOCATION = "tmp-location";
    /** temp location for files with table list */
    private static final String TABLE_LIST_LOCATION = "table-list-location";

    private static final String TLS_VERSION = "tls-version";


    /** User*/
    private static final String USER = "cloudera-user";
    /** User psswd*/
    private static final String USER_PASSWORD = "cloudera-user-password";
    /** Cloudera navigator API URL*/
    private static final String CLOUDERA_API = "cloudera-api-url";

    /** database name from hive, where to search tables usage*/
    private static final String DATABASE_NAME = "database_name";
    /**
     * begin of a period for which it is needed to get data in milliseconds
     */
    private static final String START_TIME = "start_time";
    /**
     * end of a period for which it is needed to get data in milliseconds
     */
    private static final String END_TIME = "end_time";

    private static final String OFFSET = "offset";

    private final String tmpLocation;
    private final String tableListLocation;

    private final String tlsVersion;

    private final String clouderaUser;
    private final String clouderaUserPassword;

    private final String baseUrl;

    private final String databaseName;

    private final String startTime;
    private final String endTime;


    private final int offset;


    public ImportOptions(final String[] args) {
        final OptionSet options = PARSER.parse(args);

        tmpLocation = options.valueOf(TMP_LOCATION).toString();
        tableListLocation = options.valueOf(TABLE_LIST_LOCATION).toString();


        tlsVersion = options.valueOf(TLS_VERSION).toString();

        clouderaUser = options.valueOf(USER).toString();;
        clouderaUserPassword = options.valueOf(USER_PASSWORD).toString();

        baseUrl = options.valueOf(CLOUDERA_API).toString();

        databaseName = options.valueOf(DATABASE_NAME).toString();
        startTime = options.valueOf(START_TIME).toString();
        endTime = options.valueOf(END_TIME).toString();
        offset = Integer.parseInt(options.valueOf(OFFSET).toString());
    }

    private static OptionParser createOptionParser() {
        OptionParser optionParser = new OptionParser();

        optionParser.accepts(TMP_LOCATION).withRequiredArg().ofType(String.class).defaultsTo("C:\\Users\\anatoly.pelevin\\Documents\\REMOVE_TABLES_TASK\\tmp");
        optionParser.accepts(TABLE_LIST_LOCATION).withRequiredArg().ofType(String.class).defaultsTo("C:\\Users\\anatoly.pelevin\\Documents\\REMOVE_TABLES_TASK\\tableList");
        optionParser.accepts(TLS_VERSION).withOptionalArg().defaultsTo("TLSv1.2");

        //TODO add cred
        optionParser.accepts(USER).withRequiredArg().ofType(String.class).defaultsTo("anatoly.pelevin");
        optionParser.accepts(USER_PASSWORD).withRequiredArg().ofType(String.class).defaultsTo("!12");

        optionParser.accepts(CLOUDERA_API).withRequiredArg().ofType(String.class).defaultsTo("http://sjc01-c01-hdc03.c01.ringcentral.com:7187");

        optionParser.accepts(DATABASE_NAME).withRequiredArg().ofType(String.class).defaultsTo("lookup_tables_production");

        Long dateStartMS  = LocalDateTime.now().minusYears(1).toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();

        optionParser.accepts(END_TIME).withRequiredArg().ofType(String.class).defaultsTo(Long.toString(System.currentTimeMillis()));
        optionParser.accepts(START_TIME).withRequiredArg().ofType(String.class).defaultsTo(Long.toString(dateStartMS));

        optionParser.accepts(OFFSET).withRequiredArg().ofType(String.class).defaultsTo(Integer.toString(0));

        return optionParser;
    }

    public String getTmpLocation() {
        return tmpLocation;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }

    public String getClouderaUser() {
        return clouderaUser;
    }

    public String getClouderaUserPassword() {
        return clouderaUserPassword;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public int getOffset() {
        return offset;
    }

    public String getTableListLocation() {
        return tableListLocation;
    }

}
