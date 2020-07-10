package com.ringcentral.analytics.anaplan.utils;


import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ImportOptions {
    private static final String TLS_VERSION = "tls-version";

    private static final String ANAPLAN_AUTH_SERVER = "anaplan-auth-server";
    private static final String ANAPLAN_SERVER = "anaplan-server";
    private static final String ANAPLAN_CLIENT_NAME = "anaplan-client-name";
    private static final String ANAPLAN_PASSWORD = "anaplan-password";

    private static final OptionParser PARSER = createOptionParser();

    private final String tlsVersion;

    private final String anaplanAuthServer;
    private final String anaplanServer;
    private final String anaplanClientName;
    private final String anaplanPassword;

    public ImportOptions(final String[] args) {
        final OptionSet options = PARSER.parse(args);

        tlsVersion = options.valueOf(TLS_VERSION).toString();

        anaplanAuthServer = options.valueOf(ANAPLAN_AUTH_SERVER).toString();
        anaplanServer = options.valueOf(ANAPLAN_SERVER).toString();
        anaplanClientName = options.valueOf(ANAPLAN_CLIENT_NAME).toString();
        anaplanPassword = options.valueOf(ANAPLAN_PASSWORD).toString();
    }

    private static OptionParser createOptionParser() {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts(TLS_VERSION).withOptionalArg().defaultsTo("TLSv1.2");

        optionParser.accepts(ANAPLAN_AUTH_SERVER).withRequiredArg().ofType(String.class).defaultsTo("https://auth.anaplan.com/token/");
        optionParser.accepts(ANAPLAN_SERVER).withRequiredArg().ofType(String.class).defaultsTo("https://api.anaplan.com/2/0/");
        //TODO add cred
        optionParser.accepts(ANAPLAN_CLIENT_NAME).withRequiredArg().ofType(String.class);
        optionParser.accepts(ANAPLAN_PASSWORD).withRequiredArg().ofType(String.class);

        return optionParser;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }
    public String getAnaplanAuthServer() {
        return anaplanAuthServer;
    }

    public String getAnaplanServer() {
        return anaplanServer;
    }

    public String getAnaplanClientName() {
        return anaplanClientName;
    }

    public String getAnaplanPassword() {
        return anaplanPassword;
    }

}
