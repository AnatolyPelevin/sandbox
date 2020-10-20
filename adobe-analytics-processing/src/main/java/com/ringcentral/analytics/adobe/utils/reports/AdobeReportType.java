package com.ringcentral.analytics.adobe.utils.reports;

public enum AdobeReportType {
    GTM_DECK("GTM_Deck", GTMDeckEnum.class),
    WEB_MARKETING_TRACKER("Web_Marketing_Tracker", WebMarketingTrackerEnum.class);


    public final String reportPath;
    public final Class enumClass;


    private AdobeReportType(String reportPath, Class enumClass) {
        this.reportPath = reportPath;
        this.enumClass=enumClass;
    }

    public String getReportPath() {
        return reportPath;
    }

    public Class getEnumClass() {
        return enumClass;
    }
}
