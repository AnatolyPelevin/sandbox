package com.ringcentral.analytics.adobe.utils.reports;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public enum WebMarketingTrackerEnum {
    GLOBAL_UV_PAGEVIEWS(1, "Global_UV_And_Pageviews_UV_and_Pageviews"),
    GLOBAL_PROSPECT_VS_CUSTOMER(2,"Global_UV_And_Pageviews_Prospect_vs_Customers"),
    GLOBAL_UV_PAGEVIEWS_PER_CHANNEL(3,"Global_UV_And_Pageviews_Per_Channel"),
    USCA_SEGMENT(4,"Web_Marketing_Tracker_USCA_Segment"),
    UK_SEGMENT(5,"Web_Marketing_Tracker_UK_Segment"),
    RC_MEETING_SEGMENT(6,"Web_Marketing_RC_Meeting_Segment");

    public final int loadOrder;
    public final String fileName;


    private WebMarketingTrackerEnum(int loadOrder, String fileName) {
        this.loadOrder = loadOrder;
        this.fileName=fileName;
    }

    public int getLoadOrder() {
        return loadOrder;
    }

    public String getFileName() {
        return fileName;
    }

    public static List<String> getReportLoadOrder (){
        return  EnumSet.allOf(WebMarketingTrackerEnum.class)
                .stream()
                .sorted(Comparator.comparingInt(WebMarketingTrackerEnum::getLoadOrder))
                .map(WebMarketingTrackerEnum::getFileName)
                .collect(Collectors.toList());
    }
}
