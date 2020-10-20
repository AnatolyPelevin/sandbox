package com.ringcentral.analytics.adobe.utils.reports;

import java.util.*;
import java.util.stream.Collectors;

public enum GTMDeckEnum {
    GLOBAL_WITH_MEET(1, "Global_UV_With_Meetings_Net_Of_COVID_Table"),
    GLOBAL_WITH_MEET_PER_CHANNEL(2,"Global_UV_Per_Channel_With_Meetings_Net_Of_COVID_Table"),
    GLOBAL_WITHOUT_MEET(3,"Global_UV_WithOut_Meetings_Net_Of_COVID_Table"),
    GLOBAL_WITHOUT_MEET_PER_CHANNEL(4,"Global_UV_Per_Channel_WithOut_Meetings_Net_Of_COVID_Table");


    public final int loadOrder;
    public final String fileName;


    private GTMDeckEnum(int loadOrder, String fileName) {
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
        return  EnumSet.allOf(GTMDeckEnum.class)
                .stream()
                .sorted(Comparator.comparingInt(GTMDeckEnum::getLoadOrder))
                .map(GTMDeckEnum::getFileName)
                .collect(Collectors.toList());
    }
}
