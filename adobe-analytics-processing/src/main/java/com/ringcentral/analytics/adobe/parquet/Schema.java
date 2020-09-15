package com.ringcentral.analytics.adobe.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class Schema {
    public static final MessageType GTM_DECK_SCHEMA =
            Types.buildMessage()
                    .optional(BINARY).as(UTF8).named("week")
                    .optional(INT96).named("UniqueVisitors_WithMeetings")
                    .optional(INT96).named("UniqueVisitors_WithOutMeetings")
                    .optional(INT96).named("Channel_WalkOn_WithMeetings")
                    .optional(INT96).named("Channel_SEO_WithMeetings")
                    .optional(INT96).named("Channel_SEM_WithMeetings")
                    .optional(INT96).named("Channel_Affiliates_WithMeetings")
                    .optional(INT96).named("Channel_Retargeting_WithMeetings")
                    .optional(INT96).named("Channel_Other_WithMeetings")
                    .optional(INT96).named("Channel_WalkOn_WithOutMeetings")
                    .optional(INT96).named("Channel_SEO_WithOutMeetings")
                    .optional(INT96).named("Channel_SEM_WithOutMeetings")
                    .optional(INT96).named("Channel_Affiliates_WithOutMeetings")
                    .optional(INT96).named("Channel_Retargeting_WithOutMeetings")
                    .optional(INT96).named("Channel_Other_WithOutMeetings")
                    .named("gtm_deck");


    private Schema() {
    }
}
