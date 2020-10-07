package com.ringcentral.analytics.sns.backup.request.generator.model

object RequestType extends Enumeration {
    type RequestType = Value
    val APPEND, DELETE, REPLACE = Value
}
