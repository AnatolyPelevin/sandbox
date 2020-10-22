package com.ringcentral.analytics.etl.config

case class TableDefinition(hiveTableName: String, isPartitioned: Boolean = false)
