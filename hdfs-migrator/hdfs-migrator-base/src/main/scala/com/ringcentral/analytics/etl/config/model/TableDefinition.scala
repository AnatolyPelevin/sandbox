package com.ringcentral.analytics.etl.config.model

class TableDefinition(val hiveTableName: String, val isPartitioned: Boolean = false) extends Serializable {

    override def equals(tableDefinition: Any): Boolean = {
        tableDefinition match {
            case p: TableDefinition => hiveTableName == p.hiveTableName && isPartitioned == p.isPartitioned
            case _ => false
        }
    }

    override def hashCode(): Int = {
        Seq(hiveTableName, isPartitioned).map(_.hashCode())
            .foldLeft(0)((a, b) => 31 * a + b)
    }

    override def toString: String = {
        "TableDefinition[" +
            s"hiveTableName=$hiveTableName, " +
            s"isPartitioned=$isPartitioned, " +
            "]"
    }
}

object TableDefinition {
    def apply(hiveTableName: String, isPartitioned: Boolean): TableDefinition = {
        new TableDefinition(hiveTableName, isPartitioned)
    }
}
