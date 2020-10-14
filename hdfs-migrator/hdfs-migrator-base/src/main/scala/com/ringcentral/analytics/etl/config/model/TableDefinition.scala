package com.ringcentral.analytics.etl.config.model

import java.sql.Timestamp

import com.ringcentral.analytics.etl.config.MisconfigurationException

class TableDefinition(
                         val hiveTableName: String,
                         val version: Int,
                         val insertTime: Timestamp,
                         val tableQuery: String = "",
                         val destinationTableName: Option[String] = None,
                         val isPartitioned: Boolean = false,
                         val priority: Long = 0L,
                         val misc: Map[String, Any] = Map.empty[String, Any],
                         var fieldDefinitions: Seq[FieldDefinition] = Seq.empty[FieldDefinition]
                     ) extends Serializable {

    private val suffix: MiscProperty[String] = MiscProperty[String]("adbSuffix")
    private val chunkedExp: MiscProperty[ChunkedExport] = MiscProperty[ChunkedExport]("chunkedExport")
    private val insertTimestamp: MiscProperty[Boolean] = MiscProperty[Boolean]("addInsertTimestamp")
    private val oldPartitionsRotation: MiscProperty[Boolean] = MiscProperty[Boolean]("rotateOldPartitions")
    private val timestampsImpalaCompatible: MiscProperty[Boolean] = MiscProperty[Boolean]("makeTimestampsImpalaCompatible")
    private val incrementalExp: MiscProperty[IncrementalExport] = MiscProperty[IncrementalExport]("incrementalExport")
    private val onChangeExp: MiscProperty[Boolean] = MiscProperty[Boolean]("onChangeExport")

    def getMiscValueOpt[T](miscProperty: MiscProperty[T]): Option[T] = {
        misc.get(miscProperty.name).map(_.asInstanceOf[T])
    }

    def getMiscValue[T](miscProperty: MiscProperty[T]): T = {
        misc.get(miscProperty.name)
            .map(_.asInstanceOf[T])
            .getOrElse(throw new MisconfigurationException(s"${miscProperty.name} is not specified for $this"))
    }

    def getMiscValueObject[T](miscProperty: MiscProperty[T], constructFun: Map[String, Any] => T): T = {
        misc.get(miscProperty.name)
            .map(_.asInstanceOf[Map[String, Any]])
            .map(constructFun.apply)
            .getOrElse(throw new MisconfigurationException(s"${miscProperty.name} is not specified for $this"))
    }

    def getMiscValueObjectOpt[T](miscProperty: MiscProperty[T], constructFun: Map[String, Any] => T): Option[T] = {
        misc.get(miscProperty.name)
            .map(_.asInstanceOf[Map[String, Any]])
            .map(constructFun.apply)
    }


    def isByPodQuery: Boolean = {
        getMiscValueOpt(suffix).isDefined
    }

    def adbSuffix(): String = {
        getMiscValue(suffix)
    }

    def isChunkedExport: Boolean = {
        getMiscValueObjectOpt(chunkedExp, ChunkedExport.apply).isDefined
    }

    def chunkedExport: ChunkedExport = {
        getMiscValueObject(chunkedExp, ChunkedExport.apply)
    }

    def addInsertTimestamp(): Boolean = {
        getMiscValue(insertTimestamp)
    }

    def rotateOldPartitions: Boolean = {
        getMiscValue(oldPartitionsRotation)
    }

    def makeTimestampsImpalaCompatible: Boolean = {
        getMiscValue(timestampsImpalaCompatible)
    }

    def isIncrementalExport: Boolean = {
        getMiscValueObjectOpt(incrementalExp, IncrementalExport.apply).isDefined
    }

    def incrementalExport: Option[IncrementalExport] = {
        getMiscValueObjectOpt(incrementalExp, IncrementalExport.apply)
    }

    def isOnChangeExport: Boolean = {
        getMiscValue(onChangeExp)
    }

    override def equals(tableDefinition: Any): Boolean = {
        tableDefinition match {
            case p: TableDefinition =>
                hiveTableName == p.hiveTableName &&
                    version == p.version &&
                    insertTime == p.insertTime &&
                    tableQuery == p.tableQuery &&
                    destinationTableName == p.destinationTableName &&
                    isPartitioned == p.isPartitioned &&
                    priority == p.priority &&
                    misc == p.misc &&
                    fieldDefinitions == p.fieldDefinitions
            case _ => false
        }
    }

    override def hashCode(): Int = {
        val state = Seq(suffix,
            chunkedExp,
            insertTimestamp,
            oldPartitionsRotation,
            timestampsImpalaCompatible,
            incrementalExp,
            onChangeExp,
            hiveTableName,
            version,
            insertTime,
            tableQuery,
            destinationTableName,
            isPartitioned,
            priority,
            misc,
            fieldDefinitions)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

    override def toString: String = {
        "TableDefinition[" +
            s"hiveTableName=$hiveTableName, " +
            s"version=$version, " +
            s"insertTime=$insertTime, " +
            s"tableQuery=$tableQuery, " +
            s"destinationTableName=$destinationTableName, " +
            s"isPartitioned=$isPartitioned, " +
            s"priority=$priority, " +
            s"misc=$misc, " +
            s"fieldDefinitions=$fieldDefinitions, " +
            "]"
    }
}

object TableDefinition {
    def apply(hiveTableName: String,
              version: Int,
              insertTime: Timestamp,
              tableQuery: String,
              destinationTableName: Option[String],
              isPartitioned: Boolean,
              priority: Long,
              misc: Map[String, Any],
              fieldDefinitions: Seq[FieldDefinition]): TableDefinition = {
        new TableDefinition(hiveTableName,
            version,
            insertTime,
            tableQuery,
            destinationTableName,
            isPartitioned,
            priority,
            misc,
            fieldDefinitions)
    }

    def apply(hiveTableName: String,
              version: Int,
              tableQuery: String,
              destinationTableName: Option[String],
              isPartitioned: Boolean,
              priority: Long,
              misc: Map[String, Any],
              insertTime: Timestamp): TableDefinition = {
        new TableDefinition(hiveTableName,
            version,
            insertTime,
            tableQuery,
            destinationTableName,
            isPartitioned,
            priority,
            misc)
    }

    def apply(hiveTableName: String, version: Int, insertTime: Timestamp): TableDefinition = {
        new TableDefinition(hiveTableName, version, insertTime)
    }
}

case class MiscProperty[T](name: String)

case class ChunkedExport(chunkByColumn: String, numberOfChunks: Int)

object ChunkedExport {
    def apply(values: Map[String, Any]): ChunkedExport = ChunkedExport(values("chunkByColumn").toString, values("numberOfChunks").asInstanceOf[Int])
}

case class IncrementalExport(incrementByField: String, incrementType: String, dateFormat: String)

object IncrementalExport {
    def apply(values: Map[String, Any]): IncrementalExport = IncrementalExport(values("incrementByField").toString,
        values("incrementType").toString, values("dateFormat").toString)
}
