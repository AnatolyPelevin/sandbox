package com.ringcentral.analytics.etl.config.model

import java.sql.Timestamp

import com.ringcentral.analytics.etl.config.DataTypeParser
import com.ringcentral.analytics.etl.config.MisconfigurationException

class FieldDefinition(
                         val sourceColumnName: String,
                         val hiveName: String,
                         val destinationName: String,
                         val dataType: DataTypeDefinition,
                         val order: Int,
                         val isNull: Boolean,
                         val misc: Map[String, Any],
                         val insertTime: Timestamp
                     )  extends Serializable {

    private val partOfDestinationSegmentation: MiscProperty[Boolean] = MiscProperty[Boolean]("partOfDestinationSegmentation")

    def getMiscValueOpt[T](miscProperty: MiscProperty[T]): Option[T] = {
        misc.get(miscProperty.name).map(_.asInstanceOf[T])
    }

    def getMiscValue[T](miscProperty: MiscProperty[T]): T = {
        misc.get(miscProperty.name).map(_.asInstanceOf[T]).getOrElse(throw new MisconfigurationException(s"${miscProperty.name} is not specified for $this"))
    }

    def isPartOfDestinationSegmentation: Boolean = {
        getMiscValueOpt(partOfDestinationSegmentation).getOrElse(false)
    }

    override def equals(fieldDefinition: Any): Boolean = {
        fieldDefinition match {
            case p: FieldDefinition =>
                sourceColumnName == p.sourceColumnName &&
                    hiveName == p.hiveName &&
                    destinationName == p.destinationName &&
                    dataType == p.dataType &&
                    order == p.order &&
                    isNull == p.isNull &&
                    misc == p.misc &&
                    insertTime == p.insertTime
            case _ => false
        }
    }

    override def toString: String = {
        "FieldDefinition[" +
            s"sourceColumnName = $sourceColumnName, " +
            s"hiveName = $hiveName, " +
            s"destinationName = $destinationName, " +
            s"dataType = $dataType, " +
            s"order = $order, " +
            s"isNull = $isNull, " +
            s"misc = $misc" +
            s"insertTime = $insertTime" +
            "]"
    }

    override def hashCode(): Int = {
        val state = Seq(partOfDestinationSegmentation, sourceColumnName, hiveName, destinationName, dataType, order, isNull, misc, insertTime)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}

object FieldDefinition {
    def apply(columnName: String,
              dataType: String,
              order: Int,
              insertTime: Timestamp,
              isNull: Boolean = false,
              misc: Map[String, Any] = Map.empty[String, Any]): FieldDefinition = {
        val dataTypeParsed = DataTypeParser.dataTypeFromString(dataType)
        new FieldDefinition(columnName, columnName, columnName, dataTypeParsed, order, isNull, misc, insertTime)
    }

    def apply(sourceColumnName: String,
              hiveName: String,
              destinationName: String,
              dataType: DataTypeDefinition,
              order: Int,
              isNull: Boolean,
              misc: Map[String, Any],
              insertTime: Timestamp): FieldDefinition = {
        new FieldDefinition(sourceColumnName, hiveName, destinationName, dataType, order, isNull, misc, insertTime)
    }
}
