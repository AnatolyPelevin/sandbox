package com.ringcentral.analytics.etl.config.model

class SourceDbDefinition(val jdbcUrl: String,
                         val jdbcUser: String,
                         val jdbcPassword: String,
                         val jdbcDriver: String,
                         val version: Int,
                         val dbProperties: Map[String, String],
                         val misc: Map[String, Any]) {

    override def equals(sourceDbDefinition: Any): Boolean = {
        sourceDbDefinition match {
            case p: SourceDbDefinition =>
                jdbcUrl == p.jdbcUrl &&
                jdbcUser == p.jdbcUser &&
                jdbcPassword == p.jdbcPassword &&
                jdbcDriver == p.jdbcDriver &&
                version == p.version &&
                dbProperties == p.dbProperties &&
                misc == p.misc
            case _ => false
        }
    }

    override def toString: String = {
        "SourceDbDefinition[" +
            s"jdbcUrl=$jdbcUrl, " +
            s"jdbcUser=$jdbcUser, " +
            s"jdbcPassword=$jdbcPassword, " +
            s"jdbcDriver=$jdbcDriver, " +
            s"version=$version, " +
            s"dbProperties=$dbProperties, " +
            s"misc=$misc, " +
            "]"
    }

    override def hashCode(): Int = {
        val state = Seq(jdbcUrl, jdbcUser, jdbcPassword, jdbcDriver, version, dbProperties, misc)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}


object SourceDbDefinition {
    def apply(jdbcUrl: String,
              jdbcUser: String,
              jdbcPassword: String,
              jdbcDriver: String,
              version: Int,
              dbProperties: Map[String, String],
              misc: Map[String, Any]): SourceDbDefinition = {
        new SourceDbDefinition(jdbcUrl, jdbcUser, jdbcPassword, jdbcDriver, version, dbProperties, misc)
    }
}
