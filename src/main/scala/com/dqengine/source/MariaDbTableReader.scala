package com.dqengine.source

import com.dqengine.domain._
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import scala.util.Try

class MariaDbTableReader extends SourceReader {
  val sourceType: SourceType = SourceType.MariaDbTable

  def resolveScope(entry: SourceCatalogEntry, param: PartitionParameter, config: EngineConfig): Either[SourceError, (ReadScope, ScopeLabel)] = {
    val connRef = entry.connectionRef.getOrElse(return Left(SourceError(s"${entry.tableName}: connection_ref not configured")))
    (entry.filterColumn, param) match {
      case (Some(col), PartitionParameter.Specific(v)) => Right((ReadScope.Filter(col, v), ScopeLabel.FilterValue(v)))
      case _                                           => Right((ReadScope.FullTable, ScopeLabel.NotApplicable))
    }
  }

  def read(spark: SparkSession, entry: SourceCatalogEntry, scope: ReadScope, config: EngineConfig): Either[SourceError, ResolvedSource] = {
    val connRef   = entry.connectionRef.getOrElse(return Left(SourceError(s"${entry.tableName}: connection_ref not configured")))
    val physTable = entry.physicalTableName.getOrElse(return Left(SourceError(s"${entry.tableName}: physical_table_name not configured")))
    val dataConf  = config.dataSourceConfigs.getOrElse(connRef,
      return Left(SourceError(s"${entry.tableName}: no data source config for connection_ref '$connRef'")))

    val (scopeLabel, filterInfo) = scope match {
      case ReadScope.Filter(col, v) => (ScopeLabel.FilterValue(v), Some((col, v)))
      case _                        => (ScopeLabel.NotApplicable, None)
    }

    Try {
      Class.forName("org.mariadb.jdbc.Driver")
      val conn = DriverManager.getConnection(dataConf.jdbcUrl, dataConf.user, dataConf.password)
      conn.setReadOnly(true)
      Try(conn.close())  // we don't hold the connection open at read time; check_sql opens its own
    }

    Right(ResolvedSource(
      entry,
      SqlTarget.JdbcConnection(dataConf.jdbcUrl, dataConf.user, dataConf.password, "org.mariadb.jdbc.Driver"),
      scopeLabel
    ))
  }
}
