package com.dqengine.source

import com.dqengine.domain._
import org.apache.spark.sql.SparkSession
import scala.util.Try

class HiveTableReader extends SourceReader {
  val sourceType: SourceType = SourceType.HiveTable

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
    val hiveCfg   = config.hiveConfigs.getOrElse(connRef,
      return Left(SourceError(s"${entry.tableName}: no Hive config for connection_ref '$connRef'")))

    // Verify connectivity
    val connTest = Try {
      Class.forName(hiveCfg.driverClass)
      val conn = java.sql.DriverManager.getConnection(hiveCfg.jdbcUrl, hiveCfg.user, hiveCfg.password)
      conn.setReadOnly(true)
      Try(conn.close())
    }
    connTest.toEither.left.map(e =>
      SourceError(s"${entry.tableName}: cannot connect to HiveServer2 at ${hiveCfg.jdbcUrl}: ${e.getMessage}")) match {
      case Left(err) => Left(err)
      case Right(_)  =>
        val (scopeLabel, _) = scope match {
          case ReadScope.Filter(col, v) => (ScopeLabel.FilterValue(v), Some((col, v)))
          case _                        => (ScopeLabel.NotApplicable, None)
        }
        Right(ResolvedSource(
          entry,
          SqlTarget.JdbcConnection(hiveCfg.jdbcUrl, hiveCfg.user, hiveCfg.password, hiveCfg.driverClass),
          scopeLabel
        ))
    }
  }
}
