package com.dqengine.source

import com.dqengine.domain._
import org.apache.spark.sql.SparkSession

class SourceResolver(
  parquetReader: SourceReader  = new ParquetSourceReader(),
  mariaDbReader: SourceReader  = new MariaDbTableReader(),
  hiveReader: SourceReader     = new HiveTableReader()
) {
  def resolveAndRead(
    tableName: String, catalog: Seq[SourceCatalogEntry],
    param: PartitionParameter, spark: SparkSession, config: EngineConfig
  ): Either[SourceError, ResolvedSource] = {
    val matching = catalog.filter(_.tableName == tableName)
    matching.size match {
      case 0 => Left(SourceError(s"table_name '$tableName' not found in Source_Catalog"))
      case n if n > 1 => Left(SourceError(s"table_name '$tableName' has $n duplicate entries in Source_Catalog"))
      case _ =>
        val entry = matching.head
        val reader = entry.sourceType match {
          case SourceType.Parquet      => parquetReader
          case SourceType.MariaDbTable => mariaDbReader
          case SourceType.HiveTable    => hiveReader
        }
        reader.resolveScope(entry, param, config) match {
          case Left(e) => Left(e)
          case Right((scope, _)) => reader.read(spark, entry, scope, config)
        }
    }
  }
}
