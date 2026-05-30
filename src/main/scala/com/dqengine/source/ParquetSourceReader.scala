package com.dqengine.source

import com.dqengine.domain._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scala.util.Try

class ParquetSourceReader extends SourceReader {
  val sourceType: SourceType = SourceType.Parquet

  def resolveScope(entry: SourceCatalogEntry, param: PartitionParameter, config: EngineConfig): Either[SourceError, (ReadScope, ScopeLabel)] = {
    val location = entry.parquetLocation.getOrElse(return Left(SourceError(s"${entry.tableName}: parquet_location not configured")))
    val partCol  = entry.partitionColumn.getOrElse(return Left(SourceError(s"${entry.tableName}: partition_column not configured")))
    listPartitions(location, partCol) match {
      case Left(e) => Left(e)
      case Right(partitions) => param match {
        case PartitionParameter.Specific(v) =>
          if (partitions.contains(v)) Right((ReadScope.Partition(v), ScopeLabel.PartitionValue(v)))
          else Left(SourceError(s"${entry.tableName}: partition '$v' not found at $location"))
        case PartitionParameter.Latest =>
          if (partitions.isEmpty) Left(SourceError(s"${entry.tableName}: no partitions at $location"))
          else { val latest = partitions.max; Right((ReadScope.Partition(latest), ScopeLabel.PartitionValue(latest))) }
      }
    }
  }

  def read(spark: SparkSession, entry: SourceCatalogEntry, scope: ReadScope, config: EngineConfig): Either[SourceError, ResolvedSource] = {
    val location = entry.parquetLocation.get
    val partCol  = entry.partitionColumn.get
    scope match {
      case ReadScope.Partition(value) =>
        val path = s"$location/$partCol=$value"
        Try {
          val df = spark.read.parquet(path)
          df.createOrReplaceTempView(entry.tableName)  // logical name as view so check_sql references it directly
          ResolvedSource(entry, SqlTarget.SparkTempView(entry.tableName), ScopeLabel.PartitionValue(value))
        }.toEither.left.map(e => SourceError(s"${entry.tableName}: cannot read partition '$value' at $path: ${e.getMessage}"))
      case _ => Left(SourceError(s"${entry.tableName}: unexpected scope $scope for parquet source"))
    }
  }

  private def listPartitions(location: String, partCol: String): Either[SourceError, Seq[String]] =
    Try {
      val conf = new org.apache.hadoop.conf.Configuration()
      val path = new Path(location)
      val fs   = FileSystem.get(path.toUri, conf)
      if (!fs.exists(path)) return Left(SourceError(s"Parquet location not found: $location"))
      fs.listStatus(path)
        .filter(_.isDirectory)
        .map(_.getPath.getName)
        .filter(_.startsWith(s"$partCol="))
        .map(_.stripPrefix(s"$partCol="))
        .sorted.toSeq
    }.toEither.left.map(e => SourceError(s"Cannot list partitions at $location: ${e.getMessage}"))
}
