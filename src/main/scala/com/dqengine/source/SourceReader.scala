package com.dqengine.source

import com.dqengine.domain._
import org.apache.spark.sql.SparkSession

trait SourceReader {
  def sourceType: SourceType
  def resolveScope(entry: SourceCatalogEntry, param: PartitionParameter, config: EngineConfig): Either[SourceError, (ReadScope, ScopeLabel)]
  def read(spark: SparkSession, entry: SourceCatalogEntry, scope: ReadScope, config: EngineConfig): Either[SourceError, ResolvedSource]
}
