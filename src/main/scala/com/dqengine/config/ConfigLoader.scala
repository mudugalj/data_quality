package com.dqengine.config

import com.dqengine.domain._
import com.dqengine.store.DqStore
import java.time.Instant
import java.util.UUID

class ConfigLoader(store: DqStore) {

  def resolveVersion(param: Option[String]): Either[String, ConfigVersionInfo] =
    try Right(store.resolveVersion(param))
    catch { case e: MariaDbStoreException => Left(e.getMessage) }

  def initialUpload(defs: Seq[CheckDefinition]): ConfigVersionInfo = {
    val info = newVersion(); store.registerVersion(info)
    store.writeCheckDefinitions(info.configVersion, info.configVersionDate, defs); info
  }
  def reupload(defs: Seq[CheckDefinition]): ConfigVersionInfo = {
    val info = newVersion(); store.registerVersion(info)
    store.writeCheckDefinitions(info.configVersion, info.configVersionDate, defs); info
  }
  def amend(baseVersion: String, amendments: Seq[CheckDefinition]): ConfigVersionInfo = {
    val currentRows = store.loadCheckDefinitions(baseVersion)
    val (currentDefs, _) = validateCheckRows(currentRows)
    val amendMap = amendments.map(a => a.checkId -> a).toMap
    val merged = currentDefs.map(d => amendMap.getOrElse(d.checkId, d)) ++
                 amendments.filter(a => !currentDefs.exists(_.checkId == a.checkId))
    val info = newVersion(); store.registerVersion(info)
    store.writeCheckDefinitions(info.configVersion, info.configVersionDate, merged); info
  }

  private def newVersion(): ConfigVersionInfo =
    ConfigVersionInfo(UUID.randomUUID().toString.replace("-","").take(16), Instant.now())

  def loadChecks(version: ConfigVersionInfo): (Seq[CheckDefinition], Seq[ConfigError]) =
    validateCheckRows(store.loadCheckDefinitions(version.configVersion))

  def validateCheckRows(rows: Seq[Map[String, Option[String]]]): (Seq[CheckDefinition], Seq[ConfigError]) = {
    val errors = scala.collection.mutable.ListBuffer[ConfigError]()
    val valid  = scala.collection.mutable.ListBuffer[CheckDefinition]()

    val idCounts   = rows.groupBy(r => r.get("check_id").flatten.getOrElse(""))
    val duplicates = idCounts.filter(_._2.size > 1).keySet.filter(_.nonEmpty)
    if (duplicates.nonEmpty)
      errors += ConfigError(s"Duplicate check_id(s) within config_version: ${duplicates.mkString(", ")}")

    rows.foreach { row =>
      def get(k: String): Option[String] = row.get(k).flatten.map(_.trim).filter(_.nonEmpty)
      val id = get("check_id").getOrElse("")
      if (duplicates.contains(id)) { /* skip duplicates */ }
      else {
        val missing = Seq("check_id","table_name","column_name","check_type","check_sql","evaluation_mode")
          .filterNot(k => get(k).isDefined)
        if (missing.nonEmpty) {
          errors += ConfigError(s"check_id='$id': missing required field(s): ${missing.mkString(", ")}")
        } else {
          val checkType = CheckType.fromWire(get("check_type").get)
          if (checkType.isEmpty)
            errors += ConfigError(s"check_id='$id': invalid check_type '${get("check_type").get}' (must be completeness|validity|consistency)")
          val evalMode = EvaluationMode.fromWire(get("evaluation_mode").get)
          if (evalMode.isEmpty)
            errors += ConfigError(s"check_id='$id': invalid evaluation_mode '${get("evaluation_mode").get}' (must be violation|consistency_delta|consistency_mom)")

          if (checkType.isDefined && evalMode.isDefined) {
            val devTol = get("deviation_tolerance").map(s => BigDecimal(s)).getOrElse(BigDecimal(0))
            valid += CheckDefinition(
              checkId            = id,
              tableName          = get("table_name").get,
              columnName         = get("column_name").get,
              dataType           = get("data_type"),
              checkType          = checkType.get,
              businessGlossary   = get("business_glossary"),
              businessRuleFilter = get("business_rule_filter"),
              deviationTolerance = devTol,
              checkSql           = get("check_sql").get,
              evaluationMode     = evalMode.get,
              appCode            = get("app_code")
            )
          }
        }
      }
    }
    (valid.toList, errors.toList)
  }

  def loadCatalog(): (Seq[SourceCatalogEntry], Seq[ConfigError]) =
    loadCatalog(store.loadSourceCatalog())

  // Test-friendly overload: validate catalog rows without hitting the store
  def loadCatalog(rows: Seq[Map[String, Option[String]]]): (Seq[SourceCatalogEntry], Seq[ConfigError]) = {
    val errors = scala.collection.mutable.ListBuffer[ConfigError]()
    val valid  = scala.collection.mutable.ListBuffer[SourceCatalogEntry]()
    rows.foreach { row =>
      def get(k: String): Option[String] = row.get(k).flatten.map(_.trim).filter(_.nonEmpty)
      val tableName  = get("table_name")
      val sourceType = get("source_type")
      if (tableName.isEmpty || sourceType.isEmpty) {
        errors += ConfigError(s"Source_Catalog record missing required field(s): ${Seq("table_name","source_type").filterNot(k => get(k).isDefined).mkString(", ")}")
      } else {
        SourceType.fromWire(sourceType.get) match {
          case None =>
            errors += ConfigError(s"table_name='${tableName.get}': unsupported source_type '${sourceType.get}'")
          case Some(st @ SourceType.Parquet) =>
            val missing = Seq("parquet_location","partition_column").filterNot(k => get(k).isDefined)
            if (missing.nonEmpty) errors += ConfigError(s"table_name='${tableName.get}' (parquet): missing ${missing.mkString(", ")}")
            else valid += SourceCatalogEntry(tableName.get, st, get("parquet_location"), get("partition_column"), None, None, None)
          case Some(st @ SourceType.MariaDbTable) =>
            val missing = Seq("connection_ref","physical_table_name").filterNot(k => get(k).isDefined)
            if (missing.nonEmpty) errors += ConfigError(s"table_name='${tableName.get}' (mariadb_table): missing ${missing.mkString(", ")}")
            else valid += SourceCatalogEntry(tableName.get, st, None, None, get("connection_ref"), get("physical_table_name"), get("filter_column"))
          case Some(st @ SourceType.HiveTable) =>
            val missing = Seq("connection_ref","physical_table_name").filterNot(k => get(k).isDefined)
            if (missing.nonEmpty) errors += ConfigError(s"table_name='${tableName.get}' (hive_table): missing ${missing.mkString(", ")}")
            else valid += SourceCatalogEntry(tableName.get, st, None, None, get("connection_ref"), get("physical_table_name"), get("filter_column"))
        }
      }
    }
    (valid.toList, errors.toList)
  }

  def writeSourceCatalog(entries: Seq[SourceCatalogEntry]): Unit =
    store.writeSourceCatalog(entries)
}
