package com.dqengine.engine

import com.dqengine.config.ConfigLoader
import com.dqengine.domain._
import com.dqengine.exec.{SqlCheckExecutor, IssueClassifier}
import com.dqengine.source.SourceResolver
import com.dqengine.store.DqStore
import org.apache.spark.sql.SparkSession
import java.time.Instant
import java.util.UUID

class DqEngine(
  store: DqStore, configLoader: ConfigLoader,
  sourceResolver: SourceResolver, engineConfig: EngineConfig
)(implicit spark: SparkSession) {

  def run(
    selectedCheckTypes: Seq[String],
    partitionParam: PartitionParameter,
    configVersionParam: Option[String],
    appCodeFilter: Option[String] = None
  ): EngineRunResult = {

    val runId        = UUID.randomUUID().toString
    val runTimestamp = Instant.now()

    // Validate check types
    if (selectedCheckTypes.isEmpty)
      return TerminatedRun("Selected_Check_Types must not be empty")
    val unknown = selectedCheckTypes.filterNot(CheckType.all.contains)
    if (unknown.nonEmpty)
      return TerminatedRun(s"Unknown check_type value(s): ${unknown.mkString(", ")} (must be completeness|validity|consistency)")
    val selectedSet = selectedCheckTypes.toSet

    // Resolve config version
    val configVersion: ConfigVersionInfo = try store.resolveVersion(configVersionParam)
    catch { case e: MariaDbStoreException => return TerminatedRun(s"Cannot load configuration: ${e.getMessage}") }

    // Load checks and catalog
    val (checkDefs, configErrors) = try configLoader.loadChecks(configVersion)
    catch { case e: MariaDbStoreException => return TerminatedRun(s"Cannot read Config_Store: ${e.getMessage}") }
    configErrors.foreach(e => warn(s"Config rejection: ${e.message}"))

    val (catalog, catalogErrors) = try configLoader.loadCatalog()
    catch { case e: MariaDbStoreException => return TerminatedRun(s"Cannot read Source_Catalog: ${e.getMessage}") }
    catalogErrors.foreach(e => warn(s"Catalog rejection: ${e.message}"))

    // Filter by check_type then app_code
    val filtered = checkDefs
      .filter(d => selectedSet.contains(d.checkType.wire))
      .filter(d => appCodeFilter.forall(ac => d.appCode.contains(ac)))

    val ctx = RunContext(runId, runTimestamp, configVersion.configVersion,
                        configVersion.configVersionDate, partitionParam, selectedSet)
    val executor = new SqlCheckExecutor(store)
    val results  = scala.collection.mutable.ListBuffer[DqResult]()

    filtered.foreach { defn =>
      val result = try {
        sourceResolver.resolveAndRead(defn.tableName, catalog, partitionParam, spark, engineConfig) match {
          case Left(err) => errored(defn, ctx, err.message)
          case Right(src) => executor.execute(defn, src, ctx)
        }
      } catch {
        case e: Exception => errored(defn, ctx, s"Unexpected error: ${e.getMessage}")
      }
      results += result
    }

    val report      = ReportGenerator.generate(results.toList, ctx)
    val writeErrors = new ResultWriter(store).writeAll(results.toList)
    writeErrors.foreach(e => warn(s"Result write error: $e"))
    CompletedRun(report, writeErrors)
  }

  private def errored(defn: CheckDefinition, ctx: RunContext, msg: String): DqResult = DqResult(
    dqRefId = UUID.randomUUID().toString, checkId = defn.checkId, appCode = defn.appCode,
    configVersion = ctx.configVersion, configVersionDate = ctx.configVersionDate,
    runId = ctx.runId, runTimestamp = ctx.runTimestamp, tableName = defn.tableName,
    sourceType = SourceType.Parquet, columnName = defn.columnName, partitionValue = None,
    checkType = defn.checkType, evaluationMode = defn.evaluationMode,
    status = Status.Errored, measures = Measures.Empty, description = Some(msg),
    issueStatus = IssueClassifier.classify(store, defn.tableName, defn.columnName,
      defn.checkType.wire, defn.appCode, ctx.runTimestamp, Status.Errored))

  private def warn(msg: String): Unit = println(s"[WARN] $msg")
}
