package com.dqengine.domain

import java.time.Instant

// ─── Sealed trait enums ─────────────────────────────────────────────────────

sealed trait CheckType { def wire: String }
object CheckType {
  case object Completeness extends CheckType { val wire = "completeness" }
  case object Validity     extends CheckType { val wire = "validity"     }
  case object Consistency  extends CheckType { val wire = "consistency"  }
  def fromWire(s: String): Option[CheckType] =
    Seq(Completeness, Validity, Consistency).find(_.wire == s)
  val all: Set[String] = Set("completeness", "validity", "consistency")
}

sealed trait EvaluationMode { def wire: String }
object EvaluationMode {
  case object Violation        extends EvaluationMode { val wire = "violation"         }
  case object ConsistencyDelta extends EvaluationMode { val wire = "consistency_delta" }
  case object ConsistencyMom   extends EvaluationMode { val wire = "consistency_mom"   }
  def fromWire(s: String): Option[EvaluationMode] =
    Seq(Violation, ConsistencyDelta, ConsistencyMom).find(_.wire == s)
}

sealed trait SourceType { def wire: String }
object SourceType {
  case object Parquet      extends SourceType { val wire = "parquet"       }
  case object MariaDbTable extends SourceType { val wire = "mariadb_table" }
  case object HiveTable    extends SourceType { val wire = "hive_table"    }
  def fromWire(s: String): Option[SourceType] =
    Seq(Parquet, MariaDbTable, HiveTable).find(_.wire == s)
}

sealed trait Status { def wire: String }
object Status {
  case object Passed  extends Status { val wire = "passed"  }
  case object Failed  extends Status { val wire = "failed"  }
  case object Errored extends Status { val wire = "errored" }
  def fromWire(s: String): Option[Status] =
    Seq(Passed, Failed, Errored).find(_.wire == s)
  /** Statuses that indicate a quality issue requiring classification. */
  val failLike: Set[Status] = Set(Failed, Errored)
}

sealed trait IssueStatus { def wire: String }
object IssueStatus {
  case object New       extends IssueStatus { val wire = "new"       }
  case object Recurring extends IssueStatus { val wire = "recurring" }
  case object Resolved  extends IssueStatus { val wire = "resolved"  }
  def fromWire(s: String): Option[IssueStatus] =
    Seq(New, Recurring, Resolved).find(_.wire == s)
}

// ─── Config types ────────────────────────────────────────────────────────────

case class ConfigVersionInfo(configVersion: String, configVersionDate: Instant)

case class CheckDefinition(
  checkId: String,
  tableName: String,
  columnName: String,
  dataType: Option[String],
  checkType: CheckType,
  businessGlossary: Option[String],
  businessRuleFilter: Option[String],
  deviationTolerance: BigDecimal,         // absolute for delta; percentage for mom
  checkSql: String,
  evaluationMode: EvaluationMode,
  appCode: Option[String]                 // owning application code
)

case class SourceCatalogEntry(
  tableName: String,
  sourceType: SourceType,
  parquetLocation: Option[String],
  partitionColumn: Option[String],
  connectionRef: Option[String],
  physicalTableName: Option[String],
  filterColumn: Option[String]
) {
  def isPartitionedFileSource: Boolean = sourceType == SourceType.Parquet
}

// ─── Run types ───────────────────────────────────────────────────────────────

sealed trait PartitionParameter
object PartitionParameter {
  case class Specific(value: String) extends PartitionParameter
  case object Latest extends PartitionParameter
}

case class RunContext(
  runId: String,
  runTimestamp: Instant,
  configVersion: String,
  configVersionDate: Instant,
  partitionParameter: PartitionParameter,
  selectedCheckTypes: Set[String]
)

// ─── Source resolution ───────────────────────────────────────────────────────

sealed trait ReadScope
object ReadScope {
  case class Partition(value: String)              extends ReadScope
  case class Filter(column: String, value: String) extends ReadScope
  case object FullTable                            extends ReadScope
}

sealed trait ScopeLabel
object ScopeLabel {
  case class PartitionValue(value: String) extends ScopeLabel
  case class FilterValue(value: String)    extends ScopeLabel
  case object NotApplicable                extends ScopeLabel

  def toPartitionValueString(label: ScopeLabel): Option[String] = label match {
    case PartitionValue(v) => Some(v)
    case FilterValue(v)    => Some(v)
    case NotApplicable     => scala.None
  }
}

/** How check_sql is executed against the source. */
sealed trait SqlTarget
object SqlTarget {
  /** Parquet: check_sql runs as Spark SQL over this temp view. */
  case class SparkTempView(viewName: String) extends SqlTarget
  /** MariaDB or Hive: check_sql runs via a direct JDBC connection. */
  case class JdbcConnection(
    jdbcUrl: String,
    user: String,
    password: String,
    driverClass: String
  ) extends SqlTarget
}

case class ResolvedSource(
  entry: SourceCatalogEntry,
  sqlTarget: SqlTarget,
  scopeLabel: ScopeLabel
)

// ─── Measures ────────────────────────────────────────────────────────────────

sealed trait Measures
object Measures {
  case class ViolationCount(count: Long) extends Measures
  case class ConsistencyMeasure(
    currentValue: BigDecimal,
    priorValue: Option[BigDecimal],
    deviation: Option[BigDecimal]         // absolute for delta; percentage for mom
  ) extends Measures
  case object Empty extends Measures
}

// ─── DQ output ───────────────────────────────────────────────────────────────

case class DqResult(
  dqRefId: String,
  checkId: String,
  appCode: Option[String],
  configVersion: String,
  configVersionDate: Instant,
  runId: String,
  runTimestamp: Instant,
  tableName: String,
  sourceType: SourceType,
  columnName: String,
  partitionValue: Option[String],
  checkType: CheckType,
  evaluationMode: EvaluationMode,
  status: Status,
  measures: Measures,
  description: Option[String],
  issueStatus: Option[IssueStatus]
)

case class DqReport(
  runId: String,
  runTimestamp: Instant,
  configVersion: String,
  configVersionDate: Instant,
  results: Seq[DqResult],
  executed: Int,
  passed: Int,
  failed: Int,
  errored: Int
) {
  /** Results with a fail-like status (failed | errored). */
  def exceptionReport: Seq[DqResult] =
    results.filter(r => Status.failLike.contains(r.status))
}

// ─── Engine results ───────────────────────────────────────────────────────────

sealed trait EngineRunResult
case class TerminatedRun(error: String) extends EngineRunResult
case class CompletedRun(report: DqReport, writeErrors: Seq[String]) extends EngineRunResult

// ─── Error types ─────────────────────────────────────────────────────────────

case class ConfigError(message: String)
case class SourceError(message: String)

class MariaDbStoreException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

class ResultStoreUnavailable(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

// ─── Connection configuration ─────────────────────────────────────────────────

case class MariaDbConfig(host: String, port: Int, database: String, user: String, password: String) {
  def jdbcUrl: String = s"jdbc:mariadb://$host:$port/$database"
}

case class HiveConfig(host: String, port: Int, database: String, user: String, password: String) {
  val driverClass: String = "org.apache.hive.jdbc.HiveDriver"
  def jdbcUrl: String = s"jdbc:hive2://$host:$port/$database"
}

case class EngineConfig(
  storeConfig: MariaDbConfig,
  dataSourceConfigs: Map[String, MariaDbConfig] = Map.empty,
  hiveConfigs: Map[String, HiveConfig] = Map.empty
)
