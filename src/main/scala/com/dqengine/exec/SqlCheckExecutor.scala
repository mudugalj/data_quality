package com.dqengine.exec

import com.dqengine.domain._
import com.dqengine.store.DqStore
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import java.time.{Instant, Duration}
import java.util.UUID
import scala.util.{Try, Success, Failure}

class SqlCheckExecutor(store: DqStore)(implicit spark: SparkSession) {

  private val RetentionWindow = Duration.ofDays(183)
  private val MomWindowHigh   = Duration.ofDays(28)
  private val MomWindowLow    = Duration.ofDays(31)

  def execute(defn: CheckDefinition, source: ResolvedSource, ctx: RunContext): DqResult = {
    val dqRefId = UUID.randomUUID().toString
    try {
      SqlSafetyValidator.validate(defn.checkSql, source.sqlTarget, Some(spark)) match {
        case Left(err) =>
          buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty,
            Some(s"check_sql rejected for ${defn.checkId}@${defn.tableName}: $err"), classify(defn, ctx, Status.Errored))
        case Right(safeSql) =>
          val (effectiveSql, filterErr) = applyFilter(safeSql, defn.businessRuleFilter, source)
          filterErr match {
            case Some(err) =>
              buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty,
                Some(s"business_rule_filter error for ${defn.checkId}@${defn.tableName}: $err"), classify(defn, ctx, Status.Errored))
            case None =>
              judgeResult(dqRefId, defn, source, ctx, effectiveSql)
          }
      }
    } catch {
      case e: Exception =>
        buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty,
          Some(s"Unexpected error for ${defn.checkId}@${defn.tableName}: ${e.getMessage}"), classify(defn, ctx, Status.Errored))
    }
  }

  // ── Filter application ────────────────────────────────────────────────────

  private def applyFilter(sql: String, filter: Option[String], source: ResolvedSource): (String, Option[String]) =
    filter.filter(_.trim.nonEmpty) match {
      case None => (sql, None)
      case Some(f) => source.sqlTarget match {
        case SqlTarget.SparkTempView(name) =>
          // Apply the filter via the DataFrame API and re-register the SAME view.
          // We must NOT use `CREATE OR REPLACE TEMP VIEW name AS SELECT * FROM name WHERE ...`
          // because that keeps `name` as an unresolved self-reference and Spark rejects it
          // as a recursive view. Reading the current view into a DataFrame first captures
          // its already-resolved plan, so replacing the view with the filtered DataFrame is
          // safe and check_sql can still reference the source by its logical name.
          Try {
            val filtered = spark.table(name).where(f)
            filtered.createOrReplaceTempView(name)
          } match {
            case Success(_) => (sql, None)
            case Failure(e) => (sql, Some(s"Cannot apply business_rule_filter '$f': ${e.getMessage}"))
          }
        case _: SqlTarget.JdbcConnection =>
          // For JDBC, user embeds the filter in check_sql; no transform needed
          (sql, None)
      }
    }

  // ── Mode dispatch ─────────────────────────────────────────────────────────

  private def judgeResult(dqRefId: String, defn: CheckDefinition, source: ResolvedSource, ctx: RunContext, sql: String): DqResult =
    defn.evaluationMode match {
      case EvaluationMode.Violation        => executeViolation(dqRefId, defn, source, ctx, sql)
      case EvaluationMode.ConsistencyDelta => executeConsistencyDelta(dqRefId, defn, source, ctx, sql)
      case EvaluationMode.ConsistencyMom   => executeConsistencyMom(dqRefId, defn, source, ctx, sql)
    }

  // ── Violation mode ────────────────────────────────────────────────────────

  private def executeViolation(dqRefId: String, defn: CheckDefinition, source: ResolvedSource, ctx: RunContext, sql: String): DqResult = {
    Try(runSql(sql, source)) match {
      case Failure(e) =>
        buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty,
          Some(s"check_sql failed for ${defn.checkId}: ${e.getMessage}"), classify(defn, ctx, Status.Errored))
      case Success(df) =>
        val count  = Try(df.count()).getOrElse(0L)
        val status = if (count == 0) Status.Passed else Status.Failed
        buildResult(dqRefId, defn, source, ctx, status, Measures.ViolationCount(count),
          if (count > 0) Some(s"$count violation(s) found") else None,
          classify(defn, ctx, status))
    }
  }

  // ── Consistency delta mode ────────────────────────────────────────────────

  private def executeConsistencyDelta(dqRefId: String, defn: CheckDefinition, source: ResolvedSource, ctx: RunContext, sql: String): DqResult = {
    getSingleNumeric(sql, source, defn) match {
      case Left(err) =>
        buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty, Some(err), classify(defn, ctx, Status.Errored))
      case Right(current) =>
        val windowStart = ctx.runTimestamp.minus(RetentionWindow)
        Try(store.findPreviousResult(defn.tableName, defn.columnName, defn.checkType.wire,
                                     defn.appCode, ctx.runTimestamp, windowStart)) match {
          case Failure(e) =>
            buildResult(dqRefId, defn, source, ctx, Status.Errored,
              Measures.ConsistencyMeasure(current, None, None),
              Some(s"Cannot retrieve prior result: ${e.getMessage}"),
              classify(defn, ctx, Status.Errored))
          case Success(None) =>
            buildResult(dqRefId, defn, source, ctx, Status.Passed,
              Measures.ConsistencyMeasure(current, None, None),
              Some("no prior baseline within retention window"),
              classify(defn, ctx, Status.Passed))
          case Success(Some(prev)) =>
            prev.measures match {
              case Measures.ConsistencyMeasure(pv, _, _) =>
                val deviation = (current - pv).abs
                val passed    = deviation <= defn.deviationTolerance
                val status    = if (passed) Status.Passed else Status.Failed
                buildResult(dqRefId, defn, source, ctx, status,
                  Measures.ConsistencyMeasure(current, Some(pv), Some(deviation)),
                  if (!passed) Some(s"deviation $deviation exceeds tolerance ${defn.deviationTolerance}") else None,
                  classify(defn, ctx, status))
              case _ =>
                buildResult(dqRefId, defn, source, ctx, Status.Passed,
                  Measures.ConsistencyMeasure(current, None, None),
                  Some("cannot compare; no prior measure"),
                  classify(defn, ctx, Status.Passed))
            }
        }
    }
  }

  // ── Consistency MOM mode ──────────────────────────────────────────────────

  private def executeConsistencyMom(dqRefId: String, defn: CheckDefinition, source: ResolvedSource, ctx: RunContext, sql: String): DqResult = {
    getSingleNumeric(sql, source, defn) match {
      case Left(err) =>
        buildResult(dqRefId, defn, source, ctx, Status.Errored, Measures.Empty, Some(err), classify(defn, ctx, Status.Errored))
      case Right(current) =>
        val momHigh = ctx.runTimestamp.minus(MomWindowHigh)  // 28 days ago
        val momLow  = ctx.runTimestamp.minus(MomWindowLow)   // 31 days ago
        Try(store.findMomResult(defn.tableName, defn.columnName, defn.checkType.wire,
                                 defn.appCode, momLow, momHigh)) match {
          case Failure(e) =>
            buildResult(dqRefId, defn, source, ctx, Status.Errored,
              Measures.ConsistencyMeasure(current, None, None),
              Some(s"Cannot retrieve MOM prior result: ${e.getMessage}"),
              classify(defn, ctx, Status.Errored))
          case Success(None) =>
            buildResult(dqRefId, defn, source, ctx, Status.Passed,
              Measures.ConsistencyMeasure(current, None, None),
              Some("no prior baseline within retention window"),
              classify(defn, ctx, Status.Passed))
          case Success(Some(prev)) =>
            prev.measures match {
              case Measures.ConsistencyMeasure(pv, _, _) =>
                if (pv == 0) {
                  buildResult(dqRefId, defn, source, ctx, Status.Passed,
                    Measures.ConsistencyMeasure(current, Some(pv), None),
                    Some("prior MoM value is zero; cannot compute % — passed by default"),
                    classify(defn, ctx, Status.Passed))
                } else {
                  val pctDev = ((current - pv).abs / pv.abs) * 100
                  val passed = pctDev <= defn.deviationTolerance
                  val status = if (passed) Status.Passed else Status.Failed
                  buildResult(dqRefId, defn, source, ctx, status,
                    Measures.ConsistencyMeasure(current, Some(pv), Some(pctDev)),
                    if (!passed) Some(s"MOM deviation ${pctDev.setScale(2, BigDecimal.RoundingMode.HALF_UP)}% exceeds tolerance ${defn.deviationTolerance}%") else None,
                    classify(defn, ctx, status))
                }
              case _ =>
                buildResult(dqRefId, defn, source, ctx, Status.Passed,
                  Measures.ConsistencyMeasure(current, None, None),
                  Some("cannot compare; no prior measure"),
                  classify(defn, ctx, Status.Passed))
            }
        }
    }
  }

  // ── Issue classification ──────────────────────────────────────────────────

  private def classify(defn: CheckDefinition, ctx: RunContext, currentStatus: Status): Option[IssueStatus] =
    IssueClassifier.classify(store, defn.tableName, defn.columnName, defn.checkType.wire,
                             defn.appCode, ctx.runTimestamp, currentStatus)

  // ── SQL execution ─────────────────────────────────────────────────────────

  private def runSql(sql: String, source: ResolvedSource) = source.sqlTarget match {
    case SqlTarget.SparkTempView(_) => spark.sql(sql)
    case SqlTarget.JdbcConnection(url, user, pw, driver) =>
      Class.forName(driver)
      val conn = java.sql.DriverManager.getConnection(url, user, pw)
      conn.setReadOnly(true)
      try {
        val stmt = conn.createStatement()
        val rs   = stmt.executeQuery(sql)
        val meta = rs.getMetaData
        val n    = meta.getColumnCount
        val cols = (1 to n).map(i => meta.getColumnLabel(i))
        val rows = scala.collection.mutable.ListBuffer[Row]()
        while (rs.next()) rows += Row.fromSeq((1 to n).map(i => rs.getString(i)))
        val schema = StructType(cols.map(c => StructField(c, StringType, nullable = true)))
        spark.createDataFrame(spark.sparkContext.parallelize(rows.toList), schema)
      } finally Try(conn.close())
  }

  private def getSingleNumeric(sql: String, source: ResolvedSource, defn: CheckDefinition): Either[String, BigDecimal] =
    Try(runSql(sql, source)) match {
      case Failure(e) => Left(s"check_sql execution failed for ${defn.checkId}: ${e.getMessage}")
      case Success(df) =>
        Try {
          val rows = df.collect()
          if (rows.length != 1 || df.columns.length != 1)
            throw new IllegalStateException("consistency check_sql must return exactly one row and one column")
          val v = rows(0).get(0)
          if (v == null) throw new IllegalStateException("consistency check_sql returned null")
          BigDecimal(v.toString)
        }.toEither.left.map(e => s"${defn.checkId}@${defn.tableName}: ${e.getMessage}")
    }

  // ── Result builder ────────────────────────────────────────────────────────

  private def buildResult(
    dqRefId: String, defn: CheckDefinition, source: ResolvedSource,
    ctx: RunContext, status: Status, measures: Measures,
    description: Option[String], issueStatus: Option[IssueStatus]
  ): DqResult = DqResult(
    dqRefId           = dqRefId,
    checkId           = defn.checkId,
    appCode           = defn.appCode,
    configVersion     = ctx.configVersion,
    configVersionDate = ctx.configVersionDate,
    runId             = ctx.runId,
    runTimestamp      = ctx.runTimestamp,
    tableName         = defn.tableName,
    sourceType        = source.entry.sourceType,
    columnName        = defn.columnName,
    partitionValue    = ScopeLabel.toPartitionValueString(source.scopeLabel),
    checkType         = defn.checkType,
    evaluationMode    = defn.evaluationMode,
    status            = status,
    measures          = measures,
    description       = description,
    issueStatus       = issueStatus
  )
}
