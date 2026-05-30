package com.dqengine.store

import com.dqengine.domain._
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import java.util.UUID
import scala.util.Try

class DqStore(config: MariaDbConfig) {

  private def getConnection(): Connection =
    try {
      Class.forName("org.mariadb.jdbc.Driver")
      DriverManager.getConnection(config.jdbcUrl, config.user, config.password)
    } catch {
      case e: Exception =>
        throw new MariaDbStoreException(s"Cannot connect to MariaDB_Store at ${config.jdbcUrl}: ${e.getMessage}", e)
    }

  private def withConnection[A](f: Connection => A): A = {
    val conn = getConnection()
    try f(conn) finally Try(conn.close())
  }

  // ── Config version ────────────────────────────────────────────────────────

  def resolveVersion(param: Option[String]): ConfigVersionInfo = withConnection { conn =>
    param match {
      case Some(v) =>
        val ps = conn.prepareStatement(
          "SELECT config_version, config_version_date FROM config_versions WHERE config_version = ?")
        ps.setString(1, v)
        val rs = ps.executeQuery()
        if (rs.next()) ConfigVersionInfo(rs.getString("config_version"), rs.getTimestamp("config_version_date").toInstant)
        else throw new MariaDbStoreException(s"config_version '$v' not found in Config_Store")
      case None =>
        val ps = conn.prepareStatement(
          "SELECT config_version, config_version_date FROM config_versions ORDER BY config_version_date DESC LIMIT 1")
        val rs = ps.executeQuery()
        if (rs.next()) ConfigVersionInfo(rs.getString("config_version"), rs.getTimestamp("config_version_date").toInstant)
        else throw new MariaDbStoreException("No config_version exists in Config_Store")
    }
  }

  def registerVersion(info: ConfigVersionInfo): Unit = withConnection { conn =>
    val ps = conn.prepareStatement(
      "INSERT INTO config_versions (config_version, config_version_date) VALUES (?, ?)")
    ps.setString(1, info.configVersion)
    ps.setTimestamp(2, Timestamp.from(info.configVersionDate))
    ps.executeUpdate()
  }

  // ── Check definitions ─────────────────────────────────────────────────────

  def loadCheckDefinitions(configVersion: String): Seq[Map[String, Option[String]]] = withConnection { conn =>
    val ps = conn.prepareStatement(
      """SELECT check_id, table_name, column_name, data_type, check_type,
        |       business_glossary, business_rule_filter, deviation_tolerance,
        |       check_sql, evaluation_mode, app_code
        |FROM config_store WHERE config_version = ?""".stripMargin)
    ps.setString(1, configVersion)
    val rs = ps.executeQuery()
    val cols = Seq("check_id","table_name","column_name","data_type","check_type",
                   "business_glossary","business_rule_filter","deviation_tolerance",
                   "check_sql","evaluation_mode","app_code")
    val buf = scala.collection.mutable.ListBuffer[Map[String, Option[String]]]()
    while (rs.next()) buf += cols.map(c => c -> Option(rs.getString(c))).toMap
    buf.toList
  }

  def writeCheckDefinitions(configVersion: String, configVersionDate: Instant, defs: Seq[CheckDefinition]): Unit =
    withConnection { conn =>
      val ps = conn.prepareStatement(
        """INSERT INTO config_store
          |(config_version, check_id, table_name, column_name, data_type, check_type,
          | business_glossary, business_rule_filter, deviation_tolerance,
          | check_sql, evaluation_mode, app_code)
          |VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin)
      defs.foreach { d =>
        ps.setString(1, configVersion)
        ps.setString(2, d.checkId)
        ps.setString(3, d.tableName)
        ps.setString(4, d.columnName)
        setOpt(ps, 5, d.dataType)
        ps.setString(6, d.checkType.wire)
        setOpt(ps, 7, d.businessGlossary)
        setOpt(ps, 8, d.businessRuleFilter)
        if (d.deviationTolerance == 0) ps.setNull(9, java.sql.Types.DECIMAL)
        else ps.setBigDecimal(9, d.deviationTolerance.bigDecimal)
        ps.setString(10, d.checkSql)
        ps.setString(11, d.evaluationMode.wire)
        setOpt(ps, 12, d.appCode)
        ps.addBatch()
      }
      ps.executeBatch()
    }

  // ── Source catalog ────────────────────────────────────────────────────────

  def loadSourceCatalog(): Seq[Map[String, Option[String]]] = withConnection { conn =>
    val ps = conn.prepareStatement(
      """SELECT table_name, source_type, parquet_location, partition_column,
        |       connection_ref, physical_table_name, filter_column
        |FROM source_catalog""".stripMargin)
    val rs = ps.executeQuery()
    val cols = Seq("table_name","source_type","parquet_location","partition_column",
                   "connection_ref","physical_table_name","filter_column")
    val buf = scala.collection.mutable.ListBuffer[Map[String, Option[String]]]()
    while (rs.next()) buf += cols.map(c => c -> Option(rs.getString(c))).toMap
    buf.toList
  }

  def writeSourceCatalog(entries: Seq[SourceCatalogEntry]): Unit = withConnection { conn =>
    val ps = conn.prepareStatement(
      """INSERT INTO source_catalog
        |(table_name, source_type, parquet_location, partition_column,
        | connection_ref, physical_table_name, filter_column)
        |VALUES (?,?,?,?,?,?,?)""".stripMargin)
    entries.foreach { e =>
      ps.setString(1, e.tableName); ps.setString(2, e.sourceType.wire)
      setOpt(ps, 3, e.parquetLocation); setOpt(ps, 4, e.partitionColumn)
      setOpt(ps, 5, e.connectionRef);   setOpt(ps, 6, e.physicalTableName)
      setOpt(ps, 7, e.filterColumn)
      ps.addBatch()
    }
    ps.executeBatch()
  }

  // ── Result store ──────────────────────────────────────────────────────────

  def writeResult(result: DqResult): Unit = {
    val conn = try getConnection() catch {
      case e: MariaDbStoreException =>
        throw new ResultStoreUnavailable(
          s"Cannot connect to Result_Store for ${result.tableName}.${result.columnName}: ${e.getMessage}", e)
    }
    try {
      val ps = conn.prepareStatement(
        """INSERT INTO result_store
          |(dq_ref_id, check_id, app_code, config_version, config_version_date,
          | run_id, run_timestamp, table_name, source_type, column_name,
          | partition_value, check_type, evaluation_mode, status, issue_status,
          | violation_count, current_value, prior_value, deviation, description)
          |VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin)
      ps.setString(1, result.dqRefId)
      ps.setString(2, result.checkId)
      setOpt(ps, 3, result.appCode)
      ps.setString(4, result.configVersion)
      ps.setTimestamp(5, Timestamp.from(result.configVersionDate))
      ps.setString(6, result.runId)
      ps.setTimestamp(7, Timestamp.from(result.runTimestamp))
      ps.setString(8, result.tableName)
      ps.setString(9, result.sourceType.wire)
      ps.setString(10, result.columnName)
      setOpt(ps, 11, result.partitionValue)
      ps.setString(12, result.checkType.wire)
      ps.setString(13, result.evaluationMode.wire)
      ps.setString(14, result.status.wire)
      setOpt(ps, 15, result.issueStatus.map(_.wire))
      result.measures match {
        case Measures.ViolationCount(n) =>
          ps.setLong(16, n)
          ps.setNull(17, java.sql.Types.DECIMAL)
          ps.setNull(18, java.sql.Types.DECIMAL)
          ps.setNull(19, java.sql.Types.DECIMAL)
        case Measures.ConsistencyMeasure(cur, priorOpt, devOpt) =>
          ps.setNull(16, java.sql.Types.BIGINT)
          ps.setBigDecimal(17, cur.bigDecimal)
          priorOpt match { case Some(v) => ps.setBigDecimal(18, v.bigDecimal); case None => ps.setNull(18, java.sql.Types.DECIMAL) }
          devOpt  match { case Some(v) => ps.setBigDecimal(19, v.bigDecimal); case None => ps.setNull(19, java.sql.Types.DECIMAL) }
        case Measures.Empty =>
          ps.setNull(16, java.sql.Types.BIGINT)
          ps.setNull(17, java.sql.Types.DECIMAL)
          ps.setNull(18, java.sql.Types.DECIMAL)
          ps.setNull(19, java.sql.Types.DECIMAL)
      }
      setOpt(ps, 20, result.description)
      ps.executeUpdate()
    } catch {
      case e: ResultStoreUnavailable => throw e
      case e: Exception =>
        throw new ResultStoreUnavailable(
          s"Failed to write result for ${result.tableName}.${result.columnName}: ${e.getMessage}", e)
    } finally Try(conn.close())
  }

  def readResultById(dqRefId: String): Option[DqResult] = withConnection { conn =>
    val ps = conn.prepareStatement("SELECT * FROM result_store WHERE dq_ref_id = ?")
    ps.setString(1, dqRefId)
    val rs = ps.executeQuery()
    if (rs.next()) Some(rowToResult(rs)) else None
  }

  /** Most recent result for (table, col, checkType, appCode) strictly before beforeTs and within windowStart. */
  def findPreviousResult(
    tableName: String, columnName: String, checkType: String, appCode: Option[String],
    beforeRunTimestamp: Instant, windowStart: Instant
  ): Option[DqResult] = withConnection { conn =>
    val sql = appCode match {
      case Some(_) =>
        """SELECT * FROM result_store
          |WHERE table_name=? AND column_name=? AND check_type=? AND app_code=?
          |  AND run_timestamp < ? AND run_timestamp >= ?
          |ORDER BY run_timestamp DESC LIMIT 1""".stripMargin
      case None =>
        """SELECT * FROM result_store
          |WHERE table_name=? AND column_name=? AND check_type=? AND app_code IS NULL
          |  AND run_timestamp < ? AND run_timestamp >= ?
          |ORDER BY run_timestamp DESC LIMIT 1""".stripMargin
    }
    val ps = conn.prepareStatement(sql)
    ps.setString(1, tableName); ps.setString(2, columnName); ps.setString(3, checkType)
    appCode match {
      case Some(ac) =>
        ps.setString(4, ac)
        ps.setTimestamp(5, Timestamp.from(beforeRunTimestamp))
        ps.setTimestamp(6, Timestamp.from(windowStart))
      case None =>
        ps.setTimestamp(4, Timestamp.from(beforeRunTimestamp))
        ps.setTimestamp(5, Timestamp.from(windowStart))
    }
    val rs = ps.executeQuery()
    if (rs.next()) Some(rowToResult(rs)) else None
  }

  /** Most recent result in the 28-31-day window (month-on-month lookup). */
  def findMomResult(
    tableName: String, columnName: String, checkType: String, appCode: Option[String],
    momWindowLow: Instant, momWindowHigh: Instant
  ): Option[DqResult] = withConnection { conn =>
    val sql = appCode match {
      case Some(_) =>
        """SELECT * FROM result_store
          |WHERE table_name=? AND column_name=? AND check_type=? AND app_code=?
          |  AND run_timestamp BETWEEN ? AND ?
          |ORDER BY run_timestamp DESC LIMIT 1""".stripMargin
      case None =>
        """SELECT * FROM result_store
          |WHERE table_name=? AND column_name=? AND check_type=? AND app_code IS NULL
          |  AND run_timestamp BETWEEN ? AND ?
          |ORDER BY run_timestamp DESC LIMIT 1""".stripMargin
    }
    val ps = conn.prepareStatement(sql)
    ps.setString(1, tableName); ps.setString(2, columnName); ps.setString(3, checkType)
    appCode match {
      case Some(ac) =>
        ps.setString(4, ac)
        ps.setTimestamp(5, Timestamp.from(momWindowLow))
        ps.setTimestamp(6, Timestamp.from(momWindowHigh))
      case None =>
        ps.setTimestamp(4, Timestamp.from(momWindowLow))
        ps.setTimestamp(5, Timestamp.from(momWindowHigh))
    }
    val rs = ps.executeQuery()
    if (rs.next()) Some(rowToResult(rs)) else None
  }

  def purgeOlderThan(cutoff: Instant): Long = withConnection { conn =>
    val ps = conn.prepareStatement("DELETE FROM result_store WHERE run_timestamp < ?")
    ps.setTimestamp(1, Timestamp.from(cutoff))
    ps.executeLargeUpdate()
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def setOpt(ps: PreparedStatement, idx: Int, v: Option[String]): Unit =
    v match { case Some(s) => ps.setString(idx, s); case None => ps.setNull(idx, java.sql.Types.VARCHAR) }

  private def optDecimal(rs: ResultSet, col: String): Option[BigDecimal] = {
    val v = rs.getBigDecimal(col); if (rs.wasNull()) None else Some(BigDecimal(v))
  }

  private def rowToResult(rs: ResultSet): DqResult = {
    val violationCount = { val v = rs.getLong("violation_count"); if (rs.wasNull()) None else Some(v) }
    val currentValue   = optDecimal(rs, "current_value")
    val priorValue     = optDecimal(rs, "prior_value")
    val deviation      = optDecimal(rs, "deviation")

    val measures: Measures = violationCount match {
      case Some(n) => Measures.ViolationCount(n)
      case None    => currentValue match {
        case Some(cur) => Measures.ConsistencyMeasure(cur, priorValue, deviation)
        case None      => Measures.Empty
      }
    }
    val evalMode = EvaluationMode.fromWire(rs.getString("evaluation_mode"))
      .getOrElse(EvaluationMode.Violation)

    DqResult(
      dqRefId           = rs.getString("dq_ref_id"),
      checkId           = rs.getString("check_id"),
      appCode           = Option(rs.getString("app_code")),
      configVersion     = rs.getString("config_version"),
      configVersionDate = rs.getTimestamp("config_version_date").toInstant,
      runId             = rs.getString("run_id"),
      runTimestamp      = rs.getTimestamp("run_timestamp").toInstant,
      tableName         = rs.getString("table_name"),
      sourceType        = SourceType.fromWire(rs.getString("source_type")).getOrElse(SourceType.Parquet),
      columnName        = rs.getString("column_name"),
      partitionValue    = Option(rs.getString("partition_value")),
      checkType         = CheckType.fromWire(rs.getString("check_type")).getOrElse(CheckType.Completeness),
      evaluationMode    = evalMode,
      status            = Status.fromWire(rs.getString("status")).getOrElse(Status.Errored),
      measures          = measures,
      description       = Option(rs.getString("description")),
      issueStatus       = Option(rs.getString("issue_status")).flatMap(IssueStatus.fromWire)
    )
  }
}
