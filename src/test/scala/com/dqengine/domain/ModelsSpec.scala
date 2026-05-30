package com.dqengine.domain

import org.scalatest.funsuite.AnyFunSuite
import java.time.Instant

class ModelsSpec extends AnyFunSuite {

  test("CheckType.fromWire round-trips for all values") {
    Seq(CheckType.Completeness, CheckType.Validity, CheckType.Consistency).foreach { ct =>
      assert(CheckType.fromWire(ct.wire) === Some(ct))
    }
  }
  test("CheckType.fromWire returns None for unknown or case-mismatched string") {
    assert(CheckType.fromWire("accuracy")     === None)
    assert(CheckType.fromWire("Completeness") === None)
  }

  test("EvaluationMode.fromWire round-trips for all values") {
    Seq(EvaluationMode.Violation, EvaluationMode.ConsistencyDelta, EvaluationMode.ConsistencyMom).foreach { em =>
      assert(EvaluationMode.fromWire(em.wire) === Some(em))
    }
  }
  test("EvaluationMode.fromWire returns None for old mode names") {
    assert(EvaluationMode.fromWire("metric")      === None)
    assert(EvaluationMode.fromWire("consistency") === None)
  }

  test("SourceType.fromWire round-trips for all three source types") {
    assert(SourceType.fromWire("parquet")       === Some(SourceType.Parquet))
    assert(SourceType.fromWire("mariadb_table") === Some(SourceType.MariaDbTable))
    assert(SourceType.fromWire("hive_table")    === Some(SourceType.HiveTable))
    assert(SourceType.fromWire("csv")           === None)
  }

  test("Status.failLike contains Failed and Errored but not Passed") {
    assert(Status.failLike.contains(Status.Failed))
    assert(Status.failLike.contains(Status.Errored))
    assert(!Status.failLike.contains(Status.Passed))
  }

  test("IssueStatus.fromWire round-trips for all values") {
    Seq(IssueStatus.New, IssueStatus.Recurring, IssueStatus.Resolved).foreach { is =>
      assert(IssueStatus.fromWire(is.wire) === Some(is))
    }
  }

  test("IssueStatus.fromWire returns None for removed statuses") {
    assert(IssueStatus.fromWire("none")    === None)
    assert(IssueStatus.fromWire("unknown") === None)
  }

  test("isPartitionedFileSource is true only for parquet") {
    val parquet = SourceCatalogEntry("t", SourceType.Parquet, Some("/data"), Some("dt"), None, None, None)
    val maria   = SourceCatalogEntry("t", SourceType.MariaDbTable, None, None, Some("ref"), Some("tbl"), None)
    val hive    = SourceCatalogEntry("t", SourceType.HiveTable, None, None, Some("hive-ref"), Some("db.tbl"), None)
    assert(parquet.isPartitionedFileSource === true)
    assert(maria.isPartitionedFileSource   === false)
    assert(hive.isPartitionedFileSource    === false)
  }

  test("CheckDefinition deviationTolerance defaults to 0 and appCode is optional") {
    val defn = CheckDefinition("id1", "tbl", "col", None, CheckType.Completeness,
      None, None, BigDecimal(0), "SELECT 1", EvaluationMode.Violation, None)
    assert(defn.deviationTolerance === BigDecimal(0))
    assert(defn.appCode === None)
  }

  test("DqReport.exceptionReport returns only failed/errored") {
    val now = Instant.now()
    def mkResult(status: Status) = DqResult(
      java.util.UUID.randomUUID().toString, "c1", None, "v1", now, "r1", now,
      "tbl", SourceType.Parquet, "col", None, CheckType.Completeness,
      EvaluationMode.Violation, status, Measures.Empty, None, None)
    val results = Seq(
      mkResult(Status.Passed), mkResult(Status.Failed), mkResult(Status.Errored))
    val report = DqReport("r1", now, "v1", now, results, 3, 1, 1, 1)
    val exc = report.exceptionReport
    assert(exc.size === 2)
    assert(exc.map(_.status).toSet === Set(Status.Failed, Status.Errored))
  }

  test("ScopeLabel.toPartitionValueString returns Some for PartitionValue and FilterValue, None for NotApplicable") {
    assert(ScopeLabel.toPartitionValueString(ScopeLabel.PartitionValue("2024-01-01")) === Some("2024-01-01"))
    assert(ScopeLabel.toPartitionValueString(ScopeLabel.FilterValue("42"))            === Some("42"))
    assert(ScopeLabel.toPartitionValueString(ScopeLabel.NotApplicable)                === scala.None)
  }

  test("HiveConfig produces correct JDBC URL") {
    val cfg = HiveConfig("localhost", 10000, "default", "hive", "")
    assert(cfg.jdbcUrl === "jdbc:hive2://localhost:10000/default")
    assert(cfg.driverClass === "org.apache.hive.jdbc.HiveDriver")
  }
}
