package com.dqengine.exec

import com.dqengine.domain._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.time.Instant

class SqlCheckExecutorSpec extends AnyFunSuite with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("DqExecutorTest")
      .master("local[1]").config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def mkStore = new com.dqengine.store.DqStore(
    MariaDbConfig("localhost", 3306, "x", "x", "x")) {
    override def findPreviousResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) = None
    override def findMomResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) = None
  }

  private def ctx = RunContext("run1", Instant.now(), "v1", Instant.now(),
    PartitionParameter.Latest, Set("completeness"))

  private def defn(mode: EvaluationMode, sql: String, tol: BigDecimal = 0,
                   appCode: Option[String] = None) =
    CheckDefinition("chk1","tbl","col",None,CheckType.Completeness,
      None,None,tol,sql,mode,appCode)

  private def parquetSource(view: String) = ResolvedSource(
    SourceCatalogEntry("tbl",SourceType.Parquet,Some("/tmp"),Some("dt"),None,None,None),
    SqlTarget.SparkTempView(view), ScopeLabel.PartitionValue("2024-01-01"))

  // ── Violation mode ─────────────────────────────────────────────────────────

  test("violation: 0 rows -> passed") {
    spark.sql("CREATE OR REPLACE TEMP VIEW viol_pass AS SELECT 1 AS x WHERE 1=0")
    val r = new SqlCheckExecutor(mkStore).execute(
      defn(EvaluationMode.Violation, "SELECT x FROM viol_pass"), parquetSource("viol_pass"), ctx)
    assert(r.status === Status.Passed)
    assert(r.measures === Measures.ViolationCount(0L))
  }

  test("violation: 3 rows -> failed") {
    spark.sql("CREATE OR REPLACE TEMP VIEW viol_fail AS SELECT id FROM (VALUES (1),(2),(3)) t(id)")
    val r = new SqlCheckExecutor(mkStore).execute(
      defn(EvaluationMode.Violation, "SELECT id FROM viol_fail"), parquetSource("viol_fail"), ctx)
    assert(r.status === Status.Failed)
    assert(r.measures === Measures.ViolationCount(3L))
  }

  test("violation: app_code carried through to result") {
    spark.sql("CREATE OR REPLACE TEMP VIEW viol_app AS SELECT 1 AS x WHERE 1=0")
    val r = new SqlCheckExecutor(mkStore).execute(
      defn(EvaluationMode.Violation, "SELECT x FROM viol_app", appCode = Some("CRM")),
      parquetSource("viol_app"), ctx)
    assert(r.appCode === Some("CRM"))
  }

  // ── Consistency delta mode ─────────────────────────────────────────────────

  test("consistency_delta: no prior run -> inconclusive") {
    spark.sql("CREATE OR REPLACE TEMP VIEW cons_view AS SELECT 500 AS cnt")
    val r = new SqlCheckExecutor(mkStore).execute(
      defn(EvaluationMode.ConsistencyDelta, "SELECT cnt FROM cons_view"),
      parquetSource("cons_view"), ctx)
    assert(r.status === Status.Inconclusive)
  }

  test("consistency_delta: within tolerance -> passed") {
    spark.sql("CREATE OR REPLACE TEMP VIEW cons_delta_pass AS SELECT 105 AS cnt")
    val store = new com.dqengine.store.DqStore(MariaDbConfig("localhost",3306,"x","x","x")) {
      override def findPreviousResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) =
        Some(DqResult("r","c",None,"v1",Instant.now(),"run0",Instant.now(),"tbl",
          SourceType.Parquet,"col",None,CheckType.Consistency,EvaluationMode.ConsistencyDelta,
          Status.Passed, Measures.ConsistencyMeasure(BigDecimal(100), None, None),
          None, IssueStatus.None))
      override def findMomResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) = None
    }
    val r = new SqlCheckExecutor(store).execute(
      defn(EvaluationMode.ConsistencyDelta, "SELECT cnt FROM cons_delta_pass", tol = BigDecimal(10)),
      parquetSource("cons_delta_pass"), ctx)
    assert(r.status === Status.Passed)
  }

  // ── Consistency MOM mode ───────────────────────────────────────────────────

  test("consistency_mom: no 28-31d prior -> inconclusive") {
    spark.sql("CREATE OR REPLACE TEMP VIEW mom_view AS SELECT 500 AS cnt")
    val r = new SqlCheckExecutor(mkStore).execute(
      defn(EvaluationMode.ConsistencyMom, "SELECT cnt FROM mom_view"),
      parquetSource("mom_view"), ctx)
    assert(r.status === Status.Inconclusive)
  }

  test("consistency_mom: prior=0 -> inconclusive (division by zero)") {
    spark.sql("CREATE OR REPLACE TEMP VIEW mom_zero AS SELECT 100 AS cnt")
    val store = new com.dqengine.store.DqStore(MariaDbConfig("localhost",3306,"x","x","x")) {
      override def findPreviousResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) = None
      override def findMomResult(a: String, b: String, c: String, d: Option[String], e: Instant, f: Instant) =
        Some(DqResult("r","c",None,"v1",Instant.now(),"run0",Instant.now(),"tbl",
          SourceType.Parquet,"col",None,CheckType.Consistency,EvaluationMode.ConsistencyMom,
          Status.Passed, Measures.ConsistencyMeasure(BigDecimal(0), None, None),
          None, IssueStatus.None))
    }
    val r = new SqlCheckExecutor(store).execute(
      defn(EvaluationMode.ConsistencyMom, "SELECT cnt FROM mom_zero"),
      parquetSource("mom_zero"), ctx)
    assert(r.status === Status.Inconclusive)
    assert(r.description.exists(_.contains("zero")))
  }

  // ── Safety validator ────────────────────────────────────────────────────────

  test("safety validator rejects INSERT") {
    val r = SqlSafetyValidator.validate("INSERT INTO foo VALUES (1)", SqlTarget.SparkTempView("v"), None)
    assert(r.isLeft)
  }

  test("safety validator accepts SELECT") {
    val r = SqlSafetyValidator.validate("SELECT COUNT(*) FROM v", SqlTarget.SparkTempView("v"), None)
    assert(r.isRight)
  }

  test("safety validator rejects multi-statement") {
    val r = SqlSafetyValidator.validate("SELECT 1; DROP TABLE foo",
      SqlTarget.JdbcConnection("url","u","p","driver"), None)
    assert(r.isLeft)
  }
}
