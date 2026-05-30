package com.dqengine.engine

import com.dqengine.domain._
import org.scalatest.funsuite.AnyFunSuite
import java.time.Instant

class ReportGeneratorSpec extends AnyFunSuite {
  private val now = Instant.now()
  private def ctx = RunContext("r1",now,"v1",now,PartitionParameter.Latest,Set("completeness"))
  private def mkResult(status: Status, appCode: Option[String] = None) = DqResult(
    java.util.UUID.randomUUID().toString, "c1", appCode, "v1", now, "r1", now,
    "tbl", SourceType.Parquet, "col", None, CheckType.Completeness,
    EvaluationMode.Violation, status, Measures.Empty, None, IssueStatus.None)

  test("empty run -> all-zero counts") {
    val r = ReportGenerator.empty(ctx)
    assert(r.executed === 0); assert(r.passed === 0)
  }

  test("executed = passed + failed + errored + inconclusive") {
    val results = Seq(mkResult(Status.Passed), mkResult(Status.Passed),
                      mkResult(Status.Failed), mkResult(Status.Errored), mkResult(Status.Inconclusive))
    val r = ReportGenerator.generate(results, ctx)
    assert(r.passed === 2); assert(r.failed === 1); assert(r.errored === 1); assert(r.inconclusive === 1)
    assert(r.executed === 5)
  }

  test("exceptionReport returns only failed + errored + inconclusive") {
    val results = Seq(mkResult(Status.Passed), mkResult(Status.Failed), mkResult(Status.Errored))
    val exc = ReportGenerator.generate(results, ctx).exceptionReport
    assert(exc.size === 2); assert(!exc.exists(_.status == Status.Passed))
  }

  test("app_code is carried through results") {
    val r = ReportGenerator.generate(Seq(mkResult(Status.Failed, Some("CRM"))), ctx)
    assert(r.results.head.appCode === Some("CRM"))
  }

  test("ResultWriter: single failure does not stop remaining writes") {
    var count = 0
    val fakeStore = new com.dqengine.store.DqStore(MariaDbConfig("localhost",3306,"db","u","p")) {
      override def writeResult(r: DqResult): Unit = {
        count += 1; if (count == 2) throw new ResultStoreUnavailable("simulated")
      }
    }
    val errors = new ResultWriter(fakeStore).writeAll(
      Seq(mkResult(Status.Passed), mkResult(Status.Failed), mkResult(Status.Passed)))
    assert(count === 3); assert(errors.size === 1)
  }
}
