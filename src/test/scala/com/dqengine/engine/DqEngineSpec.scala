package com.dqengine.engine

import com.dqengine.config.ConfigLoader
import com.dqengine.domain._
import com.dqengine.source.SourceResolver
import com.dqengine.store.DqStore
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.time.Instant

class DqEngineSpec extends AnyFunSuite with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("DqEngineSpec")
      .master("local[1]").config("spark.sql.shuffle.partitions","1")
      .config("spark.ui.enabled","false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val storeConfig  = MariaDbConfig("localhost",3306,"dq_store","dq_engine","dq_engine_pw")
  private val engineConfig = EngineConfig(storeConfig)
  private val now          = Instant.now()
  private val versionInfo  = ConfigVersionInfo("v1", now)

  private def failingStore = new DqStore(storeConfig) {
    override def resolveVersion(p: Option[String]) = throw new MariaDbStoreException("store failure")
  }

  private def emptyStore = new DqStore(storeConfig) {
    override def resolveVersion(p: Option[String]) = versionInfo
    override def loadCheckDefinitions(v: String) = Nil
    override def loadSourceCatalog() = Nil
    override def findPreviousResult(a: String,b: String,c: String,d: Option[String],e: Instant,f: Instant) = None
    override def findMomResult(a: String,b: String,c: String,d: Option[String],e: Instant,f: Instant) = None
    override def writeResult(r: DqResult) = ()
  }

  test("MariaDB_Store failure -> TerminatedRun") {
    val engine = new DqEngine(failingStore, new ConfigLoader(failingStore), new SourceResolver(), engineConfig)
    assert(engine.run(Seq("completeness"), PartitionParameter.Latest, None).isInstanceOf[TerminatedRun])
  }

  test("empty Selected_Check_Types -> TerminatedRun") {
    val engine = new DqEngine(emptyStore, new ConfigLoader(emptyStore), new SourceResolver(), engineConfig)
    val r = engine.run(Nil, PartitionParameter.Latest, None)
    assert(r.isInstanceOf[TerminatedRun])
    assert(r.asInstanceOf[TerminatedRun].error.toLowerCase.contains("empty"))
  }

  test("unknown check_type -> TerminatedRun") {
    val engine = new DqEngine(emptyStore, new ConfigLoader(emptyStore), new SourceResolver(), engineConfig)
    val r = engine.run(Seq("accuracy"), PartitionParameter.Latest, None)
    assert(r.isInstanceOf[TerminatedRun])
    assert(r.asInstanceOf[TerminatedRun].error.contains("accuracy"))
  }

  test("empty config_store -> CompletedRun with zero executed") {
    val engine = new DqEngine(emptyStore, new ConfigLoader(emptyStore), new SourceResolver(), engineConfig)
    engine.run(Seq("completeness"), PartitionParameter.Latest, None) match {
      case CompletedRun(report, _) => assert(report.executed === 0)
      case t: TerminatedRun => fail(s"Unexpected TerminatedRun: ${t.error}")
    }
  }

  test("appCodeFilter excludes non-matching checks") {
    val storeWithCrmCheck = new DqStore(storeConfig) {
      override def resolveVersion(p: Option[String]) = versionInfo
      override def loadCheckDefinitions(v: String) = Seq(Map(
        "check_id" -> Some("c1"), "table_name" -> Some("tbl"), "column_name" -> Some("col"),
        "check_type" -> Some("completeness"), "check_sql" -> Some("SELECT 1 WHERE 1=0"),
        "evaluation_mode" -> Some("violation"), "app_code" -> Some("CRM"),
        "data_type" -> None, "business_glossary" -> None, "business_rule_filter" -> None, "deviation_tolerance" -> None
      ))
      override def loadSourceCatalog() = Nil
      override def findPreviousResult(a: String,b: String,c: String,d: Option[String],e: Instant,f: Instant) = None
      override def findMomResult(a: String,b: String,c: String,d: Option[String],e: Instant,f: Instant) = None
      override def writeResult(r: DqResult) = ()
    }
    val engine = new DqEngine(storeWithCrmCheck, new ConfigLoader(storeWithCrmCheck), new SourceResolver(), engineConfig)
    // Filter to FINANCE -- no CRM checks should run
    engine.run(Seq("completeness"), PartitionParameter.Latest, None, Some("FINANCE")) match {
      case CompletedRun(report, _) => assert(report.executed === 0, "CRM check should be excluded")
      case t: TerminatedRun => fail(s"Unexpected: ${t.error}")
    }
  }
}
