package com.dqengine.config

import com.dqengine.domain._
import org.scalatest.funsuite.AnyFunSuite

class ConfigLoaderSpec extends AnyFunSuite {

  private def loader = new ConfigLoader(null)

  private def row(extra: (String, Option[String])*): Map[String, Option[String]] = {
    val base: Map[String, Option[String]] = Map(
      "check_id"            -> Some("chk1"),
      "table_name"          -> Some("tbl"),
      "column_name"         -> Some("col"),
      "check_type"          -> Some("completeness"),
      "check_sql"           -> Some("SELECT customer_id FROM tbl WHERE col IS NULL"),
      "evaluation_mode"     -> Some("violation"),
      "data_type"           -> None, "business_glossary" -> None,
      "business_rule_filter"-> None, "deviation_tolerance" -> None, "app_code" -> None
    )
    base ++ extra.toMap
  }

  test("valid violation completeness check is accepted") {
    val (defs, errors) = loader.validateCheckRows(Seq(row()))
    assert(errors.isEmpty, errors)
    assert(defs.size === 1)
    assert(defs.head.evaluationMode === EvaluationMode.Violation)
    assert(defs.head.deviationTolerance === BigDecimal(0))
    assert(defs.head.appCode === None)
  }

  test("app_code is carried through when present") {
    val (defs, errors) = loader.validateCheckRows(Seq(row("app_code" -> Some("CRM"))))
    assert(errors.isEmpty, errors)
    assert(defs.head.appCode === Some("CRM"))
  }

  test("missing check_id is rejected") {
    val (_, errors) = loader.validateCheckRows(Seq(row("check_id" -> None)))
    assert(errors.exists(_.message.contains("check_id")))
  }

  test("invalid check_type is rejected") {
    val (defs, errors) = loader.validateCheckRows(Seq(row("check_type" -> Some("accuracy"))))
    assert(defs.isEmpty)
    assert(errors.exists(_.message.contains("check_type")))
  }

  test("invalid evaluation_mode is rejected") {
    val (defs, errors) = loader.validateCheckRows(Seq(row("evaluation_mode" -> Some("metric"))))
    assert(defs.isEmpty)
    assert(errors.exists(_.message.contains("evaluation_mode")))
  }

  test("consistency_delta and consistency_mom are valid evaluation_mode values") {
    val (defs1, e1) = loader.validateCheckRows(Seq(row("evaluation_mode" -> Some("consistency_delta"), "check_type" -> Some("consistency"))))
    assert(e1.isEmpty, e1); assert(defs1.head.evaluationMode === EvaluationMode.ConsistencyDelta)
    val (defs2, e2) = loader.validateCheckRows(Seq(row("evaluation_mode" -> Some("consistency_mom"), "check_type" -> Some("consistency"))))
    assert(e2.isEmpty, e2); assert(defs2.head.evaluationMode === EvaluationMode.ConsistencyMom)
  }

  test("duplicate check_id is rejected") {
    val r1 = row("check_id" -> Some("dup"))
    val r2 = row("check_id" -> Some("dup"), "table_name" -> Some("other"))
    val (defs, errors) = loader.validateCheckRows(Seq(r1, r2))
    assert(defs.isEmpty); assert(errors.exists(_.message.contains("dup")))
  }

  test("empty rows produce empty valid set with no error") {
    val (defs, errors) = loader.validateCheckRows(Nil)
    assert(defs.isEmpty); assert(errors.isEmpty)
  }

  test("hive_table source type is accepted in catalog validation") {
    val hiveRow: Map[String, Option[String]] = Map(
      "table_name" -> Some("txns"), "source_type" -> Some("hive_table"),
      "parquet_location" -> None, "partition_column" -> None,
      "connection_ref" -> Some("main-hive"), "physical_table_name" -> Some("dq_demo.transactions"),
      "filter_column" -> None
    )
    val (entries, errors) = loader.loadCatalog(Seq(hiveRow))
    assert(errors.isEmpty, errors)
    assert(entries.size === 1)
    assert(entries.head.sourceType === SourceType.HiveTable)
  }

  test("ConfigParser: header excluded, 14-field row accepted") {
    import java.nio.file.Files
    val tmp = Files.createTempFile("dq_checks_", ".csv")
    val content =
      "check_id,table_name,column_name,data_type,check_type,business_glossary,business_rule_filter,deviation_tolerance,check_sql,evaluation_mode,app_code,r1,r2,r3\n" +
      "chk1,tbl,col,,completeness,,,, SELECT col FROM tbl WHERE col IS NULL,violation,CRM,,,\n"
    Files.write(tmp, content.getBytes("UTF-8"))
    val (defs, errors) = ConfigParser.parse(tmp.toString)
    assert(errors.isEmpty, errors)
    assert(defs.size === 1)
    assert(defs.head.appCode === Some("CRM"))
    Files.delete(tmp)
  }

  test("ConfigParser: wrong field count is rejected") {
    import java.nio.file.Files
    val tmp = Files.createTempFile("dq_checks_", ".csv")
    Files.write(tmp, "h1,h2,h3\nonly,two\n".getBytes("UTF-8"))
    val (defs, errors) = ConfigParser.parse(tmp.toString)
    assert(defs.isEmpty); assert(errors.exists(_.message.contains("Row")))
    Files.delete(tmp)
  }
}
