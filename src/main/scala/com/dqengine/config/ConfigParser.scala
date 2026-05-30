package com.dqengine.config

import com.dqengine.domain._
import scala.io.Source
import scala.util.Try

object ConfigParser {
  private val ExpectedFieldCount = 14
  private val Header = Seq(
    "check_id","table_name","column_name","data_type","check_type",
    "business_glossary","business_rule_filter","deviation_tolerance","check_sql",
    "evaluation_mode","app_code","_r1","_r2","_r3"  // 3 reserved future fields
  )

  def parse(csvPath: String): (Seq[CheckDefinition], Seq[ConfigError]) = {
    val lines = Try(Source.fromFile(csvPath).getLines().toList).getOrElse(Nil)
    if (lines.isEmpty) return (Nil, Nil)
    val dataLines = lines.drop(1).zipWithIndex
    if (dataLines.isEmpty) return (Nil, Nil)

    val errors  = scala.collection.mutable.ListBuffer[ConfigError]()
    val rawRows = scala.collection.mutable.ListBuffer[Map[String, Option[String]]]()

    dataLines.foreach { case (line, idx) =>
      val rowNum = idx + 2
      if (line.trim.nonEmpty) {
        val fields = parseCsvLine(line)
        if (fields.size != ExpectedFieldCount)
          errors += ConfigError(s"Row $rowNum: expected $ExpectedFieldCount fields, found ${fields.size}")
        else
          rawRows += Header.zip(fields.map(f => if (f.trim.isEmpty) None else Some(f.trim))).toMap
      }
    }
    if (rawRows.isEmpty) return (Nil, errors.toList)
    val loader = new ConfigLoader(null)
    val (defs, valErrors) = loader.validateCheckRows(rawRows.toList)
    (defs, errors.toList ++ valErrors)
  }

  private def parseCsvLine(line: String): Seq[String] = {
    val fields  = scala.collection.mutable.ListBuffer[String]()
    val current = new StringBuilder
    var inQuotes = false; var i = 0
    while (i < line.length) {
      val c = line(i)
      if (c == '"') {
        if (inQuotes && i+1 < line.length && line(i+1) == '"') { current.append('"'); i += 1 }
        else inQuotes = !inQuotes
      } else if (c == ',' && !inQuotes) { fields += current.toString; current.clear() }
      else current.append(c)
      i += 1
    }
    fields += current.toString
    fields.toList
  }
}
