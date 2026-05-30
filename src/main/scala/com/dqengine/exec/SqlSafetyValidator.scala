package com.dqengine.exec

import com.dqengine.domain.SqlTarget
import org.apache.spark.sql.SparkSession
import scala.util.Try

object SqlSafetyValidator {

  def validate(sql: String, target: SqlTarget, spark: Option[SparkSession] = None): Either[String, String] = {
    val trimmed = sql.trim
    if (trimmed.isEmpty) return Left("check_sql is empty")
    target match {
      case _: SqlTarget.SparkTempView   => validateSpark(trimmed, spark)
      case _: SqlTarget.JdbcConnection  => validateJdbc(trimmed)
    }
  }

  private def validateSpark(sql: String, sparkOpt: Option[SparkSession]): Either[String, String] =
    sparkOpt match {
      case Some(spark) =>
        Try {
          val plan = spark.sessionState.sqlParser.parsePlan(sql)
          val cls  = plan.getClass.getName
          if (cls.contains("Command") || cls.contains("CreateTable") ||
              cls.contains("Insert")  || cls.contains("DropTable"))
            throw new IllegalArgumentException("check_sql is a DDL/DML command, not a read-only query")
        }.toEither.left.map(e => s"check_sql rejected: ${e.getMessage}").flatMap(_ => Right(sql))
      case None => validateJdbc(sql)
    }

  private def validateJdbc(sql: String): Either[String, String] = {
    val stripped = sql.replaceAll("--[^\n]*", " ").replaceAll("/\\*[\\s\\S]*?\\*/", " ").trim
    if (stripped.isEmpty) return Left("check_sql is empty after stripping comments")
    val noStrings = stripped.replaceAll("'[^']*'", "''").replaceAll("\"[^\"]*\"", "\"\"")
    if (noStrings.contains(";")) return Left("check_sql contains multiple statements (semicolon detected)")
    val firstWord = stripped.toUpperCase.split("\\s+", 2).headOption.getOrElse("")
    if (firstWord != "SELECT" && firstWord != "WITH")
      Left(s"check_sql must be a read-only SELECT or CTE (WITH), got: $firstWord")
    else Right(sql)
  }
}
