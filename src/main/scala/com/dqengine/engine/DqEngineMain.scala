package com.dqengine.engine

import com.dqengine.config.ConfigLoader
import com.dqengine.domain._
import com.dqengine.source.SourceResolver
import com.dqengine.store.DqStore
import org.apache.spark.sql.SparkSession

object DqEngineMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DQ Engine").getOrCreate()
    implicit val s: SparkSession = spark

    val selectedCheckTypes = args.headOption.getOrElse("completeness,validity,consistency")
      .split(",").map(_.trim).filter(_.nonEmpty).toSeq

    val partitionParam: PartitionParameter = args.lift(1).map(_.trim) match {
      case None | Some("latest") | Some("") => PartitionParameter.Latest
      case Some(v) => PartitionParameter.Specific(v)
    }

    val configVersionParam: Option[String] = args.lift(2).map(_.trim).filter(_.nonEmpty)
    val appCodeFilter:      Option[String] = args.lift(3).map(_.trim).filter(_.nonEmpty)

    // Build engine config from environment variables
    val storeConfig = MariaDbConfig(
      host     = sys.env.getOrElse("DQ_STORE_HOST", "localhost"),
      port     = sys.env.getOrElse("DQ_STORE_PORT", "3306").toInt,
      database = sys.env.getOrElse("DQ_STORE_DB", "dq_store"),
      user     = sys.env.getOrElse("DQ_STORE_USER", "dq_engine"),
      password = sys.env.getOrElse("DQ_STORE_PASSWORD", "dq_engine_pw")
    )

    // Discover MariaDB data sources from DQ_DATASOURCE_<KEY>_HOST env vars
    val mariaDbSources: Map[String, MariaDbConfig] = {
      val prefix = "DQ_DATASOURCE_"
      sys.env.keys.filter(k => k.startsWith(prefix) && k.endsWith("_HOST")).flatMap { hostKey =>
        val p = hostKey.stripSuffix("_HOST")
        val connRef = p.stripPrefix(prefix).toLowerCase.replace("_", "-")
        for {
          host     <- sys.env.get(s"${p}_HOST")
          port     =  sys.env.getOrElse(s"${p}_PORT", "3307")
          database <- sys.env.get(s"${p}_DB")
          user     <- sys.env.get(s"${p}_USER")
          password <- sys.env.get(s"${p}_PASSWORD")
        } yield connRef -> MariaDbConfig(host, port.toInt, database, user, password)
      }.toMap
    }

    // Discover Hive sources from DQ_HIVE_<KEY>_HOST env vars
    val hiveSources: Map[String, HiveConfig] = {
      val prefix = "DQ_HIVE_"
      sys.env.keys.filter(k => k.startsWith(prefix) && k.endsWith("_HOST")).flatMap { hostKey =>
        val p = hostKey.stripSuffix("_HOST")
        val connRef = p.stripPrefix(prefix).toLowerCase.replace("_", "-")
        for {
          host     <- sys.env.get(s"${p}_HOST")
          port     =  sys.env.getOrElse(s"${p}_PORT", "10000")
          database <- sys.env.get(s"${p}_DB")
          user     =  sys.env.getOrElse(s"${p}_USER", "")
          password =  sys.env.getOrElse(s"${p}_PASSWORD", "")
        } yield connRef -> HiveConfig(host, port.toInt, database, user, password)
      }.toMap
    }

    val engineConfig = EngineConfig(storeConfig, mariaDbSources, hiveSources)
    val store        = new DqStore(storeConfig)
    val engine       = new DqEngine(store, new ConfigLoader(store), new SourceResolver(), engineConfig)

    println(s"[DQ Engine] Starting run: checkTypes=${selectedCheckTypes.mkString(",")}, " +
            s"partition=$partitionParam, configVersion=${configVersionParam.getOrElse("latest")}, " +
            s"appCode=${appCodeFilter.getOrElse("all")}")

    engine.run(selectedCheckTypes, partitionParam, configVersionParam, appCodeFilter) match {
      case TerminatedRun(error) =>
        System.err.println(s"[DQ Engine] RUN TERMINATED: $error"); System.exit(1)
      case CompletedRun(report, writeErrors) =>
        println(s"""[DQ Engine] Run complete: run_id=${report.runId}
                   |  config_version : ${report.configVersion} (${report.configVersionDate})
                   |  executed       : ${report.executed}
                   |  passed         : ${report.passed}
                   |  failed         : ${report.failed}
                   |  errored        : ${report.errored}
                   |  exception count: ${report.exceptionReport.size}""".stripMargin)
        if (writeErrors.nonEmpty)
          System.err.println(s"[DQ Engine] WARNING: ${writeErrors.size} result write error(s)")
        if (report.failed > 0 || report.errored > 0) {
          println("[DQ Engine] Exception report:")
          report.exceptionReport.foreach { r =>
            println(s"  [${r.status.wire.toUpperCase}][${r.appCode.getOrElse("-")}] ${r.tableName}.${r.columnName} (${r.checkId}): ${r.description.getOrElse("")}")
          }
        }
    }
    spark.stop()
  }
}
