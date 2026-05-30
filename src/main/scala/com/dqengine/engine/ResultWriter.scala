package com.dqengine.engine

import com.dqengine.domain._
import com.dqengine.store.DqStore
import scala.util.{Try, Success, Failure}

class ResultWriter(store: DqStore) {
  def writeAll(results: Seq[DqResult]): Seq[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()
    results.foreach { r =>
      Try(store.writeResult(r)) match {
        case Success(_) =>
        case Failure(e) => errors += s"Failed to write ${r.tableName}.${r.columnName} (${r.dqRefId}): ${e.getMessage}"
      }
    }
    errors.toList
  }
}
