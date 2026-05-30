package com.dqengine.exec

import com.dqengine.domain._
import com.dqengine.store.DqStore
import java.time.{Instant, Duration}
import scala.util.{Try, Success, Failure}

/** Classifies a result as new / recurring / resolved by comparing to the most
 *  recent in-window prior result for (table, column, check_type, app_code).
 *  Returns None when there is no case to track (clean pass, or lookup failed). */
object IssueClassifier {
  private val RetentionWindow = Duration.ofDays(183)

  def classify(store: DqStore, tableName: String, columnName: String,
               checkType: String, appCode: Option[String],
               runTimestamp: Instant, currentStatus: Status): Option[IssueStatus] = {
    val windowStart = runTimestamp.minus(RetentionWindow)
    Try(store.findPreviousResult(tableName, columnName, checkType, appCode, runTimestamp, windowStart)) match {
      case Failure(_) => None
      case Success(priorOpt) =>
        val currentOpen = Status.failLike.contains(currentStatus)
        val priorOpen   = priorOpt.exists(r => Status.failLike.contains(r.status))
        (currentOpen, priorOpen) match {
          case (true,  true)  => Some(IssueStatus.Recurring)
          case (true,  false) => Some(IssueStatus.New)
          case (false, true)  => Some(IssueStatus.Resolved)
          case (false, false) => scala.None
        }
    }
  }
}
