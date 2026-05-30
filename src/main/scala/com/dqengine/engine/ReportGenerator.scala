package com.dqengine.engine

import com.dqengine.domain._

object ReportGenerator {
  def generate(results: Seq[DqResult], ctx: RunContext): DqReport = {
    val passed       = results.count(_.status == Status.Passed)
    val failed       = results.count(_.status == Status.Failed)
    val errored      = results.count(_.status == Status.Errored)
    val inconclusive = results.count(_.status == Status.Inconclusive)
    DqReport(ctx.runId, ctx.runTimestamp, ctx.configVersion, ctx.configVersionDate,
             results, passed + failed + errored + inconclusive, passed, failed, errored, inconclusive)
  }
  def empty(ctx: RunContext): DqReport = generate(Nil, ctx)
}
