package test.option.iq.services

import test.option.iq.services.FetchTaskFactory.FetchTask

trait ScheduleRunManager {
  def runTask(task: FetchTask): Unit
}

class SimpleScheduleRunManager extends ScheduleRunManager {

  private val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor()

  override def runTask(task: FetchTask): Unit = {
    executor.scheduleAtFixedRate(
      task.task,
      task.firstDelayTime,
      task.repeateRate,
      task.timeUnit
    )
  }
}
