package test.option.iq.services

import test.option.iq.services.ExtractTaskFactory.ExtractTask

trait ScheduleRunManager {
  def runTask(task: ExtractTask): Unit
}

class SimpleScheduleRunManager extends ScheduleRunManager {

  private val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor()

  override def runTask(task: ExtractTask): Unit = {
    executor.scheduleAtFixedRate(
      task.task,
      task.firstDelayTime,
      task.repeateRate,
      task.timeUnit
    )
  }
}
