package test.option.iq

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import test.option.iq.utils.{AppSettings, JobServiceImpl, Settings}


object Extractor extends App with LazyLogging {

      val settings: Settings = new AppSettings(ConfigFactory.load("app.conf").resolve())

      val session = SparkSession
        .builder()
        .master(settings.spark().url())
        .appName(settings.spark().appName())
        .getOrCreate()

      logger.info("Session created")

      val jobService = new JobServiceImpl(settings)
      val connectionProperties = settings.db().connectionProperties()
      val broadcastConnect = session.sparkContext.broadcast(connectionProperties)

      try {
            val loadedData = jobService.loadData(session)
            logger.info("Data loaded")
            val vacancies = jobService.getVacancies(loadedData, session)
            jobService.closeVacancies(vacancies.open, vacancies.loaded, broadcastConnect)
      } finally {
            session.close()
      }

}
