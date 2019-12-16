package test.option.iq

import java.io.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession


object Extractor extends App with LazyLogging {

      val settings: Settings = new AppSettings(ConfigFactory.parseFile(new File("src/main/resources/app.conf")))

      val session = SparkSession
        .builder()
        .master(settings.spark().url())
        .appName(settings.spark().appName())
        .getOrCreate()
      logger.info("Session created")

      val jobService = new JobServiceImpl(settings)
      val databaseUrl = settings.db().url()
      val connectionProperties = settings.db().connectionProperties()
      val broadcastConnect = session.sparkContext.broadcast(connectionProperties)

      val loadedData = jobService.loadData(session)
      logger.info("Data loaded")
      val vacancies = jobService.getVacancies(loadedData, session)
      jobService.closeVacancies(vacancies.open, vacancies.loaded, broadcastConnect)

      session.close()
}
