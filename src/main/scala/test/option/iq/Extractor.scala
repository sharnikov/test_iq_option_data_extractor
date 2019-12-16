package test.option.iq

import java.io.File
import java.sql.{DriverManager, Statement}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import test.option.iq.ColumnsUtils._


object Extractor extends App with LazyLogging {

      val config: Settings = new AppSettings(ConfigFactory.parseFile(new File("src/main/resources/app.conf")))

      val sparkSession = SparkSession
        .builder()
        .master(config.spark().url())
        .appName(config.spark().appName())
        .getOrCreate()

      logger.info("Session created")

      val loadedData = sparkSession.read
        .option("sep", config.hdfs().delimeter())
        .format(config.hdfs().format())
        .schema(getRawSchema())
        .csv(config.hdfs().url())

      logger.info("Data loaded")

      val databaseUrl = config.db().url()
      val connectionProperties = config.db().connectionProperties()
      val broadcastConnect = sparkSession.sparkContext.broadcast(connectionProperties)

      logger.info("Broadcast connection established")
      val listOfFetchedColumns = loadVacanciesColumns()


      val loadedVacancies = loadedData
        .select(listOfFetchedColumns: _*)
        .where(col("id").isNotNull).persist()

      logger.info("Vacancies loaded")

      val openVacancies = sparkSession
        .read
        .jdbc(databaseUrl, "vacancies", connectionProperties)
        .select("*")
        .where(col("is_open").equalTo(true)).persist()

      val newVacancies = loadedVacancies
        .except(openVacancies.drop("is_open")).persist()

      newVacancies.show()

      newVacancies
        .write
        .mode(SaveMode.Append)
        .jdbc(databaseUrl, "vacancies", connectionProperties)

      val vacanciesIdsToClose = openVacancies
        .select("id")
        .where(col("adress_id").isNotNull && col("employer_id").isNotNull)
        .except(loadedVacancies.select("id")).persist()

      vacanciesIdsToClose.show()

      vacanciesIdsToClose.foreachPartition { partition: Iterator[Row] =>

        val connectionProperties = broadcastConnect.value
        val url = connectionProperties.getProperty("url")
        val user = connectionProperties.getProperty("user")
        val password = connectionProperties.getProperty("password")
        val jdbcClass = connectionProperties.getProperty("Driver")

        Class.forName(jdbcClass)

        val conn = DriverManager.getConnection(url, user, password)
        val batchSize = 1000
        val statement: Statement = conn.createStatement()

        partition.grouped(batchSize).foreach { rows =>
          val idsToClose = rows.map(_(0)).toList
          statement.addBatch(f"update vacancies set is_open = false where id in ('${idsToClose.mkString("','")}')")
        }

        statement.executeBatch()

        conn.close()
      }

      sparkSession.close()
}
