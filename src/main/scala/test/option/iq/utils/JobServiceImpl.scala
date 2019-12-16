package test.option.iq.utils

import java.sql.{DriverManager, Statement}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import test.option.iq.utils.ColumnsUtils.{Vacancies, getRawSchema, loadVacanciesColumns}

class JobServiceImpl(settings: Settings) extends JobService with LazyLogging {


  override def loadData(session: SparkSession): DataFrame = {
    session.read
      .option("sep", settings.hdfs().delimeter())
      .format(settings.hdfs().format())
      .schema(getRawSchema())
      .csv(settings.hdfs().url())
  }

  override def closeVacancies(openVacancies: DataFrame,
                              loadedVacancies: DataFrame,
                              broadcastConnect: Broadcast[Properties]): Unit = {

    val vacanciesIdsToClose = openVacancies
      .select("id")
      .where(col("adress_id").isNotNull && col("employer_id").isNotNull)
      .except(loadedVacancies.select("id")).persist()

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
  }

  override def getVacancies(loadedData: DataFrame,
                            session: SparkSession): Vacancies = {

    val listOfFetchedColumns = loadVacanciesColumns()

    val loadedVacancies = loadedData
      .select(listOfFetchedColumns: _*)
      .where(col("id").isNotNull).persist()

    logger.info("Vacancies loaded")

    val databaseUrl = settings.db().url()
    val connectionProperties = settings.db().connectionProperties()

    val openVacancies = session
      .read
      .jdbc(databaseUrl, "vacancies", connectionProperties)
      .select("*")
      .where(col("is_open").equalTo(true)).persist()

    val newVacancies = loadedVacancies
      .except(openVacancies.drop("is_open"))

    newVacancies
      .write
      .mode(SaveMode.Append)
      .jdbc(databaseUrl, "vacancies", connectionProperties)

    Vacancies(open = openVacancies, loaded = loadedVacancies)
  }
}
