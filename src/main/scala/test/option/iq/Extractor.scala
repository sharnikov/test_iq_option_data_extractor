package test.option.iq

import java.sql.{DriverManager, Statement}
import java.util.Properties
import java.util.concurrent.TimeUnit

import SchemaFactory.getRawSchema
import org.apache.parquet.format.LogicalTypes.TimeUnits
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col


object Extractor extends App {

  private val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor()

  val task = new Runnable {
    override def run(): Unit = {
      val sparkSession = SparkSession
        .builder()
        .master("local")
        .appName("extractor")
        .getOrCreate()

      val user = "some_user"
      val password = "some_pass"


      //    val loadedData = sparkSession.read
      //      .option("sep", ";")
      //      .schema(getRawSchema())
      //      .csv("hdfs://172.17.0.2:9000/data.csv")

      val databaseUrl = "jdbc:postgresql://localhost:5432/vacancies_db"

      val connectionProperties = new Properties()
      connectionProperties.put("Driver", "org.postgresql.Driver")
      connectionProperties.put("user", user)
      connectionProperties.put("password", password)
      connectionProperties.put("url", databaseUrl)

      val broadcastConnect = sparkSession.sparkContext.broadcast(connectionProperties)

      val loadedData = sparkSession.read
        .option("sep", ";")
        .schema(getRawSchema())
        .csv("/home/osharnikov/IdeaProjects/DataFetcher/data1.csv")
        .persist()


      val listOfFetchedColumns = List(
        "id",
        "premium",
        "name",
        "department_id",
        "department_name",
        "has_test",
        "response_letter_required",
        "area_id",
        "area_name",
        "salary_from",
        "salary_to",
        "salary_currency",
        "salary_gross",
        "adress_id",
        "employer_id",
        "employer_name",
        "created_at",
        "url",
        "alternate_url",
        "snippet_requirement",
        "snippet_responsibility"
      ).map(col)


      val loadedVacancies = loadedData
        .select(listOfFetchedColumns: _*)
        .where(col("id").isNotNull).persist()

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

      val vacanciesIdsToClose = openVacancies.select("id").except(loadedVacancies.select("id")).persist()

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
  }

  executor.scheduleAtFixedRate(task, 0, 30, TimeUnit.SECONDS)
}
