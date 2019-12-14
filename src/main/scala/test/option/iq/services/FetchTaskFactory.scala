package test.option.iq.services

import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import test.option.iq.SchemaFactory.getRawSchema
import test.option.iq.services.FetchTaskFactory.FetchTask

trait FetchTaskFactory {
    def getFetchTask(): FetchTask
}

class VacanciesFetchTaskFactory extends FetchTaskFactory {


  override def getFetchTask(): FetchTask = {

    val task = new Runnable {
      override def run(): Unit = {

        val sparkSession = SparkSession
          .builder()
          .master("local")
          .appName("extractor")
          .getOrCreate()

        val databaseUrl = "jdbc:postgresql://localhost:5432/postgres"

        val connectionProperties = new Properties()
        connectionProperties.put("Driver", "org.postgresql.Driver")
        connectionProperties.put("user", "postgres")
        connectionProperties.put("password", "PGPASSWORD")

        val loadedData = sparkSession.read
          .option("sep", ";")
          .schema(getRawSchema())
          .csv("/home/osharnikov/IdeaProjects/DataFetcher/data.csv")
          .persist()

        if (loadedData.count() > 0) {
          val loadedVacancies = loadedData.select(
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
          )

          loadedVacancies
            .write
            .mode(SaveMode.Ignore)
            .jdbc(databaseUrl, "vacancies", connectionProperties)

          val openVacancies = sparkSession.read.jdbc(databaseUrl, "vacancies", connectionProperties)
            .select("id", "is_open")
            .where(col("is_open").equalTo(true))

          val allLoadedIds = loadedData.select("id").collect().map(_(0)).toList
          val openIdsList = openVacancies.select("id").collect().map(_(0)).toList
          val idsToClose = openIdsList.filterNot(allLoadedIds.contains)

          if (idsToClose.nonEmpty) {
            val conn = DriverManager.getConnection(databaseUrl, "postgres", "PGPASSWORD")
            val statement = conn.createStatement()
            statement.execute(f"update vacancies set is_open = false where id in ('${idsToClose.mkString("','")}')")

            statement.close()
            conn.close()
          }

          //      val loadedEmploeesData = loadedData.select(
          //        "adress_id",
          //        "employer_id",
          //        "adress_street",
          //        "adress_building",
          //        "adress_description",
          //        "adress_lat",
          //        "adress_lng",
          //        "adress_raw",
          //        "employer_name",
          //        "employer_url",
          //        "employer_vacancies_url",
          //        "employer_trusted"
          //      ).where(col("adress_id").isNotNull && col("employer_id").isNotNull)
          //        .dropDuplicates(Seq("adress_id", "employer_id"))
          //
          //      loadedEmploeesData
          //        .write
          //        .mode(SaveMode.Ignore)
          //        .jdbc(databaseUrl, "employers", connectionProperties)
          //
          //      val openEmployes = sparkSession.read.jdbc(databaseUrl, "employers", connectionProperties)
          //        .select("adress_id", "employer_id")
          //        .where(col("has_open").equalTo(true))
          //
          //      val allEmployesIds = loadedData.select("adress_id", "employer_id").collect().map(c => (c(0), c(1))).toList
          //
          //      val openemployesIdsList = openEmployes.select("adress_id", "employer_id").collect().map(c => (c(0), c(1))).toList
          //      val employesToClose = openemployesIdsList.filterNot(allEmployesIds.contains)
          //
          //      if (employesToClose.nonEmpty) {
          //        val conn = DriverManager.getConnection(databaseUrl, "postgres", "PGPASSWORD")
          //        val statement = conn.createStatement()
          //        statement.execute(f"update employers set has_open = false where id in ('${idsToClose.mkString("','")}')")
          //
          //        statement.close()
          //        conn.close()
          //      }

        }
      }
    }

    FetchTask(
      task = task,
      firstDelayTime = 0,
      repeateRate = 1,
      TimeUnit.MINUTES
    )

  }
}

object FetchTaskFactory {

  case class FetchTask(task: Runnable,
                       firstDelayTime: Long,
                       repeateRate: Long,
                       timeUnit: TimeUnit)
}