package test.option.iq

import java.sql.DriverManager
import java.util.Properties

import SchemaFactory.getRawSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}



object Extractor extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("extractor")
    .getOrCreate()

  import sparkSession.sqlContext.implicits._

//    val loadedData = sparkSession.read
//      .option("sep", ";")
//      .schema(getRawSchema())
//      .csv("hdfs://172.17.0.2:9000/data.csv")

    val databaseUrl = "jdbc:postgresql://localhost:5432/postgres"

    val connectionProperties = new Properties()
    connectionProperties.put("Driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "PGPASSWORD")

    val loadedData = sparkSession.read
      .option("sep", ";")
      .schema(getRawSchema())
      .csv("/home/osharnikov/IdeaProjects/DataFetcher/data.csv")

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
        "employer_id",
        "employer_name",
        "created_at",
        "url",
        "alternate_url",
        "snippet_requirement",
        "snippet_responsibility"
      ).persist()


      loadedVacancies
        .write
        .mode(SaveMode.Append)
        .jdbc(databaseUrl, "vacancies", connectionProperties)

      val openVacancies = sparkSession.read.jdbc(databaseUrl, "vacancies", connectionProperties)
        .select("id", "is_open")
        .where(col("is_open").equalTo(true)).persist()

      val openIdsList = openVacancies.select("id").collect().map(_(0)).toList
      val allLoadedIds = loadedVacancies.select("id").collect().map(_(0)).toList
      val idsToClose = openIdsList.filterNot(allLoadedIds.contains)

      if (idsToClose.nonEmpty) {
        val conn = DriverManager.getConnection(databaseUrl, "postgres", "PGPASSWORD")
        val statement = conn.createStatement()
        statement.execute(f"update vacancies set is_open = false where id in ('${idsToClose.mkString("','")}')")

        statement.close()
        conn.close()
      }

//      val loadedEmploeesData = loadedVacancies.select(
//        "adress_street",
//        "adress_building",
//        "adress_description",
//        "adress_lat",
//        "adress_lng",
//        "adress_raw",
//        "adress_id",
//        "employer_id",
//        "employer_name",
//        "employer_url",
//        "employer_vacancies_url",
//        "employer_trusted",
//        "has_open"
//      )
//
//      val loadedEmplyeIds = loadedEmploeesData.select("employer_id").collect().map(_(0)).toList
//      val current = sparkSession.read.jdbc(databaseUrl, "employers", connectionProperties)
//        .select("employer_id")
//        .where(col("has_open").equalTo(true))
//        .where()



    }
}
