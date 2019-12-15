package test.option.iq

import java.sql.DriverManager
import java.util.Properties

import SchemaFactory.getRawSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col



object Extractor extends App {

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
        .mode(SaveMode.Append)
        .jdbc(databaseUrl, "vacancies", connectionProperties)

      val openVacancies = sparkSession.read.jdbc(databaseUrl, "vacancies", connectionProperties)
        .select("id", "is_open")
        .where(col("is_open").equalTo(true))

      val allLoadedIds = loadedData.select("id").collect().map(_(0)).toList
      val openIdsList = openVacancies.select("id").collect().map(_(0)).toList
      val idsToClose = openIdsList.filterNot(allLoadedIds.contains)

      if (idsToClose.nonEmpty) {
        val conn = DriverManager.getConnection(databaseUrl, user, password)
        val statement = conn.createStatement()
        statement.execute(f"update vacancies set is_open = false where id in ('${idsToClose.mkString("','")}')")

        statement.close()
        conn.close()
      }


    }
}
