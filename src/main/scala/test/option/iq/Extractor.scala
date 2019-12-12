package test.option.iq

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

  import sparkSession.sqlContext.implicits._

//  sparkSession.sparkContext.textFile("hdfs://172.17.0.2:9000/data.csv").foreach(println)
//  val file = sparkSession.sparkContext.textFile("data.csv").foreach(println)

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
      )

//      loadedVacancies.show()

      val openIds = sparkSession.read.jdbc(databaseUrl, "vacancies", connectionProperties)
        .select("id")
        .where(col("is_open").equalTo(true))

      val openIdsList = openIds.collect().map(_(0)).toList

      val newCacancies = loadedVacancies.where(!col("id").isin(openIdsList:_*))
      newCacancies.write
        .mode(SaveMode.Append)
        .jdbc(databaseUrl, "vacancies", connectionProperties)

      newCacancies.show()
//      val deleteVacanciesIds = openIds.where()
    }






//    vacanciesToSave
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(databaseUrl, "vacancies", connectionProperties)


//    val df = sparkSession.read.jdbc(databaseUrl, "(select * from vacancies) as vac", connectionProperties)
//    df.show()
}
