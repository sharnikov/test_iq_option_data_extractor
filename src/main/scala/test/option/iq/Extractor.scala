package test.option.iq

import java.util.Properties

import SchemaFactory.getRawSchema
import org.apache.spark.sql.{SaveMode, SparkSession}


object Extractor extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("extractor")
    .getOrCreate()

//  sparkSession.sparkContext.textFile("hdfs://172.17.0.2:9000/data.csv").foreach(println)
//  val file = sparkSession.sparkContext.textFile("data.csv").foreach(println)

    val databaseUrl = "jdbc:postgresql://localhost:5432/postgres"

    val newData = sparkSession.read
      .option("sep", ";")
      .schema(getRawSchema())
      .csv("/home/osharnikov/IdeaProjects/DataFetcher/data.csv")

    val dataToSave = newData.select(
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

    dataToSave.show()

    val connectionProperties = new Properties()
    connectionProperties.put("Driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "PGPASSWORD")

  dataToSave
      .write
      .mode(SaveMode.Append)
      .jdbc(databaseUrl, "vacancies", connectionProperties)


//    val df = sparkSession.read.jdbc(databaseUrl, "(select * from vacancies) as vac", connectionProperties)
//    df.show()
}
