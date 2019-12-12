package test.option.iq

import java.util.Properties
import SchemaFactory.getVacanciesSchema
import org.apache.spark.sql.SparkSession


object Extractor extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("extractor")
    .getOrCreate()

//  sparkSession.sparkContext.textFile("hdfs://172.17.0.2:9000/data.csv").foreach(println)
//  val file = sparkSession.sparkContext.textFile("data.csv").foreach(println)

    val url = "jdbc:postgresql://localhost:5432/postgres"

    val df = sparkSession.read
      .option("sep", ";")
      .schema(getVacanciesSchema())
      .csv("/home/osharnikov/IdeaProjects/DataFetcher/data.csv")
    df.show()

    val connectionProperties = new Properties()
    connectionProperties.put("Driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "PGPASSWORD")



//    val df: DataFrame = sparkSession.read.jdbc(url, "(select * from vacancies) as vac", connectionProperties)
//    df.show()
}
