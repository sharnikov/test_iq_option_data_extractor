package test.option.iq

import org.apache.spark.sql.SparkSession

object Extractor extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("extractor")
    .getOrCreate()


  sparkSession.sparkContext.textFile("hdfs://172.17.0.2:9000/data.csv").foreach(println)

}
