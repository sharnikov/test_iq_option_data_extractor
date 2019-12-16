package test.option.iq.utils

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import test.option.iq.utils.ColumnsUtils.Vacancies

trait JobService {

  def closeVacancies(openVacancies: Dataset[Row],
                     loadedVacancies: Dataset[Row],
                     broadcastConnect: Broadcast[Properties]): Unit

   def getVacancies(loadedData: DataFrame,
                    session: SparkSession): Vacancies

   def loadData(session: SparkSession): DataFrame
}
