package test.option.iq.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object ColumnsUtils {

  def getRawSchema() = {
    new StructType()
      .add("id", "string", nullable = false)
      .add("premium", "boolean", nullable = false)
      .add("name", "string", nullable = false)
      .add("department_id", "string")
      .add("department_name", "string")
      .add("has_test", "boolean", nullable = false)
      .add("response_letter_required", "boolean", nullable = false)
      .add("area_id", "string", nullable = false)
      .add("area_name", "string", nullable = false)
      .add("salary_from", "double")
      .add("salary_to", "double")
      .add("salary_currency", "string")
      .add("salary_gross", "boolean")
      .add("adress_street", "string")
      .add("adress_building", "string")
      .add("adress_description", "string")
      .add("adress_lat", "double")
      .add("adress_lng", "double")
      .add("adress_raw", "string")
      .add("adress_id", "string")
      .add("employer_id", "string")
      .add("employer_name", "string")
      .add("employer_url", "string")
      .add("employer_vacancies_url", "string")
      .add("employer_trusted", "boolean")
      .add("created_at", "date")
      .add("url", "string")
      .add("alternate_url", "string")
      .add("snippet_requirement", "string")
      .add("snippet_responsibility", "string")
  }

  def loadVacanciesColumns(): List[Column] = {
    List(
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
  }

  case class Vacancies(open: DataFrame, loaded: DataFrame)
}