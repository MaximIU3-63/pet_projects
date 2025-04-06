package scd2.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

// Объект с описаниями правил отбора актуальных и неактуальных записей
private[scd2] object Filters {
  lazy val isActualRecord: String => Column = (colName: String) => col(colName) === lit(1)
  lazy val isNonActualRecord: String => Column = (colName: String) => col(colName) === lit(0)
}
