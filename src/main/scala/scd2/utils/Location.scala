package scd2.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

object Location {

  def getTableLocation(tableName: String, spark: SparkSession): String = {
    val catalog = spark.sessionState.catalog
    val tableMetadata = catalog.getTableMetadata(TableIdentifier(tableName))

    val location = tableMetadata.storage.locationUri match {
      case Some(uri) => uri.toString
      case None => throw new RuntimeException("Location not defined")
    }

    location
  }

  def getSpecPartitionLocation(tableName: String, partitionSpec: Map[String, Int], spark: SparkSession): String = {
    val partLocation = spark.sql(
      s"""
         |DESC FORMATTED $tableName
         |PARTITION (${partitionSpec.map(kv => s"${kv._1}='${kv._2}'").mkString(",")})
         |""".stripMargin)
      .filter(col("col_name") === "Location")
      .select("data_type")
      .collect()
      .headOption
      .map(_.getString(0))
      .getOrElse(throw new RuntimeException("Partition location not found"))

    partLocation
  }
}
