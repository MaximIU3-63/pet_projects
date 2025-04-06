package scd2.utils

import org.apache.spark.sql.SparkSession

import scala.util.Try

private[scd2] object CacheManager {
  // Функция очистики кэша
  def clearCache(spark: SparkSession): Try[Unit] = Try {
    spark.sharedState.cacheManager.clearCache()
  }
}
