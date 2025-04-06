package scd2

import org.apache.spark.sql.functions.{coalesce, col, concat, current_date, current_timestamp, lit, sha1}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scd2.load.{SCD2WriteConfig, SCD2Writer}
import scd2.utils.{CacheManager, Constants, DateFormat, Dates, Filters, ISO_8601, ISO_8601_EXTENDED, Messages, SCD2PartitioningConfig}

import scala.util.{Failure, Success}

case class TechnicalColumns(sysdateDt: String = "sysdate_dt", sysdateDttm: String = "sysdate_dttm") {
  // Формируем список технических колонок
  val getParamsAsSeq: List[String] = this.productIterator.map(_.toString).toList
}

private case class SCD2Config(
                               primaryKeyColumns: Seq[String],
                               sensitiveKeysColumns: Seq[String],
                               effectiveDateFrom: String = "effective_from_dt",
                               effectiveDateTo: String = "effective_to_dt",
                               isActiveCol: String = "is_active",
                               technicalColumn: TechnicalColumns
                             ) {
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("primaryKeyColumns"))
  require(primaryKeyColumns.nonEmpty, Messages.requireMessage("sensitiveKeysColumns"))
  require(primaryKeyColumns.intersect(sensitiveKeysColumns).isEmpty, "Primary and sensitive columns must not overlap")
}

private class SCD2Processor(config: SCD2Config) {

  /** Основной метод обработки SCD2 */
  def process(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    // 1. Отбор строк, которые не требуется обрабатывать
    val existingNoActiveDF = filterInactive(existingDF)

    // 2. Отбор активных строк из существующей таблицы
    val existingActiveDF = filterActive(existingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 3. Формирование дата фрейма строк, где есть изменения
    val changesDF = detectChanges(existingActiveDF, incomingDF).
      persist(StorageLevel.MEMORY_AND_DISK)

    // 4. Формирование дата фрейма с данными, для которых нет изменений
    val unchangedActiveRecordsDF: DataFrame = getUnchangedActiveRecords(existingActiveDF, changesDF)

    val existingSchema = existingDF.schema

    // 5. Формирование дата фрейма с данными, которые переходят в статус "неактуальные"
    val updateExistingDF = castColumnsToTargetSchema(existingSchema, expireOldRecords(existingActiveDF, changesDF))

    // 6. Формирование дата фрейма с обновленными и новыми данными
    val upsertRecordsDF = castColumnsToTargetSchema(existingSchema, prepareUpsertRecords(changesDF))

    // 7. Объединение все данных
    val scd2DF = combineDataFrames(existingNoActiveDF, unchangedActiveRecordsDF, updateExistingDF, upsertRecordsDF)

    // 8. Возврат результирующего дата фрейма
    scd2DF
  }

  /** Фильтрация неактивных записей */
  private def filterInactive(existingDF: DataFrame): DataFrame =
    existingDF.filter(Filters.isNonActualRecord(config.isActiveCol))

  /** Фильтрация активных записей */
  private def filterActive(existingDF: DataFrame): DataFrame =
    existingDF.filter(Filters.isActualRecord(config.isActiveCol))

  /** Детектирование изменений с использованием хеширования */
  private def detectChanges(existingDF: DataFrame, incomingDF: DataFrame): DataFrame = {

    val hashColumns = (config.primaryKeyColumns ++ config.sensitiveKeysColumns).map(col)

    val existingWithHashDF = existingDF.
      select(config.primaryKeyColumns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    val incomingWithHashDF = incomingDF.
      select(incomingDF.columns.map(col) :+ sha1(concat(hashColumns: _*)).as(Constants.HASH_COLUMN): _*)

    existingWithHashDF.alias("existing").
      join(incomingWithHashDF.as("incoming"), config.primaryKeyColumns, "full_outer").
      filter(coalesce(
        existingWithHashDF(Constants.HASH_COLUMN) =!= incomingWithHashDF(Constants.HASH_COLUMN) &&
          incomingWithHashDF(Constants.HASH_COLUMN).isNotNull,
        existingWithHashDF(Constants.HASH_COLUMN).isNotNull || incomingWithHashDF(Constants.HASH_COLUMN).isNotNull
      )).
      select(incomingDF.columns.map(col): _*)
  }

  /** Нахождение активных записей без изменений */
  private def getUnchangedActiveRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {
    existingActiveDF.join(
      changesDF.select(config.primaryKeyColumns.map(col): _*),
      config.primaryKeyColumns,
      "left_anti"
    )
  }

  /** Закрытие устаревших записей */
  private def expireOldRecords(existingActiveDF: DataFrame, changesDF: DataFrame): DataFrame = {

    val joinCondition = config.primaryKeyColumns.
      map(colName => existingActiveDF(colName) <=> changesDF(colName)).
      reduce(_ && _)

    existingActiveDF.join(changesDF, joinCondition, "left_semi").
      withColumn(config.effectiveDateTo, DateFormat.formatDateColumn(current_date(), ISO_8601)).
      withColumn(config.isActiveCol, lit("false"))
  }

  /** Формирование дата фрейма с новыми и обновленными данными */
  private def prepareUpsertRecords(changesDF: DataFrame): DataFrame = {
    changesDF
      .withColumn(config.effectiveDateFrom, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(config.effectiveDateTo, lit(Dates.closeDateValue))
      .withColumn(config.isActiveCol, lit("true"))
      .withColumn(config.technicalColumn.sysdateDt, lit(DateFormat.formatDateColumn(current_date(), ISO_8601)))
      .withColumn(config.technicalColumn.sysdateDttm, lit(DateFormat.formatDateColumn(current_timestamp(), ISO_8601_EXTENDED)))
  }

  /** Объединение дата фреймов */
  private def combineDataFrames(dfs: DataFrame*): DataFrame =
    dfs.reduceLeft(_ unionByName _)
      .orderBy(config.primaryKeyColumns.map(col): _*)

  private def castColumnsToTargetSchema(existingSrcSchema: StructType, incomingDF: DataFrame): DataFrame = {
    val existingSchema = existingSrcSchema.map {
      f => (f.name, f.dataType)
    }.toMap

    val incomingCastedDF = incomingDF.columns.map {
      colName => existingSchema.get(colName) match {
        case Some(colType) => incomingDF(colName).cast(colType).as(colName)
        case None => incomingDF(colName)
      }
    }

    incomingDF.
      select(incomingCastedDF: _*).
      select(existingSrcSchema.map(c => col(c.name)): _*)
  }
}

object SCD2Processor extends App {

  val spark = SparkSession.builder()
    .appName("Test DataFrames")
    .master("local[*]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("WARN")

  private val historicalDF = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .csv("src/main/resources/scd2/config/historical_data.csv")

  private val incrementalDF = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .csv("src/main/resources/scd2/config/incremental_data.csv")

  private val scd2Config = SCD2Config(
    primaryKeyColumns = Seq("user_id"),
    sensitiveKeysColumns = Seq("email", "address"),
    technicalColumn = TechnicalColumns()
  )

  private val processor = new SCD2Processor(scd2Config)

  private val scd2DF = processor.process(historicalDF, incrementalDF)

  private val scd2WriteConfig = SCD2WriteConfig(
    scd2DF,
    "test",
    SCD2PartitioningConfig("active_flg", Seq(0, 1))
  )

  SCD2Writer.write(scd2WriteConfig, spark)

  //Очистка кэша, если использовался во время активной сессии spark
  CacheManager.clearCache(spark) match {
    case Success(_) => println("Cache was cleared successfully.")
    case Failure(e) => println(s"Cache was cleared unsuccessfully: ${e.getMessage}")
  }
}