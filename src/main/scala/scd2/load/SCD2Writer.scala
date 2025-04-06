package scd2.load

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scd2.utils.{Active, BackupManager, ErrorCollector, HdfsBackupManager, Inactive, Location, PathGenerator, SCD2Defaults, SCD2PartitioningConfig}

import java.net.URI
import java.nio.file.Paths
import scala.util.{Failure, Success, Try}

case class SCD2WriteConfig(
                            incomingScd2DF: DataFrame,
                            targetTableName: String,
                            partitionConfig: SCD2PartitioningConfig = SCD2Defaults.DefaultPartition,
                            tempDirPattern: String = "_temp_active_",
                            backupDirPattern: String = "_backup_active_"
                          )
object SCD2Writer {
  def write(config: SCD2WriteConfig, spark: SparkSession): Unit = {

    // Формирование необходимых директорий для работы функционала
    val tableLocation = Location.getTableLocation(config.targetTableName, spark)

    // Определяем расположение партиции с активными записями в HDFS
    val targetPartition = {
      Location.
        getSpecPartitionLocation(
          config.targetTableName,
          Map(config.partitionConfig.colName -> Active.value),
          spark)
    }

    val pathGenerator = new PathGenerator {}

    val targetPath = new Path(targetPartition)
    val tempPath = new Path(pathGenerator.generateTempPath(tableLocation, config.tempDirPattern))
    val backupPath = new Path(pathGenerator.generateTempPath(tableLocation, config.backupDirPattern))

    val fs = FileSystem.get(new URI(tableLocation), spark.sparkContext.hadoopConfiguration)

    // Инициализация объекта класса сбора ошибок.
    val errorCollector = new ErrorCollector()
    // Инициализация объекта класса по работе с бэкапом
    val hdfsBackupManager = new HdfsBackupManager(fs)
    // Инициализация объекта скрытого класса SCD2Writer
    val scd2Writer = new SCD2Writer(fs, hdfsBackupManager)

    // 1. Отбор активных и неактивных записей из инкремента.
    val activeRecordsDF = scd2Writer.
      filterByPartitionValue(
        config.incomingScd2DF,
        config.partitionConfig.colName,
        Active.value)

    val nonActiveRecordsDF = scd2Writer.
      filterByPartitionValue(
        config.incomingScd2DF,
        config.partitionConfig.colName,
        Inactive.value)

    Try {
      // 2. Запись исторических данных
      scd2Writer.processHistoricalData(nonActiveRecordsDF, config.targetTableName)

      // 3. Запись актуальных данных
      scd2Writer.processActiveData(
        activeRecordsDF,
        targetPath,
        tempPath,
        backupPath
      )

    } match {
      case Success(_) =>
        if (fs.exists(tempPath) && !fs.delete(tempPath, true)) {
          throw new RuntimeException(s"Error deleting $tempPath")
        }

        if (fs.exists(backupPath) && !fs.delete(backupPath, true)) {
          throw new RuntimeException(s"Error deleting $backupPath")
        }

      case Failure(e) =>
        errorCollector.addCriticalError(e.getMessage)
        hdfsBackupManager.restore(backupPath, targetPath, tempPath)
        throw e
    }

    // Выход из приложения с ошибкой, если таковы были в процессе работы.
    errorCollector.throwIfErrorsExists()
  }
}

private[scd2] class SCD2Writer(fs: FileSystem, backupManager: BackupManager) {

  /**
   * Создание временной таблицы
   * @param incomingDF Входной дата фрейм данных
   * @param tempPath Расположение временной таблицы в HDFS
   */
  private def createTempTable(incomingDF: DataFrame, tempPath: String): Unit = {
    incomingDF.
      write.
      option("mergeSchema", "true"). // Для совместимости схем
      mode(SaveMode.Overwrite).
      parquet(tempPath)
  }

  /** Фильтрует записи по полю партиции.*/
  private def filterByPartitionValue(
                                      df: DataFrame,
                                      column: String,
                                      value: Int
                                    ): DataFrame = df.filter(col(column) === value)

  /**  Обработка архивных данных */
  private def processHistoricalData(incomingDF: DataFrame, tableName: String): Unit = {
    incomingDF.
      repartition(1).
      write.
      mode(SaveMode.Append).
      insertInto(tableName)
  }

  /** Обработка архивных данных */
  private def processActiveData(
                                 incomingDF: DataFrame,
                                 targetPath: Path,
                                 tempPath: Path,
                                 backupPath: Path
                               ): Unit = {
    // 1. Запись во временную директорию с проверкой схемы
    createTempTable(incomingDF, tempPath.toString)

    // 2. Создание бэкапа
    backupManager.createBackup(backupPath, targetPath)

    // 3. Атомарная замена партиций
    if(!fs.rename(tempPath, targetPath)) {
      throw new RuntimeException("Atomic replace failed")
    }
  }
}
