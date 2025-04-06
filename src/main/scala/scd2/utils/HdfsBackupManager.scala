package scd2.utils

import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.IOException

trait BackupManager {
  def createBackup(backupPath: Path, targetPath: Path): Unit

  def restore(
               backupPath: Path,
               targetPath: Path,
               tempPath: Path
             ): Unit
}

class HdfsBackupManager(fs: FileSystem) extends BackupManager {

  override def createBackup(backupPath: Path, targetPath: Path): Unit = {
    if(fs.exists(backupPath) && fs.exists(targetPath) && !fs.rename(backupPath, targetPath)) {
      throw new IOException(s"Error creating backup $backupPath from $targetPath.")
    }
  }

  override def restore(backupPath: Path, targetPath: Path, tempPath: Path): Unit = {
    if (fs.exists(backupPath)) {
      // Удаление поврежденных данных
      if (fs.exists(targetPath) && !fs.delete(targetPath, true)) {
        throw new IOException(s"Error deleting $targetPath")
      }
      // Восстановление бэкапа
      if(!fs.rename(backupPath, targetPath)) {
        throw new IOException(s"Critical error. Error restore from backup path $backupPath to $targetPath.")
      }
    }

    // Очистка временных данных
    if (fs.exists(tempPath) && !fs.delete(tempPath, true)) {
      throw new RuntimeException(s"Error deleting $tempPath")
    }
  }
}
