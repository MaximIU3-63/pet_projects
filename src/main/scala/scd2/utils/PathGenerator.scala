package scd2.utils

import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

trait PathGenerator {
  private val localDateTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  private lazy val generateId: String = s"${localDateTime}_${UUID.randomUUID().toString}"

  def generateTempPath(location: String, pattern: String): String = {
    Paths.get(location, pattern + generateId).toString
  }
}
