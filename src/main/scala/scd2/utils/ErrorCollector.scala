package scd2.utils

import scala.collection.mutable.ListBuffer

/**
 * Класс по сбору ошибок.
 * Реализует следующие методы:
 *  - addCriticalError(errorMessage: String): Unit - добавление ошибки errorMessage в буфер ошибок.
 *  - throwIfErrorsExists() - метод, выбрасывающий исключение с текстом всех ошибок из буфера.
 * */
class ErrorCollector {
  // Инициализация буфера для коллекционирования ошибок работы функционала
  private val errorBuffer: ListBuffer[String] = new ListBuffer[String]()

  private lazy val getErrorsListAsString: String = errorBuffer.mkString("\n")

  def addCriticalError(errorMessage: String): Unit = {
    errorBuffer.append(errorMessage)
  }

  @throws[Exception]("Выбрасывает ошибку с текстом всех ошибок из буфера.")
  def throwIfErrorsExists(): Unit = {
    if(errorBuffer.nonEmpty) {
      throw new Exception(s"The following errors were found:\n $getErrorsListAsString")
    }
  }
}
