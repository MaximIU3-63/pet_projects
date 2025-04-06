package scd2.utils

private[scd2] object Messages {
  def requireMessage(value: String): String = {
    s"""
       | Val $value shouldn't be empty.
       |""".stripMargin
  }
}
