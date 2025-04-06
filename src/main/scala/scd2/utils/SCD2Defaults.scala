package scd2.utils

case class SCD2PartitioningConfig(colName: String, allowedValues: Seq[Int]) {
  require(allowedValues.nonEmpty, Messages.requireMessage("allowedValues"))
}

sealed trait ActiveFlag {
  val value: Int
}

case object Active extends ActiveFlag { val value = 1 }
case object Inactive extends ActiveFlag { val value = 0 }

object SCD2Defaults {
  val DefaultPartition: SCD2PartitioningConfig = SCD2PartitioningConfig(
    colName = "active_flg",
    allowedValues = Seq(Inactive.value, Active.value)
  )
}
