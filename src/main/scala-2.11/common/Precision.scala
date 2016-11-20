package common

/**
  * Created by Dennis Hunziker on 16/11/16.
  */
case class Precision(precision: Double)

// http://stackoverflow.com/a/8385791/2850516
object Precision {

  implicit val precision = Precision(0.01)

  class WithAlmostEquals(value1: Double) {
    def ~=(value2: Double)(implicit precision: Precision) = (value1 - value2).abs <= precision.precision
  }

  implicit def add_~=(value: Double) = new WithAlmostEquals(value)

}
