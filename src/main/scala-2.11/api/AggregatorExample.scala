package api

import common.Data._
import common.Fixture
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * Created by Dennis Hunziker on 14/11/16.
  */
object AggregatorExample extends App with Fixture {

  // More examples available on GitHub:
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/typedaggregators.scala
  // https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/DatasetAggregatorSuite.scala

  def aggregator(f: (Double, Double) => Boolean, zeroValue: Double) = {

    // Aggregator to find the postcode for the min or max estimated dog population
    new Aggregator[DogDensity, DogDensity, String] with Serializable {
      def zero: DogDensity = DogDensity("", zeroValue)

      def reduce(b: DogDensity, a: DogDensity): DogDensity =
        if (f(a.estimatedDogPopulation, b.estimatedDogPopulation)) DogDensity(a.postcodeDistrict, a.estimatedDogPopulation) else b

      def merge(b1: DogDensity, b2: DogDensity): DogDensity = if (f(b1.estimatedDogPopulation, b2.estimatedDogPopulation)) b1 else b2

      def finish(r: DogDensity): String = r.postcodeDistrict

      // Required in version 2.x
      def bufferEncoder: Encoder[DogDensity] = Encoders.product[DogDensity]

      def outputEncoder: Encoder[String] = Encoders.STRING
    }.toColumn
  }

  val min = aggregator(Ordering.Double.lteq, Double.MaxValue)

  val max = aggregator(Ordering.Double.gteq, Double.MinValue)

  dogDensity.select(min name "min_postcode", max name "max_postcode").show()

}
