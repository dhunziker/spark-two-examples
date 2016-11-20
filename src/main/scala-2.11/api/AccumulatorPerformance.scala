package api

import common.Fixture
import common.Precision._
import org.scalameter.picklers.noPickler._
import org.scalameter.{Bench, Gen}

/**
  * Created by Dennis Hunziker on 12/11/16.
  */
object AccumulatorPerformance extends Bench.LocalTime with Fixture {

  dogDensity.cache().count()

  val acc1 = spark.sparkContext.accumulator(0.0)
  val acc2 = spark.sparkContext.doubleAccumulator

  val gen = Gen.single("iterations")(100)

  performance of "accumulator" in {
    measure method "add-v2" in {
      using(gen) in {
        iterations =>
          for (i <- 0 until iterations) {
            dogDensity.foreach(row => acc2.add(row.estimatedDogPopulation))
            assert(acc2.sum ~= 11599823.99)
            acc2.reset()
          }
      }
    }
    measure method "add-v1" in {
      using(gen) in {
        iterations =>
          for (i <- 0 until iterations) {
            dogDensity.foreach(acc1 += _.estimatedDogPopulation)
            assert(acc1.value ~= 11599823.99)
            acc1.setValue(acc1.zero)
          }
      }
    }
  }

}
