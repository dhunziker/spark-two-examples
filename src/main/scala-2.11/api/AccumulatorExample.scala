package api

import common.Fixture

/**
  * Created by Dennis Hunziker on 12/11/16.
  */
object AccumulatorExample extends App with Fixture {

  // Deprecated API
  val acc1 = spark.sparkContext.accumulator(0.0, "dog_counter") // Provide name for UI, does it work?
  dogDensity.foreach(acc1 += _.estimatedDogPopulation)

  print(s"Dog accumulator V1 (sum): ${format(acc1.value)}")

  // AccumulatorV2, keeps track of sum and count
  val acc2 = spark.sparkContext.doubleAccumulator("dog_counter")
  dogDensity.foreach(row => acc2.add(row.estimatedDogPopulation))

  print(f"Dog accumulator V2 (sum): ${format(acc2.value)}")
  print(f"Dog accumulator V2 (average): ${format(acc2.avg)}")

  awaitTermination()

}
