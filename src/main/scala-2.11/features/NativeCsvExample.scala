package features

import common.Data._
import common.Spark

/**
  * Created by Dennis Hunziker on 14/11/16.
  */
object NativeCsvExample extends App with Spark {

  // Previous version
  val df1 = spark.read
    .format("com.databricks.spark.csv")
    .option("header", true)
    .schema(DogDensitySchema)
    .load(DogDensityCsv)

  // Native support
  val df2 = spark.read
    .option("header", true)
    .schema(DogDensitySchema)
    .csv(DogDensityCsv) // Really only a shortcut for format("csv") + load

}
