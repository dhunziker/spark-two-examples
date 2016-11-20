package common

import common.Data._
import org.apache.spark.sql.Dataset

/**
  * Created by Dennis Hunziker on 14/11/16.
  */
trait Fixture extends Spark {

  import spark.implicits._

  val dogDensity: Dataset[DogDensity] = spark.read
    .option("header", true)
    .schema(DogDensitySchema)
    .csv(DogDensityCsv)
    .as

  def copy(ds: Dataset[DogDensity]) = ds.map(row => row)

}
