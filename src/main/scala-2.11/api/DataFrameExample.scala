package api

import common.Data._
import common.Spark
import org.apache.spark.sql.DataFrame

/**
  * Created by Dennis Hunziker on 06/11/16.
  */
object DataFrameExample extends App with Spark {

  import spark.implicits._

  val dogDensity: DataFrame = spark.read
    .option("header", true)
    .schema(DogDensitySchema)
    .csv(DogDensityCsv)

  val postcodeDistricts: DataFrame = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(PostcodeDistrictCsv)
    .withColumnRenamed("Town/Area", "TownArea")
    .withColumnRenamed("Active postcodes", "ActivePostcodes")

  val result: DataFrame = dogDensity
    .join(postcodeDistricts, $"postcodeDistrict" === $"postcode")
    .filter($"postcodeDistrict" === "E1")
    .select($"townArea", $"estimatedDogPopulation")

  result.show(truncate = false)

}
