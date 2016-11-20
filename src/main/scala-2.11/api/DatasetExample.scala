package api

import common.Data._
import common.Spark
import org.apache.spark.sql.Dataset

/**
  * Created by Dennis Hunziker on 06/11/16.
  */
object DatasetExample extends App with Spark {

  import spark.implicits._

  val dogDensity: Dataset[DogDensity] = spark.read
    .option("header", true)
    .schema(DogDensitySchema)
    .csv(DogDensityCsv)
    .as

  val postcodeDistricts: Dataset[PostcodeDistrict] = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(PostcodeDistrictCsv)
    .withColumnRenamed("Town/Area", "townArea")
    .withColumnRenamed("Active postcodes", "activePostcodes")
    .as

  val result: Dataset[(String, Double)] = dogDensity
    .joinWith(postcodeDistricts, $"postcodeDistrict" === $"postcode")
    .filter(_._2.postcode == "E1")
    .select($"_2.townArea".as[String], $"_1.estimatedDogPopulation".as[Double])

  result.show(truncate = false)

}
