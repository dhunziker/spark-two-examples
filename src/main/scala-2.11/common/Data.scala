package common

import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

/**
  * Created by Dennis Hunziker on 17/11/16.
  */
object Data {

  private val DataDir = "src/main/resources/data"

  val DogDensityCsv = s"$DataDir/APHA0375-Dog_Density_Postcode_District.csv"

  val CatDensityCsv = s"$DataDir/APHA0372-Cat_Density_Postcode_District.csv"

  val DogsPerHouseholdCsv = s"$DataDir/APHA0381-Dogs_Per_Household"

  val CatsPerHouseholdCsv = s"$DataDir/APHA0378-Cats_Per_Household.csv"

  val CountryCodeCsv = s"$DataDir/iso_3166_2_countries.csv"

  val PostcodeDistrictCsv = s"$DataDir/Postcode districts.csv"

  case class DogDensity(postcodeDistrict: String, estimatedDogPopulation: Double)

  case class CatDensity(postcodeDistrict: String, estimatedCatPopulation: Double)

  case class DogsPerHousehold(postcodeDistrict: String, dogsPerHousehold: Double)

  case class CatsPerHousehold(postcodeDistrict: String, catsPerHousehold: Double)

  case class PostcodeDistrict(postcode: String,
                              latitude: Double,
                              longitude: Double,
                              easting: Int,
                              northing: Int,
                              gridRef: String,
                              townArea: String,
                              region: String,
                              postcodes: Int,
                              activePostcodes: Int,
                              population: Option[Int],
                              households: Option[Int])

  // Schema inference doesn't seem to recognize doubles with thousands separator
  val DogDensitySchema = (new StructType)
    .add("postcodeDistrict", StringType)
    .add("estimatedDogPopulation", DoubleType) // Enforce DoubleType

  val CatDensitySchema = (new StructType)
    .add("postcodeDistrict", StringType)
    .add("estimatedCatPopulation", DoubleType)

  private val TempDir = sys.props("java.io.tmpdir")

  val RsvpDir = s"$TempDir/rsvp"

}
