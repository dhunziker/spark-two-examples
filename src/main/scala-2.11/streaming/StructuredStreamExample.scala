package streaming

import common.Data._
import common.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}

import scala.concurrent.duration._

/**
  * Created by Dennis Hunziker on 17/11/16.
  */
object StructuredStreamExample extends App with Spark {

  import spark.implicits._

  // We need to extract the schema using the batch API in order to pass it to the read stream
  val derivedSchema = spark.read
    .json(RsvpDir)
    .schema

  print(derivedSchema.treeString)

  // Read stream definition creates a data frame
  val rsvps: DataFrame = spark
    .readStream
    .schema(derivedSchema) // This is not being derived
    .json(RsvpDir)

  // Batch API is used to create data frame to join in country names
  val countries = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(CountryCodeCsv)
    .select($"ISO 3166-1 2 Letter Code" alias "country_code", $"Common Name" alias "country_name")

  // Batch API is used to define stream (CSV > Temp Table)
  val query = rsvps
    .join(countries, $"group.group_country" === lower($"country_code"))
    .withColumn("timestamp", from_unixtime($"mtime" / 1000)) // Ignoring milliseconds, column required for windowing function
    .groupBy(window($"timestamp", "3 minute", "3 minute"), $"country_name")
    .agg(count(lit(1)) as "count")
    .writeStream
    .trigger(ProcessingTime(5 seconds)) // How often the query gets executed
    .outputMode(OutputMode.Complete()) // Complete for aggregates, Append for full datasets
    .format("memory")
    .queryName("rsvps")
    .start()

  // For demonstration only
  while (query.isActive) {
    spark.sql(
      """select a.*
        |from rsvps a
        |where a.window = (select max(b.window) from rsvps b)
        |order by count desc
        |limit 10""".stripMargin)
      .show(truncate = false)
    pause(15 seconds)
  }

  query.awaitTermination() // Pointless!

}
