package common

import java.text.NumberFormat
import java.util.Locale

import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._

/**
  * Created by Dennis Hunziker on 09/11/16.
  */
trait Spark {

  lazy val spark = SparkSession.builder()
    .appName("My App")
    .master("local[1]")

    // Use the recommended serializer
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Default is 1g
    //.config("spark.driver.memory", "2g")
    //.config("spark.executor.memory", "2g")

    // Enable off-heap storage
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", 1000000000) // 100mb

    //.enableHiveSupport()
    .getOrCreate()

  def print(message: String) = println(message)

  def format(value: Double) = NumberFormat.getInstance(Locale.UK).format(value)

  def awaitTermination() = {
    print("Awaiting termination...")
    Thread.sleep((1 hour).toMillis)
  }

  def pause(duration: Duration) = {
    print(s"Going to sleep for ${duration.toSeconds} seconds")
    Thread.sleep(duration.toMillis)
  }

}
