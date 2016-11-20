package api

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Dennis Hunziker on 05/11/16.
  */
object SessionExample extends App {

  val conf = new SparkConf()
    .setAppName("My App")
    .setMaster("local[2]")

  // SparkContext for creating RDDs as usual
  val sc = new SparkContext(conf)

  // Deprecated SQLContext and HiveContext, these are wrapping the new SparkSession
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)

  // New SparkSession, uses existing SparkContext if available
  val spark = SparkSession.builder() // Replaces SQLContext
    .appName("My App")
    .master("local[2]")
    .enableHiveSupport() // Replaces HiveContext
    .getOrCreate()

  val spark2 = SparkSession.builder()
    .getOrCreate() // Gets existing SparkSession

  assert(spark == spark2)
  assert(spark == sqlContext.sparkSession)
  assert(spark == hiveContext.sparkSession)
  assert(spark.sparkContext == sc)
  assert(spark.sqlContext != sqlContext)

}
