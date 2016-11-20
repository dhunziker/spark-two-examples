package behaviour

import common.Spark

/**
  * Created by Dennis Hunziker on 20/11/16.
  */
object FloatingPointInference extends App with Spark {

  spark.sql("select 1, 2.2, 3.33").printSchema()

}
