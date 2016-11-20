package performance

import common.Fixture
import org.apache.spark.sql.execution.debug._

/**
  * Created by Dennis Hunziker on 15/11/16.
  */
object CodeGenExample extends App with Fixture {

  dogDensity.createOrReplaceTempView("dog_density")

  val query =
    """select count(1)
      |from dog_density
      |where estimatedDogPopulation > 10000""".stripMargin

  spark.conf.set("spark.sql.codegen.wholeStage", true) // This is the default
  spark.sql(query).debugCodegen()

}
