package performance

import common.Fixture
import org.scalameter.picklers.noPickler._
import org.scalameter.{Bench, Gen}

/**
  * Created by Dennis Hunziker on 15/11/16.
  */
object CodeGenPerformance extends Bench.LocalTime with Fixture {

  dogDensity.cache().createOrReplaceTempView("dog_density")

  val query =
    """select count(1)
      |from dog_density
      |where estimatedDogPopulation > 10000""".stripMargin

  val gen = Gen.crossProduct(
    Gen.single("iterations")(10),
    Gen.enumeration("codegen-enabled")(true, false))

  performance of "spark" in {
    measure method "sql" in {
      using(gen) in {
        params => {
          spark.conf.set("spark.sql.codegen.wholeStage", params._2)
          for (i <- 0 until params._1) spark.sql(query).count()
        }
      }
    }
  }

}
