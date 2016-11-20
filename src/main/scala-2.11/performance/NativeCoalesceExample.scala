package performance

import common.FileSystem._
import common.Fixture
import org.apache.hadoop.fs.Path

//import scalax.file.Path

/**
  * Created by Dennis Hunziker on 14/11/16.
  */
object NativeCoalesceExample extends App with Fixture {

  // Prepare file system
  val path = new Path("./target/dog_density")
  fs.delete(path, true)

  // Write down the same data with 10 partitions / 10 part files
  val dogDensity1 = dogDensity.repartition(10)

  print(s"Partitions in original dataset: ${dogDensity1.rdd.getNumPartitions}")
  dogDensity1.write.csv(path.toString)

  val parts = fs.listStatus(path, (p: Path) => p.getName.endsWith(".csv"))
  print(s"Parts:\n${parts.map(_.getPath.getName).mkString("\n")}")
  print(s"Parts written: ${parts.length}")

  val ds2 = spark.read.csv(path.toString)
  print(s"Partitions in re-loaded dataset: ${ds2.rdd.getNumPartitions}")

}
