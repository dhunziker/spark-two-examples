package common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}

import scalax.io.{Codec, Resource}

/**
  * Created by Dennis Hunziker on 15/11/16.
  */
object FileSystem {

  private lazy val config = new Configuration()

  lazy val fs = org.apache.hadoop.fs.FileSystem.get(config)

  implicit def filterConverter(f: Path => Boolean): PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = f(path)
  }

  def writeStringToFile(str: String, file: String) = {
    val out = Resource.fromFile(file)
    out.write(str)(Codec.UTF8)
  }

}
