package features

import common.Fixture
import org.apache.spark.storage.StorageLevel

/**
  * Created by Dennis Hunziker on 16/11/16.
  */
object OffHeapExample extends App with Fixture {

  copy(dogDensity).persist(StorageLevel.MEMORY_ONLY).count()

  copy(dogDensity).persist(StorageLevel.MEMORY_ONLY_SER).count()

  // http://localhost:4040/storage/
  // Defaults to disk if not enabled
  copy(dogDensity).persist(StorageLevel.OFF_HEAP).count()

  awaitTermination()

}
