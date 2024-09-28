package Training
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}
import utils.{VectorWritable, encoding}

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*

object EmbeddingReducer:
  class Reduce extends Reducer[VectorWritable, VectorWritable, VectorWritable, VectorWritable]:
    @throws[IOException]
    override def reduce(key: VectorWritable, values: lang.Iterable[VectorWritable], context: Reducer[VectorWritable,
      VectorWritable, VectorWritable, VectorWritable]#Context): Unit = {
      val valuesScala = values.asScala
      if (valuesScala.nonEmpty) {
        val firstValue = valuesScala.head
        context.write(firstValue, firstValue)
      } else {
        // Log a warning or handle the case where there are no values
        context.getCounter("Reducer", "EmptyValues").increment(1)
      }
    }

