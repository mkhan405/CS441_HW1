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

object EmbeddingMapper:
  class Map extends Mapper[LongWritable, Text, VectorWritable, VectorWritable]:
    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, VectorWritable, VectorWritable]#Context): Unit = {
      val tokens = value.toString.split(";").map(_.toFloat)
      val newVec = new VectorWritable(tokens)
      context.write(newVec, newVec)
    }