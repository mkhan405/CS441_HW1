package DataSamples

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

object SlidingWindowReducer:
  class Reduce extends Reducer[Text, VectorWritable, Text, NullWritable]:
    override def reduce(key: Text, values: lang.Iterable[VectorWritable], context: Reducer[Text, VectorWritable,
      Text, Text]#Context): Unit =
      val sentences = values.asScala.toArray
      val sentence_encodings = sentences.map(s => s.values.mkString(" ")).head
      sentence_encodings.foreach(t => context.write(key, new Text(sentence_encodings)))