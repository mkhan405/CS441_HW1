package WordCount

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

import utils.encoding

object WordCountMapper:
  val logger: Logger = LoggerFactory.getLogger("Mapper")
  class Map extends Mapper[LongWritable, Text, Text, Text]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val textOut = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit =
      val line: String = value.toString
      line.split(" ").foreach { token =>
        word.set(token)
        val encoded_token_list = encoding.encode(token).toArray.map(_.toString).toList
        val encoded_token = encoded_token_list.mkString(";")
        textOut.set(s"1,${encoded_token}")
        context.write(word, textOut)
      }
