package WordCount

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.mapreduce.Reducer

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*
import utils.encoding

object WordCountReducer:
  val logger: Logger = LoggerFactory.getLogger("WordCountReducer")
  class Reduce extends Reducer[Text, Text, Text, Text]:
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit =
      logger.info(s"Getting count for word: ${key}")
      val records = values.asScala.toList
      val metrics = records.map(_.toString.split(",").toList)
      val count = metrics.map(_.head.toInt).sum
      val tokenIDs = metrics.head.last
      context.getCounter("stats","vocabSize").increment(tokenIDs.length)
      context.write(new Text(s"${key}"), new Text(s"${count},${tokenIDs}"))