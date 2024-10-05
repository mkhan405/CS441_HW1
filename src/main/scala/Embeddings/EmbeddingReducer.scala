package Embeddings

import java.{lang, util}
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import scala.jdk.CollectionConverters.*
import com.knuddels.jtokkit.api.IntArrayList
import org.slf4j.{Logger, LoggerFactory}
import utils.VectorWritable


object EmbeddingReducer:
  val logger: Logger = LoggerFactory.getLogger("EmbeddingReducer")

  class Word2VecReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text,
      Text, Text]#Context): Unit = {
      try {
        val args = values.asScala.toArray.map(l => l.toString.split("-"))
        val vectorList = args.map(a => a.head).map(v => v.split(";")).map(v => v.map(_.toFloat))
          .map(v => new VectorWritable(v))

        val similarWords = args.map(a => a.last).flatMap(l => l.split(";"))
        val frequencyMap = similarWords.groupBy(identity).view.mapValues(_.length)
        val topKMostFrequent = frequencyMap.toSeq.sortBy(-_._2).take(3).map(e => e._1).map(w => {
          val tempList = new IntArrayList()
          tempList.add(w.toFloat.floor.toInt)
          utils.encoding.decode(tempList)
        }).mkString(", ")
        logger.info(s"Most Frequent: ${topKMostFrequent}")

        val tokenIDList = new IntArrayList()
        tokenIDList.add(key.toString.toFloat.floor.toInt)
        val sumOfVectors = vectorList.reduce((a, b) => a.add(b))
        context.write(new Text(utils.encoding.decode(tokenIDList)),
          new Text(s"${sumOfVectors.divide(vectorList.length)} : ${topKMostFrequent}"))
      } catch
        case e: Exception =>
          logger.error(s"Cannot decode key: ${key}")
    }
  }