package com.mapreduce.task.WordCount

import com.mapreduce.task.utils.encoding
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*

object WordCountReducer:
  // Initialize a logger for the WordCountReducer class
  val logger: Logger = LoggerFactory.getLogger("WordCountReducer")

  // Define the Reduce class that extends Hadoop's Reducer class
  class Reduce extends Reducer[Text, Text, Text, NullWritable]:
    // Override the reduce method to define the reduction logic
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, NullWritable]#Context): Unit =
      // Log the current word for which the count is being processed
      logger.info(s"Getting count for word: ${key}")

      // Convert the Iterable of values to a Scala List for processing
      val records = values.asScala.toList

      // Split each record by comma and create a list of metrics
      val metrics = records.map(_.toString.split(",").toList)

      // Sum the first element (count) of each metric to get the total count for the word
      val count = metrics.map(_.head.toInt).sum

      // Get the last element from the first metric, which contains token IDs
      val tokenIDs = metrics.head.last

      // Increment the vocabSize counter in the context by the length of token IDs
      context.getCounter("stats", "vocabSize").increment(tokenIDs.length)

      // Write the combined output: word, total count, and token IDs to the context
      context.write(new Text(s"${key},${count},${tokenIDs}"), NullWritable.get())

      // Log the results written for the current word
      logger.info(s"Wrote ${count},${tokenIDs} for ${key}")