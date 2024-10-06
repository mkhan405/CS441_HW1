package com.mapreduce.task.DataSamples

import com.mapreduce.task.utils.{VectorWritable, encoding}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*

object SlidingWindowReducer:
  // Class definition for the reducer that processes VectorWritable values grouped by a key
  class Reduce extends Reducer[Text, VectorWritable, Text, NullWritable]:
    // Method to handle the reduction of values associated with a given key
    @throws[IOException]
    override def reduce(key: Text, values: lang.Iterable[VectorWritable], context: Reducer[Text, VectorWritable,
      Text, Text]#Context): Unit =
      // Convert the iterable of VectorWritable values into an array for processing
      val sentences = values.asScala.toArray

      // Create a string representation of the values by joining their underlying float arrays
      // Here, we assume the first VectorWritable's values represent the sentence encodings
      val sentence_encodings = sentences.map(s => s.values.mkString(" ")).head

      // For each token in the sentence encodings, write the key and the sentence encodings to the context
      // This effectively outputs the reduced sentence encoding for the given key
      sentence_encodings.foreach(t => context.write(key, new Text(sentence_encodings)))