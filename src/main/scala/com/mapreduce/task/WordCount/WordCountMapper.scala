package com.mapreduce.task.WordCount

import com.mapreduce.task.utils.encoding
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object WordCountMapper:
  // Initialize a logger for the WordCountMapper class
  val logger: Logger = LoggerFactory.getLogger("WordCountMapper")

  // Define the Map class that extends Hadoop's Mapper class
  class Map extends Mapper[LongWritable, Text, Text, Text]:
    // Constant to represent the number one
    private final val one = new IntWritable(1)
    // Variable to hold the current word being processed
    private val word = new Text()
    // Variable to hold the output text for each token
    private val textOut = new Text()

    // Override the map method to define the mapping logic
    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit =
      // Convert the incoming Text value to a String
      val line: String = value.toString
      // Split the line into tokens based on spaces
      line.split(" ").foreach { token =>
        // Log the token being processed
        logger.info(s"Computing BPE for ${token}")
        // Set the current token as the word
        word.set(token)
        // Encode the token using a jtolkit encoding and convert it to a List of Floats
        val encoded_token_list = encoding.encode(token).toArray.map(_.toString).toList
        // Join the encoded tokens into a single string separated by semicolons
        val encoded_token = encoded_token_list.mkString(";")
        // Set the output text to include the count (1) and the encoded token
        textOut.set(s"1,${encoded_token}")
        // Write the word and the corresponding output text to the context
        context.write(word, textOut)
      }
