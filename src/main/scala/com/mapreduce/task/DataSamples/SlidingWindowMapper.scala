package com.mapreduce.task.DataSamples

import com.mapreduce.task.utils.{VectorWritable, encoding}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*

object SlidingWindowMapper:
  val logger: Logger = LoggerFactory.getLogger("SlidingWindowMapper")

  /**
   * Generates a sliding window of samples from the given token array.
   *
   * This function creates overlapping samples from an input array of tokens using a sliding window approach.
   * Each sample will be of a fixed size defined by `window_size`. If the sliding window does not
   * completely fill the sample, it will be padded with the specified `pad_token`.
   *
   * @param tokens      An array of integer tokens from which samples are generated.
   * @param window_size The size of each sliding window sample.
   * @param stride      The number of tokens to skip after each window extraction.
   * @param pad_token   The token used for padding shorter windows (default is 0).
   * @return An array of samples, where each sample is an array of floats.
   *
   *         Example:
   *
   *         Given the following input:
   *         val tokens = Array(1, 2, 3, 4, 5)
   *         val window_size = 3
   *         val stride = 2
   *
   *         Calling generate_samples(tokens, window_size, stride) would yield:
   *         Array(
   *         Array(1.0, 2.0, 3.0),
   *         Array(3.0, 4.0, 5.0),
   *         Array(0.0, 0.0, 0.0) // Padded sample
   *         )
   */
  def generate_samples(tokens: Array[Int], window_size: Int, stride: Int,
                       pad_token: Int = 0): Array[Array[Float]] =
    tokens.sliding(window_size, stride).map(window => window.map(s => (s).toFloat)).map(window =>
      window ++ List.fill(window_size - window.length)(pad_token.toFloat)
    ).toArray

  class Map extends Mapper[LongWritable, Text, Text, VectorWritable]:
    // Text variable to hold the current word being processed
    private val word = new Text()

    // Text variable to hold the output for the current mapping
    private val textOut = new Text()

    // Modifiable configuration/job parameters
    private var window_size: Int = 0 // Size of the sliding window
    private var stride = 0 // Number of tokens to skip for each sliding window
    private var pad_token = 0 // Token used for padding shorter windows
    private var inputFileName = "" // Name of the input file being processed

    // Setup method to initialize job parameters and retrieve input file name
    override def setup(context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit = {
      val conf = context.getConfiguration
      // Retrieve and set window size from configuration (default is 5)
      window_size = conf.getInt("window_size", 5)
      // Retrieve and set stride from configuration (default is 1)
      stride = conf.getInt("window_stride", 1)
      // Retrieve and set padding token from configuration (default is 0)
      pad_token = conf.getInt("window_pad_token", 0)
      // Get the input file split and retrieve its name
      val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
      inputFileName = fileSplit.getPath.getName
      // Log the name of the file being processed
      logger.info(s"Processing file: $inputFileName")
    }

    // Main map function to process each line of the input
    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit =
      // Load configuration settings
      val conf = ConfigFactory.load()

      // Convert the input text value to a string
      val line: String = value.toString
      // Generate token encodings by splitting the line into sentences
      val sentences = line.split(".?!")
      // Log the processing of the sentence encoding
      logger.info(s"Computing BPE for offset ${key} in ${inputFileName}")
      // Encode each sentence into tokens using Byte Pair Encoding (BPE)
      val sentence_encodings = sentences.map(s => s.split(" ").flatMap((token) => encoding.encode(token).toArray))
      // Iterate over each encoded sentence with its index
      sentence_encodings.zipWithIndex.foreach((s, index) => {
        // Generate samples using the sliding window technique
        val samples = generate_samples(s, this.window_size, this.stride, this.pad_token)
        // Log the generation of sliding windows
        logger.info(s"Generated sliding window for offset ${key} in ${inputFileName}")
        // Write each sample to the context with the key (inputFilename-key-index-sample_index)
        // Where inputFileName is the filename of the shard, key is the long writable offset,
        // index is the ith sentence in the encodings, and sample_index represents the jth
        // sample for this sentence. This allows us to sort the samples by shard and maintain
        // the relative order of original text
        samples.zipWithIndex.foreach((sample, sample_index) => {
          context.write(new Text(s"${inputFileName}-${key}-${index}-${sample_index}"), new VectorWritable(sample))
        })
      })

