package DataSamples

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}
import utils.{VectorWritable, encoding}

import java.io.IOException
import java.{lang, util}
import scala.jdk.CollectionConverters.*

object SlidingWindowMapper:
  val logger: Logger = LoggerFactory.getLogger("SlidingWindowMapper")

  private def generate_samples(tokens: Array[Int], window_size: Int, stride: Int,
                               pad_token: Int = 0): Array[Array[Float]] =
    tokens.sliding(window_size, stride).map(window => window.map(s => (s).toFloat)).map(window =>
      window //++ List.fill(window_size - window.length)(pad_token.toFloat)
    ).toArray

  class Map extends Mapper[LongWritable, Text, Text, VectorWritable]:
    private val word = new Text()
    private val textOut = new Text()
    // Modifiable configuration/job parameters
    private var window_size: Int = 0
    private var stride = 0
    private var pad_token = 0
    private var inputFileName = ""

    override def setup(context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit = {
      val conf = context.getConfiguration
      window_size = conf.getInt("window_size", 5)
      stride = conf.getInt("window_stride", 1)
      pad_token = conf.getInt("window_pad_token", 0)
      val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
      inputFileName = fileSplit.getPath.getName
      logger.info(s"Processing file: $inputFileName")
    }

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit =
      val conf = ConfigFactory.load()

      val line: String = value.toString
      // Generate token encodings
      val sentences = line.split(".?!")
      logger.info(s"Computing BPE for offset ${key} in ${inputFileName}")
      val sentence_encodings = sentences.map(s => s.split(" ").flatMap((token) => encoding.encode(token).toArray))
      sentence_encodings.zipWithIndex.foreach((s, index) => {
        val samples = generate_samples(s, this.window_size, this.stride, this.pad_token)
        logger.info(s"Generated sliding window for offset ${key} in ${inputFileName}")
        samples.foreach(sample => context.write(new Text(s"${inputFileName}-${key}-${index}"), new VectorWritable(sample)))
      })
