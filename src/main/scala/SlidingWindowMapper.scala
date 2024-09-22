import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

import utils.encoding
import utils.VectorWritable

object SlidingWindowMapper:
  val logger: Logger = LoggerFactory.getLogger("Mapper")

  private def generate_samples(tokens: Array[Int], window_size: Int, stride: Int,
                               pad_token: Int = 0): Array[Array[Float]] =
    tokens.sliding(window_size, stride).map(window => window.map(s => (s).toFloat)).map(window =>
      window ++ List.fill(window_size - window.length)(pad_token.toFloat)
    ).toArray

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, VectorWritable]:
    private val word = new Text()
    private val textOut = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, VectorWritable], reporter: Reporter): Unit =
      val conf = ConfigFactory.load()
      val window_size = conf.getInt("window-conf.size")
      val stride = conf.getInt("window-conf.stride")
      val pad_token = conf.getInt("window-conf.pad_token")

      val line: String = value.toString
      // Generate token encodings
      val token_encodings = line.split(" ").flatMap((token) => encoding.encode(token).toArray)
      val samples = generate_samples(token_encodings, window_size, stride, 0)
      samples.foreach(s => output.collect(value, new VectorWritable(s)))

  class Reduce extends MapReduceBase with Reducer[Text, VectorWritable, Text, NullWritable]:
    override def reduce(key: Text, values: util.Iterator[VectorWritable], output: OutputCollector[Text, NullWritable],
                        reporter: Reporter): Unit =
      values.asScala.foreach(tokens => output.collect(new Text(tokens.values.map(_.toString).mkString(",")), null))