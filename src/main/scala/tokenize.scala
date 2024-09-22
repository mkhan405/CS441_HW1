import org.slf4j.{Logger, LoggerFactory}
import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

val logger: Logger = LoggerFactory.getLogger("Mapper")
val registry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
val encoding: Encoding = registry.getEncoding(EncodingType.CL100K_BASE)

object TokenizerMapReduce:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val textOut = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val line: String = value.toString
      line.split(" ").foreach { token =>
        word.set(token)
        val encoded_token = encoding.encode(token).get(0)

        textOut.set(s"1,${encoded_token}")
        output.collect(word, textOut)
      }

  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, NullWritable]:
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, NullWritable], reporter: Reporter): Unit =
      val records = values.asScala.toList
      val metrics = records.map(_.toString.split(","))
      val count = metrics.map(_.head.toInt).sum
      val tokenID = metrics.last.last.toInt
      output.collect(new Text(s"${key},${count},${tokenID}"), NullWritable.get())

  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    // conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[NullWritable])

    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[Text])

    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, NullWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)