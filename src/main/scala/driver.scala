import WordCount.WordCountMapper
import WordCount.WordCountReducer
import org.apache.hadoop.fs.{Path, FileSystem, FileUtil}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import com.typesafe.config.ConfigFactory
import utils.ShardDriver._

def doWordCount(conf: Job): Unit =
  val job: Job = Job.getInstance(conf.getConfiguration)
  job.setJobName("WordCount")

  val config = ConfigFactory.load()
  val inputPath = config.getString("word-count-conf.input")
  val outputPath = config.getString("word-count-conf.output")

  val fs: FileSystem  = FileSystem.get(conf.getConfiguration)
  val finalOutputPath: Path = new Path(outputPath)

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[Text])

  job.setMapperClass(classOf[WordCountMapper.Map])
  job.setReducerClass(classOf[WordCountReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])
  // FileInputFormat.setInputPaths(job, new Path(input_path))
  FileInputFormat.addInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, finalOutputPath)

  if (fs.exists(finalOutputPath)) {
    fs.delete(finalOutputPath, true)
  }

  job.waitForCompletion(true)

@main def main() =
  val conf = new Configuration()
  conf.set("mapreduce.job.maps", "1")
  conf.set("mapreduce.job.reduces", "1")

  val job = Job.getInstance(conf)
  val config = ConfigFactory.load()
  val baseDir = config.getString("job-conf.base_dir")
  val inputFilename = config.getString("job-conf.input_filename")

  val numShards = shardFile(baseDir, inputFilename, 17800)
  doWordCount(job)

  cleanupShards(baseDir, inputFilename)

