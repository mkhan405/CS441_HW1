import DataSamples.SlidingWindowMapper
import DataSamples.SlidingWindowReducer
import WordCount.WordCountMapper
import WordCount.WordCountReducer
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import utils.ShardDriver.*

// TODO: Modify Word Count to Combine Files
def doWordCount(conf: Job): Unit =
  val job: Job = Job.getInstance(conf.getConfiguration)
  job.setJobName("WordCount")

  val config = ConfigFactory.load()
  val inputDir = config.getString("training-conf.input_dir")
  val outputPath = config.getString("training-conf.word-count_output_dir")

  val fs: FileSystem  = FileSystem.get(conf.getConfiguration)
  val finalInputDir: Path = new Path(inputDir)
  val finalOutputDir: Path = new Path(outputPath)

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[Text])

  job.setMapperClass(classOf[WordCountMapper.Map])
  job.setReducerClass(classOf[WordCountReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])

  fs.listStatus(finalInputDir)
    .filter(_.getPath.getName.startsWith("shard_"))
    .foreach(file => FileInputFormat.addInputPath(job, file.getPath))

  FileOutputFormat.setOutputPath(job, finalOutputDir)

  if (fs.exists(finalOutputDir)) {
    fs.delete(finalOutputDir, true)
  }

  job.waitForCompletion(true)

def computeDataSamples(conf: Job): Unit =
  val log: Logger = LoggerFactory.getLogger("Data Samples Job")
  val job: Job = Job.getInstance(conf.getConfiguration)
  job.setJobName("DataSampleComputation")

  val config = ConfigFactory.load()
  val inputDir = config.getString("training-conf.input_dir")
  val outputPath = config.getString("training-conf.data-sample_output_dir")

  val fs: FileSystem  = FileSystem.get(conf.getConfiguration)
  val finalInputDir: Path = new Path(inputDir)
  val finalOutputDir: Path = new Path(outputPath)
  val tempOutputPath = outputPath + "_dir"
  val tempOutputDir = new Path(tempOutputPath)

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[utils.VectorWritable])

  job.setMapperClass(classOf[SlidingWindowMapper.Map])
  job.setReducerClass(classOf[SlidingWindowReducer.Reduce])

  job.getConfiguration.setInt("window_size", config.getInt("window-conf.size"))
  job.getConfiguration.setInt("window_stride", config.getInt("window-conf.stride"))
  job.getConfiguration.setInt("window_pad_token", config.getInt("window-conf.pad_token"))

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

  //  FileInputFormat.addInputPaths(job, inputDir)
  fs.listStatus(finalInputDir)
    .filter(_.getPath.getName.startsWith("shard_"))
    .foreach(file => FileInputFormat.addInputPath(job, file.getPath))

  FileOutputFormat.setOutputPath(job, tempOutputDir)

  if (job.waitForCompletion(true)) {
    fs.delete(finalOutputDir, true)
    // Merge output files
    merge(fs, tempOutputDir, fs, finalOutputDir, true, conf.getConfiguration)
    log.info("Data Samples Computed and Merged")
  } else {
    log.error("Data samples job failed")
  }


def computeEmbeddings(conf: Job): Unit =
  val job: Job = Job.getInstance(conf.getConfiguration)
  job.setJobName("EmbeddingsCompute")

  val config = ConfigFactory.load()
  val inputPath = config.getString("training-conf.data-sample_output_dir")
  val outputPath = config.getString("training-conf.embedding_output_dir")

  val fs: FileSystem = FileSystem.get(conf.getConfiguration)
  val finalInputDir: Path = new Path(inputPath)
  val finalOutputDir: Path = new Path(outputPath)

  job.setOutputKeyClass(classOf[utils.VectorWritable])
  job.setOutputValueClass(classOf[utils.VectorWritable])

  job.setMapperClass(classOf[Training.EmbeddingMapper.Map])
  job.setReducerClass(classOf[Training.EmbeddingReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[utils.VectorWritable, utils.VectorWritable]])

  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, finalOutputDir)

  if (fs.exists(finalOutputDir)) {
    fs.delete(finalOutputDir, true)
  }

  job.waitForCompletion(true)


@main def main(): Unit =
  val config = ConfigFactory.load()
  val numJobs = config.getInt("job-conf.num_jobs")
  val conf = new Configuration()
  conf.set("mapreduce.job.maps", s"${numJobs}")
  conf.set("mapreduce.job.reduces", s"${numJobs}")

  val job = Job.getInstance(conf)
  val baseDir = config.getString("job-conf.base_dir")
  val inputFilename = config.getString("job-conf.input_filename")
  shardFile(baseDir, inputFilename, numJobs)
  doWordCount(job)
  computeDataSamples(job)
  // computeEmbeddings(job)
  cleanupShards(baseDir, inputFilename)

