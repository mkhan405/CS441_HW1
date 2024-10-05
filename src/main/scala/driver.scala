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
import utils.VocabExtractor.*
import java.net.URI

def runJob(job: Job, jobName: String, inputDir: String, outputDir: String): Boolean = {
  // Setup Logger and
  val log: Logger = LoggerFactory.getLogger(s"${jobName.capitalize} Logger")
  job.setJobName(jobName)

  val fs: FileSystem = FileSystem.get(job.getConfiguration)
  val finalInputDir: Path = new Path(inputDir)
  val finalOutputDir: Path = new Path(outputDir)
  val tempOutputPath = outputDir + "_dir"
  val tempOutputDir = new Path(tempOutputPath)

  // Setup Input File Paths
  fs.listStatus(finalInputDir)
    .filter(_.getPath.getName.startsWith("shard_"))
    .foreach(file => FileInputFormat.addInputPath(job, file.getPath))

  // Setup Output File Path
  FileOutputFormat.setOutputPath(job, tempOutputDir)
  if (job.waitForCompletion(true)) {
    fs.delete(finalOutputDir, true)
    print(merge(fs, tempOutputDir, fs, finalOutputDir, true, job.getConfiguration))
    log.info(s"${jobName} completed")
    true
  } else {
    log.error(s"${jobName} failed")
    false
  }
}

def doWordCount(conf: Job): Long =
  val job: Job = Job.getInstance(conf.getConfiguration)

  val config = ConfigFactory.load()
  val inputDir = config.getString("training-conf.input_dir")
  val outputPath = config.getString("training-conf.word-count_output_dir")
  val vocabPath = config.getString("training-conf.vocab_output_dir")

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[Text])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[Text])

  job.setMapperClass(classOf[WordCountMapper.Map])
  job.setReducerClass(classOf[WordCountReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

  if (runJob(job, "Word Count", inputDir, outputPath))
    // Extract vocabulary from computed results
    extractor(outputPath, vocabPath)
    job.getCounters.findCounter("stats", "vocabSize").getValue
  else
    0

def computeDataSamples(conf: Job): Unit =
  val job: Job = Job.getInstance(conf.getConfiguration)

  val config = ConfigFactory.load()
  val inputDir = config.getString("training-conf.input_dir")
  val outputPath = config.getString("training-conf.data-sample_output_dir")

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

  runJob(job, "Data Samples", inputDir, outputPath)


def computeEmbeddings(conf: Job, vocabSize: Long, numJobs: Int): Unit =
  val job: Job = Job.getInstance(conf.getConfiguration)

  val config = ConfigFactory.load()
  // val inputPath = config.getString("training-conf.data-sample_output_dir")
  val outputPath = config.getString("training-conf.embedding_output_dir")
  val vocabPath = config.getString("training-conf.vocab_output_dir")
  val baseDir = config.getString("job-conf.base_output_dir")

  shardFile(baseDir, "samples", numJobs)

  job.getConfiguration.setLong("vocabSize", vocabSize)
  job.getConfiguration.setInt("epochs", config.getInt("training-conf.epochs"))
  job.getConfiguration.setInt("embeddingDim", config.getInt("training-conf.embeddingDim"))

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[utils.VectorWritable])

  job.setMapperClass(classOf[Training.EmbeddingMapper.Map])
  job.setReducerClass(classOf[Training.EmbeddingReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, utils.VectorWritable]])
  job.addCacheFile(new URI(vocabPath))

  try {
    runJob(job, "EmbeddingsCompute", baseDir, outputPath)
  } finally {
    cleanupShards(baseDir, "samples")
  }

@main def main(): Unit =
  val config = ConfigFactory.load()
  val baseDir = config.getString("job-conf.base_dir")
  val inputFilename = config.getString("job-conf.input_filename")

  try {
    val numJobs = config.getInt("job-conf.num_jobs")
    val conf = new Configuration()
    conf.set("mapreduce.job.maps", s"${numJobs}")
    conf.set("mapreduce.job.reduces", s"${numJobs}")

    val job = Job.getInstance(conf)

    shardFile(baseDir, inputFilename, numJobs)
    val vocabSize = doWordCount(job)

    if (vocabSize == 0)
      return

    computeDataSamples(job)
    computeEmbeddings(job, vocabSize, numJobs)
  } finally {
    cleanupShards(baseDir, inputFilename)
  }

