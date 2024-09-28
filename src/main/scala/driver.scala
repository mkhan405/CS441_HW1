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
import utils.ShardDriver.*

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
  val job: Job = Job.getInstance(conf.getConfiguration)
  job.setJobName("DataSampleComputation")

  val config = ConfigFactory.load()
  val inputDir = config.getString("training-conf.input_dir")
  val outputPath = config.getString("training-conf.data-sample_output_dir")

  val fs: FileSystem  = FileSystem.get(conf.getConfiguration)
  val finalInputDir: Path = new Path(inputDir)
  val finalOutputDir: Path = new Path(outputPath)

  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[utils.VectorWritable])

  job.setMapperClass(classOf[SlidingWindowMapper.Map])
  job.setReducerClass(classOf[SlidingWindowReducer.Reduce])

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])

  //  FileInputFormat.addInputPaths(job, inputDir)
  fs.listStatus(finalInputDir)
    .filter(_.getPath.getName.startsWith("shard_"))
    .foreach(file => FileInputFormat.addInputPath(job, file.getPath))

  FileOutputFormat.setOutputPath(job, finalOutputDir)

  if (fs.exists(finalOutputDir)) {
    fs.delete(finalOutputDir, true)
  }

  job.waitForCompletion(true)

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
  val conf = new Configuration()
  conf.set("mapreduce.job.maps", "11")
  conf.set("mapreduce.job.reduces", "11")

  val job = Job.getInstance(conf)
  val config = ConfigFactory.load()
  val baseDir = config.getString("job-conf.base_dir")
  val inputFilename = config.getString("job-conf.input_filename")

  val numShards = shardFile(baseDir, inputFilename, 17800)
  doWordCount(job)
  computeDataSamples(job)
  computeEmbeddings(job)

  cleanupShards(baseDir, inputFilename)

