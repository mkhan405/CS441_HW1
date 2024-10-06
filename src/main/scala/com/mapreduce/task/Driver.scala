package com.mapreduce.task

import com.mapreduce.task.Embeddings.EmbeddingReducer.Word2VecReducer
import com.mapreduce.task.Embeddings.Embeddings.Word2VecMapper
import com.mapreduce.task.DataSamples.{SlidingWindowMapper, SlidingWindowReducer}
import com.mapreduce.task.WordCount.{WordCountMapper, WordCountReducer}
import com.mapreduce.task.utils.VectorWritable
import com.mapreduce.task.utils.ShardDriver.*
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object Driver:


  @main def main(): Unit =
    val config = ConfigFactory.load().resolve()
    val baseDir = config.getString("job-conf.base_dir")
    val inputFilename = config.getString("job-conf.input_filename")

    val numJobs = config.getInt("job-conf.num_jobs")
    val conf = new Configuration()
    conf.set("mapreduce.job.maps", s"${numJobs}")
    conf.set("mapreduce.job.reduces", s"${numJobs}")

    //    Setting credentials for S3
    //     conf.set("fs.s3a.access.key", config.getString("aws-creds.access_key"))
    //     conf.set("fs.s3a.secret.key", config.getString("aws-creds.secret_key"))

    //    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //    // For Testing with EMR
    //    // conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    //    conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    //    // Local Testing
    //    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    try {
      val job = Job.getInstance(conf)

      shardFile(baseDir, inputFilename, numJobs, conf)
      val vocabSize = doWordCount(job)

      if (vocabSize != 0)
        computeDataSamples(job)
        newEmbeddings(job)
    } finally {
      cleanupShards(baseDir, inputFilename, conf)
    }

  /**
   * Runs a Hadoop job with the specified parameters.
   *
   * This function sets up the necessary configurations for the Hadoop job, including
   * logging, input/output paths, and temporary directories. It also handles the execution
   * of the job and manages the output results, including merging temporary outputs and
   * cleaning up existing output directories.
   *
   * @param job       The Hadoop Job instance to be configured and run.
   * @param jobName   A string representing the name of the job for logging purposes.
   * @param inputDir  The input directory where the job will read data from (should start with s3a:// for S3).
   * @param outputDir The output directory where the job will write results.
   * @return A boolean indicating whether the job completed successfully.
   */
  def runJob(job: Job, jobName: String, inputDir: String, outputDir: String): Boolean = {
    // Setup Logger
    val log: Logger = LoggerFactory.getLogger(s"${jobName.capitalize} Logger")
    job.setJobName(jobName)

    val conf = job.getConfiguration

//    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    // For EMR Deployment
//    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
//    // For Local Testing
//    // conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//    conf.set("fs.s3a.endpoint", "s3.amazonaws.com")


    // Get FileSystem using the S3A URI
    // For S3
    val s3Uri = new URI(inputDir) // Make sure inputDir starts with s3a://
    // val fs: FileSystem = FileSystem.get(s3Uri, job.getConfiguration)
    // Normal Configuration
    val fs: FileSystem = FileSystem.get(conf)

    // Setup input/output and temporary paths
    val finalInputDir = new Path(inputDir)
    val finalOutputDir = new Path(outputDir)
    val tempOutputPath = outputDir + "_dir"
    val tempOutputDir = new Path(tempOutputPath)

    // Import file shards
    fs.listStatus(finalInputDir)
      .filter(_.getPath.getName.startsWith("shard_"))
      .foreach(file => FileInputFormat.addInputPath(job, file.getPath))

    // Setup Output File Path
    FileOutputFormat.setOutputPath(job, tempOutputDir)
    if (job.waitForCompletion(true)) {
      // Delete existing data
      fs.delete(finalOutputDir, true)
      merge(fs, tempOutputDir, fs, finalOutputDir, true, job.getConfiguration)
      log.info(s"${jobName} completed")
      true
    } else {
      log.error(s"${jobName} failed")
      false
    }
  }

  /**
   * Performs a word count operation using Hadoop MapReduce.
   *
   * This function configures and runs a Hadoop job that counts the occurrences of words in
   * the input data. It sets up the necessary mapper and reducer classes, input/output formats,
   * and retrieves configuration parameters for the input directory, output directory, and
   * vocabulary output.
   *
   * @param conf The Hadoop Job instance containing the configuration for the job.
   * @return A Long representing the size of the vocabulary extracted from the computed results
   *         if the job completes successfully; otherwise, returns 0.
   */
  def doWordCount(conf: Job): Long =
    val job: Job = Job.getInstance(conf.getConfiguration)

    // Load configuration parameters
    val config = ConfigFactory.load()
    val inputDir = config.getString("training-conf.input_dir")
    val outputPath = config.getString("training-conf.word-count_output_dir")
    val vocabPath = config.getString("training-conf.vocab_output_dir")

    // Set Mapper and Reducer Output Types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    // Set Mapper and Reducer Classes
    job.setMapperClass(classOf[WordCountMapper.Map])
    job.setReducerClass(classOf[WordCountReducer.Reduce])

    // Set Input/Output Format Types
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])

    if (runJob(job, "Word Count", inputDir, outputPath))
      // Extract vocabulary from computed results
      job.getCounters.findCounter("stats", "vocabSize").getValue
    else
      0

  /**
   * Computes sliding window data samples for fitting the Word2Vec model.
   *
   * This function configures and runs a Hadoop job that generates data samples
   * using a sliding window approach. The samples are intended to help the Word2Vec
   * model learn contextual embeddings by creating pairs of words within a defined
   * window size. The function sets up the necessary mapper and reducer classes,
   * input/output formats, and retrieves configuration parameters for the window size,
   * stride, and padding token.
   *
   * @param conf The Hadoop Job instance containing the configuration for the job.
   * @return A Boolean indicating whether the job was completed successfully.
   */
  def computeDataSamples(conf: Job): Boolean =
    val job: Job = Job.getInstance(conf.getConfiguration)

    // Load configuration parameters
    val config = ConfigFactory.load()
    val inputDir = config.getString("training-conf.input_dir")
    val outputPath = config.getString("training-conf.data-sample_output_dir")

    // Set Mapper and Reducer Output Types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[VectorWritable])

    // Set Mapper and Reducer Classes
    job.setMapperClass(classOf[SlidingWindowMapper.Map])
    job.setReducerClass(classOf[SlidingWindowReducer.Reduce])

    // Set job configuration parameters
    job.getConfiguration.setInt("window_size", config.getInt("window-conf.size"))
    job.getConfiguration.setInt("window_stride", config.getInt("window-conf.stride"))
    job.getConfiguration.setInt("window_pad_token", config.getInt("window-conf.pad_token"))

    // Set Input/Output Format Types
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    runJob(job, "samples", inputDir, outputPath)


  /**
   * Shards the data from computed data samples and generates a CSV file containing
   * tokens, their embeddings, and three similar words.
   *
   * This function configures and executes a Hadoop job that takes the output from
   * the previous data sampling function and processes it to create embeddings for
   * the tokens. The generated output includes a CSV file that lists each token,
   * its corresponding embedding, and three similar words based on the Word2Vec
   * model. The function handles data sharding, sets up the necessary mapper and
   * reducer classes, and defines input/output formats.
   *
   * @param conf The Hadoop Job instance containing the configuration for the job.
   * @return A Boolean indicating whether the job was completed successfully.
   */
  def newEmbeddings(conf: Job): Boolean =
    val job = Job.getInstance(conf.getConfiguration)

    val config = ConfigFactory.load().resolve()
    val inputPath = config.getString("training-conf.data-sample_output_dir")
    val outputPath = config.getString("training-conf.embedding_output_dir")
    val vocabPath = config.getString("training-conf.vocab_output_dir")
    val baseDir = config.getString("job-conf.base_output_dir")

    shardFile(baseDir, "samples", 2, job.getConfiguration)

    // Set Mapper and Reducer Output Types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    // Set Mapper and Reducer Classes
    job.setMapperClass(classOf[Word2VecMapper])
    job.setReducerClass(classOf[Word2VecReducer])

    // Set Input/Output Format Types
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])

    // Set job configuration parameters
    job.getConfiguration.setInt("window_size", config.getInt("window-conf.size"))
    job.getConfiguration.setInt("epochs", config.getInt("training-conf.epochs"))
    job.getConfiguration.setInt("embeddingDim", config.getInt("training-conf.embeddingDim"))

    try {
      runJob(job, "Word2Vec", baseDir, outputPath)
    } finally {
       cleanupShards(baseDir, "samples", job.getConfiguration)
    }

