package com.mapreduce.task.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import java.net.URI
import scala.io.Source

object ShardDriver {
  /**
   * Shards a specified input file into multiple output files based on the given number of shards.
   *
   * This function reads the input file located at the specified base directory and splits it into
   * smaller files (shards) to facilitate parallel processing or reduce the size of individual files.
   * Each shard will contain approximately an equal amount of data, determined by the total size of
   * the input file divided by the number of shards.
   *
   * @param baseDir   The base directory where the input file is located and the output shards will be saved.
   * @param filename  The name of the input file to be sharded.
   * @param numShards The number of shards to create from the input file.
   *                  Must be greater than zero.
   * @param conf      The Hadoop Configuration object that contains the necessary settings
   *                  to access the file system (e.g., HDFS, local file system).
   * @throws IllegalArgumentException If the input file does not exist at the specified path.
   * @note The output files will be named as "shard_0", "shard_1", ..., "shard_N" where N is the
   *       number of shards created. The function will read the input file and write the shards
   *       to the same base directory specified.
   */
  def shardFile(baseDir: String, filename: String, numShards: Int, conf: Configuration): Unit = {

//    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
//    // For local testing
//    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    // For EMR Deployment
    // conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")


    // Create the input path by combining the base directory and the filename
    val inputPath = Path(baseDir, filename)

    // Get the FileSystem instance for the specified base directory and configuration
    val fs = FileSystem.get(new URI(baseDir), conf)

    // Check if the input file exists in the specified path
    if (!fs.exists(inputPath))
      throw new IllegalArgumentException(s"Input file does not exist: $baseDir/$filename")

    // Get the size of the input file
    val fileSize = fs.getFileStatus(inputPath).getLen

    // Calculate the block size for each shard based on the total file size and the number of shards
    val blockSize = Math.ceil(fileSize / numShards).toInt

    // Open an input stream to read from the input file
    val inputStream = fs.open(inputPath)

    try {
      // For loop used to repeat sharding computations numShard number of times
      // Iterate over each shard index to create and write to output files
      for (shardIndex <- 0 until numShards) {
        // Define the output filename for the current shard
        val outputFilename = s"shard_${shardIndex}"

        // Create the output path by combining the base directory and the output filename
        val outputPath = new Path(baseDir, outputFilename)

        // Create an output stream to write data to the output file
        val outputStream = fs.create(outputPath)
        try {
          // Create a buffer to hold bytes read from the input stream
          val buffer = new Array[Byte](blockSize)

          // Read bytes from the input stream into the buffer
          val bytesRead = inputStream.read(buffer)

          // If bytes were read, write them to the output stream
          if (bytesRead > 0)
            outputStream.write(buffer, 0, bytesRead)
        } finally {
          // Close the output stream after writing
          outputStream.close()
        }
      }
    } finally {
      // Close the input stream after all shards have been processed
      inputStream.close()
    }
  }

  /**
   * Merges all files in a specified source directory into a single destination file.
   *
   * This function checks if the source path is a directory. If it is, it creates a destination
   * file and copies the content of each file from the source directory into this destination file,
   * sorted by the file names. Optionally, the source directory can be deleted after the merge.
   *
   * @param srcFS        The FileSystem object representing the source file system (e.g., HDFS).
   * @param srcDir       The source directory containing files to be merged.
   * @param dstFS        The FileSystem object representing the destination file system (e.g., HDFS).
   * @param dstFile      The destination file path where the merged content will be written.
   * @param deleteSource A boolean flag indicating whether to delete the source directory after merging.
   * @param conf         The Hadoop Configuration object that contains the necessary settings to access the file system.
   * @return A boolean value indicating the success of the merge operation;
   *         returns true if the source is a directory and the merge is successful,
   *         returns false otherwise.
   */
  def merge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path,
            deleteSource: Boolean, conf: Configuration): Boolean = {
    // Check if the source directory exists and is indeed a directory
    if (srcFS.getFileStatus(srcDir).isDirectory) {
      // Create the output file in the destination file system
      val outputFile = dstFS.create(dstFile)
      try {
        // List the files in the source directory, sort them by name, and copy their contents
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            // Process only regular files
            case status if status.isFile =>
              // Open each input file for reading
              val inputFile = srcFS.open(status.getPath)
              try {
                // Copy the input file's content to the output file
                IOUtils.copyBytes(inputFile, outputFile, conf, false)
              } finally {
                // Ensure the input file is closed after copying
                inputFile.close()
              }
          }
        // Optionally delete the source directory if specified
        if (deleteSource) srcFS.delete(srcDir, true) else true
      } finally {
        // Close the output file after all files have been merged
        outputFile.close()
      }
    } else false // Return false if the source is not a directory
  }

  /**
   * Cleans up shard files in the specified base directory by deleting files that
   * start with the prefix "shard_".
   *
   * This function checks if the provided base directory exists and is a directory.
   * If valid, it lists all files in the directory and deletes those that match
   * the shard file naming convention.
   *
   * @param baseDir  The base directory where shard files are located.
   * @param filename The name of the original file (not used in this function,
   *                 but provided for context).
   * @param conf     The Hadoop Configuration object that contains the necessary settings
   *                 to access the file system.
   * @throws IllegalArgumentException If the specified base directory does not exist
   *                                  or is not a directory.
   */
  def cleanupShards(baseDir: String, filename: String, conf: Configuration): Unit = {
    // Create a Path object for the base directory
    val directory = new Path(baseDir)

    // Get the FileSystem instance for the specified base directory and configuration
    val fs = FileSystem.get(new URI(baseDir), conf)

    // Check if the directory exists and is indeed a directory
    if (!fs.exists(directory) || !fs.getFileStatus(directory).isDirectory)
      throw new IllegalArgumentException(s"Invalid directory: ${directory}")

    // Define the prefix used for shard files
    val shardPrefix = s"shard_"

    // List all files in the directory, filter those starting with the shard prefix,
    // and delete each matched file
    fs.listStatus(directory)
      .filter(status => status.getPath.getName.startsWith(shardPrefix))
      .foreach(status => fs.delete(status.getPath, false))
  }

}