package utils

import org.apache.hadoop.conf.Configuration

import java.nio.file.{Files, Paths}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.{FileInputStream, FileOutputStream, PrintWriter}
import scala.io.Source

object ShardDriver {
  def shardFile(baseDir: String, filename: String, numShards: Int): Unit = {
    val inputPath = Paths.get(baseDir, filename)
    val inputFile = inputPath.toFile

    if (!inputFile.exists())
      throw new IllegalArgumentException(s"Input file does not exist: $baseDir/$inputFile")

    val fileSize = inputFile.length()
    val blockSize = Math.ceil(fileSize / numShards).toInt
//    val numShards = Math.ceil(fileSize.toDouble / blockSize).toInt

    using(new FileInputStream(inputFile)) { inputStream =>
      for (shardIndex <- 0 until numShards) {
        val outputFilename = s"shard_$shardIndex"
        val outputPath = Paths.get(baseDir, outputFilename)

        using(new FileOutputStream(outputPath.toFile)) { outputStream =>
          val buffer = new Array[Byte](blockSize)
          val bytesRead = inputStream.read(buffer)

          if (bytesRead > 0)
            outputStream.write(buffer, 0, bytesRead)

        }
      }
    }
  }

  def merge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path,
            deleteSource: Boolean, conf: Configuration): Boolean = {
    if (srcFS.getFileStatus(srcDir).isDirectory) {
      val outputFile = dstFS.create(dstFile)
      try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile = srcFS.open(status.getPath)
              try {IOUtils.copyBytes(inputFile, outputFile, conf, false)}
              finally {inputFile.close()}
          }
        if (deleteSource) srcFS.delete(srcDir, true) else true
      }
    } else false


  }

  def cleanupShards(baseDir: String, filename: String): Unit = {
    val directory = Paths.get(baseDir)
    if (!Files.isDirectory(directory))
        throw new IllegalArgumentException(s"Invalid directory: ${directory}")

    val shardPrefix = s"shard_"
    Files.list(directory)
      .filter(path => path.getFileName.toString.startsWith(shardPrefix))
      .forEach(Files.delete)
  }

  private def using[A <: AutoCloseable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }
}