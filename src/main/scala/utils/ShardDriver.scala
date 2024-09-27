package utils

import java.nio.file.{Files, Paths}
import java.io.{FileInputStream, FileOutputStream}

object ShardDriver {
  def shardFile(baseDir: String, filename: String, blockSize: Int): Int = {
    val inputPath = Paths.get(baseDir, filename)
    val inputFile = inputPath.toFile

    if (!inputFile.exists())
      throw new IllegalArgumentException(s"Input file does not exist: $baseDir/$inputFile")

    val fileSize = inputFile.length()
    val numShards = Math.ceil(fileSize.toDouble / blockSize).toInt

    using(new FileInputStream(inputFile)) { inputStream =>
      for (shardIndex <- 0 until numShards) {
        val outputFilename = s"${filename}_shard_$shardIndex"
        val outputPath = Paths.get(baseDir, outputFilename)

        using(new FileOutputStream(outputPath.toFile)) { outputStream =>
          val buffer = new Array[Byte](blockSize)
          val bytesRead = inputStream.read(buffer)

          if (bytesRead > 0)
            outputStream.write(buffer, 0, bytesRead)

        }
      }
    }
    numShards
  }

  def cleanupShards(baseDir: String, filename: String): Unit = {
    val directory = Paths.get(baseDir)
    if (!Files.isDirectory(directory))
        throw new IllegalArgumentException(s"Invalid directory: ${directory}")

    val shardPrefix = s"${filename}_shard_"
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