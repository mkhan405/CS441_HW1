package utils

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}

object VocabExtractor {
  def extractor(inputFilePath: String, outputFilePath: String): Unit = {
    // Using BufferedReader to read large files efficiently line-by-line
    val reader = new BufferedReader(new FileReader(inputFilePath))
    val writer = new BufferedWriter(new FileWriter(outputFilePath))

    try {
      var line: String = reader.readLine()
      while (line != null) {
        // Assuming the format is: word,frequency,tokenIDs
        val word = line.split("\t")(0)  // Extract the word (first column)

        // Write the word to the output file
        writer.write(word)
        writer.newLine()

        // Read the next line
        line = reader.readLine()
      }
    } finally {
      // Close resources
      reader.close()
      writer.close()
    }
  }
}