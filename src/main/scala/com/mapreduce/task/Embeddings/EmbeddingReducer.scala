package com.mapreduce.task.Embeddings

import com.knuddels.jtokkit.api.IntArrayList
import com.mapreduce.task.utils
import com.mapreduce.task.utils.{VectorWritable, encoding}
import com.mapreduce.task.utils
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.*
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.jdk.CollectionConverters.*


object EmbeddingReducer {
  val logger: Logger = LoggerFactory.getLogger("EmbeddingReducer") // Logger for the reducer

  /**
   * Word2VecReducer processes the output from the Word2VecMapper.
   * The reducer takes a unique word as the key and a list of strings as values, where each string
   * contains an embedding and its corresponding similar words.
   */
  class Word2VecReducer extends Reducer[Text, Text, Text, NullWritable] {
    /**
     * The reduce method aggregates the embeddings and identifies the most frequent similar words.
     *
     * @param key The unique word from the original text.
     * @param values A list of strings where each string has the format:
     *               "x;y;z;a;b;c;d;e;f-similarWord1,similarWord2,..."
     * @param context The Reducer context used for outputting the results.
     */
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text,
      Text, NullWritable]#Context): Unit = {
      try {
        // Separate out the embedding vector and the list of similar words for each item in values
        val args = values.asScala.toArray.map(l => l.toString.split("-"))
        // Convert the list of embedding vectors to VectorWritable objects
        val vectorList = args.map(a => a.head).map(v => v.split(";")).map(v => v.map(_.toFloat))
          .map(v => new VectorWritable(v)) // Convert float arrays to VectorWritable

        val similarWords = args.map(a => a.last).flatMap(l => l.split(";")) // Extract similar words
        val frequencyMap = similarWords.groupBy(identity).view.mapValues(_.length) // Count occurrences
        // Build list of top 3 most frequent tokens from the list and join with ", "
        val topKMostFrequent = frequencyMap.toSeq.sortBy(-_._2).take(3).map(e => e._1).map(w => {
          val tempList = new IntArrayList() // Prepare for decoding
          tempList.add(w.toFloat.floor.toInt)
          utils.encoding.decode(tempList) // Decode to original token ID
        }).mkString(", ")
        logger.info(s"Most Frequent: ${topKMostFrequent}") // Log the most frequent similar words

        val tokenIDList = new IntArrayList() // Prepare list for token IDs
        tokenIDList.add(key.toString.toFloat.floor.toInt) // Add the key as a token ID
        val sumOfVectors = vectorList.reduce((a, b) => a.add(b)) // Sum the embedding vectors
        // Output the average vector and most frequent similar words to context
        context.write(new Text(s"${utils.encoding.decode(tokenIDList)},${sumOfVectors.divide(vectorList.length)},${topKMostFrequent}"), NullWritable.get())
      } catch {
        case e: Exception =>
          logger.error(s"Cannot decode key: ${key}") // Log error if decoding fails
      }
    }
  }
}