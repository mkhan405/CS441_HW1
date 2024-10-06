package com.mapreduce.task.Embeddings

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.*
import org.deeplearning4j.models.word2vec.*
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory

import java.io.InputStream

object Embeddings:
/**
 * HadoopLineIterator is a custom implementation of the SentenceIterator interface
 * designed for use with the Word2Vec model in Deeplearning4j.
 * It iterates over lines of text provided by a Hadoop Mapper context, converting
 * those lines into sentences suitable for training the Word2Vec model.
 *
 * @param context The Mapper context providing access to the key-value pairs emitted
 *                by the Mapper during Hadoop job execution.
 */
  class HadoopLineIterator(context: Mapper[LongWritable, Text, Text, Text]#Context)
    extends SentenceIterator {

    // The current line being processed
    private var currentLine: String = null

    // Flag indicating whether more lines are available for processing
    private var hasMoreLines = true

    // Pre-processor for sentences, can be used to modify sentences before they're returned
    private var sentencePreProcessor: SentencePreProcessor = null

    /**
     * Retrieves the next sentence from the input.
     *
     * @return The next sentence as a String, or null if there are no more sentences.
     */
    override def nextSentence(): String = {
      // Check if there are more sentences to process
      if (!hasNext()) {
        return null
      }
      // Store the current line and reset the currentLine for the next call
      val line = currentLine
      currentLine = null
      line
    }

    /**
     * Checks if there are more sentences available for processing.
     *
     * @return true if there is another sentence to process, false otherwise.
     */
    override def hasNext(): Boolean = {
      // Return true if currentLine already has a value
      if (currentLine != null) {
        return true
      }
      // Try to get the next line from the Mapper context
      try {
        val value = context.getCurrentValue

        // If a value is present, process it
        if (value != null) {
          // Extract the token IDs from the value, convert them into a space-separated string
          val tokenIDs = value.toString.split("\t").last
          currentLine = tokenIDs.split(";").mkString(" ")
          true
        } else {
          // No more lines to read
          hasMoreLines = false
          false
        }
        // Move to the next key-value pair in the context
        context.nextKeyValue()
      } catch {
        case _: Exception =>
          // Handle any exceptions that occur and set hasMoreLines to false
          hasMoreLines = false
          false
      }
    }

    /**
     * Retrieves the current sentence pre-processor.
     *
     * @return The current SentencePreProcessor, or null if none is set.
     */
    override def getPreProcessor: SentencePreProcessor = sentencePreProcessor

    /**
     * Sets a SentencePreProcessor to modify sentences before they are returned.
     *
     * @param preProcessor The SentencePreProcessor to be set.
     */
    override def setPreProcessor(preProcessor: SentencePreProcessor): Unit = {
      sentencePreProcessor = preProcessor
    }

    /**
     * Resets the iterator to its initial state.
     * This method can contain cleanup code if necessary.
     */
    override def reset(): Unit = {
      // Cleanup code if needed
    }

    /**
     * Finalizes the iterator and releases any resources.
     * This method can contain cleanup code if necessary.
     */
    override def finish(): Unit = {
      // Cleanup if needed
    }
  }

  /**
   * Word2VecMapper is a Hadoop Mapper that trains a Word2Vec model using input text data.
   * The mapper processes text data in chunks and learns word embeddings based on the specified parameters.
   */
  class Word2VecMapper extends Mapper[LongWritable, Text, Text, Text] {

    /**
     * Setup method is called once at the beginning of the mapping process.
     * It initializes the Word2Vec model using configuration parameters and prepares the data for training.
     *
     * @param context The Mapper context providing access to job configuration and input data.
     */
    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      // Initialize Word2Vec in setup
      val iterator = new HadoopLineIterator(context) // Create an iterator to process lines from the context
      val tokenizerFactory = new DefaultTokenizerFactory() // Tokenizer for processing input text
      val epochs = context.getConfiguration.getInt("epochs", 10) // Number of training epochs
      val embeddingDim = context.getConfiguration.getInt("embeddingDim", 20) // Dimension of word embeddings
      val windowSize = context.getConfiguration.getInt("window_size", 5) // Window size for context words

      // Build and initialize the Word2Vec model with specified parameters
      val word2Vec = new Word2Vec.Builder()
        .minWordFrequency(1) // Minimum word frequency for inclusion in the model
        .iterations(epochs) // Number of training iterations
        .layerSize(embeddingDim) // Size of the word vector
        .seed(42) // Seed for random number generator
        .windowSize(windowSize) // Context window size
        .iterate(iterator) // Set the input iterator for training
        .tokenizerFactory(tokenizerFactory) // Set the tokenizer for the model
        .build() // Build the Word2Vec model

      // Train Word2Vec on this chunk of data
      word2Vec.fit() // Fit the model to the training data

      // Output the learned embeddings
      val vocab = word2Vec.getVocab.words() // Get the vocabulary learned by the model
      vocab.forEach { word =>
        // For each word in the vocabulary, get its embedding and similar words
        val embedding = word2Vec.getWordVector(word).map(_.toFloat).mkString(";") // Convert embedding to a string
        val similarWords = word2Vec.wordsNearest(word, 5).toArray().mkString(";") // Find the nearest words
        // Output the word and its corresponding embedding and similar words to context
        context.write(new Text(word), new Text(s"${embedding}-${similarWords}")) // Write output to context
      }
    }

    /**
     * The map method processes input key-value pairs.
     * In this implementation, the map function does not perform any operations.
     *
     * @param key The input key (offset of the line in the input file).
     * @param value The input value (text of the line).
     * @param context The Mapper context for outputting key-value pairs.
     */
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      // No mapping operations are defined in this implementation
    }
  }
