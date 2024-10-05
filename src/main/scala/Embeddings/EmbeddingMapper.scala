package Embeddings

import utils.VectorWritable
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.deeplearning4j.models.word2vec.*
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}
import org.deeplearning4j.models.sequencevectors.interfaces.SequenceIterator
import org.deeplearning4j.models.sequencevectors.sequence.Sequence
import org.deeplearning4j.text.documentiterator.*
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory

import java.io.InputStream

object Embeddings:
//  class BPESequenceIterator(context: Mapper[LongWritable, Text, Text, Text]#Context)
//    extends SequenceIterator[VocabWord] {
//
//    private var currentSequence: Seq[VocabWord] = null
//    private var moreSequences = true
//
//    // Convert BPE token ID to VocabWord
//    private def tokenToVocabWord(tokenId: Int): VocabWord = {
//      val word = new VocabWord(1.0, String.valueOf(tokenId))
//      word.setElementFrequency(1)
//      word
//    }
//
//    override def hasMoreSequences(): Boolean = {
//      if (currentSequence != null) {
//        return true
//      }
//
//      try {
//        val key = context.getCurrentKey
//        val value = context.getCurrentValue
//
//        if (value != null) {
//          // Assuming each line is a space-separated sequence of BPE token IDs
//          val tokens = value.toString.trim.split(";").map(_.toInt)
//
//          if (tokens.nonEmpty) {
//            currentSequence = tokens.map(tokenToVocabWord).toSeq
//            true
//          } else {
//            false
//          }
//
//        } else {
//          moreSequences = false
//          false
//        }
//      } catch {
//        case _: Exception =>
//          moreSequences = false
//          false
//      }
//    }
//
//    override def nextSequence(): Sequence[VocabWord] = {
//      if (!hasMoreSequences()) {
//        return null
//      }
//
//      val sequence = new Sequence[VocabWord]
//      currentSequence.foreach(word => sequence.addElement(word))
//
//      currentSequence = null
//      sequence
//    }
//
//    override def reset(): Unit = {
//      // Reset not supported in Hadoop streaming context
//    }
//  }

  class HadoopLineIterator(context: Mapper[LongWritable, Text, Text, Text]#Context)
    extends SentenceIterator {

    private var currentLine: String = null
    private var hasMoreLines = true
    private var sentencePreProcessor: SentencePreProcessor = null

    override def nextSentence(): String = {
      if (!hasNext()) {
        return null
      }
      val line = currentLine
      currentLine = null
      line
    }

    override def hasNext(): Boolean = {
      if (currentLine != null) {
        return true
      }
      // Try to get next line from context
      try {
        val value = context.getCurrentValue

        if (value != null) {
          val tokenIDs = value.toString.split("\t").last
          currentLine = tokenIDs.split(";").mkString(" ")
          true
        } else {
          hasMoreLines = false
          false
        }
        context.nextKeyValue()
      } catch {
        case _: Exception =>
          hasMoreLines = false
          false
      }
    }

    override def getPreProcessor: SentencePreProcessor = sentencePreProcessor

    override def setPreProcessor(preProcessor: SentencePreProcessor): Unit = {
      sentencePreProcessor = preProcessor
    }

    override def reset(): Unit = {
      // Cleanup code if needed
    }

    override def finish(): Unit = {
      // Cleanup if needed
    }
  }

  class Word2VecMapper extends Mapper[LongWritable, Text, Text, Text] {
    private var word2Vec: Word2Vec = null

    // TODO: Include Configuration Parameters
    override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      // Initialize Word2Vec in setup
      val iterator = new HadoopLineIterator(context)
      val tokenizerFactory = new DefaultTokenizerFactory()

      word2Vec = new Word2Vec.Builder()
        .minWordFrequency(1)
        .iterations(10)
        .layerSize(100)
        .seed(42)
        .windowSize(10)
        .iterate(iterator)
        .tokenizerFactory(tokenizerFactory)
        .build()

      // Train Word2Vec on this chunk of data
      word2Vec.fit()

      // Output the learned embeddings
      val vocab = word2Vec.getVocab.words()
      vocab.forEach { word =>
        val embedding = word2Vec.getWordVector(word).map(_.toFloat).mkString(";")
        val similarWords = word2Vec.wordsNearest(word, 5).toArray().mkString(";")
        // val embeddingStr = embedding.mkString(",")
        context.write(new Text(word), new Text(s"${embedding}-${similarWords}"))
      }
    }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    }

    override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      if (word2Vec != null) {
        // Save model or perform cleanup
      }
    }
  }
