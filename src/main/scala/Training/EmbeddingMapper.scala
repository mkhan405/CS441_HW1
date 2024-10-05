package Training

import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.slf4j.{Logger, LoggerFactory}
import org.deeplearning4j.models.word2vec
import org.deeplearning4j.nn.conf.layers.{EmbeddingLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import utils.{VectorWritable, encoding}

import java.io.{BufferedReader, File, FileReader, IOException}
import java.{lang, util}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

object EmbeddingMapper:
  class Map extends Mapper[LongWritable, Text, Text, VectorWritable]:
    private val runtime: Runtime = Runtime.getRuntime
    private val log: Logger = LoggerFactory.getLogger("EmbeddingMapper")
    // Configuration parameters to be set
    private var (embeddingDim, epochs) = (0,0)
    private var vocabSize: Long = 0
    // Embedding Model Instance
    private var model: MultiLayerNetwork = null
    // Input Shard File Name
    private var inputFilename = ""

    @throws[IOException]
    override def setup(context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit = {
      log.info("Entered setup")
      log.info(s"Runtime max memory: ${runtime.maxMemory()}")
      log.info(s"Runtime free memory: ${runtime.freeMemory()}")
      log.info(s"Runtime total memory: ${runtime.totalMemory()}")
      log.info(s"Runtime used memory: ${runtime.totalMemory() - runtime.freeMemory()}")
      log.info(s"Runtime used memory = Runtime total memory - Runtime free memory")

      val fileSplit = context.getInputSplit.asInstanceOf[FileSplit]
      inputFilename = fileSplit.getPath.getName
      log.info(s"Processing file: ${inputFilename}")

      log.info("Retrieving configuration parameters")
      val conf: Configuration = context.getConfiguration
      vocabSize = conf.getLong("vocabSize", 10)
      embeddingDim = conf.getInt("embeddingDim", 500)
      epochs = conf.getInt("epochs", 100)

      log.info("Initializing embedding model")
      // Setup model configuration
      val layerConfig: MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
        .list()
        .layer(new EmbeddingLayer.Builder()
          .nIn(vocabSize)
          .nOut(embeddingDim)
          .activation(Activation.IDENTITY)
          .build())
        .layer(new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
          .nOut(vocabSize)
          .activation(Activation.SOFTMAX)
          .build())
        .build()

      // Initialize model
      model = new MultiLayerNetwork(layerConfig)
      model.init()
      log.info("Embedding model - INITIALIZED")
    }

    @throws[IOException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text,
      VectorWritable]#Context): Unit = {
      val sentenceInput = value.toString.split("\t").last
      val sentences = sentenceInput.split(":")
      val tokenized_sentences = sentences.map(s => s.split(";").map(t => t.toFloat))
      // Generate pairwise mappings to get input/output features (e.g. (tokenized_sentences[0], tokenized_sentences[1])
      // (tokenized_sentences[1], tokenized_sentences[2])
      log.info("Extracting input/output features")
      val featureSamples = tokenized_sentences.sliding(2, 1).toArray
      val inputs = featureSamples.map(f => f.head)
      val outputs = featureSamples.map(f => f.last)
      val inputFeatures: INDArray = Nd4j.create(inputs)
      val outputLabels: INDArray = Nd4j.create(outputs)
      // Train model
      log.info("Beginning Training Model with Input/Output Features")
      // For-Loop used to start model training
      for (i <- 0 to epochs) do
        model.fit(inputFeatures, outputLabels)
    }

    @throws[IOException]
    override def cleanup(context: Mapper[LongWritable, Text, Text, VectorWritable]#Context): Unit = {
      log.info("Model training completed")
      log.info("Starting Token Embedding Computation")
      val cachedFiles = context.getCacheFiles
      if (cachedFiles != null && cachedFiles.nonEmpty) {
        val vocabFile = cachedFiles.find(_.getPath.contains("vocab")).get
        log.info(s"Reading Vocabulary File at ${vocabFile.getPath}")

        val reader = new BufferedReader(new FileReader(vocabFile.getPath))

        try {
          // Reading over file line-by-line using while loop
          var word = reader.readLine()
          while (word != null) {
            // Generate BPE encoding
            val tokenized_word = word.split(" ").flatMap(w => utils.encoding.encode(w).toArray.map(_.toFloat))
            log.debug(tokenized_word.mkString(" "))
            try {
              // Get Embedding Prediction
              val embedding = model.predict(Nd4j.create(tokenized_word)).map(_.toFloat)
              // Send to reducer
              context.write(new Text(word), new VectorWritable(embedding))
            } catch {
              case e: Exception =>
                log.error(s"Error processing word '$word': ${e.getMessage}")
            } finally {
              // Grab next word in vocabulary
              word = reader.readLine()
            }
          }
          log.info(s"Embeddings generated for shard: ${inputFilename}")
        } finally {
          reader.close()
        }

      } else {
        log.error("No cache files found")
      }
    }
