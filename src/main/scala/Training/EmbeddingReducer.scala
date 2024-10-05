//package Training
//import com.typesafe.config.ConfigFactory
//import org.apache.hadoop.conf.*
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.*
//import org.apache.hadoop.mapreduce.*
//import org.apache.hadoop.util.*
//import org.slf4j.{Logger, LoggerFactory}
//import utils.{VectorWritable, encoding}
//
//import java.io.IOException
//import java.{lang, util}
//import scala.jdk.CollectionConverters.*
//
//object EmbeddingReducer:
//  class Reduce extends Reducer[Text, VectorWritable, Text, VectorWritable]:
//    @throws[IOException]
//    override def reduce(key: Text, values: lang.Iterable[VectorWritable], context: Reducer[Text,
//      VectorWritable, Text, VectorWritable]#Context): Unit = {
//      val vectors = values.asScala.toList
//      val sumOfVectors = vectors.reduce((a, b) => a.add(b))
//      context.write(key, sumOfVectors.divide(vectors.length))
//    }
//
