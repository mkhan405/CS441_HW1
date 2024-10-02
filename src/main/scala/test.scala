import org.deeplearning4j.text.sentenceiterator.{BasicLineIterator, SentenceIterator}
import utils.encoding
import org.deeplearning4j.text.tokenization.tokenizerfactory.*
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.*

@main def test() =
  val factory = new DefaultTokenizerFactory()
  factory.setTokenPreProcessor(new CommonPreprocessor())
  val iter = new BasicLineIterator("/home/shayan283/IdeaProjects/CS441_HW1/src/main/resources/input/textbook.txt")
  while(iter.hasNext) {

  }
//  val tokens_together = encoding.encode("abc abc abc").toArray
//  val tokens_sep = "abc abc abc".split(" ").map((t => encoding.encode(t).toArray))
//
//  print(tokens_together.mkString("Array(", ", ", ")"))
//  println()
//  tokens_sep.foreach { row => row foreach print; println}
