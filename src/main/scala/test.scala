import utils.encoding

@main def test() =
  val tokens_together = encoding.encode("abc abc abc").toArray
  val tokens_sep = "abc abc abc".split(" ").map((t => encoding.encode(t).toArray))

  print(tokens_together.mkString("Array(", ", ", ")"))
  println()
  tokens_sep.foreach { row => row foreach print; println}
