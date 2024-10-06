import org.scalatest.funsuite.AnyFunSuite
import com.mapreduce.task.DataSamples.SlidingWindowMapper
import com.mapreduce.task.utils.VectorWritable

class Example extends AnyFunSuite {

  test("generate_samples should generate correct sliding windows") {
    val tokens = Array(1, 2, 3, 4, 5)
    val windowSize = 3
    val stride = 1
    val expected = Array(
      Array(1.0f, 2.0f, 3.0f),
      Array(2.0f, 3.0f, 4.0f),
      Array(3.0f, 4.0f, 5.0f)
    )

    val result = SlidingWindowMapper.generate_samples(tokens, windowSize, stride)

    assert(result.length == expected.length && result.zip(expected).forall { case (a, b) =>
      a.length == b.length && a.zip(b).forall { case (x, y) => x == y }
    })
  }

  test("generate_samples should pad the last window if necessary") {
    val tokens = Array(1, 2)
    val windowSize = 3
    val stride = 1
    val expected = Array(
      Array(1.0f, 2.0f, 0.0f) // Padding with the default pad_token 0
    )

    val result = SlidingWindowMapper.generate_samples(tokens, windowSize, stride)
    assert(result.length == expected.length && result.zip(expected).forall { case (a, b) =>
      a.length == b.length && a.zip(b).forall { case (x, y) => x == y }
    })
  }

  test("generate_samples should return an empty array for an empty input") {
    val tokens = Array.empty[Int]
    val windowSize = 3
    val stride = 1
    val expected = Array.empty[Array[Float]]

    val result = SlidingWindowMapper.generate_samples(tokens, windowSize, stride)
    assert(result.length == expected.length && result.zip(expected).forall { case (a, b) =>
      a.length == b.length && a.zip(b).forall { case (x, y) => x == y }
    })
  }

  test("toString should correctly format vector values") {
    val vector = new VectorWritable(Array(1.0f, 2.5f, 3.7f))
    assert(vector.toString === "1.0;2.5;3.7")

    val emptyVector = new VectorWritable(Array[Float]())
    assert(emptyVector.toString === "")
  }

  test("compareTo should correctly compare vector magnitudes") {
    val vector1 = new VectorWritable(Array(1.0f, 1.0f)) // magnitude = 1
    val vector2 = new VectorWritable(Array(2.0f, 2.0f)) // magnitude = 4
    val vector3 = new VectorWritable(Array(1.0f, 1.0f)) // magnitude = 1

    assert(vector1.compareTo(vector2) === -1) // vector1 < vector2
    assert(vector2.compareTo(vector1) === 1) // vector2 > vector1
    assert(vector1.compareTo(vector3) === 0) // vector1 == vector3
  }

  test("divide should correctly divide vector by scalar") {
    val vector = new VectorWritable(Array(2.0f, 4.0f, 6.0f))
    val result = vector.divide(2)

    assert(result.values.length === 3)
    assert(result.values(0) === 1.0f)
    assert(result.values(1) === 2.0f)
    assert(result.values(2) === 3.0f)
  }

  test("add should correctly add two vectors of same length") {
    val vector1 = new VectorWritable(Array(1.0f, 2.0f, 3.0f))
    val vector2 = new VectorWritable(Array(4.0f, 5.0f, 6.0f))
    val result = vector1.add(vector2)

    assert(result.values.length === 3)
    assert(result.values(0) === 5.0f)
    assert(result.values(1) === 7.0f)
    assert(result.values(2) === 9.0f)
  }

  test("add should handle null values array") {
    val vector1 = new VectorWritable(null)
    val vector2 = new VectorWritable(Array(1.0f, 2.0f, 3.0f))
    val result = vector1.add(vector2)

    assert(result.values.length === 3)
    assert(result.values.sameElements(vector2.values))
  }

  test("add should return original values when lengths don't match") {
    val vector1 = new VectorWritable(Array(1.0f, 2.0f))
    val vector2 = new VectorWritable(Array(1.0f, 2.0f, 3.0f))
    val result = vector1.add(vector2)

    assert(result.values.length === 2)
    assert(result.values.sameElements(vector1.values))
  }

  test("average calculation of two vectors should be correct") {
    val vector1 = new VectorWritable(Array(2.0f, 4.0f, 6.0f))
    val vector2 = new VectorWritable(Array(4.0f, 8.0f, 12.0f))

    // Average = (vector1 + vector2) / 2
    val sum = vector1.add(vector2)
    val average = sum.divide(2)

    assert(average.values.length === 3)
    assert(average.values(0) === 3.0f)
    assert(average.values(1) === 6.0f)
    assert(average.values(2) === 9.0f)
  }
}