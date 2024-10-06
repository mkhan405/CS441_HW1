package com.mapreduce.task.utils

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.{DataInput, DataOutput, IOException}
import java.util
import scala.jdk.CollectionConverters.*

/**
 * A writable class that represents a vector of floats. This class implements
 * the Writable and WritableComparable interfaces to support serialization and
 * comparison operations in a Hadoop environment.
 *
 * @param values An array of float values representing the vector components.
 */
class VectorWritable(var values: Array[Float]) extends Writable, WritableComparable[VectorWritable] {

  // Default constructor that initializes the vector with null values
  def this() = this(null)

  /**
   * Constructs a VectorWritable instance from a given array of floats,
   * allowing specification of a start and end position to copy elements.
   *
   * @param v        The source array of floats from which to copy values.
   * @param startPos The starting index in the source array to begin copying.
   * @param endPos   The ending index in the source array to stop copying (exclusive).
   */
  def this(v: Array[Float], startPos: Int = 0, endPos: Int = -1) = {
    this()
    val defaultEndPos = v.length
    // Initialize the values array with the appropriate length
    values = new Array[Float](v.length)
    // Copy values from the source array to the values array
    Array.copy(v, startPos, values, startPos, (if (endPos != -1) endPos else defaultEndPos))
  }

  /**
   * Adds another VectorWritable to this one and returns a new VectorWritable
   * containing the result.
   *
   * @param v The VectorWritable to add to this instance.
   * @return A new VectorWritable instance containing the sum of this vector
   *         and the provided vector.
   */
  def add(v: VectorWritable): VectorWritable = {
    // Create a new array to store the result values
    val resultValues = if (this.values == null) {
      val newValues = new Array[Float](v.values.length)
      Array.copy(v.values, 0, newValues, 0, v.values.length)
      newValues
    } else if (v.values.length == values.length) {
      // Element-wise addition of the two vectors
      values.zip(v.values).map { case (a, b) => a + b }
    } else {
      // Return original values if lengths don't match
      values
    }

    // Return a new VectorWritable containing the result values
    new VectorWritable(resultValues)
  }

  /**
   * Divides each component of the vector by a given integer.
   *
   * @param c The divisor.
   * @return A new VectorWritable instance with each value divided by c.
   */
  def divide(c: Int): VectorWritable = {
    // Divide each element of the values array by the specified integer
    val resultValues = values.map(v => v / c)
    new VectorWritable(resultValues)
  }

  /**
   * Reads the vector values from the provided DataInput stream.
   *
   * @param in The input stream to read values from.
   */
  override def readFields(in: DataInput): Unit = {
    // Read the size of the array from the input stream
    val size: Int = in.readInt()
    // Initialize the values array and read float values
    values = Array.fill(size)(in.readFloat())
  }

  /**
   * Writes the vector values to the provided DataOutput stream.
   *
   * @param out The output stream to write values to.
   */
  override def write(out: DataOutput): Unit = {
    // Write the length of the values array
    out.writeInt(values.length)
    // Write each float value to the output stream
    values.foreach(v => out.writeFloat(v))
  }

  /**
   * Returns a string representation of the vector values,
   * with values separated by semicolons.
   *
   * @return A string representation of the vector.
   */
  override def toString: String = values.mkString(";")

  /**
   * Compares this VectorWritable to another VectorWritable based on their
   * magnitudes.
   *
   * @param o The VectorWritable to compare against.
   * @return A negative integer, zero, or a positive integer as this vector's
   *         magnitude is less than, equal to, or greater than the specified
   *         vector's magnitude.
   */
  override def compareTo(o: VectorWritable): Int = {
    // Calculate the magnitude of the vectors
    val magnitude = (v: VectorWritable) => v.values.reduce((a, b) => math.pow(a, 2.0).toFloat * math.pow(b, 2.0).toFloat)
    val x = magnitude(this)
    val y = magnitude(o)

    // Compare the magnitudes and return appropriate values
    if (x == y)
      0
    else if (x < y)
      -1
    else
      1
  }
}
