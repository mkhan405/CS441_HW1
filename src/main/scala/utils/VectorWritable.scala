package utils

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}

import java.io.{DataInput, DataOutput, IOException}
import java.util
import scala.jdk.CollectionConverters.*


class VectorWritable(var values: Array[Float]) extends Writable {
  def this() = this(null)

  def this(v: Array[Float], startPos: Int = 0, endPos: Int = -1) = {
    this()
    val defaultEndPos = v.length
    values = new Array[Float](v.length)
    Array.copy(v, startPos, values, startPos, (if (endPos != -1) endPos else defaultEndPos))
  }

  def add(v: VectorWritable): Unit = {
    if (this.values == null) {
      values = new Array[Float](v.values.length)
      Array.copy(v.values, 0, this.values, 0, v.values.length)
    } else if (v.values.length == values.length) {
      values = values.zip(v.values).map((a, b) => a + b)
    }
  }

  def divide(c: Int): Unit = values = values.map(v => v / c)

  override def readFields(in: DataInput): Unit = {
    val size: Int = in.readInt()
    values = Array.fill(size)(in.readFloat())
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt(values.length)
    values.foreach(v => out.writeFloat(v))
  }
}