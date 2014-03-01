package com.eklavya.scqla

import akka.util.{ByteStringBuilder, ByteString, ByteIterator}
import java.util.{UUID => uu}
import java.net.InetAddress
import com.eklavya.scqla.Header._
import scala.Some

/**
 * Created by eklavya on 13/2/14.
 */
object Frame {
  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def readString(it: ByteIterator): String = {
    val n = new Array[Byte](it.getShort)
    it.getBytes(n)
    ByteString(n).utf8String
  }

  def writeString(s: String): ByteString = {
    if (s.length > 65535) println("String larger than capacity, going to be truncated.")
    val builder = new ByteStringBuilder
    builder.putShort(s.length).append(ByteString.fromString(s))
    builder.result
  }

  def readBytes(it: ByteIterator): Option[ByteString] = {
    val n = it.getInt
    if (n >= 0) {
      val na = new Array[Byte](n)
      it.getBytes(na)
      Some(ByteString(na))
    } else {
      None
    }
  }

  def writeBytes(ba: Array[Byte]): ByteString = {
    val b = new ByteStringBuilder
    b.putShort(ba.length)
    b.putBytes(ba)
    b.result
  }

  def readShortBytes(it: ByteIterator): Option[ByteString] = {
    val n = it.getShort
    if (n >= 0) {
      val na = new Array[Byte](n)
      it.getBytes(na)
      Some(ByteString(na))
    } else {
      None
    }
  }

  def readType(option: Short, it: ByteIterator) = {
    option match {
      case CUSTOM | BLOB => readBytes(it)

      case ASCII | TEXT | VARCHAR => readBytes(it).map(_.utf8String)

      case BIGINT | VARINT => readBytes(it).map(_.iterator.getLong)

      case BOOLEAN => readBytes(it).map(bs => if (bs.iterator.getByte == 0) false else true)

      case TIMESTAMP | COUNTER => readBytes(it).map(_.utf8String.toLong)

      case DECIMAL => readBytes(it).map(bs => BigDecimal(bs.toArray.map(_.toChar)))

      case DOUBLE => readBytes(it).map(_.iterator.getDouble)

      case FLOAT => readBytes(it).map(_.iterator.getFloat)

      case INT => readBytes(it).map(_.iterator.getInt)

      case UUID | TIMEUUID => readBytes(it).map(bs => new uu(bs.iterator.getLong, bs.iterator.getLong))

      case INET => readBytes(it).map { bs =>
        val it = bs.iterator
        val ip = it.getByte match {
          case 4 =>
            val i = new Array[Byte](4)
            it.getBytes(i)
            InetAddress.getByAddress(i)
          case 16 =>
            val i = new Array[Byte](16)
            it.getBytes(i)
            InetAddress.getByAddress(i)
        }
        val port = it.getInt
        (ip, port)
      }

      case LIST | SET => readBytes(it).map { bs =>
        val it = bs.iterator
        val num = it.getShort
        (0 until num).map(_ => readShortBytes(it))
      }

      case MAP => readBytes(it).map { bs =>
        val it = bs.iterator
        val num = it.getShort
        (0 until num).map(i => readShortBytes(it) -> readShortBytes(it)).toMap
      }
    }
  }

  def writeType(option: Short, param: Any, builder: ByteStringBuilder) = {
    option match {
      case CUSTOM | BLOB => builder.append(writeBytes(param.asInstanceOf[Array[Byte]]))

      case ASCII | TEXT | VARCHAR => val b = ByteString(param.asInstanceOf[String]);builder.putInt(b.length).append(b)

      case BIGINT | VARINT => builder.putInt(8).putLong(param.asInstanceOf[Long])

      case BOOLEAN => builder.putInt(1).putByte({if (param.asInstanceOf[Boolean]) 1 else 0})

      case TIMESTAMP | COUNTER => builder.append(writeBytes(param.toString.toCharArray.map(_.toByte)))

      case DECIMAL => builder.append(writeBytes(param.toString.toCharArray.map(_.toByte)))

      case DOUBLE => builder.putInt(8).putDouble(param.asInstanceOf[Double])//append(writeBytes(param.asInstanceOf[Double].toString.toCharArray.map(_.toByte)))

      case FLOAT => builder.append(writeBytes(param.asInstanceOf[Float].toString.toCharArray.map(_.toByte)))

      case INT => builder.putInt(4).putInt(param.asInstanceOf[Int])

      case UUID | TIMEUUID =>
        val uuid = param.asInstanceOf[uu]
        builder.putInt(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getMostSignificantBits)

    }
  }


  def readList(it: ByteIterator) = {
    val num = it.getShort
    (0 until num).map(_ => readString(it))
  }

  def readMultiMap(it: ByteIterator) = {
    val num = it.getShort
    (0 until num).map { i =>
      readString(it) -> readList(it)
    }.toMap
  }

  def startupFrame(stream: Byte): ByteString = {
    val builder = new ByteStringBuilder
    val mapLen: Short = 1
    val cqls = "CQL_VERSION"
    val version = "3.0.0"
    builder.putByte(VERSION).putByte(FLAGS).putByte(stream).putByte(STARTUP)
    val body = new ByteStringBuilder
    body.putShort(mapLen).putShort(cqls.length).
      append(ByteString.fromString(cqls)).putShort(version.length).
      append(ByteString.fromString(version))
    builder.putInt(body.length) ++= body.result
    builder.result
  }

  def credentialsFrame(creds: Map[String, String], stream: Byte): ByteString = {
    val builder = new ByteStringBuilder
    builder.putShort(creds.size)
    creds.foreach { case(k, v) => builder.append(writeString(k)).append(writeString(v))}
    builder.result
  }

  def optionsFrame(stream: Byte): ByteString = {
    ByteString(Array[Byte](VERSION, FLAGS, stream, OPTIONS, 0, 0, 0, 0))
  }

  def queryFrame(q: String, stream: Byte, consistency: Short): ByteString = {
    val builder = new ByteStringBuilder
    val length = q.length
    builder.putByte(VERSION).putByte(FLAGS).putByte(stream).putByte(QUERY)
    val body = new ByteStringBuilder
    body.putInt(length).append(ByteString.fromString(q)).putShort(consistency)
    builder.putInt(body.length) ++= body.result
    builder.result
  }

  def prepareFrame(q: String, stream: Byte): ByteString = {
    val builder = new ByteStringBuilder
    val length = q.length
    builder.putByte(VERSION).putByte(FLAGS).putByte(stream).putByte(PREPARE)
    val body = new ByteStringBuilder
    body.putInt(length).append(ByteString.fromString(q))
    builder.putInt(body.length) ++= body.result
    builder.result
  }

  def executeFrame(b: ByteString, stream: Byte, consistency: Short): ByteString = {
    val builder = new ByteStringBuilder
    builder.putByte(VERSION).putByte(FLAGS).putByte(stream).putByte(EXECUTE)
    val body = new ByteStringBuilder
    body.append(b)
    body.putShort(consistency)
    builder.putInt(body.length) ++= body.result
    builder.result
  }
}
