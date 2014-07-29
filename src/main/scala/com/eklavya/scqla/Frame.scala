package com.eklavya.scqla

import java.net.InetAddress
import java.util.{UUID => uu}

import akka.util.{ByteIterator, ByteString, ByteStringBuilder}
import com.eklavya.scqla.Header._

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

  def readInet(it: ByteIterator): InetAddress = {
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
    ip
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

      case ASCII | TEXT | VARCHAR =>
        val b = ByteString(param.asInstanceOf[String]); builder.putInt(b.length).append(b)

      case BIGINT | VARINT => builder.putInt(8).putLong(param.asInstanceOf[Long])

      case BOOLEAN => builder.putInt(1).putByte({ if (param.asInstanceOf[Boolean]) 1 else 0 })

      case TIMESTAMP | COUNTER => builder.append(writeBytes(param.toString.toCharArray.map(_.toByte)))

      case DECIMAL => builder.append(writeBytes(param.toString.toCharArray.map(_.toByte)))

      case DOUBLE => builder.putInt(8).putDouble(param.asInstanceOf[Double]) //append(writeBytes(param.asInstanceOf[Double].toString.toCharArray.map(_.toByte)))

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

  def writeList(l: List[String]): ByteString = {
    val builder = new ByteStringBuilder
    builder.putShort(l.size)
    l.foreach(s => builder.append(writeString(s)))
    builder.result
  }

  def readMultiMap(it: ByteIterator) = {
    val num = it.getShort
    (0 until num).map { i =>
      readString(it) -> readList(it)
    }.toMap
  }

  def getOpt(it: ByteIterator): Array[Short] = {
    val opt = it.getShort
    opt match {
      case CUSTOM =>
        val n = new Array[Byte](it.getShort)
        it.getBytes(n)
        val s = ByteString(n)
        Array(opt) ++ s.toArray.map(_.toShort)

      case ASCII | BIGINT | BLOB | BOOLEAN |
        COUNTER | DECIMAL | DOUBLE | FLOAT |
        INT | TEXT | TIMESTAMP | UUID |
        VARCHAR | VARINT | TIMEUUID | INET => Array(opt)

      case LIST =>
        val id = it.getShort
        Array(opt, id)

      case MAP =>
        val id1 = it.getShort
        val id2 = it.getShort
        Array(opt, id1, id2)

      case SET =>
        val id = it.getShort
        Array(opt, id)
    }
  }

  def parseMetaData(it: ByteIterator) = {
    val flags = it.getInt
    val columnCount = it.getInt

    var cols = Vector[(String, Array[Short])]()

    if ((flags & 0x0001) == 1) {
      val keyspace = readString(it)
      val tablename = readString(it)
      println(s"table name : $tablename")
      (0 until columnCount) foreach { i =>
        val name = readString(it)
        val opt = getOpt(it)
        cols = cols :+ (name, opt)
      }
    } else {

      (0 until columnCount) foreach { i =>
        val keyspace = readString(it)
        val tableName = readString(it)
        val name = keyspace + tableName + readString(it)
        val opt = getOpt(it)
        cols = cols :+ (name, opt)
      }
    }
    cols
  }

  def parseRows(body: ByteString) = {
    val it = body.iterator
    val cols = parseMetaData(it)

    val rowCount = it.getInt

    (0 until rowCount).map { i =>
      (0 until cols.size).map { j =>
        readType(cols(j)._2(0), it)
      }
    }
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
    creds.foreach { case (k, v) => builder.append(writeString(k)).append(writeString(v)) }
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

  def registerFrame(b: ByteString, stream: Byte): ByteString = {
    val builder = new ByteStringBuilder
    builder.putByte(VERSION).putByte(FLAGS).putByte(stream).putByte(REGISTER)
    val body = new ByteStringBuilder
    body.append(b)
    builder.putInt(body.length) ++= body.result
    builder.result
  }
}
