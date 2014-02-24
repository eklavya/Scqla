package com.eklavya.scqla

import scala.concurrent.{Future, Await, Promise}
import Header._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.ByteStringBuilder
import akka.util._
import java.util.{UUID => uu}
import java.net.InetAddress
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import Frame._
import akka.pattern.ask
import scala.reflect.ClassTag

object Scqla {
  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00
  val system = ActorSystem("Scqla")
  val consistency = ANY
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  implicit val timeout = Timeout(10 seconds)

  val receiver = system.actorOf(Props[Receiver])
  val client = system.actorOf(Props(new Sender(receiver)))

  def connect = {
    Await.result(receiver ? ShallWeStart, 8 seconds)
  }

  def query[T <: ResultResponse : ClassTag](q: String): Future[T] = {
    (client ? Query(q)).mapTo[T]
  }

  def queryAs[T: ClassTag](q: String) = {

    val claas = implicitly[ClassTag[T]].runtimeClass

    query[ResultRows](q).map { rr =>

      if (claas.isPrimitive)  {
        rr.l.map(_.flatten.head.asInstanceOf[T])
      }

      else if (claas.getName.equals("java.lang.String")) {
        rr.l.map(_.flatten.head)
      }

      else {
        rr.l.map { l =>
          val params = l.flatten
          claas.getConstructors()(0).newInstance(params map { _.asInstanceOf[AnyRef] } : _*).asInstanceOf[T]
        }
      }
    }
  }

  def prepare(q: String) = {
    println("Preapreing query to be sent.")
    (client ? Prepare(q)).mapTo[Prepared]
  }

  def execute(bs: ByteString) = {
    (client ? Execute(bs)).mapTo[Successful.type]
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
}

case class FullFill(stream: Byte, actor: ActorRef)
case class FullFilled(stream: Byte)

case object ShallWeStart
case object Start

abstract class Request

case class Statrtup extends Request
case class Credentials extends Request
case class Options extends Request
case class Query(q: String) extends Request
case class Prepare(q: String) extends Request
case class Execute(bs: ByteString) extends Request
//case class Register extends Request

abstract class Response

case class Prepared(qid: ByteString, cols: Vector[(String, Array[Short])]) extends Response {

  def execute(params: Any*) = {
    val builder = new ByteStringBuilder
    builder.putShort(qid.length)
    builder.append(qid).putShort(cols.length)
    if (params.length == cols.size) {
      (params zip cols) foreach { case (param, option) =>
        writeType(option._2(0), param, builder)
      }
    }
    Scqla.execute(builder.result)
  }

  def insert[A](a: A) = {

  }
}

abstract class ResultResponse

case class Error extends Response
case class Ready extends Response
case class Authenticate extends Response
case class Supported extends Response
case class ResultRows(l: IndexedSeq[IndexedSeq[Option[Any]]]) extends ResultResponse
case object Successful extends ResultResponse
case object SetKeyspace extends ResultResponse
case class Event extends Response