package com.eklavya.scqla

import scala.concurrent.{ Future, Await, Promise }
import Header._
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.util.ByteStringBuilder
import akka.util._
import java.util.{ UUID => uu }
import java.net.InetAddress
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import Frame._
import akka.pattern.ask
import scala.reflect.ClassTag
import com.typesafe.config._
import akka.routing.FromConfig
import akka.routing.RoundRobinRouter

object Scqla {

  val cnfg = ConfigFactory.load

  val nodes = cnfg.getStringList("nodes")

  val port = cnfg.getInt("port")

  val numNodes = cnfg.getInt("nodesToConnect")

  val system = ActorSystem("Scqla")

  val numConnections = if (nodes.size > numNodes) numNodes else nodes.size

  val actors = (0 until numConnections).map { i =>
    val receiver = system.actorOf(Props[Receiver], s"receiver$i")
    val client = system.actorOf(Props(new Sender(receiver, nodes.get(i), port)), s"sender$i")
    s"/user/sender$i"
  }

  val router = system.actorOf(Props.empty.withRouter(RoundRobinRouter(routees = actors)), "router")

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  implicit val timeout = Timeout(10 seconds)

  def connect = {
    (0 until numConnections).map { i =>
      val res = Await.result(system.actorFor(s"/user/receiver$i") ? ShallWeStart, 8 seconds)
    }
  }

  private[this] def rowsToClass[T: ClassTag](rr: ResultRows): IndexedSeq[T] = {
    val claas = implicitly[ClassTag[T]].runtimeClass
    if (claas.isPrimitive) {
      rr.l.map(_.flatten.head.asInstanceOf[T])
    } else if (claas.getName.equals("java.lang.String")) {
      rr.l.map(_.flatten.head.asInstanceOf[T])
    } else {
      rr.l.map { l =>
        val params = l.flatten
        claas.getConstructors()(0).newInstance(params map { _.asInstanceOf[AnyRef] }: _*).asInstanceOf[T]
      }
    }
  }

  def query(q: String)(implicit tag: ClassTag[Response]): Future[Either[String, Response]] = (router ? Query(q)).map {
    case e: Error => Left(e.e)
    case x: Response => Right(x)
  }

  def queryAs[T: ClassTag](q: String): Future[Either[String, IndexedSeq[T]]] = query(q).map(_.fold(
  	error => Left(error),
    valid => Right(rowsToClass[T](valid.asInstanceOf[ResultRows]))
  ))

  def prepare(q: String): Future[Either[String, Prepared]] = (router ? Prepare(q)).map {
    case p: Prepared => Right(p)
    case e: Error => Left(e.e)
  }

  def execute(bs: ByteString): Future[Either[String, Response]] = (router ? Execute(bs)).map {
    case e: Error => Left(e.e)
    case x: Response => Right(x)
  }

  def executeGet[T: ClassTag](bs: ByteString): Future[Either[String, IndexedSeq[T]]] = (router ? Execute(bs)).map {
    case rr: ResultRows => Right(rowsToClass[T](rr))
    case e: Error => Left(e.e)
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

  def register(w: DBEvent) = w match {
    case _: TopologyEvent =>
    case _: StatusEvent =>
    case _: SchemaEvent =>
  }

  def unRegister(w: DBEvent) = w match {
    case _: TopologyEvent =>
    case _: StatusEvent =>
    case _: SchemaEvent =>
  }
}

trait DBEvent

trait TopologyEvent extends DBEvent {
  def onNewNode(i: InetAddress): Unit
  def onRemovedNode(i: InetAddress): Unit
}

trait StatusEvent extends DBEvent {
  def onNodeUp(i: InetAddress): Unit
  def onNodeDown(i: InetAddress): Unit
}

trait SchemaEvent extends DBEvent {
  def onCreated(ks: String, table: String): Unit
  def onUpdated(ks: String, table: String): Unit
  def onDropped(ks: String, table: String): Unit
}

case class FulFill(stream: Byte, actor: ActorRef)
case class FulFilled(stream: Byte)

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

  private def buildBS(params: Any*) = {
    val builder = new ByteStringBuilder
    builder.putShort(qid.length)
    builder.append(qid).putShort(cols.length)
    if (params.length == cols.size) {
      (params zip cols) foreach {
        case (param, option) =>
          writeType(option._2(0), param, builder)
      }
    }
    builder.result
  }

  def execute(params: Any*) = Scqla.execute(buildBS(params: _*))

  def executeGet[T: ClassTag](params: Any*) = Scqla.executeGet[T](buildBS(params: _*))

  def insert[A](a: A) = {

  }
}

case class Error(e: String) extends Response
case class Ready extends Response
case class Authenticate extends Response
case class Supported extends Response
case class ResultRows(l: IndexedSeq[IndexedSeq[Option[Any]]]) extends Response
case object Successful extends Response
case object SetKeyspace extends Response
case object SchemaChange extends Response
case class Event extends Response