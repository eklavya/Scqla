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
import EventHandler._
import scala.collection.JavaConversions._

object Scqla {

  val cnfg = ConfigFactory.load

  val nodes = cnfg.getStringList("nodes")

  val port = cnfg.getInt("port")

  val numNodes = cnfg.getInt("nodesToConnect")

  val system = ActorSystem("Scqla")

  val numConnections = cnfg.getInt("numConnections")

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  implicit val timeout = Timeout(10 seconds)

  val actors = nodes.flatMap { node =>
    (0 until numConnections).map { i =>
      val receiver = system.actorOf(Props[Receiver], s"receiver-$node-$i")
      val client = system.actorOf(Props(new Sender(receiver, node, port)), s"sender-$node-$i")
      s"/user/sender-$node-$i"
    }
  }.toVector

  val router = system.actorOf(Props.empty.withRouter(RoundRobinRouter(routees = actors)), "router")
  val eventListener = system.actorOf(Props(new EventListener(nodes.get(0), port)))

  def connect = {
    nodes.foreach { node =>
      (0 until numConnections).foreach { i =>
        val res = Await.result(system.actorFor(s"/user/receiver-$node-$i") ? ShallWeStart, 8 seconds)
      }
    }
    Await.result(eventListener ? ShallWeStart, 8 seconds)
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
    valid => Right(rowsToClass[T](valid.asInstanceOf[ResultRows]))))

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
}

case class FulFill(stream: Byte, actor: ActorRef)
case class FulFilled(stream: Byte)

case object ShallWeStart
case object Start

abstract class Request
abstract class Response

case class Statrtup extends Request
case class Credentials extends Request
case class Options extends Request
case class Query(q: String) extends Request
case class Prepare(q: String) extends Request
case class Execute(bs: ByteString) extends Request
case class Error(e: String) extends Response
case class Ready extends Response
case class Authenticate extends Response
case class Supported extends Response
case class ResultRows(l: IndexedSeq[IndexedSeq[Option[Any]]]) extends Response
case object Successful extends Response
case object SetKeyspace extends Response
case object SchemaChange extends Response

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
